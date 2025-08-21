import csv
import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import RequestException, Timeout

endpoint = os.getenv("AZURE_LANGUAGE_SERVICE_ENDPOINT")
resource_key = os.getenv("AZURE_LANGUAGE_API_KEY")
api_version = "2025-05-15-preview"
input_dir = "input"
output_dir = "output"
max_concurrent_requests = os.getenv("MAX_CONCURRENCY", "100")
# CSV parsing configuration
CSV_DELIMITER = os.getenv("CSV_DELIMITER", "|")  # set to "," to use comma-delimited CSVs

# Retry configuration (tunable via environment variables)
MAX_HTTP_RETRIES = int(os.getenv("MAX_HTTP_RETRIES", "5"))
HTTP_BACKOFF_FACTOR = float(os.getenv("HTTP_BACKOFF_FACTOR", "1.5"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))

# Polling configuration
INITIAL_POLL_INTERVAL_SECONDS = float(os.getenv("INITIAL_POLL_INTERVAL_SECONDS", "2"))
MAX_POLL_INTERVAL_SECONDS = float(os.getenv("MAX_POLL_INTERVAL_SECONDS", "15"))
POLL_TIMEOUT_SECONDS = float(os.getenv("POLL_TIMEOUT_SECONDS", "1200"))  # 20 minutes

# Per-file processing retries
MAX_FILE_RETRIES = int(os.getenv("MAX_FILE_RETRIES", "3"))

if not endpoint:
    raise ValueError("AZURE_LANGUAGE_SERVICE_ENDPOINT environment variable is not set.")

if not resource_key:
    raise ValueError("AZURE_LANGUAGE_API_KEY environment variable is not set.")

# Normalize endpoint to include a trailing slash
if endpoint and not endpoint.endswith("/"):
    endpoint = endpoint + "/"

def load_conversation_from_csv(filename: str) -> tuple[dict, dict[str, str | None]]:
    """Load conversation from a CSV file and return
    (conversation_payload, timestamps_by_id).
    
    Expected format:
        Delimiter is configurable via env var CSV_DELIMITER (default: "|").
        Example with pipe:
            Timestamp|Participant|Transcript
            2025-07-27 10:00:00.006 | [internal] | Good morning.
            2025-07-27 10:00:01.132 | [internal] | Can I have your name?
            2025-07-27 10:00:02.258 | [external] | Sure that is John Doe.
            2025-07-27 10:00:03.384 | [internal] | Thank you.
            2025-07-27 10:00:04.510 | [external] | You're welcome.
            2025-07-27 10:00:05.636 | [internal] | Can I have your email address?
            2025-07-27 10:00:06.762 | [external] | john.doe@example.com
            2025-07-27 10:00:07.888 | [internal] | Thank you.
        Example with comma (set CSV_DELIMITER=","):
            Timestamp,Participant,Transcript
            2025-07-27 10:00:00.006,[internal],Good morning.
    """
    # Use csv module with configurable delimiter and skipinitialspace to tolerate spaces after delimiter
    with open(filename, 'r', encoding='utf-8-sig', newline='') as file:
        reader = csv.DictReader(file, delimiter=CSV_DELIMITER, skipinitialspace=True)

        conversation: dict = {
            "id": os.path.splitext(os.path.basename(filename))[0],
            "language": "en",
            "modality": "text",
            "conversationItems": [],
        }

        timestamps_by_id: dict[str, str | None] = {}

        # Build conversation items
        idx = 0
        for row in reader:
            # Defensive: handle missing or empty rows
            if not row:
                continue
            timestamp = (row.get('Timestamp') or '').strip()
            participant = (row.get('Participant') or '').strip()
            text = (row.get('Transcript') or '').strip()

            # Skip rows that don't have any meaningful content
            if not (timestamp or participant or text):
                continue

            idx += 1
            item_id = f"conversationId_{idx}"
            conversation["conversationItems"].append({
                "participantId": participant,
                "id": item_id,
                "text": text,
            })
            # Track original timestamp by item id for later merging into results
            timestamps_by_id[item_id] = timestamp or None  # None -> null in JSON

    return conversation, timestamps_by_id

def redact_conversation(conversation: dict, timestamps_by_id: dict[str, str | None]) -> dict:
    analyze_endpoint = f"{endpoint}language/analyze-conversations/jobs?api-version={api_version}"

    request_headers = {
        "Content-Type": "application/json",
        "Ocp-Apim-Subscription-Key": resource_key,
    }
    request_body = {
        "kind": "Conversation",
        "analysisInput": {
            "conversations": [conversation],
        },
        "tasks": [{
            "kind": "ConversationalPIITask",
            "parameters": {
                "modelVersion": "latest",
                "piiCategories": [],
                "redactAudioTiming": False,
                "redactionPolicy": {
                    "policyKind": "CharacterMask",
                    "redactionCharacter": "*",
                },
                "redactionSource": "lexical"
            }
        }]
    }

    status_forcelist = {429, 500, 502, 503, 504}

    def _sleep_with_backoff(attempt: int, base: float = 0.5) -> None:
        # Exponential backoff with jitter
        delay = base * (HTTP_BACKOFF_FACTOR ** attempt)
        # Add jitter up to 25% of base delay
        delay += random.uniform(0, base * 0.25)
        time.sleep(min(delay, MAX_POLL_INTERVAL_SECONDS))

    session = requests.Session()

    # Submit analyze job with retries
    last_exc: Exception | None = None
    response = None
    for attempt in range(MAX_HTTP_RETRIES):
        try:
            response = session.post(
                analyze_endpoint,
                headers=request_headers,
                json=request_body,
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            if response.status_code == 202:
                break
            # Respect Retry-After when rate-limited
            if response.status_code in status_forcelist:
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else None
                print(f"Transient error submitting job (status={response.status_code}). Attempt {attempt+1}/{MAX_HTTP_RETRIES}. Retrying...")
                if wait is not None:
                    time.sleep(min(wait, MAX_POLL_INTERVAL_SECONDS))
                else:
                    _sleep_with_backoff(attempt)
                continue
            # Non-retryable
            response.raise_for_status()
        except (ReqConnectionError, Timeout) as e:
            last_exc = e
            print(f"Network timeout/connection error submitting job. Attempt {attempt+1}/{MAX_HTTP_RETRIES}. Retrying...")
            _sleep_with_backoff(attempt)
        except RequestException as e:
            # For non-transient HTTP errors, don't retry
            raise

    if response is None:
        raise RuntimeError("No response from analyze job submission.")
    if response.status_code != 202:
        # If we exhausted retries but still failed
        msg = f"Failed to submit analyze job after {MAX_HTTP_RETRIES} attempts. status={response.status_code} body={response.text}"
        if last_exc:
            msg += f" last_exc={last_exc}"
        raise RuntimeError(msg)

    # Get the job id from operation-location header
    job_endpoint = response.headers.get("Operation-Location")
    if not job_endpoint:
        raise ValueError("Job status endpoint not found in response headers.")

    print("Job created:", job_endpoint)

    # Poll for the job status until complete (with backoff + timeout)
    start = time.monotonic()
    poll_interval = INITIAL_POLL_INTERVAL_SECONDS
    while True:
        # Timeout guard
        if time.monotonic() - start > POLL_TIMEOUT_SECONDS:
            raise TimeoutError(f"Polling timed out after {POLL_TIMEOUT_SECONDS} seconds for {job_endpoint}")

        try:
            response = session.get(job_endpoint, headers=request_headers, timeout=HTTP_TIMEOUT_SECONDS)
            if response.status_code == 200:
                body = response.json()
                status = body.get("status")
                if status == "succeeded":
                    print("Job completed successfully", job_endpoint)
                    task_result = body
                    break
                if status == "failed":
                    print("Job failed:", body.get("error"))
                    raise RuntimeError("Job failed")
                # still running
                print("Job is still processing...", job_endpoint)
            else:
                if response.status_code in status_forcelist:
                    retry_after = response.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else None
                    print(f"Transient polling error (status={response.status_code}). Backing off...")
                    if wait is not None:
                        time.sleep(min(wait, MAX_POLL_INTERVAL_SECONDS))
                    else:
                        # fall through to backoff sleep below
                        pass
                else:
                    response.raise_for_status()
        except (ReqConnectionError, Timeout) as e:
            print(f"Network error when polling job: {e}. Backing off...")
        # Backoff before next poll
        time.sleep(poll_interval)
        poll_interval = min(poll_interval * HTTP_BACKOFF_FACTOR, MAX_POLL_INTERVAL_SECONDS)

    analysed_conversation = task_result["tasks"]["items"][0]["results"]["conversations"][0]

    # Create a mapping of conversation item IDs to participant IDs from the original conversation
    id_to_participant = {item["id"]: item["participantId"] for item in conversation["conversationItems"]}

    # Create a new redacted conversation json with timestamp (from original CSV), participant
    # (either "internal" or "external" as per original conversation)
    # and the new "redactedContent" from the analysis results as the "text" field
    redacted_conversation = {
        "id": analysed_conversation["id"],
        "metadata": {
            # Add any custom metadata to include here
        },
        "conversation": [
            {
                "timestamp": timestamps_by_id.get(item["id"]),
                "participant": id_to_participant[item["id"]],
                "text": item["redactedContent"]["text"],
            }
            for item in analysed_conversation["conversationItems"]
        ]
    }
    return redacted_conversation

def main():
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Collect CSV files to process
    csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]
    if not csv_files:
        print("No CSV files found to process.")
        return

    # Filter out files that already have a corresponding JSON in output (idempotent reruns)
    pending_files: list[str] = []
    skipped = 0
    for f in csv_files:
        base = os.path.splitext(f)[0]
        expected_out = os.path.join(output_dir, f"{base}.json")
        if os.path.exists(expected_out):
            print(f"↷ Skipping '{f}' because '{base}.json' already exists in output/")
            skipped += 1
        else:
            pending_files.append(f)

    if not pending_files:
        print(f"All {len(csv_files)} CSV files are already processed. Nothing to do.")
        return

    # Allow tuning concurrency via env var; default to a modest level to avoid service throttling
    max_workers = int(max_concurrent_requests)
    max_workers = max(1, min(max_workers, len(csv_files)))

    print(f"Processing {len(pending_files)} CSV file(s) with concurrency={max_workers}...")

    def process_csv_file(filename: str) -> str:
        """Process a single CSV file end-to-end and return the output path.
        Includes retries for intermittent service errors.
        """
        filepath = os.path.join(input_dir, filename)
        # Early skip guard if output already exists
        pre_base = os.path.splitext(filename)[0]
        pre_out = os.path.join(output_dir, f"{pre_base}.json")
        if os.path.exists(pre_out):
            return pre_out
        last_exc: Exception | None = None
        for attempt in range(1, MAX_FILE_RETRIES + 1):
            try:
                conversation, timestamps_by_id = load_conversation_from_csv(filepath)
                redacted_conversation = redact_conversation(conversation, timestamps_by_id)

                # Export results to output folder (as json file)
                out_path = os.path.join(output_dir, f"{redacted_conversation['id']}.json")
                with open(out_path, "w", encoding="utf-8") as outfile:
                    json.dump(redacted_conversation, outfile, indent=4)
                return out_path
            except Exception as e:
                last_exc = e
                if attempt < MAX_FILE_RETRIES:
                    # Backoff between attempts
                    delay = min((HTTP_BACKOFF_FACTOR ** (attempt - 1)) * 2, MAX_POLL_INTERVAL_SECONDS)
                    print(f"Retrying '{filename}' attempt {attempt+1}/{MAX_FILE_RETRIES} after error: {e}. Sleeping {delay:.1f}s...")
                    time.sleep(delay)
                else:
                    break
        # If we got here, all attempts failed
        raise RuntimeError(f"All retries exhausted for '{filename}'. Last error: {last_exc}")

    # Run work in parallel threads (network-bound -> threads are effective)
    successes = 0
    failures = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(process_csv_file, f): f for f in pending_files}
        for future in as_completed(future_map):
            fname = future_map[future]
            try:
                out_path = future.result()
                print(f"✔ Processed '{fname}' -> '{out_path}'")
                successes += 1
            except Exception as e:
                print(f"✖ Failed processing '{fname}': {e}")
                failures += 1

    print(f"Done. {successes} succeeded, {failures} failed, {skipped} skipped (already existed).")

if __name__ == "__main__":
    main()
