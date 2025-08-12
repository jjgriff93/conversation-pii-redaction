import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

endpoint = os.getenv("AZURE_LANGUAGE_SERVICE_ENDPOINT")
resource_key = os.getenv("AZURE_LANGUAGE_API_KEY")
api_version = "2025-05-15-preview"
input_dir = "input"
output_dir = "output"
max_concurrent_requests = os.getenv("MAX_CONCURRENCY", "100")

if not endpoint:
    raise ValueError("AZURE_LANGUAGE_SERVICE_ENDPOINT environment variable is not set.")

if not resource_key:
    raise ValueError("AZURE_LANGUAGE_API_KEY environment variable is not set.")

def load_conversation_from_csv(filename: str) -> dict:
    """Load conversation from a CSV file.
    
    Expected format:
        Timestamp,Participant,Transcript
        2025-07-27 10:00:00.006,[internal],Good morning.
        2025-07-27 10:00:01.132,[internal],Can I have your name?
        2025-07-27 10:00:02.258,[external],Sure that is John Doe.
        2025-07-27 10:00:03.384,[internal],Thank you.
        2025-07-27 10:00:04.510,[external],You're welcome.
        2025-07-27 10:00:05.636,[internal],Can I have your email address?
        2025-07-27 10:00:06.762,[external],john.doe@example.com
        2025-07-27 10:00:07.888,[internal],Thank you.
    """
    with open(filename, 'r') as file:
        csv_content = file.read()

    conversation = {
        "id": filename.split("/")[-1].replace(".csv", ""),
        "language": "en",
        "modality": "text",
        "conversationItems": [],
    }

    for line in csv_content.strip().split("\n")[1:]:
        timestamp, participant, text = line.split(",", 2)
        conversation["conversationItems"].append({
            "participantId": participant,
            "id": f"conversationId_{len(conversation['conversationItems']) + 1}",
            "text": text,
        })

    return conversation

def redact_conversation(conversation: dict) -> dict:
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

    response = requests.post(analyze_endpoint, headers=request_headers, json=request_body)

    if response.status_code != 202:
        print("Error:", response.status_code, response.text)
    
    # Get the job id from operation-location header
    job_endpoint = response.headers.get("Operation-Location")
    if not job_endpoint:
        raise ValueError("Job status endpoint not found in response headers.")

    print("Job created:", job_endpoint)

    # Poll for the job status until complete
    while True:
        time.sleep(5)
        response = requests.get(job_endpoint, headers=request_headers)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            break

        status = response.json().get("status")
        if status == "succeeded":
            print("Job completed successfully", job_endpoint)
            break
        elif status == "failed":
            print("Job failed:", response.json().get("error"))
            raise ValueError("Job failed.", job_endpoint)

        print("Job is still processing...", job_endpoint)

    task_result = response.json()

    analysed_conversation = task_result["tasks"]["items"][0]["results"]["conversations"][0]
    
    # Create a mapping of conversation item IDs to participant IDs from the original conversation
    id_to_participant = {item["id"]: item["participantId"] for item in conversation["conversationItems"]}
    
    # Create a new redacted conversation json with just participant
    # (either "internal" or "external" as per original conversation)
    # and the new "redactedContent" from the analysis results as the "text" field
    redacted_conversation = {
        "id": analysed_conversation["id"],
        "conversation": [
            {
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
    csv_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
    if not csv_files:
        print("No CSV files found to process.")
        return

    # Allow tuning concurrency via env var; default to a modest level to avoid service throttling
    max_workers = int(max_concurrent_requests)
    max_workers = max(1, min(max_workers, len(csv_files)))

    print(f"Processing {len(csv_files)} CSV file(s) with concurrency={max_workers}...")

    def process_csv_file(filename: str) -> str:
        """Process a single CSV file end-to-end and return the output path."""
        # Load conversation from CSV file
        conversation = load_conversation_from_csv(os.path.join(input_dir, filename))
        redacted_conversation = redact_conversation(conversation)

        # Export results to output folder (as json file)
        out_path = os.path.join(output_dir, f"{redacted_conversation['id']}.json")
        with open(out_path, "w") as outfile:
            json.dump(redacted_conversation, outfile, indent=4)
        return out_path

    # Run work in parallel threads (network-bound -> threads are effective)
    successes = 0
    failures = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(process_csv_file, f): f for f in csv_files}
        for future in as_completed(future_map):
            fname = future_map[future]
            try:
                out_path = future.result()
                print(f"✔ Processed '{fname}' -> '{out_path}'")
                successes += 1
            except Exception as e:
                print(f"✖ Failed processing '{fname}': {e}")
                failures += 1

    print(f"Done. {successes} succeeded, {failures} failed.")

if __name__ == "__main__":
    main()
    main()
