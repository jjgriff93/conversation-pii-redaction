# Conversation PII Redaction

## Overview

This project provides a simple script to redact personally identifiable information (PII) from conversation transcripts using Azure's Language Service. Learn more in the Microsoft Docs: [Redact PII in conversations (Language Service)](https://learn.microsoft.com/en-us/azure/ai-services/language-service/personally-identifiable-information/how-to/redact-conversation-pii?tabs=rest-api).

## Requirements

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/)
- Azure subscription with [Azure AI Foundry](https://learn.microsoft.com/en-us/azure/ai-foundry/what-is-azure-ai-foundry) deployed

## Setup

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd conversation-pii-redaction
   ```

2. Create a virtual environment and activate it:

    - macOS/Linux

       ```bash
       uv sync
       source .venv/bin/activate
       ```

    - Windows (PowerShell)

       ```powershell
       uv sync
       .\\.venv\\Scripts\\Activate.ps1
       ```

3. Set the following environment variables (you can find these in your AI Foundry portal):

    - macOS/Linux

   ```bash
      export AZURE_LANGUAGE_SERVICE_ENDPOINT="<your-endpoint>"
      export AZURE_LANGUAGE_API_KEY="<your-api-key>"
      # Optional tuning
      export MAX_CONCURRENCY="50"
      export MAX_HTTP_RETRIES="5"
      export HTTP_BACKOFF_FACTOR="1.5"
      export HTTP_TIMEOUT_SECONDS="30"
      export MAX_FILE_RETRIES="3"
      # CSV delimiter: defaults to '|'; set to ',' for comma-delimited files
      export CSV_DELIMITER="|"
   ```

    - Windows (PowerShell)

   ```powershell
      $env:AZURE_LANGUAGE_SERVICE_ENDPOINT="<your-endpoint>"
      $env:AZURE_LANGUAGE_API_KEY="<your-api-key>"
      # Optional tuning
      $env:MAX_CONCURRENCY="50"
      $env:MAX_HTTP_RETRIES="5"
      $env:HTTP_BACKOFF_FACTOR="1.5"
      $env:HTTP_TIMEOUT_SECONDS="30"
      $env:MAX_FILE_RETRIES="3"
      # CSV delimiter: defaults to '|'; set to ',' for comma-delimited files
      $env:CSV_DELIMITER="|"
   ```

## Usage

Place your conversation CSV or JSON files in the `input` directory. The script will process each file and output the redacted JSON files to the `output` directory.

```bash
uv run main.py
```

On Windows PowerShell, ensure you have activated the virtual environment as above. If `uv` is not on PATH, install it from the official docs: [Astral UV installation guide](https://docs.astral.sh/uv/).

### Using comma-delimited CSV files

By default, the script expects pipe-delimited (`|`) CSVs. To use comma-delimited files, set `CSV_DELIMITER` to `,` before running:

- macOS/Linux

   ```bash
   export CSV_DELIMITER=,
   uv run main.py
   ```

- Windows (PowerShell)

   ```powershell
   $env:CSV_DELIMITER=","; uv run main.py
   ```

Quoted fields with commas are handled correctly by the CSV parser when the delimiter is set to `,`.

### Using JSON files

You can also provide conversations in JSON format. Configure where the conversation items live and which fields to read using environment variables:

- JSON_CONVERSATION_PATH: dot path to the array containing items (e.g. `phrases`, `payload.items`). If omitted, the script will try common keys like `phrases`, `messages`, `conversation`, `items` or a top-level array.
- JSON_PARTICIPANT_FIELD: field name for the participant inside each item (default: `participant`).
- JSON_TEXT_FIELD: field name for the text inside each item (default: `text`).
- JSON_TIMESTAMP_FIELD: optional field name for a timestamp inside each item (optional; if not provided, output timestamps will be `null`).
- JSON_MULTI_DOC: when `true`, and the top-level JSON is an array, treat each element as a separate document (conversation). Outputs will be suffixed with `_001`, `_002`, etc.

Example for a file like `dummy.json` that contains an array of phrases with shape `{ participantPurpose: string, text: string }`:

- macOS/Linux

```bash
export JSON_CONVERSATION_PATH=phrases
export JSON_PARTICIPANT_FIELD=participantPurpose
export JSON_TEXT_FIELD=text
uv run main.py
```

- Windows (PowerShell)

```powershell
$env:JSON_CONVERSATION_PATH="phrases"
$env:JSON_PARTICIPANT_FIELD="participantPurpose"
$env:JSON_TEXT_FIELD="text"
uv run main.py
```

Place `dummy.json` in the `input/` folder. The tool will produce `output/dummy.json` with redacted content.

#### Multi-document JSON example

If your input JSON is a top-level array where each element is a separate document containing `phrases`, enable multi-document mode:

- macOS/Linux

```bash
export JSON_MULTI_DOC=true
export JSON_CONVERSATION_PATH=phrases
export JSON_PARTICIPANT_FIELD=participantPurpose
export JSON_TEXT_FIELD=text
uv run main.py
```

Given an input file `input/sessions.json` like `[{"phrases":[...]},{"phrases":[...]}]`, this will emit `output/sessions_001.json`, `output/sessions_002.json`, etc.

### Retry and resilience

This tool includes built-in retries for intermittent errors:

- Per-request retries for transient HTTP errors (429/5xx, timeouts) with exponential backoff and jitter
- Polling respects Retry-After where provided and gradually increases the interval
- Per-file retries when a file fails to process end-to-end

You can control behavior via environment variables shown in Setup step 3.

### Idempotent runs

The script is safe to run multiple times. If an output JSON with the same base name already exists in `output/`, the corresponding CSV in `input/` is skipped. The summary will show how many files were skipped.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.
