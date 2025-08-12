# Conversation PII Redaction

## Overview

This project provides a simple script to redact personally identifiable information (PII) from conversation transcripts using Azure's Language Service. Learn more [here](https://learn.microsoft.com/en-us/azure/ai-services/language-service/personally-identifiable-information/how-to/redact-conversation-pii?tabs=rest-api).

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

   ```bash
   uv sync
   source .venv/bin/activate
   ```

3. Set the following environment variables (you can find these in your AI Foundry portal):

   ```bash
   export AZURE_LANGUAGE_SERVICE_ENDPOINT="<your-endpoint>"
   export AZURE_LANGUAGE_API_KEY="<your-api-key>"
   ```

## Usage

Place your conversation CSV files in the `input` directory. The script will process each file and output the redacted JSON files to the `output` directory.

```bash
uv run main.py
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.
