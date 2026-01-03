# RAG Pipeline with Barq DB

This example demonstrates how to build a Retrieval-Augmented Generation (RAG) pipeline using Barq DB and Python.

## Prerequisites

- Python 3.8+
- Barq DB running locally (see main README)
- OpenAI API Key (optional, defaults to mock LLM)

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set environment variables (optional):
   ```bash
   export OPENAI_API_KEY="sk-..."
   ```

## Usage

Run the demo script:
```bash
python rag_demo.py
```

The script will:
1. Create a `knowledge-base` collection in Barq DB.
2. Ingest sample documents about Barq DB features.
3. Accept a user query.
4. Retrieve relevant contexts using hybrid search.
5. Generate an answer.
