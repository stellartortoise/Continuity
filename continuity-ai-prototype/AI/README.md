# Continuity AI Prototype

Entity extraction and fact validation system with vector database support.

## Features

- **Entity Extraction**: BERT-based NER + LLM fact extraction
- **Fact Validation**: DeBERTa NLI for contradiction/entailment detection
- **Vector Database**: ChromaDB for semantic search and RAG
- **Web API**: FastAPI REST interface with async job processing
- **Web UI**: Interactive test interface

## Quick Start

```bash
cd continuity-ai-prototype/AI
pip install -r requirements.txt
python main.py
```

Open `test-ui/index.html` in your browser to use the interface.

## Core Components

### Entity Extraction
- **NER**: NuNER-v2.0 (state-of-the-art named entity recognition)
- **Fact Extraction**: LLM-powered fact extraction from text
- **Validation**: On-demand contradiction detection using DeBERTa

### Vector Database
- **Storage**: ChromaDB for embeddings
- **Embeddings**: SentenceTransformers (all-MiniLM-L6-v2)
- **RAG**: Retrieval-augmented generation pipeline

### API Endpoints
- `POST /entities/extract/start` - Start entity extraction job
- `GET /entities/status/{job_id}` - Check job status
- `GET /entities/result/{job_id}` - Get extraction results
- `POST /entities/validate-facts` - Validate facts for contradictions

## Models Used

- **NER**: `numind/NuNER-v2.0` - High-performance entity recognition
- **Fact Validation**: `microsoft/deberta-large-mnli` - Contradiction detection
- **LLM**: Qwen 2.5 3B Instruct (GGUF) - Fact extraction
- **Embeddings**: `all-MiniLM-L6-v2` - Vector search

## Model Storage

All models are stored **locally within the project folder**:

```
continuity-ai-prototype/AI/
├── models/
│   ├── qwen2.5-3b-instruct-q6_k.gguf        # LLM (already present)
│   └── huggingface_cache/                    # HuggingFace models (auto-downloaded)
│       ├── models--numind--NuNER-v2.0/       # ~500MB
│       ├── models--microsoft--deberta-large-mnli/  # ~1.5GB
│       └── models--sentence-transformers--all-MiniLM-L6-v2/  # ~90MB
└── data/
    └── vector_db/                            # ChromaDB storage
```

**Benefits:**
- ✅ All models in one place
- ✅ Easy to backup/transfer entire project
- ✅ No conflicts with other projects
- ✅ Portable across machines

## Project Structure

```
├── config/          # Configuration
├── models/          # NER, LLM, fact extraction & validation
├── database/        # Entity storage & VectorDB
├── rag/             # RAG pipeline
├── interfaces/      # FastAPI web API
├── test-ui/         # Web interface
└── main.py          # Entry point
```
