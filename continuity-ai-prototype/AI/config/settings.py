# settings.py
"""Configuration settings for entity extraction."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ---------------- Model Configuration ----------------
# Primary (legacy) model path (used elsewhere in the app)
# Use Qwen 2.5 3B Q6_K for best quality fact extraction
MODEL_NAME = os.getenv("MODEL_NAME", "qwen2.5-3b-instruct-q6_k.gguf")
MODEL_PATH = os.getenv("MODEL_PATH", "./models/qwen2.5-3b-instruct-q6_k.gguf")

# NEW: Fact extraction model (Qwen 2.5 3B Instruct Q6_K or any GGUF)
# Accept either a full path (FACT_MODEL_PATH), or a bare filename we resolve under ./models
FACT_MODEL_PATH = os.getenv("FACT_MODEL_PATH", "").strip()
if FACT_MODEL_PATH:
    # If it's not an absolute/relative path to a file, assume it's a filename under ./models
    p = Path(FACT_MODEL_PATH)
    if not p.exists():
        candidate = Path("./models") / FACT_MODEL_PATH
        if candidate.exists():
            FACT_MODEL_PATH = str(candidate.resolve())
        else:
            # Fall back to MODEL_PATH if nothing resolves
            FACT_MODEL_PATH = MODEL_PATH
else:
    # If not provided, default to the legacy MODEL_PATH
    FACT_MODEL_PATH = MODEL_PATH

# NER Model Configuration
# Use BERT-base NER (reliable, well-tested)
NER_MODEL_NAME = os.getenv("NER_MODEL_NAME", "dslim/bert-base-NER")
NER_CONFIDENCE_THRESHOLD = float(os.getenv("NER_CONFIDENCE_THRESHOLD", "0.85"))

# Local Model Cache Configuration
# Store HuggingFace models in project folder instead of user cache
MODELS_CACHE_DIR = os.getenv("MODELS_CACHE_DIR", "./models/huggingface_cache")
os.makedirs(MODELS_CACHE_DIR, exist_ok=True)

# Vector Database Configuration (for RAG)
VECTOR_DB_PATH = os.getenv("VECTOR_DB_PATH", "./data/vector_db")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
TOP_K_RESULTS = int(os.getenv("TOP_K_RESULTS", "5"))

# ---------------- Web API Configuration ----------------
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))  # .env sets 8002; this cast will pick that up
API_TITLE = "Entity Extraction API"
API_VERSION = "1.0.0"

# ---------------- Logging Configuration ----------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = "./logs/app.log"

# ---------------- Performance Configuration ----------------
RESPONSE_TIMEOUT = 120  # seconds for LLM classification
MAX_CONCURRENT_REQUESTS = 10


# Export directory (where we write the final response JSON)
EXPORT_JSON_DIR = os.getenv("EXPORT_JSON_DIR", "./exports")
# Ensure absolute path resolution if you like (optional)
EXPORT_JSON_DIR = str(Path(EXPORT_JSON_DIR).resolve())
