# embedder.py
import logging
from typing import List
import numpy as np
from sentence_transformers import SentenceTransformer
from config.settings import EMBEDDING_MODEL, MODELS_CACHE_DIR

logger = logging.getLogger(__name__)


class Embedder:
    """SentenceTransformer-based text embedding."""

    def __init__(self, model_name: str = EMBEDDING_MODEL):
        logger.info("Loading embedding model: %s (cache: %s)", model_name, MODELS_CACHE_DIR)
        self.model = SentenceTransformer(model_name, cache_folder=MODELS_CACHE_DIR)
        logger.info("Embedding model loaded from cache")

    def embed_text(self, text: str) -> np.ndarray:
        try:
            return self.model.encode(text, convert_to_tensor=False)
        except Exception as e:
            logger.error("Error embedding text: %s", e)
            raise

    def embed_batch(self, texts: List[str]) -> List[np.ndarray]:
        try:
            embeddings = self.model.encode(texts, convert_to_tensor=False)
            return embeddings.tolist()
        except Exception as e:
            logger.error("Error batch embedding: %s", e)
            raise

    def get_embedding_dimension(self) -> int:
        return self.model.get_sentence_embedding_dimension()