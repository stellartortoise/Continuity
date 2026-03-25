"""Vector database interface for storing and retrieving embeddings."""
import logging
import os
from typing import List, Dict, Any, Tuple
import chromadb
from models.embedder import Embedder
from config.settings import (
    VECTOR_DB_PATH,
    EMBEDDING_MODEL,
    TOP_K_RESULTS,
)

logger = logging.getLogger(__name__)


class VectorDB:
    """Manages vector database operations using ChromaDB."""

    def __init__(self, collection_name: str = "documents"):
        """
        Initialize vector database.

        Args:
            collection_name: Name of the collection to use
        """
        os.makedirs(VECTOR_DB_PATH, exist_ok=True)
        
        # Use new ChromaDB API (0.5.x)
        self.client = chromadb.PersistentClient(
            path=VECTOR_DB_PATH,
        )
        
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"},
        )
        self.embedder = Embedder(EMBEDDING_MODEL)
        logger.info(f"VectorDB initialized with collection: {collection_name}")

    def add_documents(
        self,
        documents: List[str],
        metadata: List[Dict[str, Any]] = None,
        ids: List[str] = None,
    ) -> None:
        """
        Add documents to the vector database.

        Args:
            documents: List of document texts
            metadata: List of metadata dicts (one per document)
            ids: List of document IDs
        """
        try:
            embeddings = self.embedder.embed_batch(documents)
            
            if metadata is None:
                metadata = [{} for _ in documents]
            
            self.collection.add(
                documents=documents,
                embeddings=embeddings,
                metadatas=metadata,
                ids=ids or [f"doc_{i}" for i in range(len(documents))],
            )
            logger.info(f"Added {len(documents)} documents to vector DB")
        except Exception as e:
            logger.error(f"Error adding documents: {e}")
            raise

    def upsert_documents(
        self,
        documents: List[str],
        metadata: List[Dict[str, Any]] = None,
        ids: List[str] = None,
    ) -> None:
        """Upsert by deleting existing IDs before add."""
        if not documents:
            return
        ids = ids or [f"doc_{i}" for i in range(len(documents))]
        if ids:
            try:
                self.collection.delete(ids=ids)
            except Exception:
                # Missing IDs are fine; add will still proceed.
                pass
        self.add_documents(documents=documents, metadata=metadata, ids=ids)

    def search(self, query: str, top_k: int = TOP_K_RESULTS) -> List[Dict[str, Any]]:
        """
        Search for documents similar to query.

        Args:
            query: Query text
            top_k: Number of results to return

        Returns:
            List of dicts with 'text', 'metadata', 'distance', and 'id'
        """
        try:
            query_embedding = self.embedder.embed_text(query)
            
            results = self.collection.query(
                query_embeddings=[query_embedding.tolist()],
                n_results=top_k,
                include=["embeddings", "distances", "documents", "metadatas"],
            )
            
            # Format results
            formatted_results = []
            for i in range(len(results["documents"][0])):
                formatted_results.append({
                    "text": results["documents"][0][i],
                    "metadata": results["metadatas"][0][i],
                    "distance": results["distances"][0][i],
                    "id": results["ids"][0][i] if "ids" in results else f"result_{i}",
                })
            
            logger.debug(f"Search returned {len(formatted_results)} results")
            return formatted_results
        except Exception as e:
            logger.error(f"Error searching vector DB: {e}")
            raise

    def delete_documents(self, ids: List[str]) -> None:
        """Delete documents by ID."""
        try:
            self.collection.delete(ids=ids)
            logger.info(f"Deleted {len(ids)} documents")
        except Exception as e:
            logger.error(f"Error deleting documents: {e}")
            raise

    def get_collection_info(self) -> Dict[str, Any]:
        """Get information about the collection."""
        try:
            count = self.collection.count()
            return {
                "name": self.collection.name,
                "document_count": count,
                "embedding_dimension": self.embedder.get_embedding_dimension(),
            }
        except Exception as e:
            logger.error(f"Error getting collection info: {e}")
            raise

    def clear_collection(self) -> None:
        """Clear all documents from collection."""
        try:
            self.client.delete_collection(name=self.collection.name)
            self.collection = self.client.get_or_create_collection(
                name=self.collection.name,
                metadata={"hnsw:space": "cosine"},
            )
            logger.info("Collection cleared")
        except Exception as e:
            logger.error(f"Error clearing collection: {e}")
            raise
