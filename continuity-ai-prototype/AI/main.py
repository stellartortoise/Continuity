import asyncio
import logging
from pathlib import Path
import sys

from models.ner_extractor import HybridNERExtractor
from interfaces.web_api import create_app
from utils.logger import setup_logging
from config.settings import API_HOST, API_PORT, EXPORT_JSON_DIR, FACT_MODEL_PATH
from models.fact_extractor import FactExtractor
from models.llm_manager import LLMManager
from database.vector_db import VectorDB

setup_logging()
logger = logging.getLogger(__name__)
Path(EXPORT_JSON_DIR).mkdir(parents=True, exist_ok=True)

async def run_web_api(ner_extractor: HybridNERExtractor):
    import uvicorn
    import threading

    llm = LLMManager(model_path=FACT_MODEL_PATH)
    fact_extractor = FactExtractor(
        llm=llm,
        use_llm=True,
        max_facts_per_entity=3,
        rules_fallback=True,
        temperature=0.2,
        max_tokens=120,
    )

    vector_db = VectorDB(collection_name="canon_facts")

    app = create_app(ner_extractor, fact_extractor=fact_extractor, vector_db=vector_db)
    logger.info("Starting Entity Extraction API on %s:%s", API_HOST, API_PORT)

    def run_server():
        uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="info")

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    logger.info("Server thread started, keeping process alive...")

    try:
        while True:
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        logger.info("Server interrupted")
        raise

async def main():
    logger.info("Initializing entity extraction system...")
    try:
        logger.info("Loading BERT NER model...")
        ner_extractor = HybridNERExtractor(model_name="dslim/bert-base-NER")
        logger.info("[OK] NER model loaded successfully")
        logger.info("Starting web API server...")
        await run_web_api(ner_extractor)
    except Exception as e:
        logger.error("Fatal error in main: %s: %s", type(e).__name__, e, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        print("\n✓ Application terminated")