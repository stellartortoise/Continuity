import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from models.ner_extractor import HybridNERExtractor
from models.fact_extractor import FactExtractor
from config.settings import EXPORT_JSON_DIR
from database.vector_db import VectorDB

logger = logging.getLogger(__name__)

# ---------------- Models ----------------
class ExtractRequest(BaseModel):
    text: str
    time_id: Optional[str] = "t_001"
    use_llm: Optional[bool] = None

class ValidateFactsRequest(BaseModel):
    entities: List[Dict[str, Any]]


class CanonFact(BaseModel):
    id: str
    entity_id: Optional[str] = None
    story_id: Optional[str] = None
    fact: str
    sourceText: Optional[str] = None
    evidence: Optional[Dict[str, Any]] = None


class CanonSyncRequest(BaseModel):
    project_id: str
    approved_facts: List[CanonFact] = []
    rejected_fact_ids: List[str] = []

# ---------------- In-memory job store ----------------
JOBS: Dict[str, Dict[str, Any]] = {}
# schema:
# {
#   "status": "queued"|"running"|"done"|"error",
#   "phase": "ner"|"facts"|"finalize",
#   "progress": 0.0..1.0,
#   "message": str,
#   "processed": int,
#   "total": int,
#   "currentEntityId": str|None,
#   "currentEntityName": str|None,
#   "result": {...} | None,
#   "createdAt": ts
# }

# ---------------- App factory ----------------
def create_app(
    ner_extractor: HybridNERExtractor,
    fact_extractor: Optional[FactExtractor] = None,
    vector_db: Optional[VectorDB] = None,
) -> FastAPI:
    app = FastAPI(title="Entity Extraction API", version="1.2.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {"status": "ok"}
    
    # ---------- New: async job with progress ----------
    @app.post("/entities/extract/start")
    async def extract_start(req: ExtractRequest) -> Dict[str, str]:
        job_id = uuid.uuid4().hex
        JOBS[job_id] = {
            "status": "queued",
            "phase": "ner",
            "progress": 0.0,
            "message": "Queued",
            "processed": 0,
            "total": 0,
            "currentEntityId": None,
            "currentEntityName": None,
            "result": None,
            "createdAt": time.time(),
        }

        # launch background task
        asyncio.create_task(_run_job(job_id, req, ner_extractor, fact_extractor))
        return {"jobId": job_id}

    @app.get("/entities/status/{job_id}")
    async def extract_status(job_id: str) -> Dict[str, Any]:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        # do not include result here (keep payload light)
        return {
            "status": job["status"],
            "phase": job["phase"],
            "progress": job["progress"],
            "message": job["message"],
            "processed": job["processed"],
            "total": job["total"],
            "currentEntityId": job["currentEntityId"],
            "currentEntityName": job["currentEntityName"],
        }

    @app.get("/entities/result/{job_id}")
    async def extract_result(job_id: str) -> Dict[str, Any]:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job["status"] != "done" or job["result"] is None:
            raise HTTPException(status_code=202, detail="Job not finished")
        return job["result"]

    @app.post("/entities/validate-facts")
    async def validate_facts(req: ValidateFactsRequest) -> Dict[str, Any]:
        """
        Validate facts for entities using NLI-based contradiction detection.

        This endpoint takes entities with facts and returns them with validation metadata
        (contradicts, entails, validation_confidence).
        """
        if not fact_extractor:
            raise HTTPException(status_code=503, detail="Fact extractor not available")

        try:
            validated_entities = fact_extractor.validate_entity_facts(req.entities)
            return {
                "entities": validated_entities,
                "count": len(validated_entities),
                "message": f"Validated facts for {len(validated_entities)} entities"
            }
        except Exception as e:
            logger.error(f"Fact validation error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

    @app.post("/canon/sync")
    async def canon_sync(req: CanonSyncRequest) -> Dict[str, Any]:
        if not vector_db:
            raise HTTPException(status_code=503, detail="Vector DB not available")

        approved_ids = [f"fact_{f.id}" for f in req.approved_facts if f.id]
        rejected_ids = [f"fact_{fid}" for fid in req.rejected_fact_ids if fid]

        all_ids = sorted(set(approved_ids + rejected_ids))
        if all_ids:
            try:
                vector_db.delete_documents(all_ids)
            except Exception:
                logger.debug("Delete step skipped or partial during canon sync", exc_info=True)

        docs = [f.fact for f in req.approved_facts if f.fact]
        if docs:
            metadata = [
                {
                    "project_id": req.project_id,
                    "fact_id": f.id,
                    "entity_id": f.entity_id,
                    "story_id": f.story_id,
                    "source_text": f.sourceText,
                }
                for f in req.approved_facts
                if f.fact
            ]
            ids = [f"fact_{f.id}" for f in req.approved_facts if f.fact]
            try:
                vector_db.add_documents(documents=docs, metadata=metadata, ids=ids)
            except Exception as ex:
                logger.error("Canon sync add failed: %s", ex, exc_info=True)
                raise HTTPException(status_code=500, detail=f"Canon sync failed: {type(ex).__name__}")

        return {
            "project_id": req.project_id,
            "approvedCount": len(docs),
            "rejectedCount": len(rejected_ids),
            "message": "Canon sync completed",
        }

    return app


# ---------------- Job runner ----------------
async def _run_job(
    job_id: str,
    req: ExtractRequest,
    ner_extractor: HybridNERExtractor,
    fact_extractor: Optional[FactExtractor],
):
    job = JOBS[job_id]
    try:
        job["status"] = "running"
        job["phase"] = "ner"
        job["message"] = "Extracting entities…"
        job["progress"] = 0.02

        text = (req.text or "").strip()
        time_id = req.time_id or "t_001"

        # --- Phase 1: NER ---
        entities: List[Dict[str, Any]] = await ner_extractor.extract_entities(text=text, time_id=time_id)
        job["progress"] = 0.30  # mark NER as 30% of the total
        job["message"] = f"Found {len(entities)} entities."

        # --- Phase 2: Facts ---
        facts_map: Dict[str, List[Dict[str, Any]]] = {}
        if entities and fact_extractor:
            if req.use_llm is not None:
                fact_extractor.use_llm = bool(req.use_llm)

            job["phase"] = "facts"
            job["message"] = "Aggregating facts…"

            # progress callback from fact extractor
            def _progress_cb(info: Dict[str, Any]):
                # info: {"processed": int, "total": int, "entityId": str, "entityName": str}
                processed = int(info.get("processed", 0))
                total = max(1, int(info.get("total", 1)))
                # facts are 70% of overall progress
                frac = processed / total
                job["progress"] = 0.30 + 0.70 * frac
                job["processed"] = processed
                job["total"] = total
                job["currentEntityId"] = info.get("entityId")
                job["currentEntityName"] = info.get("entityName")

            facts_map = await fact_extractor.extract_facts_for_entities(
                text, entities, time_id, progress=_progress_cb
            )
        else:
            job["phase"] = "finalize"

        # Merge + finalize
        for e in entities:
            e["facts"] = facts_map.get(e.get("id"), [])

        payload: Dict[str, Any] = {"entities": entities, "count": len(entities)}
        # export response JSON
        try:
            ts = time.strftime("%Y%m%d-%H%M%S")
            rid = uuid.uuid4().hex[:8]
            out_path = Path(EXPORT_JSON_DIR) / f"extract_{ts}_{rid}.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            payload["exportPath"] = str(out_path)
        except Exception as ex:
            print(f"[export] Failed to write JSON: {ex}")

        job["phase"] = "finalize"
        job["message"] = "Done"
        job["progress"] = 1.0
        job["status"] = "done"
        job["result"] = payload

    except Exception as e:
        job["status"] = "error"
        job["message"] = f"{type(e).__name__}: {e}"
        job["phase"] = "finalize"
        job["progress"] = 1.0