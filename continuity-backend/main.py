import time
from typing import Any, Dict, List, Optional

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

import Document_Controls

app = FastAPI()
controller = Document_Controls
API_BASE = "http://localhost:8002"


class SegmentRequest(BaseModel):
    body: str = Field(min_length=1)
    title: Optional[str] = None
    time_id: Optional[str] = None


class FactDecisionRequest(BaseModel):
    status: str = Field(pattern="^(pending|approved|rejected)$")
    reviewed_by: Optional[str] = None
    decision_reason: Optional[str] = None
    confirm_assignment: bool = False
    confirm_low_quality: bool = False


class FactEntityAssignmentRequest(BaseModel):
    entity_id: str


class ReviewSubmitRequest(BaseModel):
    submitted_by: Optional[str] = None


class CanonEntityCreateRequest(BaseModel):
    name: str
    type: Optional[str] = None
    aliases: Optional[List[str]] = None
    description: Optional[str] = None
    notes: Optional[str] = None
    confidence: Optional[float] = None
    status: Optional[str] = None


class CanonEntityUpdateRequest(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    aliases: Optional[List[str]] = None
    description: Optional[str] = None
    notes: Optional[str] = None
    confidence: Optional[float] = None


class CanonEntityMergeRequest(BaseModel):
    source_entity_id: str
    target_entity_id: str


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return RedirectResponse("/docs")


# Projects Controller
@app.get("/projects")
async def get_project_by_id(id: Optional[str] = None):
    return controller.get_project(id)

@app.get("/projects/all")
async def get_all_projects():
    return controller.get_all_projects()

@app.post("/projects")
async def create_project(name: str, description: str):
    return controller.create_project(name, description)

@app.put("/projects/{id}")
async def modify_project(id: str, name: str, description: str):
    return controller.modify_project(id, name, description)

@app.delete("/projects/{id}")
async def delete_project(id: str):
    return controller.delete_project(id)


# Stories Controller
@app.get("/stories")
async def get_all_stories(project_id: Optional[str] = None):
    return controller.get_all_stories(project_id=project_id)

@app.get("/stories/{id}")
async def get_story_by_id(id: str):
    return controller.get_story(id)

@app.post("/stories")
async def create_story(project_id: str, title: str, body: str):
    return controller.create_story(project_id, title, body)

@app.post("/projects/{project_id}/segments")
async def upload_segment(project_id: str, req: SegmentRequest):
    project = controller.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    count = len(controller.get_all_stories(project_id=project_id)) + 1
    title = req.title or f"Segment {count}"
    created_story = controller.create_story(project_id=project_id, title=title, body=req.body)
    extraction = _extract_and_persist(story_id=created_story["id"], time_id=req.time_id)
    return {
        "story": created_story,
        "entities": extraction["entities"],
        "pendingFactsCount": extraction["pendingFactsCount"],
        "conflictsDetected": extraction["conflictsDetected"],
        "reviewSessionId": extraction["reviewSessionId"],
    }


@app.post("/stories/{story_id}/extract-entities")
async def extract_story_entities(story_id: str, time_id: Optional[str] = None):
    return _extract_and_persist(story_id=story_id, time_id=time_id)


@app.get("/stories/{story_id}/entities")
async def get_story_entities(story_id: str):
    return {"entities": controller.get_entities_by_story(story_id)}


@app.get("/projects/{project_id}/entities")
async def get_project_entities(project_id: str):
    return {"entities": controller.get_entities_by_project(project_id)}


@app.get("/projects/{project_id}/conflicts")
async def get_project_conflicts(project_id: str):
    return {"conflicts": controller.get_project_conflicts(project_id)}


@app.get("/stories/{story_id}/conflicts")
async def get_story_conflicts(story_id: str, include_cross_story: bool = False):
    return {"conflicts": controller.get_story_conflicts(story_id, include_cross_story=include_cross_story)}


@app.get("/projects/{project_id}/canon-index")
async def get_project_canon_index(project_id: str):
    return {"documents": controller.get_canon_index(project_id)}


@app.get("/projects/{project_id}/canon/sync-status")
async def get_project_canon_sync_status(project_id: str):
    retried = _retry_sync_for_project(project_id)
    sessions = controller.get_review_sessions_by_project(project_id)
    pending = [s for s in sessions if s.get("syncStatus") == "pending"]
    errored = [s for s in sessions if s.get("syncStatus") == "error"]
    return {
        "project_id": project_id,
        "autoRetried": len(retried),
        "totalSubmitted": len([s for s in sessions if s.get("status") == "submitted"]),
        "pendingCount": len(pending),
        "errorCount": len(errored),
        "sessions": sessions[:20],
    }


@app.post("/projects/{project_id}/canon/resync-pending")
async def retry_project_canon_sync(project_id: str):
    results = _retry_sync_for_project(project_id)
    success_count = len([row for row in results if row.get("ok")])

    return {
        "project_id": project_id,
        "retryCount": len(results),
        "successCount": success_count,
        "results": results,
    }


def _retry_sync_for_project(project_id: str) -> List[Dict[str, Any]]:
    retryable = controller.get_retryable_review_sessions(project_id)
    results: List[Dict[str, Any]] = []
    for session in retryable:
        sync = _sync_canon_index_for_session(session)
        controller.mark_review_session_sync_result(session["id"], ok=sync["ok"], message=sync["message"])
        results.append(
            {
                "sessionId": session.get("id"),
                "ok": sync["ok"],
                "message": sync["message"],
            }
        )
    return results


@app.get("/projects/{project_id}/canon/entities")
async def get_project_canon_entities(project_id: str, query: Optional[str] = None):
    entities = controller.get_entities_by_project(project_id)
    if query:
        entities = controller.search_entities(project_id, query)
    return {"entities": entities}


@app.get("/projects/{project_id}/canon/suggestions")
async def get_project_canon_suggestions(project_id: str):
    return {"entities": controller.get_suggested_entities_by_project(project_id)}


@app.post("/projects/{project_id}/canon/entities")
async def create_project_canon_entity(project_id: str, req: CanonEntityCreateRequest):
    try:
        entity = controller.create_entity(
            project_id,
            {
                "name": req.name,
                "type": req.type,
                "aliases": req.aliases or [],
                "description": req.description,
                "notes": req.notes,
                "confidence": req.confidence if req.confidence is not None else 1.0,
            },
            status=req.status or "active",
        )
        return {"entity": entity}
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex


@app.post("/projects/{project_id}/canon/entities/{entity_id}/promote")
async def promote_canon_entity(project_id: str, entity_id: str):
    entity = controller.promote_entity(entity_id)
    if not entity or entity.get("project_id") != project_id:
        raise HTTPException(status_code=404, detail="Entity not found")
    return {"entity": entity}


@app.get("/entities/{entity_id}")
async def get_canon_entity(entity_id: str):
    entity = controller.get_entity(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")
    return entity


@app.put("/entities/{entity_id}")
async def update_canon_entity(entity_id: str, req: CanonEntityUpdateRequest):
    updated = controller.update_entity(
        entity_id,
        {
            "name": req.name,
            "type": req.type,
            "aliases": req.aliases,
            "description": req.description,
            "notes": req.notes,
            "confidence": req.confidence,
        },
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Entity not found")
    return {"entity": updated}


@app.delete("/entities/{entity_id}")
async def delete_canon_entity(entity_id: str):
    deleted = controller.soft_delete_entity(entity_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Entity not found")
    return {"entity": deleted}


@app.post("/projects/{project_id}/canon/entities/merge")
async def merge_canon_entities(project_id: str, req: CanonEntityMergeRequest):
    try:
        merged = controller.merge_entities(project_id, req.source_entity_id, req.target_entity_id)
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex
    if not merged:
        raise HTTPException(status_code=404, detail="Entity not found")
    return {"entity": merged}


@app.get("/entities/{entity_id}/facts")
async def get_entity_facts(entity_id: str):
    return {"facts": controller.get_entity_facts(entity_id)}


@app.patch("/facts/{fact_id}")
async def review_fact(fact_id: str, req: FactDecisionRequest):
    try:
        updated = controller.set_fact_status(
            fact_id=fact_id,
            status=req.status,
            reviewed_by=req.reviewed_by,
            decision_reason=req.decision_reason,
            confirm_assignment=req.confirm_assignment,
            confirm_low_quality=req.confirm_low_quality,
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex

    if not updated:
        raise HTTPException(status_code=404, detail="Fact not found")
    return updated


@app.patch("/facts/{fact_id}/entity")
async def assign_fact_entity(fact_id: str, req: FactEntityAssignmentRequest):
    updated = controller.reassign_fact_entity(fact_id, req.entity_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Fact or entity not found")
    return updated


@app.post("/review-sessions/{session_id}/submit")
async def submit_review(session_id: str, req: ReviewSubmitRequest):
    try:
        result = controller.submit_review_session(session_id, submitted_by=req.submitted_by)
        sync = _sync_canon_index_for_session(result)
        controller.mark_review_session_sync_result(session_id, ok=sync["ok"], message=sync["message"])
        refreshed = controller.get_review_session(session_id) or result
        return {
            "reviewSession": refreshed,
            "message": sync["message"],
        }
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex


@app.get("/review-sessions/{session_id}")
async def get_review_session(session_id: str):
    session = controller.get_review_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Review session not found")
    return session

@app.put("/stories/{id}")
async def modify_story(id: str, title: str, body: str):
    return controller.modify_story(id, title, body)


@app.delete("/stories/{id}")
async def delete_story(id: str):
    return controller.delete_story(id)

# Events Controller
@app.get("/events")
async def get_all_events(story_id: Optional[str] = None):
    return controller.get_all_events(story_id=story_id)

@app.get("/events/{id}")
async def get_event_by_id(id: str):
    return controller.get_event(id)

@app.post("/events")
async def create_event(story_id: str, name: str, description: str, participants: list[str]):
    return controller.create_event(story_id, name, description, participants)

@app.delete("/events/{id}")
async def delete_event(id: str):
    return controller.delete_event(id)

@app.put("/events/{id}")
async def modify_event(id: str, name: str, description: str, participants: list[str]):
    return controller.modify_event(id, name, description, participants)


@app.get("/extract/entities")
async def extract_entities_legacy(project_id: str):
    stories = controller.get_all_stories(project_id=project_id)
    if not stories:
        raise HTTPException(status_code=404, detail="No stories found for project")
    # Legacy behavior redirected to first story extraction instead of brittle fixed index.
    return _extract_and_persist(story_id=stories[0]["id"], time_id="t_legacy")


def _extract_and_persist(story_id: str, time_id: Optional[str] = None) -> Dict[str, Any]:
    story = controller.get_story(story_id)
    if not story:
        raise HTTPException(status_code=404, detail="Story not found")

    canon_entities = [
        entity
        for entity in controller.get_entities_by_project(story.get("project_id"))
        if entity.get("status") == "active"
    ]

    payload = {
        "text": story.get("body", ""),
        "time_id": time_id or story_id,
        "project_id": story.get("project_id"),
        "canon_entities": canon_entities,
    }
    start = requests.post(f"{API_BASE}/entities/extract/start", json=payload, timeout=30)
    if start.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"AI start failed: {start.text}")

    job_id = start.json().get("jobId")
    if not job_id:
        raise HTTPException(status_code=502, detail="AI service did not return jobId")

    poll_status(job_id)
    final_result = requests.get(f"{API_BASE}/entities/result/{job_id}", timeout=30)
    if final_result.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"AI result failed: {final_result.text}")

    ai_payload = final_result.json()
    persisted = controller.persist_extracted_entities(
        project_id=story.get("project_id"),
        story_id=story_id,
        extracted_entities=ai_payload.get("entities", []),
    )
    story_conflicts = controller.get_story_conflicts(story_id, include_cross_story=False)
    cross_story_conflicts = [
        c for c in controller.get_story_conflicts(story_id, include_cross_story=True)
        if c.get("conflictType") == "inter-story"
    ]
    pending = sum(1 for e in persisted["entities"] for f in e.get("facts", []) if f.get("status") == "pending")
    updated_story = controller.set_story_review_metadata(
        story_id=story_id,
        review_session_id=persisted["reviewSession"]["id"],
        pending_facts_count=pending,
        conflicts_detected=len(story_conflicts),
    ) or story

    summary = _build_entity_summary(persisted["entities"], story_id)

    return {
        "story": updated_story,
        "entities": persisted["entities"],
        "summary": summary,
        "count": len(persisted["entities"]),
        "pendingFactsCount": pending,
        "conflictsDetected": len(story_conflicts),
        "crossStoryConflictsCount": len(cross_story_conflicts),
        "reviewSessionId": persisted["reviewSession"]["id"],
        "exportPath": ai_payload.get("exportPath"),
    }


def _fact_section(entity_type: str, fact_text: str) -> str:
    lower = (fact_text or "").lower()
    if entity_type == "character":
        if "eye" in lower or "hair" in lower or "height" in lower or "appearance" in lower:
            return "Physical Traits"
        if "waiting" in lower or "wants" in lower or "looking for" in lower or "goal" in lower:
            return "Current Status / Motivation"
        if "before" in lower or "used to" in lower or "many times" in lower or "history" in lower:
            return "History / Backstory"
        if " in " in lower or " at " in lower or "inside" in lower:
            return "Current Location"
    if entity_type == "location":
        if "old" in lower or "dusty" in lower or "peeling" in lower or "ruined" in lower:
            return "Age / Condition"
        if "window" in lower or "booth" in lower or "door" in lower or "hall" in lower:
            return "Architecture / Features"
    if entity_type in {"event", "object"}:
        return "Status"
    return "Facts"


def _build_entity_summary(entities: List[Dict[str, Any]], story_id: str) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    for ent in entities:
        sections: Dict[str, List[str]] = {}
        etype = ent.get("entityType") or ent.get("type") or "concept"
        for f in ent.get("facts", []):
            text = (f.get("fact") or "").strip()
            if not text:
                continue
            sec = _fact_section(etype, text)
            sections.setdefault(sec, []).append(text)

        summary.append(
            {
                "storyId": story_id,
                "entityId": ent.get("id"),
                "entityName": ent.get("name"),
                "entityType": etype,
                "sections": [
                    {"name": name, "facts": values}
                    for name, values in sections.items()
                ],
            }
        )
    return summary


def _sync_canon_index_for_session(session: Dict[str, Any]) -> Dict[str, Any]:
    fact_rows = controller.get_facts_by_ids(session.get("fact_ids", []))
    approved = [f for f in fact_rows if f.get("status") == "approved"]
    rejected_ids = [f.get("id") for f in fact_rows if f.get("status") != "approved"]

    payload = {
        "project_id": session.get("project_id"),
        "approved_facts": [
            {
                "id": f.get("id"),
                "entity_id": f.get("entity_id"),
                "story_id": f.get("story_id"),
                "fact": f.get("fact"),
                "sourceText": f.get("sourceText"),
                "evidence": f.get("evidence"),
            }
            for f in approved
        ],
        "rejected_fact_ids": [fid for fid in rejected_ids if fid],
    }

    try:
        res = requests.post(f"{API_BASE}/canon/sync", json=payload, timeout=60)
        if res.status_code >= 400:
            return {"ok": False, "message": f"Review submitted; canon sync failed ({res.status_code})"}
        data = res.json()
        return {
            "ok": True,
            "message": data.get("message", "Review submitted and canon index synchronized"),
        }
    except Exception as ex:
        return {"ok": False, "message": f"Review submitted; canon sync request failed: {type(ex).__name__}"}


def poll_status(job_id):
    while True:
        try:
            r = requests.get(
                f"{API_BASE}/entities/status/{job_id}",
                headers={"Cache-Control": "no-store"},
                timeout=30,
            )
            if r.status_code != 200:
                raise Exception(f"Status error: {r.status_code}")

            s = r.json()
            processed = s.get("processed", 0)
            total = s.get("total", 0)

            if s.get("currentEntityName"):
                print(f"Processing: {s['currentEntityName']} ({processed}/{total})")
            else:
                print(s.get("message", "Working…"))

            if s.get("status") == "done":
                print("Job finished")
                return s

            elif s.get("status") == "error":
                raise Exception(s.get("message", "Unknown error"))

        except Exception as e:
            raise e

        time.sleep(0.5)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)