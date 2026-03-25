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


class ReviewSubmitRequest(BaseModel):
    submitted_by: Optional[str] = None


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


@app.get("/projects/{project_id}/canon-index")
async def get_project_canon_index(project_id: str):
    return {"documents": controller.get_canon_index(project_id)}


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
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex

    if not updated:
        raise HTTPException(status_code=404, detail="Fact not found")
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

    payload = {
        "text": story.get("body", ""),
        "time_id": time_id or story_id,
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
    conflicts = controller.get_project_conflicts(story.get("project_id"))
    pending = sum(1 for e in persisted["entities"] for f in e.get("facts", []) if f.get("status") == "pending")
    updated_story = controller.set_story_review_metadata(
        story_id=story_id,
        review_session_id=persisted["reviewSession"]["id"],
        pending_facts_count=pending,
        conflicts_detected=len(conflicts),
    ) or story

    return {
        "story": updated_story,
        "entities": persisted["entities"],
        "count": len(persisted["entities"]),
        "pendingFactsCount": pending,
        "conflictsDetected": len(conflicts),
        "reviewSessionId": persisted["reviewSession"]["id"],
        "exportPath": ai_payload.get("exportPath"),
    }


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