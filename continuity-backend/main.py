import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import Document_Controls
import requests
import time

app = FastAPI()
controller = Document_Controls
API_BASE = "http://localhost:8002"
@app.get("/")
async def root():
    return RedirectResponse("/docs")


# Projects Controller
@app.get("/projects")
async def get_project_by_id(id: int = None):
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
async def get_all_stories():
    return controller.get_all_stories()

@app.get("/stories/{id}")
async def get_story_by_id(id: str):
    return controller.get_story(id)

@app.post("/stories")
async def create_story(project_id: str, title: str, body: str):
    return controller.create_story(project_id, title, body)

@app.get("/extract/entities")
async def extract_story_entities(project_id):
    stories = controller.get_all_stories(project_id)
    story = [story["body"] for story in stories]
    job = requests.post(f"{API_BASE}/entities/extract/start", json={"text": story[1], "time_id": "t_002", })
    jobid = job.json()['jobId']
    poll_status(jobid)
    final_result = requests.get(f"{API_BASE}/entities/result/{jobid}")
    return final_result.json()

@app.put("/stories/{id}")
async def modify_story(id: str, title: str, body: str):
    return controller.modify_story(id, title, body)
@app.delete("/stories/{id}")
async def delete_story(id: str):
    return controller.delete_story(id)

# Events Controller
@app.get("/events")
async def get_all_events():
    return controller.get_all_events()

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

def poll_status(job_id):
    step_by_phase = {"ner": 1, "facts": 2, "finalize": 3}

    while True:
        try:
            r = requests.get(f"{API_BASE}/entities/status/{job_id}", headers={"Cache-Control": "no-store"})
            if r.status_code != 200:
                raise Exception(f"Status error: {r.status_code}")

            s = r.json()

            pct = round((s.get("progress", 0)) * 100)
            phase = s.get("phase")

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

        time.sleep(0.5)  # equivalent to setInterval(..., 500)