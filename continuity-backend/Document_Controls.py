from Document_Models import *
import json
from tinydb import TinyDB, Query

db = TinyDB('cannon.json') # Main Database file
# Tables/Documents in db
project = db.table("project")
story = db.table("story")
event = db.table("event")
stats = db.table("stats")

# KEYS
PROJECT_PRE = "proj_"
STORY_PRE = "stry_"
EVENT_PRE = "evnt_"

# -------------- Projects -------------------#
def create_project(name, description):
    project_meta = Project(name=name, description=description)
    try:
        project.insert(project_meta.__dict__)
        project_count("increase")
    except:
        print("No stats found")

    return project_meta

def modify_project(project_id, **args):
    args['modified_at'] = datetime.now().timestamp()
    project.update(args, Query().id == project_id)
    return

def delete_project(project_id):
    project_stories = story.get(Query().project_id == project_id)
    if project_stories: # Check if project has stories
        for s in project_stories:
            event.remove(Query().story_id == s.id)

    story.remove(Query().project_id == project_id)
    project.remove(Query().id == project_id)
    project_count("decrease")
    return

def get_project(project_id):
    return project.get(Query().id == project_id)

def get_all_projects():
    return project.all()

# -------------- Stories  -------------------#
# Create a story
def create_story(project_id, title, body):
    story_meta = Story(project_id=project_id, title=title, body=body)
    try:
        story.insert(story_meta.__dict__)
        story_count("increase")
    except:
        print("No stats found")
    return story_meta

# Get all stories
def get_all_stories(project_id=None):
    if project_id:
        return story.search(Query().project_id == project_id)
    return story.all()

# Get story by id
def get_story(story_id):
    return story.get(Query().id == story_id)

# Delete story
def delete_story(story_id):
    # Clear Events associated with story first
    length = event.count(Query().story_id == story_id)
    event.remove(Query().story_id == story_id)
    event_count("decrease")
    # Clear story from db
    story.remove(Query().id == story_id)
    story_count("decrease")
    return

# Modify story
def modify_story(story_id, title, body):
    story_meta = Story(title=title, body=body)
    story.update(story_meta.__dict__, Query().id == story_id)
    return story_meta

#------------------ Events--------------------#
# Create an event
def create_event(story_id, name, description, participants):
    event_meta = Event(story_id=story_id, name=name, description=description, participants=participants)
    event.insert(event_meta.__dict__)
    return event_meta

# Get all events
def get_all_events(story_id):
    if story_id:
        return event.search(Query().story_id == story_id)
    return event.all()

# Get event by id
def get_event(event_id):
    return event.get(Query().id == event_id)

# Delete event
def delete_event(event_id):
    event.remove(Query().id == event_id)
    return

# Modify event
def modify_event(event_id, name, description, participants):
    event_meta = Event(name=name, description=description, participants=participants)
    event.update(event_meta.__dict__, Query().id == event_id)
    return event_meta

# Helper Functions
def exclude_fields(docs, *fields):
    return [
        {k: v for k, v in doc.items() if k not in fields}
        for doc in docs
    ]

def project_count(effect):
    if effect in ["increase", "inc", "i"]:
        total = stats.all()[0]['project_index']
        stats.update({'project_count': get_count(project), "project_index": int(total) + 1})

    elif effect in ["decrease", "dec", "d"]:
        stats.update({'project_count': get_count(project) - 1})

def event_count(effect):
    if effect in ["increase", "inc", "i"]:
        total = stats.all()[0]['event_index']
        stats.update({'event_count': get_count(project), "event_index": int(total) + 1})

    elif effect in ["decrease", "dec", "d"]:
       stats.update({'event_count': get_count(event) - 1})

def story_count(effect):
    if effect in ["increase", "inc", "i"]:
        total = stats.all()[0]['story_index']
        stats.update({'story_count': get_count(project), "story_index": int(total) + 1})

    elif effect in ["decrease", "dec", "d"]:
        stats.update({'story_count': get_count(story) - 1})

def get_count(table):
    return len(table.all())

def get_all_stats():
    return stats.all()

def id_generator(prefix, table):
    rows = str (get_count(table) + 1)
    return prefix + ("0"*(4-len(rows)) + rows)