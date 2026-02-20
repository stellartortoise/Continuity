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
        total = stats.all()[0]['project_index']
        project.insert(project_meta.__dict__)
        stats.update({'project_count': get_count(project), "project_index": int(total) + 1})
    except:
        print("No stats found")

    return project_meta

def modify_project(project_id, name, description):
    project_meta = Project(name=name, description=description)
    project.update(project_meta.__dict__, Query().id == project_id)
    return

def delete_project(project_id):
    project_stories = story.get(Query().project_id == project_id)
    for s in project_stories:
        event.remove(Query().story_id == s.id)

    story.remove(Query().project_id == project_id)
    project.remove(Query().id == project_id)
    return

def get_project(project_id):
    return project.get(Query().id == project_id)

def get_all_projects():
    return project.all()

# -------------- Stories  -------------------#
# Create a story
def create_story(project_id, title, body):
    story_meta = Story(project_id=project_id, title=title, body=body)
    story.insert(story_meta.__dict__)
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
    event.remove(Query().story_id == story_id)

    # Clear story from db
    story.remove(Query().id == story_id)
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