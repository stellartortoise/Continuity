from datetime import datetime
import Document_Controls

class Project:
    def __init__(self, name, description, id=None, created_at=None):
        self.id = id
        self.name = name
        self.description = description
        self.created_at = [datetime.now().timestamp(), created_at][created_at is not None]

    def get_stories(self):
        return Document_Controls.get_all_stories(project_id=self.id)

    def add_story(self, story):
        Document_Controls.create_story(self.id, story.title, story.body)

    def modify_story(self, story_id, title, body):
        return Document_Controls.modify_story(story_id, title, body)

    def delete_story(self, story_id):
        return Document_Controls.delete_story(story_id)

class Story:
    def __init__(self, title, body, project_id=None):
        self.id = None
        self.project_id = project_id
        self.title = title
        self.body = body

    def get_events(self):
        return Document_Controls.get_all_events(self.id)

    def add_event(self, event):
        Document_Controls.create_event(self.id, event.name, event.description, event.participants)

    def modify_event(self, event_id, name, description, participants):
        return Document_Controls.modify_event(event_id, name, description, participants)

    def delete_event(self, event_id):
        return Document_Controls.delete_event(event_id)


class Event:
    def __init__(self, story_id, name, description, participants: list[str]):
        self.id = None
        self.story_id = story_id
        self.name = name
        self.description = description
        self.participants = participants