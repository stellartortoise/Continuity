from datetime import datetime
import Document_Controls as controller

class Project:
    def __init__(self, name, description, id=None, created_at=None):
        self.id = [controller.id_generator(controller.PROJECT_PRE, controller.project), id][id is not None]
        self.name = name
        self.description = description
        self.created_at = [datetime.now().timestamp(), created_at][created_at is not None]
        self.modified_at = datetime.now().timestamp()

    def get_stories(self):
        return controller.get_all_stories(project_id=self.id)

    def add_story(self, story):
        controller.create_story(self.id, story.title, story.body)

    def modify_story(self, story_id, title, body):
        return controller.modify_story(story_id, title, body)

    def delete_story(self, story_id):
        return controller.delete_story(story_id)

class Story:
    def __init__(self, title, body, project_id=None):
        self.id = controller.id_generator(controller.STORY_PRE, controller.story)
        self.project_id = project_id
        self.title = title
        self.body = body

    def get_events(self):
        return controller.get_all_events(self.id)

    def add_event(self, event):
        controller.create_event(self.id, event.name, event.description, event.participants)

    def modify_event(self, event_id, name, description, participants):
        return controller.modify_event(event_id, name, description, participants)

    def delete_event(self, event_id):
        return controller.delete_event(event_id)


class Event:
    def __init__(self, story_id, name, description, participants: list[str]):
        self.id = None
        self.story_id = story_id
        self.name = name
        self.description = description
        self.participants = participants