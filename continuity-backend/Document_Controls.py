import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from Document_Models import Event, Project, Story
from tinydb import Query, TinyDB

db = TinyDB("cannon.json")

# Tables/Documents in db
project = db.table("project")
story = db.table("story")
event = db.table("event")
entity = db.table("entity")
fact = db.table("fact")
review_session = db.table("review_session")
canon_vector_index = db.table("canon_vector_index")
stats = db.table("stats")

# KEYS
PROJECT_PRE = "proj_"
STORY_PRE = "stry_"
EVENT_PRE = "evnt_"
ENTITY_PRE = "ent_"
FACT_PRE = "fact_"
REVIEW_PRE = "rvs_"


def _now_ts() -> float:
    return datetime.now().timestamp()


def _ensure_stats_doc() -> None:
    if stats.all():
        return
    stats.insert(
        {
            "project_count": get_count(project),
            "project_index": get_count(project),
            "story_count": get_count(story),
            "story_index": get_count(story),
            "event_count": get_count(event),
            "event_index": get_count(event),
            "entity_count": get_count(entity),
            "entity_index": get_count(entity),
            "fact_count": get_count(fact),
            "fact_index": get_count(fact),
            "review_count": get_count(review_session),
            "review_index": get_count(review_session),
        }
    )


def _normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip()).lower()


def _normalize_fact_text(text: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9\s]", "", text.lower())
    return re.sub(r"\s+", " ", value).strip()


def _infer_conflict_key(fact_text: str, entity_id: str = None) -> Optional[str]:
    """Generate a conflict key for grouping potentially contradictory facts.
    
    Strategy: Use entity-based grouping so all facts about same entity can be
    checked for contradictions, with special recognition for known attribute patterns.
    """
    lower = fact_text.lower()
    
    # Specific high-confidence attribute patterns (always group these)
    if " eye" in lower or " eyes" in lower:
        return "attribute:eyes"
    if " hair" in lower:
        return "attribute:hair"
    if " age" in lower or " years old" in lower or " b." in lower or " born" in lower:
        return "attribute:age"
    if " is from " in lower or " lives in " in lower or " located in " in lower or " home" in lower:
        return "attribute:location"
    
    # General fallback: use entity-based key so all facts about entity can be compared
    # This catches contradictory facts even if they use different predicates
    if entity_id:
        return f"entity:{entity_id}"
    
    return None


def _safe_unique(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item and item not in seen:
            out.append(item)
            seen.add(item)
    return out


# -------------- Projects -------------------#
def create_project(name: str, description: str) -> Dict[str, Any]:
    _ensure_stats_doc()
    project_meta = Project(name=name, description=description).__dict__
    project.insert(project_meta)
    project_count("increase")
    return project_meta


def modify_project(project_id: str, **args) -> Optional[Dict[str, Any]]:
    args["modified_at"] = _now_ts()
    project.update(args, Query().id == project_id)
    return get_project(project_id)


def delete_project(project_id: str) -> None:
    stories = story.search(Query().project_id == project_id)
    for s in stories:
        delete_story(s.get("id"))
    entity.remove(Query().project_id == project_id)
    fact.remove(Query().project_id == project_id)
    review_session.remove(Query().project_id == project_id)
    canon_vector_index.remove(Query().project_id == project_id)
    project.remove(Query().id == project_id)
    project_count("decrease")


def get_project(project_id: str) -> Optional[Dict[str, Any]]:
    return project.get(Query().id == project_id)


def get_all_projects() -> List[Dict[str, Any]]:
    return project.all()


# -------------- Stories  -------------------#
def create_story(project_id: str, title: str, body: str) -> Dict[str, Any]:
    _ensure_stats_doc()
    story_meta = Story(project_id=project_id, title=title, body=body).__dict__
    story_meta["created_at"] = _now_ts()
    story_meta["modified_at"] = story_meta["created_at"]
    story.insert(story_meta)
    story_count("increase")
    return story_meta


def get_all_stories(project_id: Optional[str] = None) -> List[Dict[str, Any]]:
    if project_id:
        return story.search(Query().project_id == project_id)
    return story.all()


def get_story(story_id: str) -> Optional[Dict[str, Any]]:
    return story.get(Query().id == story_id)


def delete_story(story_id: str) -> None:
    if not story_id:
        return
    event.remove(Query().story_id == story_id)
    story.remove(Query().id == story_id)
    story_count("decrease")
    event_count("decrease")

    linked_facts = fact.search(Query().story_id == story_id)
    fact_ids = [f.get("id") for f in linked_facts]
    fact.remove(Query().story_id == story_id)
    if fact_ids:
        for ent in entity.all():
            kept = [fid for fid in ent.get("fact_ids", []) if fid not in fact_ids]
            if kept != ent.get("fact_ids", []):
                ent["fact_ids"] = kept
                ent["lastUpdatedAt"] = _now_ts()
                entity.update(ent, Query().id == ent.get("id"))


def modify_story(story_id: str, title: str, body: str) -> Optional[Dict[str, Any]]:
    story.update({"title": title, "body": body, "modified_at": _now_ts()}, Query().id == story_id)
    return get_story(story_id)


def set_story_review_metadata(
    story_id: str,
    review_session_id: str,
    pending_facts_count: int,
    conflicts_detected: int,
) -> Optional[Dict[str, Any]]:
    story.update(
        {
            "reviewSessionId": review_session_id,
            "pendingFactsCount": pending_facts_count,
            "conflictsDetected": conflicts_detected,
            "modified_at": _now_ts(),
        },
        Query().id == story_id,
    )
    return get_story(story_id)


def get_pending_facts_count_for_story(story_id: str) -> int:
    rows = fact.search((Query().story_id == story_id) & (Query().status == "pending"))
    return len(rows)


#------------------ Events--------------------#
def create_event(story_id: str, name: str, description: str, participants: List[str]) -> Dict[str, Any]:
    _ensure_stats_doc()
    event_meta = Event(story_id=story_id, name=name, description=description, participants=participants).__dict__
    event_meta["id"] = event_meta.get("id") or id_generator(EVENT_PRE, event)
    event_meta["created_at"] = _now_ts()
    event.insert(event_meta)
    event_count("increase")
    return event_meta


def get_all_events(story_id: Optional[str] = None) -> List[Dict[str, Any]]:
    if story_id:
        return event.search(Query().story_id == story_id)
    return event.all()


def get_event(event_id: str) -> Optional[Dict[str, Any]]:
    return event.get(Query().id == event_id)


def delete_event(event_id: str) -> None:
    event.remove(Query().id == event_id)
    event_count("decrease")


def modify_event(event_id: str, name: str, description: str, participants: List[str]) -> Optional[Dict[str, Any]]:
    event.update(
        {
            "name": name,
            "description": description,
            "participants": participants,
            "modified_at": _now_ts(),
        },
        Query().id == event_id,
    )
    return get_event(event_id)


# -------------- Entities + Facts -------------------#
def upsert_entity(project_id: str, story_id: str, entity_payload: Dict[str, Any]) -> Dict[str, Any]:
    _ensure_stats_doc()
    name = (entity_payload.get("name") or "").strip()
    entity_type = (entity_payload.get("entityType") or "concept").strip()
    aliases = list(entity_payload.get("aliases") or [])
    confidence = float(entity_payload.get("confidence", 0.0))
    norm_name = _normalize_name(name)

    existing = entity.get(
        (Query().project_id == project_id)
        & (Query().entityType == entity_type)
        & (Query().normalized_name == norm_name)
    )

    now = _now_ts()
    if not existing:
        doc = {
            "id": id_generator(ENTITY_PRE, entity),
            "project_id": project_id,
            "story_ids": [story_id] if story_id else [],
            "entityType": entity_type,
            "name": name,
            "normalized_name": norm_name,
            "aliases": _safe_unique(aliases),
            "fact_ids": [],
            "firstMentionedAt": entity_payload.get("firstMentionedAt"),
            "lastUpdatedAt": entity_payload.get("lastUpdatedAt"),
            "version": int(entity_payload.get("version", 1)),
            "confidence": confidence,
            "created_at": now,
            "updated_at": now,
        }
        entity.insert(doc)
        entity_count("increase")
        return doc

    existing["aliases"] = _safe_unique(existing.get("aliases", []) + aliases)
    existing["story_ids"] = _safe_unique(existing.get("story_ids", []) + ([story_id] if story_id else []))
    existing["confidence"] = max(float(existing.get("confidence", 0.0)), confidence)
    existing["version"] = int(existing.get("version", 1)) + 1
    existing["updated_at"] = now
    existing["lastUpdatedAt"] = entity_payload.get("lastUpdatedAt")
    entity.update(existing, Query().id == existing["id"])
    return existing


def _attach_conflicts(entity_id: str, new_fact: Dict[str, Any]) -> Dict[str, Any]:
    conflict_key = _infer_conflict_key(new_fact.get("fact", ""), entity_id)
    if not conflict_key:
        return new_fact

    # First try: search for facts with same conflict_key
    candidates = fact.search(
        (Query().entity_id == entity_id)
        & (Query().id != new_fact["id"])
        & (Query().conflict_key == conflict_key)
        & (Query().status != "rejected")
    )
    
    # Filter to only those with different normalized facts (potential contradictions)
    conflicting = [c for c in candidates if c.get("normalized_fact") != new_fact.get("normalized_fact")]
    
    if conflicting:
        # Assign to existing conflict group or create new one
        group_id = conflicting[0].get("conflict_group_id") or id_generator("cfg_", review_session)
        new_fact["conflict_group_id"] = group_id
        new_fact["contradicts"] = _safe_unique(new_fact.get("contradicts", []) + [c["id"] for c in conflicting])

        for existing in conflicting:
            existing["conflict_group_id"] = group_id
            existing["contradicts"] = _safe_unique(existing.get("contradicts", []) + [new_fact["id"]])
            existing["updated_at"] = _now_ts()
            fact.update(existing, Query().id == existing["id"])

    return new_fact


def upsert_fact(project_id: str, story_id: str, entity_id: str, fact_payload: Dict[str, Any]) -> Dict[str, Any]:
    _ensure_stats_doc()
    text = (fact_payload.get("fact") or "").strip()
    normalized = _normalize_fact_text(text)
    evidence = fact_payload.get("evidence", {})

    existing = fact.get(
        (Query().entity_id == entity_id)
        & (Query().story_id == story_id)
        & (Query().normalized_fact == normalized)
        & (Query().evidence == evidence)
    )

    now = _now_ts()
    if existing:
        existing["confidence"] = max(float(existing.get("confidence", 0.0)), float(fact_payload.get("confidence", 0.0)))
        existing["updated_at"] = now
        fact.update(existing, Query().id == existing["id"])
        return existing

    doc = {
        "id": id_generator(FACT_PRE, fact),
        "entity_id": entity_id,
        "project_id": project_id,
        "story_id": story_id,
        "fact": text,
        "normalized_fact": normalized,
        "sourceText": fact_payload.get("sourceText"),
        "evidence": evidence,
        "confidence": float(fact_payload.get("confidence", 0.0)),
        "method": fact_payload.get("method", "llm"),
        "status": "pending",
        "conflict_key": _infer_conflict_key(text, entity_id),
        "conflict_group_id": None,
        "contradicts": [],
        "reviewed_by": None,
        "reviewed_at": None,
        "decision_reason": None,
        "created_at": now,
        "updated_at": now,
    }

    fact.insert(doc)
    fact_count("increase")
    doc = _attach_conflicts(entity_id, doc)
    fact.update(doc, Query().id == doc["id"])

    ent = entity.get(Query().id == entity_id)
    if ent:
        ent["fact_ids"] = _safe_unique(ent.get("fact_ids", []) + [doc["id"]])
        ent["updated_at"] = now
        entity.update(ent, Query().id == entity_id)

    return doc


def persist_extracted_entities(
    project_id: str,
    story_id: str,
    extracted_entities: List[Dict[str, Any]],
) -> Dict[str, Any]:
    persisted_entities: List[Dict[str, Any]] = []
    fact_ids: List[str] = []

    for item in extracted_entities:
        ent = upsert_entity(project_id, story_id, item)
        persisted_facts: List[Dict[str, Any]] = []
        for f in item.get("facts", []):
            stored = upsert_fact(project_id, story_id, ent["id"], f)
            persisted_facts.append(stored)
            fact_ids.append(stored["id"])
        ent_copy = dict(ent)
        ent_copy["facts"] = persisted_facts
        persisted_entities.append(ent_copy)

    session = create_review_session(project_id=project_id, story_id=story_id, fact_ids=_safe_unique(fact_ids))
    return {"entities": persisted_entities, "reviewSession": session}


def create_review_session(project_id: str, story_id: str, fact_ids: List[str]) -> Dict[str, Any]:
    _ensure_stats_doc()
    now = _now_ts()
    doc = {
        "id": id_generator(REVIEW_PRE, review_session),
        "project_id": project_id,
        "story_id": story_id,
        "fact_ids": _safe_unique(fact_ids),
        "status": "open",
        "syncStatus": "idle",
        "syncMessage": None,
        "created_at": now,
        "submitted_at": None,
        "submitted_by": None,
    }
    review_session.insert(doc)
    review_count("increase")
    return doc


def get_review_session(session_id: str) -> Optional[Dict[str, Any]]:
    return review_session.get(Query().id == session_id)


def set_fact_status(
    fact_id: str,
    status: str,
    reviewed_by: Optional[str] = None,
    decision_reason: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if status not in {"pending", "approved", "rejected"}:
        raise ValueError("Invalid fact status")

    doc = fact.get(Query().id == fact_id)
    if not doc:
        return None

    doc["status"] = status
    doc["reviewed_by"] = reviewed_by
    doc["decision_reason"] = decision_reason
    doc["reviewed_at"] = _now_ts()
    doc["updated_at"] = doc["reviewed_at"]
    fact.update(doc, Query().id == fact_id)
    return doc


def _session_unresolved_fact_ids(session: Dict[str, Any]) -> List[str]:
    unresolved: List[str] = []
    for fid in session.get("fact_ids", []):
        row = fact.get(Query().id == fid)
        if row and row.get("status") == "pending":
            unresolved.append(fid)
    return unresolved


def submit_review_session(session_id: str, submitted_by: Optional[str] = None) -> Dict[str, Any]:
    session = get_review_session(session_id)
    if not session:
        raise ValueError("Review session not found")
    if session.get("status") == "submitted":
        return session

    unresolved = _session_unresolved_fact_ids(session)
    if unresolved:
        raise ValueError("All facts must be addressed before submit")

    approved = []
    rejected = []
    for fid in session.get("fact_ids", []):
        row = fact.get(Query().id == fid)
        if not row:
            continue
        if row.get("status") == "approved":
            approved.append(row)
        else:
            rejected.append(row)

    # Submit-time canon sync gate: write only approved facts to index table.
    for item in approved:
        canon_vector_index.upsert(
            {
                "id": f"fact_{item['id']}",
                "fact_id": item["id"],
                "project_id": item.get("project_id"),
                "entity_id": item.get("entity_id"),
                "story_id": item.get("story_id"),
                "text": item.get("fact"),
                "sourceText": item.get("sourceText"),
                "evidence": item.get("evidence"),
                "updated_at": _now_ts(),
            },
            Query().fact_id == item["id"],
        )

    for item in rejected:
        canon_vector_index.remove(Query().fact_id == item["id"])

    session["status"] = "submitted"
    session["syncStatus"] = "pending"
    session["syncMessage"] = None
    session["submitted_by"] = submitted_by
    session["submitted_at"] = _now_ts()
    review_session.update(session, Query().id == session_id)
    return session


def mark_review_session_sync_result(session_id: str, ok: bool, message: str) -> Optional[Dict[str, Any]]:
    session = get_review_session(session_id)
    if not session:
        return None
    session["syncStatus"] = "ok" if ok else "error"
    session["syncMessage"] = message
    session["updated_at"] = _now_ts()
    review_session.update(session, Query().id == session_id)
    return session


def get_facts_by_ids(fact_ids: List[str]) -> List[Dict[str, Any]]:
    if not fact_ids:
        return []
    rows: List[Dict[str, Any]] = []
    for fid in fact_ids:
        doc = fact.get(Query().id == fid)
        if doc:
            rows.append(doc)
    return rows


def get_entities_by_story(story_id: str) -> List[Dict[str, Any]]:
    rows = entity.search(Query().story_ids.test(lambda ids: isinstance(ids, list) and story_id in ids))
    output: List[Dict[str, Any]] = []
    for e in rows:
        ec = dict(e)
        ec["facts"] = fact.search(Query().entity_id == e["id"])
        output.append(ec)
    return output


def get_entities_by_project(project_id: str) -> List[Dict[str, Any]]:
    rows = entity.search(Query().project_id == project_id)
    output: List[Dict[str, Any]] = []
    for e in rows:
        ec = dict(e)
        ec["facts"] = fact.search(Query().entity_id == e["id"])
        output.append(ec)
    return output


def get_entity_facts(entity_id: str) -> List[Dict[str, Any]]:
    return fact.search(Query().entity_id == entity_id)


def get_project_conflicts(project_id: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    rows = fact.search((Query().project_id == project_id) & (Query().conflict_group_id != None))
    for row in rows:
        gid = row.get("conflict_group_id")
        if not gid:
            continue
        grouped.setdefault(gid, []).append(row)

    conflicts: List[Dict[str, Any]] = []
    for gid, values in grouped.items():
        if len(values) < 2:
            continue
        conflicts.append(
            {
                "conflictGroupId": gid,
                "factCount": len(values),
                "facts": values,
                "resolved": all(v.get("status") != "pending" for v in values),
            }
        )
    return conflicts


def get_canon_index(project_id: Optional[str] = None) -> List[Dict[str, Any]]:
    if project_id:
        return canon_vector_index.search(Query().project_id == project_id)
    return canon_vector_index.all()


# Helper Functions
def exclude_fields(docs, *fields):
    return [{k: v for k, v in doc.items() if k not in fields} for doc in docs]


def project_count(effect):
    _ensure_stats_doc()
    row = stats.all()[0]
    if effect in ["increase", "inc", "i"]:
        row["project_index"] = int(row.get("project_index", 0)) + 1
    row["project_count"] = get_count(project)
    stats.update(row, doc_ids=[stats.all()[0].doc_id])


def event_count(effect):
    _ensure_stats_doc()
    row = stats.all()[0]
    if effect in ["increase", "inc", "i"]:
        row["event_index"] = int(row.get("event_index", 0)) + 1
    row["event_count"] = get_count(event)
    stats.update(row, doc_ids=[stats.all()[0].doc_id])


def story_count(effect):
    _ensure_stats_doc()
    row = stats.all()[0]
    if effect in ["increase", "inc", "i"]:
        row["story_index"] = int(row.get("story_index", 0)) + 1
    row["story_count"] = get_count(story)
    stats.update(row, doc_ids=[stats.all()[0].doc_id])


def entity_count(effect):
    _ensure_stats_doc()
    row = stats.all()[0]
    if effect in ["increase", "inc", "i"]:
        row["entity_index"] = int(row.get("entity_index", 0)) + 1
    row["entity_count"] = get_count(entity)
    stats.update(row, doc_ids=[stats.all()[0].doc_id])


def fact_count(effect):
    _ensure_stats_doc()
    row = stats.all()[0]
    if effect in ["increase", "inc", "i"]:
        row["fact_index"] = int(row.get("fact_index", 0)) + 1
    row["fact_count"] = get_count(fact)
    stats.update(row, doc_ids=[stats.all()[0].doc_id])


def review_count(effect):
    _ensure_stats_doc()
    row = stats.all()[0]
    if effect in ["increase", "inc", "i"]:
        row["review_index"] = int(row.get("review_index", 0)) + 1
    row["review_count"] = get_count(review_session)
    stats.update(row, doc_ids=[stats.all()[0].doc_id])


def get_count(table):
    return len(table.all())


def get_all_stats():
    _ensure_stats_doc()
    return stats.all()


def id_generator(prefix, table):
    rows = str(get_count(table) + 1)
    return prefix + ("0" * (max(0, 4 - len(rows))) + rows)