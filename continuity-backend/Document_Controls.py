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
def _entity_status(doc: Dict[str, Any]) -> str:
    return (doc.get("status") or "active").strip().lower()


def _entity_type(payload: Dict[str, Any]) -> str:
    return (payload.get("entityType") or payload.get("type") or "concept").strip()


def _hydrate_entity(doc: Dict[str, Any]) -> Dict[str, Any]:
    hydrated = dict(doc)
    hydrated["status"] = _entity_status(hydrated)
    hydrated["aliases"] = _safe_unique(list(hydrated.get("aliases", [])))
    hydrated["story_ids"] = _safe_unique(list(hydrated.get("story_ids", [])))
    hydrated["fact_ids"] = _safe_unique(list(hydrated.get("fact_ids", [])))
    hydrated.setdefault("deleted_at", None)
    hydrated.setdefault("merged_into", None)
    hydrated.setdefault("redirect_to", None)
    hydrated.setdefault("sync_status", "pending")
    hydrated.setdefault("sync_message", None)
    return hydrated


def _normalized_aliases(entity_row: Dict[str, Any]) -> List[str]:
    return [_normalize_name(alias) for alias in entity_row.get("aliases", []) or [] if alias]


def _matches_entity_name(entity_row: Dict[str, Any], name: str) -> bool:
    candidate = _normalize_name(name)
    canonical = entity_row.get("normalized_name", "")
    aliases = _normalized_aliases(entity_row)

    if candidate == canonical or candidate in aliases:
        return True

    canonical_parts = [part for part in canonical.split() if part]
    candidate_parts = [part for part in candidate.split() if part]
    if canonical_parts and candidate_parts and canonical_parts[-1] == candidate_parts[-1]:
        return True

    return any(candidate in value or value in candidate for value in ([canonical] + aliases) if value)


def _score_match_for_label(label: str, context: str) -> float:
    norm_label = _normalize_name(label)
    if not norm_label or not context:
        return 0.0

    if norm_label in context:
        return 1.0

    label_parts = [part for part in norm_label.split() if part]
    context_parts = [part for part in context.split() if part]
    if not label_parts or not context_parts:
        return 0.0

    overlap = len(set(label_parts) & set(context_parts))
    overlap_score = overlap / max(1, len(set(label_parts)))
    surname_score = 0.0
    if len(label_parts) >= 2 and label_parts[-1] in context_parts:
        surname_score = 0.75
    return max(overlap_score, surname_score)


def _match_metadata_for_fact(project_id: str, entity_id: str, fact_text: str) -> Dict[str, Any]:
    context = _normalize_name(fact_text)
    rows = [row for row in entity.search(Query().project_id == project_id) if _entity_status(row) == "active"]
    scored: List[Dict[str, Any]] = []
    for row in rows:
        labels = [row.get("name", "")] + list(row.get("aliases", []) or [])
        score = 0.0
        for label in labels:
            score = max(score, _score_match_for_label(label, context))
        if score <= 0:
            continue
        scored.append(
            {
                "entity_id": row.get("id"),
                "entity_name": row.get("name"),
                "score": round(min(1.0, score), 3),
            }
        )

    scored.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    candidates = scored[:3]
    top_score = candidates[0]["score"] if candidates else 0.0
    second_score = candidates[1]["score"] if len(candidates) > 1 else 0.0
    ambiguous = len(candidates) > 1 and top_score < 0.9 and abs(top_score - second_score) <= 0.12
    chosen = next((item for item in candidates if item.get("entity_id") == entity_id), None)
    confidence = chosen.get("score", top_score) if chosen else top_score
    return {
        "entity_match_confidence": confidence,
        "entity_match_ambiguous": ambiguous,
        "entity_match_candidates": candidates,
        "entity_assignment_confirmed": not ambiguous,
        "entity_assignment_method": "auto",
    }


def _find_active_entity(project_id: str, name: str, entity_type: str) -> Optional[Dict[str, Any]]:
    rows = entity.search(Query().project_id == project_id)
    for row in rows:
        if _entity_status(row) != "active":
            continue
        if row.get("entityType") == entity_type and _matches_entity_name(row, name):
            return row
    return None


def create_entity(project_id: str, entity_payload: Dict[str, Any], status: str = "active") -> Dict[str, Any]:
    _ensure_stats_doc()
    name = (entity_payload.get("name") or "").strip()
    if not name:
        raise ValueError("Entity name is required")

    normalized_name = _normalize_name(name)
    entity_type = _entity_type(entity_payload)

    if status == "active":
        suggested = entity.search(
            (Query().project_id == project_id)
            & (Query().entityType == entity_type)
            & (Query().normalized_name == normalized_name)
            & (Query().status == "suggested")
        )
        if suggested:
            doc = suggested[0]
            doc["status"] = "active"
            doc["name"] = name
            doc["normalized_name"] = normalized_name
            doc["aliases"] = _safe_unique(list(doc.get("aliases", [])) + list(entity_payload.get("aliases") or []))
            doc["story_ids"] = _safe_unique(list(doc.get("story_ids", [])) + list(entity_payload.get("story_ids") or []))
            doc["confidence"] = max(float(doc.get("confidence", 0.0)), float(entity_payload.get("confidence", 1.0)))
            doc["description"] = entity_payload.get("description", doc.get("description"))
            doc["notes"] = entity_payload.get("notes", doc.get("notes"))
            doc["sync_status"] = "pending"
            doc["sync_message"] = None
            doc["updated_at"] = _now_ts()
            entity.update(doc, Query().id == doc["id"])
            return _hydrate_entity(doc)

    now = _now_ts()
    doc = {
        "id": id_generator(ENTITY_PRE, entity),
        "project_id": project_id,
        "story_ids": _safe_unique(list(entity_payload.get("story_ids") or [])),
        "entityType": entity_type,
        "name": name,
        "normalized_name": normalized_name,
        "aliases": _safe_unique(list(entity_payload.get("aliases") or [])),
        "fact_ids": _safe_unique(list(entity_payload.get("fact_ids") or [])),
        "firstMentionedAt": entity_payload.get("firstMentionedAt"),
        "lastUpdatedAt": entity_payload.get("lastUpdatedAt"),
        "version": int(entity_payload.get("version", 1)),
        "confidence": float(entity_payload.get("confidence", 1.0)),
        "description": entity_payload.get("description"),
        "notes": entity_payload.get("notes"),
        "status": status,
        "deleted_at": None,
        "merged_into": None,
        "redirect_to": None,
        "sync_status": entity_payload.get("sync_status", "pending"),
        "sync_message": entity_payload.get("sync_message"),
        "created_at": now,
        "updated_at": now,
    }
    entity.insert(doc)
    entity_count("increase")
    return doc


def update_entity(entity_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    doc = entity.get(Query().id == entity_id)
    if not doc:
        return None

    now = _now_ts()
    current_name = doc.get("name") or ""
    next_name = (updates.get("name") or current_name).strip()
    next_type = _entity_type(updates) if ("entityType" in updates or "type" in updates) else (doc.get("entityType") or "concept")

    if next_name and next_name != current_name:
        doc["aliases"] = _safe_unique(list(doc.get("aliases", [])) + [current_name])
        doc["name"] = next_name
        doc["normalized_name"] = _normalize_name(next_name)

    doc["entityType"] = next_type
    if "aliases" in updates:
        doc["aliases"] = _safe_unique(list(doc.get("aliases", [])) + list(updates.get("aliases") or []))
    if "story_ids" in updates:
        doc["story_ids"] = _safe_unique(list(doc.get("story_ids", [])) + list(updates.get("story_ids") or []))
    if "description" in updates:
        doc["description"] = updates.get("description")
    if "notes" in updates:
        doc["notes"] = updates.get("notes")
    if "confidence" in updates:
        doc["confidence"] = float(updates.get("confidence") or 0.0)

    doc["version"] = int(doc.get("version", 1)) + 1
    doc["sync_status"] = "pending"
    doc["sync_message"] = None
    doc["updated_at"] = now
    entity.update(doc, Query().id == entity_id)
    return _hydrate_entity(doc)


def soft_delete_entity(entity_id: str) -> Optional[Dict[str, Any]]:
    doc = entity.get(Query().id == entity_id)
    if not doc:
        return None

    now = _now_ts()
    doc["status"] = "deleted"
    doc["deleted_at"] = now
    doc["sync_status"] = "pending"
    doc["sync_message"] = None
    doc["updated_at"] = now
    entity.update(doc, Query().id == entity_id)
    return _hydrate_entity(doc)


def merge_entities(project_id: str, source_entity_id: str, target_entity_id: str) -> Optional[Dict[str, Any]]:
    source = entity.get(Query().id == source_entity_id)
    target = entity.get(Query().id == target_entity_id)
    if not source or not target:
        return None
    if source.get("project_id") != project_id or target.get("project_id") != project_id:
        raise ValueError("Entities must belong to the same project")

    now = _now_ts()
    target["aliases"] = _safe_unique(list(target.get("aliases", [])) + [source.get("name", "")])
    target["story_ids"] = _safe_unique(list(target.get("story_ids", [])) + list(source.get("story_ids", [])))
    target["fact_ids"] = _safe_unique(list(target.get("fact_ids", [])) + list(source.get("fact_ids", [])))
    target["version"] = int(target.get("version", 1)) + 1
    target["sync_status"] = "pending"
    target["sync_message"] = None
    target["updated_at"] = now
    entity.update(target, Query().id == target_entity_id)

    for fid in source.get("fact_ids", []):
        fact_row = fact.get(Query().id == fid)
        if not fact_row:
            continue
        fact_row["entity_id"] = target_entity_id
        fact_row["updated_at"] = now
        fact.update(fact_row, Query().id == fid)

    source["status"] = "merged"
    source["merged_into"] = target_entity_id
    source["redirect_to"] = target_entity_id
    source["deleted_at"] = now
    source["sync_status"] = "pending"
    source["sync_message"] = None
    source["updated_at"] = now
    entity.update(source, Query().id == source_entity_id)
    return _hydrate_entity(target)


def resolve_entity(entity_id: str) -> Optional[Dict[str, Any]]:
    visited = set()
    current_id = entity_id
    while current_id and current_id not in visited:
        visited.add(current_id)
        doc = entity.get(Query().id == current_id)
        if not doc:
            return None
        redirect_to = doc.get("redirect_to")
        if redirect_to:
            current_id = redirect_to
            continue
        return _hydrate_entity(doc)
    return None


def search_entities(project_id: str, query: str, include_deleted: bool = False) -> List[Dict[str, Any]]:
    normalized = _normalize_name(query)
    rows = entity.search(Query().project_id == project_id)
    results: List[Dict[str, Any]] = []
    for row in rows:
        if not include_deleted and _entity_status(row) in {"deleted", "merged"}:
            continue
        haystack = [row.get("normalized_name", "")] + [_normalize_name(alias) for alias in row.get("aliases", []) or []]
        if any(normalized in item or item in normalized for item in haystack if item):
            results.append(_hydrate_entity(row))
    return results


def upsert_entity(project_id: str, story_id: str, entity_payload: Dict[str, Any], canonical: bool = True) -> Dict[str, Any]:
    _ensure_stats_doc()
    name = (entity_payload.get("name") or "").strip()
    if not name:
        raise ValueError("Entity name is required")

    entity_type = _entity_type(entity_payload)
    existing = _find_active_entity(project_id, name, entity_type)
    if existing:
        existing["aliases"] = _safe_unique(list(existing.get("aliases", [])) + list(entity_payload.get("aliases") or []))
        existing["story_ids"] = _safe_unique(list(existing.get("story_ids", [])) + ([story_id] if story_id else []))
        existing["confidence"] = max(float(existing.get("confidence", 0.0)), float(entity_payload.get("confidence", 0.0)))
        existing["version"] = int(existing.get("version", 1)) + 1
        existing["updated_at"] = _now_ts()
        existing["lastUpdatedAt"] = entity_payload.get("lastUpdatedAt")
        existing["sync_status"] = "pending"
        existing["sync_message"] = None
        entity.update(existing, Query().id == existing["id"])
        return _hydrate_entity(existing)

    suggested = entity.search(
        (Query().project_id == project_id)
        & (Query().entityType == entity_type)
        & (Query().normalized_name == _normalize_name(name))
        & (Query().status == "suggested")
    )
    if suggested:
        doc = suggested[0]
        doc["aliases"] = _safe_unique(list(doc.get("aliases", [])) + list(entity_payload.get("aliases") or []))
        doc["story_ids"] = _safe_unique(list(doc.get("story_ids", [])) + ([story_id] if story_id else []))
        doc["confidence"] = max(float(doc.get("confidence", 0.0)), float(entity_payload.get("confidence", 0.0)))
        doc["version"] = int(doc.get("version", 1)) + 1
        doc["updated_at"] = _now_ts()
        doc["lastUpdatedAt"] = entity_payload.get("lastUpdatedAt")
        doc["sync_status"] = "pending"
        doc["sync_message"] = None
        entity.update(doc, Query().id == doc["id"])
        return _hydrate_entity(doc)

    if not canonical:
        return create_entity(
            project_id,
            {
                **entity_payload,
                "story_ids": [story_id] if story_id else entity_payload.get("story_ids", []),
                "entityType": entity_type,
            },
            status="suggested",
        )

    return create_entity(
        project_id,
        {
            **entity_payload,
            "story_ids": [story_id] if story_id else entity_payload.get("story_ids", []),
            "entityType": entity_type,
        },
        status="active",
    )


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
        if "atomicity_score" in fact_payload:
            existing["atomicity_score"] = float(fact_payload.get("atomicity_score") or 0.0)
        if "schema_alignment_score" in fact_payload:
            existing["schema_alignment_score"] = float(fact_payload.get("schema_alignment_score") or 0.0)
        if "needs_review" in fact_payload:
            existing["needs_review"] = bool(fact_payload.get("needs_review"))
        if "schema_version" in fact_payload:
            existing["schema_version"] = fact_payload.get("schema_version")
        if "entity_assignment_confirmed" in fact_payload:
            existing["entity_assignment_confirmed"] = bool(fact_payload.get("entity_assignment_confirmed"))
        existing["updated_at"] = now
        fact.update(existing, Query().id == existing["id"])
        return existing

    match_meta = _match_metadata_for_fact(project_id, entity_id, fact_payload.get("sourceText") or text)

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
        "schema_version": fact_payload.get("schema_version"),
        "atomicity_score": float(fact_payload.get("atomicity_score") or 0.0),
        "schema_alignment_score": float(fact_payload.get("schema_alignment_score") or 0.0),
        "needs_review": bool(fact_payload.get("needs_review", False)),
        "conflict_key": _infer_conflict_key(text, entity_id),
        "conflict_group_id": None,
        "contradicts": [],
        "reviewed_by": None,
        "reviewed_at": None,
        "decision_reason": None,
        "entity_match_confidence": match_meta["entity_match_confidence"],
        "entity_match_ambiguous": match_meta["entity_match_ambiguous"],
        "entity_match_candidates": match_meta["entity_match_candidates"],
        "entity_assignment_confirmed": match_meta["entity_assignment_confirmed"],
        "entity_assignment_method": match_meta["entity_assignment_method"],
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
        ent = upsert_entity(project_id, story_id, item, canonical=False)
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
    confirm_assignment: bool = False,
    confirm_low_quality: bool = False,
) -> Optional[Dict[str, Any]]:
    if status not in {"pending", "approved", "rejected"}:
        raise ValueError("Invalid fact status")

    doc = fact.get(Query().id == fact_id)
    if not doc:
        return None

    if status in {"approved", "rejected"}:
        is_ambiguous = bool(doc.get("entity_match_ambiguous"))
        is_confirmed = bool(doc.get("entity_assignment_confirmed"))
        if is_ambiguous and not is_confirmed and not confirm_assignment:
            raise ValueError("Ambiguous entity assignment. Reassign or confirm assignment before review.")
        if confirm_assignment:
            doc["entity_assignment_confirmed"] = True

    if status == "approved":
        needs_review = bool(doc.get("needs_review"))
        atomicity_score = float(doc.get("atomicity_score") or 0.0)
        schema_alignment_score = float(doc.get("schema_alignment_score") or 0.0)
        if (needs_review or atomicity_score < 0.75 or schema_alignment_score < 0.55) and not confirm_low_quality:
            raise ValueError("Low-quality fact requires explicit confirmation before approval.")

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
        if not row:
            continue
        confidence = row.get("entity_match_confidence")
        if isinstance(confidence, (int, float)) and confidence <= 0:
            continue
        if row.get("status") == "pending":
            unresolved.append(fid)
            continue
        if row.get("entity_match_ambiguous") and not row.get("entity_assignment_confirmed"):
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


def get_review_sessions_by_project(project_id: str) -> List[Dict[str, Any]]:
    rows = review_session.search(Query().project_id == project_id)
    return sorted(rows, key=lambda row: row.get("created_at", 0), reverse=True)


def get_retryable_review_sessions(project_id: str) -> List[Dict[str, Any]]:
    rows = get_review_sessions_by_project(project_id)
    return [
        row
        for row in rows
        if row.get("status") == "submitted" and row.get("syncStatus") in {"pending", "error"}
    ]


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
        if _entity_status(e) in {"deleted", "merged"}:
            continue
        ec = dict(e)
        ec["facts"] = fact.search(Query().entity_id == e["id"])
        output.append(ec)
    return output


def get_entities_by_project(project_id: str) -> List[Dict[str, Any]]:
    rows = entity.search(Query().project_id == project_id)
    output: List[Dict[str, Any]] = []
    for e in rows:
        if _entity_status(e) in {"deleted", "merged", "suggested"}:
            continue
        ec = dict(e)
        ec["facts"] = fact.search(Query().entity_id == e["id"])
        output.append(ec)
    return output


def get_suggested_entities_by_project(project_id: str) -> List[Dict[str, Any]]:
    rows = entity.search((Query().project_id == project_id) & (Query().status == "suggested"))
    output: List[Dict[str, Any]] = []
    for e in rows:
        ec = dict(e)
        ec["facts"] = fact.search(Query().entity_id == e["id"])
        output.append(ec)
    return output


def promote_entity(entity_id: str) -> Optional[Dict[str, Any]]:
    doc = entity.get(Query().id == entity_id)
    if not doc:
        return None
    doc["status"] = "active"
    doc["sync_status"] = "pending"
    doc["sync_message"] = None
    doc["updated_at"] = _now_ts()
    entity.update(doc, Query().id == entity_id)
    return _hydrate_entity(doc)


def get_entity_facts(entity_id: str) -> List[Dict[str, Any]]:
    return fact.search(Query().entity_id == entity_id)


def reassign_fact_entity(fact_id: str, new_entity_id: str) -> Optional[Dict[str, Any]]:
    fact_row = fact.get(Query().id == fact_id)
    new_entity = entity.get(Query().id == new_entity_id)
    if not fact_row or not new_entity:
        return None

    old_entity_id = fact_row.get("entity_id")
    now = _now_ts()

    fact_row["entity_id"] = new_entity_id
    fact_row["entity_assignment_method"] = "manual"
    fact_row["entity_assignment_confirmed"] = True
    fact_row["entity_match_ambiguous"] = False
    fact_row["entity_match_confidence"] = 1.0
    fact_row["entity_match_candidates"] = [
        {
            "entity_id": new_entity.get("id"),
            "entity_name": new_entity.get("name"),
            "score": 1.0,
        }
    ]
    fact_row["updated_at"] = now
    fact.update(fact_row, Query().id == fact_id)

    if old_entity_id and old_entity_id != new_entity_id:
        old_entity = entity.get(Query().id == old_entity_id)
        if old_entity:
            old_entity["fact_ids"] = [fid for fid in old_entity.get("fact_ids", []) if fid != fact_id]
            old_entity["sync_status"] = "pending"
            old_entity["sync_message"] = None
            old_entity["updated_at"] = now
            entity.update(old_entity, Query().id == old_entity_id)

    new_entity["fact_ids"] = _safe_unique(list(new_entity.get("fact_ids", [])) + [fact_id])
    new_entity["sync_status"] = "pending"
    new_entity["sync_message"] = None
    new_entity["updated_at"] = now
    entity.update(new_entity, Query().id == new_entity_id)
    return fact_row


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
        story_ids = sorted(_safe_unique([v.get("story_id") for v in values if v.get("story_id")]))
        conflicts.append(
            {
                "conflictGroupId": gid,
                "factCount": len(values),
                "facts": values,
                "storyIds": story_ids,
                "conflictType": "intra-story" if len(story_ids) <= 1 else "inter-story",
                "resolved": all(v.get("status") != "pending" for v in values),
            }
        )
    return conflicts


def get_story_conflicts(story_id: str, include_cross_story: bool = True) -> List[Dict[str, Any]]:
    sdoc = get_story(story_id)
    if not sdoc:
        return []

    all_conflicts = get_project_conflicts(sdoc.get("project_id"))
    scoped: List[Dict[str, Any]] = []
    for conflict in all_conflicts:
        facts = conflict.get("facts", [])
        same_story = [f for f in facts if f.get("story_id") == story_id]
        if include_cross_story:
            if same_story:
                scoped.append(conflict)
            continue
        if len(same_story) >= 2:
            scoped.append(
                {
                    **conflict,
                    "factCount": len(same_story),
                    "facts": same_story,
                    "storyIds": [story_id],
                    "conflictType": "intra-story",
                    "resolved": all(v.get("status") != "pending" for v in same_story),
                }
            )
    return scoped


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