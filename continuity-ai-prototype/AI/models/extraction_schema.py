from __future__ import annotations

from typing import Dict, List, TypedDict


DEFAULT_SCHEMA_VERSION = "two-pass-v1"


class BaseEntitySchema(TypedDict, total=False):
    schema_version: str
    entityType: str
    name: str
    aliases: List[str]
    description: str
    atomic_facts_only: bool
    max_facts: int
    fact_focus: List[str]


class CharacterSchema(BaseEntitySchema, total=False):
    relationship_focus: List[str]


class LocationSchema(BaseEntitySchema, total=False):
    geography_focus: List[str]


class TriagedEntity(TypedDict, total=False):
    id: str
    entityType: str
    name: str
    aliases: List[str]
    schema_version: str
    schema: BaseEntitySchema
    triageScore: float
    matchedMentions: List[str]
    atomicFactsOnly: bool


SCHEMA_REGISTRY: Dict[str, BaseEntitySchema] = {
    "character": {
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "entityType": "character",
        "atomic_facts_only": True,
        "max_facts": 3,
        "fact_focus": ["identity", "actions", "relationships"],
    },
    "location": {
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "entityType": "location",
        "atomic_facts_only": True,
        "max_facts": 2,
        "fact_focus": ["place_identity", "setting_details", "spatial_relationships"],
    },
    "organization": {
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "entityType": "organization",
        "atomic_facts_only": True,
        "max_facts": 3,
        "fact_focus": ["purpose", "membership", "role"],
    },
    "event": {
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "entityType": "event",
        "atomic_facts_only": True,
        "max_facts": 3,
        "fact_focus": ["participants", "sequence", "outcome"],
    },
    "concept": {
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "entityType": "concept",
        "atomic_facts_only": True,
        "max_facts": 2,
        "fact_focus": ["definition", "usage"],
    },
    "object": {
        "schema_version": DEFAULT_SCHEMA_VERSION,
        "entityType": "object",
        "atomic_facts_only": True,
        "max_facts": 2,
        "fact_focus": ["properties", "usage", "ownership"],
    },
}


def schema_for_entity(entity_type: str) -> BaseEntitySchema:
    return dict(SCHEMA_REGISTRY.get(entity_type, SCHEMA_REGISTRY["concept"]))


def normalize_aliases(name: str, aliases: List[str] | None) -> List[str]:
    seen: Dict[str, None] = {}
    for value in [name, *(aliases or [])]:
        cleaned = " ".join(str(value or "").split()).strip()
        if cleaned:
            seen.setdefault(cleaned, None)
    return list(seen.keys())