import logging
import re
from typing import List, Dict, Any, Tuple
import torch  # type: ignore
from transformers import pipeline  # type: ignore
from config.settings import MODELS_CACHE_DIR, NER_CONFIDENCE_THRESHOLD

logger = logging.getLogger(__name__)

_PERSON_PARTICLES = {"van", "von", "de", "del", "da", "di", "la", "le"}

def _split_person_name(name: str):
    toks = [t for t in re.split(r"\s+", name.strip()) if t]
    if not toks:
        return [], "", False
    return toks, toks[-1].lower(), any(t.lower() in _PERSON_PARTICLES for t in toks[:-1])

def _is_word_boundary(text: str, start: int, end: int) -> bool:
    return (start == 0 or not text[start - 1].isalnum()) and (end == len(text) or not text[end].isalnum())

def _strip_possessive(name: str) -> str:
    return re.sub(r"[’']s\b", "", name.strip())

def _normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", _strip_possessive(name)).strip().lower()


class NERExtractor:
    """Token-classification NER + de-dup + surname merge."""

    def __init__(self, model_name: str = "dslim/bert-base-NER"):
        self.model_name = model_name
        logger.info("Loading NER model: %s (cache: %s)", model_name, MODELS_CACHE_DIR)
        try:
            # Load model and tokenizer with cache_dir, then pass to pipeline
            from transformers import AutoModelForTokenClassification, AutoTokenizer

            model = AutoModelForTokenClassification.from_pretrained(
                model_name,
                cache_dir=MODELS_CACHE_DIR
            )
            tokenizer = AutoTokenizer.from_pretrained(
                model_name,
                cache_dir=MODELS_CACHE_DIR
            )

            self.ner_pipeline = pipeline(
                "ner",
                model=model,
                tokenizer=tokenizer,
                aggregation_strategy="simple",
                device=0 if torch.cuda.is_available() else -1,
            )
            logger.info("[OK] NER model loaded from cache")
        except Exception as e:
            logger.error("Failed to load NER model: %s", e)
            raise

        self.label_mapping = {
            "PER": "character", "PERSON": "character",
            "LOC": "location", "LOCATION": "location", "GPE": "location", "FACILITY": "location",
            "ORG": "organization", "ORGANIZATION": "organization",
            "PRODUCT": "object", "ARTIFACT": "object", "WEAPON": "object",
            "EVENT": "event",
            "MISC": "concept", "NORP": "concept", "LAW": "concept", "LANGUAGE": "concept", "WORK_OF_ART": "concept",
        }

    async def extract_entities(self, text: str, time_id: str = "t_000") -> List[Dict[str, Any]]:
        try:
            logger.info("Extracting entities from %d chars...", len(text))
            ner_results = self.ner_pipeline(text)
            logger.info("NER found %d entities", len(ner_results))

            # Debug: Log raw NER results
            for r in ner_results:
                logger.debug("NER raw: entity='%s', type=%s, score=%.3f",
                           r.get("word", ""), r.get("entity_group", ""), r.get("score", 0.0))

            by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
            entity_counter = 1

            for r in ner_results:
                start, end = r.get("start"), r.get("end")
                entity_name = (text[start:end].strip() if start is not None and end is not None
                               else r.get("word", "").replace("##", "").strip())
                if len(entity_name) < 2:
                    continue
                if len(entity_name.replace(".", "").replace(",", "").replace("-", "").strip()) < 2:
                    continue
                if start is not None and end is not None and not _is_word_boundary(text, start, end):
                    logger.debug("Reject non-boundary span '%s' [%d:%d]", entity_name, start, end)
                    continue

                normalized = _normalize_name(entity_name)
                if len(normalized) == 1 and normalized.isalpha():
                    continue

                entity_type = self.label_mapping.get(r.get("entity_group", "MISC"), "concept")
                confidence = float(r.get("score", 0.0))
                # Filter by confidence threshold (configurable via NER_CONFIDENCE_THRESHOLD)
                if confidence < NER_CONFIDENCE_THRESHOLD:
                    logger.debug("Filtered entity '%s' (type=%s, confidence=%.3f < %.2f)",
                                entity_name, entity_type, confidence, NER_CONFIDENCE_THRESHOLD)
                    continue

                key = (entity_type, normalized)
                surface = _strip_possessive(entity_name).strip()
                if key not in by_key:
                    by_key[key] = {
                        "id": f"ent_{entity_counter:06d}",
                        "entityType": entity_type,
                        "name": surface,
                        "aliases": [],
                        "facts": [],
                        "firstMentionedAt": time_id,
                        "lastUpdatedAt": time_id,
                        "version": 1,
                        "confidence": confidence,
                        "_seen_forms": {surface},
                    }
                    entity_counter += 1
                else:
                    ent = by_key[key]
                    ent["_seen_forms"].add(surface)
                    if confidence > ent["confidence"]:
                        ent["confidence"] = confidence
                    ent["version"] = ent.get("version", 1) + 1
                    ent["lastUpdatedAt"] = time_id

            # Surname merge: prefer the longest full name per last name
            surname_index: Dict[str, Tuple[str, str]] = {}
            for k, ent in list(by_key.items()):
                if ent["entityType"] != "character":
                    continue
                _, last, _ = _split_person_name(ent["name"])
                if not last:
                    continue
                prev = surname_index.get(last)
                if not prev:
                    surname_index[last] = k
                else:
                    prev_ent = by_key[prev]
                    if len(prev_ent["name"].split()) < len(ent["name"].split()):
                        surname_index[last] = k

            # Merge single-surname entries into canonical full form
            for k, ent in list(by_key.items()):
                if ent["entityType"] != "character":
                    continue
                _, last, _ = _split_person_name(ent["name"])
                if not last:
                    continue
                canonical_key = surname_index.get(last)
                if not canonical_key or canonical_key == k:
                    continue
                canon = by_key[canonical_key]
                canon["_seen_forms"].update(ent.get("_seen_forms", set()) | {ent["name"]})
                canon["version"] = canon.get("version", 1) + 1
                canon["lastUpdatedAt"] = time_id
                if ent["confidence"] > canon["confidence"]:
                    canon["confidence"] = ent["confidence"]
                by_key.pop(k, None)

            # Finalize
            entities: List[Dict[str, Any]] = []
            for ent in by_key.values():
                primary = ent["name"]
                ent["aliases"] = sorted({a for a in ent["_seen_forms"] if a and a != primary})
                ent.pop("_seen_forms", None)
                entities.append(ent)

            logger.info("Successfully extracted %d entities (NER)", len(entities))
            return entities

        except Exception as e:
            logger.error("Error extracting entities: %s", e, exc_info=True)
            return []

    def get_supported_entity_types(self) -> List[str]:
        return list(set(self.label_mapping.values()))


class HybridNERExtractor(NERExtractor):
    """Adds story-pattern matches to base NER and de-dupes."""

    def __init__(self, model_name: str = "dslim/bert-base-NER"):
        super().__init__(model_name)
        self.story_patterns = {
            "character": [
                r"\bthe\s+(lighthouse\s+keeper|king|queen|prince|princess|wizard|knight|merchant|captain|doctor|professor|detective|priest|monk|sailor|guard|soldier)\b",
                r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:said|asked|replied|thought|wondered|remembered|knew|felt)\b",
            ],
            "object": [
                r"\b(Sacred|Magic|Ancient|Legendary|Cursed)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b",
                r"\b([A-Z][a-z]+)\s+(Sword|Shield|Staff|Amulet|Ring|Crown|Orb)\b",
                r"\b(?:the|a)\s+(letter|map|key|book|scroll|diary|journal|compass|lantern|mirror)\b",
            ],
            "location": [
                r"\b([A-Z][a-z]+)\s+(Kingdom|Castle|Tower|Forest|Mountain|City|Village|Island|Bay|Harbor)\b",
                r"\b(?:the|a)\s+(lighthouse|tavern|inn|church|cathedral|library|market|square|bridge|gate|harbor|port)\b",
            ],
            "event": [
                r"\b(Battle|War|Siege|Festival|Ceremony)\s+of\s+([A-Z][a-z]+)\b",
            ],
        }

    async def extract_entities(self, text: str, time_id: str = "t_000") -> List[Dict[str, Any]]:
        import re as _re
        entities = await super().extract_entities(text, time_id)
        nameset = {(e["entityType"], _normalize_name(e["name"])) for e in entities}
        entity_counter = len(entities) + 1

        for entity_type, patterns in self.story_patterns.items():
            for pattern in patterns:
                for match in _re.finditer(pattern, text):
                    name = match.group(0).strip()
                    key = (entity_type, _normalize_name(name))
                    if key in nameset or len(key[1]) < 2:
                        continue
                    entities.append({
                        "id": f"ent_{entity_counter:06d}",
                        "entityType": entity_type,
                        "name": _strip_possessive(name),
                        "aliases": [],
                        "facts": [],
                        "firstMentionedAt": time_id,
                        "lastUpdatedAt": time_id,
                        "version": 1,
                        "confidence": 0.90,
                    })
                    nameset.add(key)
                    entity_counter += 1
                    logger.debug("Pattern match: %s (%s)", name, entity_type)

        return entities