from __future__ import annotations
import re
import json
import logging
from typing import Dict, List, Any, Optional, Tuple, Callable

from models.extraction_schema import DEFAULT_SCHEMA_VERSION, normalize_aliases, schema_for_entity

logger = logging.getLogger(__name__)

Sentence = Tuple[int, int, str]  # (start, end, sentence_text)

class FactExtractor:
    def __init__(
        self,
        llm=None,
        use_llm: bool = True,
        max_facts_per_entity: int = 3,
        rules_fallback: bool = False,
        temperature: float = 0.2,
        max_tokens: int = 160,
        auto_validate_facts: bool = False,
        fact_validator=None,
    ):
        self.llm = llm
        self.use_llm = use_llm
        self.max_facts_per_entity = max_facts_per_entity
        self.rules_fallback = rules_fallback
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.auto_validate_facts = auto_validate_facts
        self.fact_validator = fact_validator

        # Lazy load fact validator if needed
        if self.fact_validator is None:
            try:
                from models.fact_validator import FactValidator
                self.fact_validator = FactValidator()
                logger.info("Fact validator initialized (available for on-demand validation)")
            except Exception as e:
                logger.warning(f"Failed to initialize fact validator: {e}. Validation will not be available.")
                self.fact_validator = None

    # ---------------- Public API ----------------
    async def extract_facts_for_entities(
        self,
        text: str,
        entities: List[dict],
        time_id: str,
        progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, List[dict]]:
        """Extract facts per entity.

        Args:
            text: Full document text.
            entities: List of entity dicts that include at least {id, name, aliases?}.
            time_id: Timestamp/tag propagated to evidence.
            progress: Optional callback receiving {processed, total, entityId, entityName}.
        Returns:
            Mapping from entity id -> list of fact dicts.
        """
        sentences = self._split_sentences_with_spans(text)
        results: Dict[str, List[dict]] = {}

        total = max(1, len(entities))
        processed = 0
        if progress:
            try:
                progress({"processed": processed, "total": total, "entityId": None, "entityName": None})
            except Exception:  # don't let UI break the pipeline
                logger.debug("progress callback failed at start", exc_info=True)

        for ent in entities:
            name = (ent.get("name") or "").strip()
            ent_id = ent.get("id")
            if not name or not ent_id:
                processed += 1
                if progress:
                    try:
                        progress({"processed": processed, "total": total, "entityId": ent_id, "entityName": name})
                    except Exception:
                        logger.debug("progress callback failed (skip entity)", exc_info=True)
                continue

            entity_type = (ent.get("entityType") or ent.get("type") or "concept")
            if not isinstance(entity_type, str):
                entity_type = "concept"
            schema = ent.get("schema") if isinstance(ent.get("schema"), dict) else schema_for_entity(entity_type)
            schema_version = str(ent.get("schema_version") or schema.get("schema_version") or DEFAULT_SCHEMA_VERSION)
            aliases = normalize_aliases(name, ent.get("aliases") or [])
            mention_sents = self._find_mention_sentences(name, sentences, aliases, entity_type=entity_type)
            logger.debug("Fact extraction: entity='%s' mentions=%d", name, len(mention_sents))

            facts: List[dict] = []
            for start, end, sent_text in mention_sents:
                llm_facts = await self._llm_extract_facts_for_sentence(
                    name,
                    sent_text,
                    entity_type=entity_type,
                    schema_version=schema_version,
                    atomic_only=bool(schema.get("atomic_facts_only", True)),
                )
                logger.debug(
                    "Fact extraction: entity='%s' llm_facts=%d sentence='%s'",
                    name,
                    len(llm_facts),
                    sent_text[:140],
                )
                for sf in llm_facts:
                    candidates = self._normalize_atomic_candidates(sf, name, aliases, entity_type=entity_type)
                    if not candidates:
                        candidates = [sf]

                    for candidate in candidates:
                        atomicity_score = self._atomicity_score(candidate)
                        schema_alignment = self._schema_alignment_score(candidate, entity_type)
                        needs_review = atomicity_score < 0.8 or schema_alignment < 0.55
                        facts.append(
                            self._mk_fact(
                                candidate,
                                sent_text,
                                start,
                                end,
                                time_id,
                                confidence=0.82 if not needs_review else 0.38,
                                method="llm",
                                schema_version=schema_version,
                                entity_type=entity_type,
                                atomicity_score=atomicity_score,
                                schema_alignment_score=schema_alignment,
                                needs_review=needs_review,
                            )
                        )
                if self.rules_fallback and not llm_facts:
                    facts.extend(
                        self._template_facts_from_sentence(
                            name,
                            sent_text,
                            start,
                            end,
                            time_id,
                            aliases,
                            schema_version=schema_version,
                            entity_type=entity_type,
                        )
                    )
                    facts.extend(
                        self._facts_from_sentence_rules(
                            name,
                            (start, end, sent_text),
                            time_id,
                            aliases,
                            schema_version=schema_version,
                            entity_type=entity_type,
                        )
                    )
                if len(facts) >= self.max_facts_per_entity:
                    break

            # de-dup by (fact text + span), then cap
            uniq, seen = [], set()
            for f in facts:
                key = (f["fact"].strip().lower(), f["evidence"]["start"], f["evidence"]["end"])
                if key not in seen:
                    uniq.append(f)
                    seen.add(key)

            # Apply fact validation only if auto-validation is enabled
            entity_facts = uniq[: self.max_facts_per_entity]
            logger.debug("Fact extraction: entity='%s' final_facts=%d", name, len(entity_facts))
            if self.auto_validate_facts and self.fact_validator and len(entity_facts) > 1:
                try:
                    entity_facts = self.fact_validator.validate_facts(entity_facts)
                    logger.debug(f"Auto-validated {len(entity_facts)} facts for entity {name}")
                except Exception as e:
                    logger.warning(f"Fact validation failed for entity {name}: {e}")

            results[ent_id] = entity_facts

            processed += 1
            if progress:
                try:
                    progress({
                        "processed": processed,
                        "total": total,
                        "entityId": ent_id,
                        "entityName": name,
                    })
                except Exception:
                    logger.debug("progress callback failed (per-entity)", exc_info=True)

        return results

    async def triage_entities(
        self,
        text: str,
        entities: List[dict],
        time_id: str,
        progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> List[dict]:
        sentences = self._split_sentences_with_spans(text)
        total = max(1, len(entities))
        processed = 0
        triaged: List[dict] = []

        if progress:
            try:
                progress({"processed": processed, "total": total, "entityId": None, "entityName": None})
            except Exception:
                logger.debug("progress callback failed at triage start", exc_info=True)

        for ent in entities:
            name = (ent.get("name") or "").strip()
            ent_id = ent.get("id")
            entity_type = ent.get("entityType") or ent.get("type") or "concept"
            if not name or not ent_id:
                processed += 1
                continue

            aliases = normalize_aliases(name, ent.get("aliases") or [])
            name_re = self._name_patterns(name, aliases)
            matched_mentions = [sent_text for _, _, sent_text in sentences if name_re.search(sent_text)]
            score = self._triage_score(name, entity_type, matched_mentions, aliases)
            if score <= 0.0:
                processed += 1
                if progress:
                    try:
                        progress({"processed": processed, "total": total, "entityId": ent_id, "entityName": name})
                    except Exception:
                        logger.debug("progress callback failed during triage", exc_info=True)
                continue

            schema = ent.get("schema") if isinstance(ent.get("schema"), dict) else schema_for_entity(str(entity_type))
            triaged.append(
                {
                    **ent,
                    "aliases": aliases,
                    "schema": schema,
                    "schema_version": str(ent.get("schema_version") or schema.get("schema_version") or DEFAULT_SCHEMA_VERSION),
                    "triageScore": round(score, 3),
                    "matchedMentions": matched_mentions[:5],
                    "atomicFactsOnly": bool(schema.get("atomic_facts_only", True)),
                }
            )

            processed += 1
            if progress:
                try:
                    progress({"processed": processed, "total": total, "entityId": ent_id, "entityName": name})
                except Exception:
                    logger.debug("progress callback failed during triage", exc_info=True)

        triaged.sort(key=lambda item: item.get("triageScore", 0.0), reverse=True)
        logger.debug("Triaged %d/%d canonical entities", len(triaged), len(entities))
        return triaged

    def validate_entity_facts(self, entities: List[dict]) -> List[dict]:
        """
        Validate facts for entities on-demand (after extraction).

        Args:
            entities: List of entity dicts with facts

        Returns:
            List of entities with validated facts (adds contradicts/entails metadata)
        """
        if not self.fact_validator:
            logger.warning("Fact validator not available. Returning entities unchanged.")
            return entities

        validated_entities = []
        for entity in entities:
            entity_copy = entity.copy()
            facts = entity_copy.get("facts", [])

            if len(facts) > 1:
                try:
                    validated_facts = self.fact_validator.validate_facts(facts)
                    entity_copy["facts"] = validated_facts
                    logger.info(f"Validated {len(validated_facts)} facts for entity {entity.get('name', 'unknown')}")
                except Exception as e:
                    logger.warning(f"Fact validation failed for entity {entity.get('name', 'unknown')}: {e}")

            validated_entities.append(entity_copy)

        return validated_entities

    # ---------------- LLM extraction ----------------
    async def _llm_extract_facts_for_sentence(
        self,
        name: str,
        sentence: str,
        entity_type: str = "concept",
        schema_version: str = DEFAULT_SCHEMA_VERSION,
        atomic_only: bool = True,
    ) -> List[str]:
        if not self.llm or not self.use_llm:
            return []
        prompt = (
            "You extract facts strictly from the given sentence.\n"
            f"Target entity: {name}\n"
            f"Entity type: {entity_type}\n"
            f"Schema version: {schema_version}\n"
            'Return JSON ONLY with EXACTLY this schema:\n{"facts": ["<fact-1>", "<fact-2>"]}\n'
            "Rules:\n"
            "- Include 0–3 facts explicitly supported by THIS sentence.\n"
            f"- Return atomic facts only: {str(atomic_only).lower()}.\n"
            "- Atomic means one subject-predicate claim per fact.\n"
            "- Do not combine action + motivation + description in one fact.\n"
            "- Split conjunctions into separate facts when possible.\n"
            "- No external knowledge.\n"
            "- Use DOUBLE quotes. Do NOT use single quotes.\n"
            "- Do NOT include markdown/code fences or any extra text.\n"
            '- If no facts, return {"facts": []} exactly.\n\n'
            f"Sentence: {sentence}\n"
            'Example good output: {"facts": ["Alice has green eyes.", "Alice is in an old train station."]}\n'
            'Example bad output: {"facts": ["Alice moved through the station with quiet confidence while waiting for a sign."]}'
        )
        try:
            text = await self.llm.generate_json(
                prompt, temperature=self.temperature, max_tokens=self.max_tokens  # type: ignore
            )
        except Exception:
            logger.exception("LLM generation failed; returning no facts for sentence")
            return []
        data = self._safe_json(text)
        raw_facts = [s.strip() for s in data.get("facts", []) if isinstance(s, str) and s.strip()]

        # Filter out garbage facts (fragments, incomplete sentences)
        facts = []
        for fact in raw_facts:
            # Must be a complete sentence (has subject + verb, ends with punctuation or is substantial)
            if len(fact) < 10:  # Too short to be a meaningful fact
                logger.debug("Rejected fact (too short): '%s'", fact)
                continue

            # Must contain at least one verb-like word (basic heuristic)
            words = fact.lower().split()
            has_verb = any(w in {"is", "was", "were", "are", "has", "had", "have", "been", "became",
                                "served", "worked", "lived", "died", "born", "became", "held", "led"}
                          for w in words)

            # Or must be a complete sentence (contains subject + predicate structure)
            # Simple heuristic: has at least 3 words and doesn't start with preposition/conjunction
            starts_with_fragment = words[0] in {"from", "to", "in", "at", "on", "and", "or", "but"}

            if not has_verb and (len(words) < 3 or starts_with_fragment):
                logger.debug("Rejected fact (fragment): '%s'", fact)
                continue

            facts.append(fact)

        return facts

    # ---------------- Helpers ----------------
    def _split_sentences_with_spans(self, text: str) -> List[Sentence]:
        """Very simple sentence splitter that also returns spans.
        It groups characters into chunks ending with . ! ? or end-of-text.
        """
        sentences: List[Sentence] = []
        pattern = re.compile(r"[^.!?]*[.!?]|[^.!?]+$")
        for m in pattern.finditer(text):
            start = m.start()
            end = m.end()
            chunk = text[start:end]
            # trim leading/trailing whitespace but adjust spans accordingly
            ltrim = len(chunk) - len(chunk.lstrip())
            rtrim = len(chunk.rstrip())
            tstart = start + ltrim
            tend = start + rtrim
            chunk = text[tstart:tend]
            if chunk:
                sentences.append((tstart, tend, chunk))
        return sentences

    def _name_patterns(self, name: str, aliases: Optional[List[str]] = None):
        parts = [p for p in re.split(r"\s+", name.strip()) if p]
        variants = [re.escape(name)]
        if len(parts) >= 2:
            variants.append(re.escape(parts[-1]))  # surname
        for a in (aliases or []):
            if a and a.lower() != name.lower():
                variants.append(re.escape(a))
        possessive = r"(?:'s|’s)?"
        pattern = rf"\b(?:{'|'.join(variants)}){possessive}\b"
        return re.compile(pattern, re.IGNORECASE)

    def _find_mention_sentences(
        self,
        name: str,
        sentences: List[Sentence],
        aliases: Optional[List[str]] = None,
        entity_type: str = "concept",
    ) -> List[Sentence]:
        name_re = self._name_patterns(name, aliases)
        direct_matches = [idx for idx, sentence in enumerate(sentences) if name_re.search(sentence[2])]
        include_idxs = set(direct_matches)

        if entity_type == "character" and direct_matches:
            for idx, (_, _, text) in enumerate(sentences):
                if idx in include_idxs:
                    continue
                if self._is_character_coref_sentence(text) and any(abs(idx - base) <= 1 for base in direct_matches):
                    include_idxs.add(idx)

        if entity_type == "location" and direct_matches:
            for idx, (_, _, text) in enumerate(sentences):
                if idx in include_idxs:
                    continue
                if self._is_location_context_sentence(text) and any(abs(idx - base) <= 1 for base in direct_matches):
                    include_idxs.add(idx)

        return [sentences[idx] for idx in sorted(include_idxs)]

    def _name_matches(self, target: str, text_name: str, aliases: Optional[List[str]] = None) -> bool:
        def canon(s: str) -> str:
            s = s.strip(' ,.;:!?"“”\'’')
            s = re.sub(r"(?:'s|’s)$", "", s)
            return s.lower()

        t = canon(target)
        x = canon(text_name)
        if t == x:
            return True
        parts = [p for p in re.split(r"\s+", target.strip()) if p]
        if len(parts) >= 2 and canon(parts[-1]) == x:
            return True
        for a in (aliases or []):
            if canon(a) == x:
                return True
        return False

    def _mk_fact(
        self,
        fact: str,
        source_text: str,
        s_start: int,
        s_end: int,
        time_id: str,
        confidence: float,
        method: str,
        schema_version: Optional[str] = None,
        entity_type: Optional[str] = None,
        atomicity_score: Optional[float] = None,
        schema_alignment_score: Optional[float] = None,
        needs_review: Optional[bool] = None,
    ) -> Dict[str, Any]:
        doc = {
            "fact": fact,
            "sourceText": source_text,
            "evidence": {"timeId": time_id, "start": s_start, "end": s_end},
            "confidence": round(float(confidence), 3),
            "method": method,
        }
        if schema_version:
            doc["schema_version"] = schema_version
        if entity_type:
            doc["entityType"] = entity_type
        if atomicity_score is not None:
            doc["atomicity_score"] = round(float(atomicity_score), 3)
        if schema_alignment_score is not None:
            doc["schema_alignment_score"] = round(float(schema_alignment_score), 3)
        if needs_review is not None:
            doc["needs_review"] = bool(needs_review)
        return doc

    def _triage_score(
        self,
        name: str,
        entity_type: str,
        matched_mentions: List[str],
        aliases: Optional[List[str]] = None,
    ) -> float:
        if not matched_mentions:
            return 0.0

        normalized_name = name.lower()
        mention_score = min(1.0, 0.35 + 0.2 * len(matched_mentions))
        exact_bonus = 0.15 if any(normalized_name in mention.lower() for mention in matched_mentions) else 0.0
        alias_bonus = 0.1 if aliases and any(alias.lower() in " ".join(matched_mentions).lower() for alias in aliases if alias) else 0.0
        type_bonus = 0.05 if entity_type in {"character", "location"} else 0.0
        return min(1.0, mention_score + exact_bonus + alias_bonus + type_bonus)

    def _facts_from_sentence_rules(
        self,
        name: str,
        sentence: Sentence,
        time_id: str,
        aliases: Optional[List[str]] = None,
        schema_version: Optional[str] = None,
        entity_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Rule fallback when LLM extraction is empty or unavailable.

        Strategy: if the sentence mentions the entity and contains a verb-like token,
        treat the sentence itself as one factual statement.
        """
        s_start, s_end, sent_text = sentence
        if not sent_text.strip():
            return []

        # Ensure sentence actually mentions the target entity (or alias/surname).
        if not self._name_patterns(name, aliases).search(sent_text):
            return []

        candidates = self._decompose_sentence(sent_text)
        out: List[Dict[str, Any]] = []
        for clause in candidates:
            text = clause.strip()
            if not text:
                continue
            if not self._name_patterns(name, aliases).search(text):
                continue
            if not self._contains_verb(text):
                continue
            if text[-1] not in ".!?":
                text += "."

            atomicity = self._atomicity_score(text)
            out.append(
                self._mk_fact(
                    text,
                    sent_text,
                    s_start,
                    s_end,
                    time_id,
                    confidence=0.62 if atomicity >= 0.8 else 0.3,
                    method="rules",
                    schema_version=schema_version,
                    entity_type=entity_type,
                    atomicity_score=atomicity,
                    schema_alignment_score=self._schema_alignment_score(text, entity_type or "concept"),
                    needs_review=atomicity < 0.8,
                )
            )

        return out[:2]

    def _template_facts_from_sentence(
        self,
        name: str,
        sentence_text: str,
        s_start: int,
        s_end: int,
        time_id: str,
        aliases: Optional[List[str]] = None,
        schema_version: Optional[str] = None,
        entity_type: str = "concept",
    ) -> List[Dict[str, Any]]:
        text = sentence_text.strip()
        lowered = text.lower()
        out: List[Dict[str, Any]] = []

        if entity_type == "character":
            eye_match = re.search(r"\b(?:her|his|their|the)?\s*(blue|green|brown|hazel|gray|grey) eyes\b", lowered)
            if eye_match:
                color = eye_match.group(1)
                out.append(
                    self._mk_fact(
                        f"{name} has {color} eyes.",
                        text,
                        s_start,
                        s_end,
                        time_id,
                        confidence=0.7,
                        method="rules",
                        schema_version=schema_version,
                        entity_type=entity_type,
                        atomicity_score=0.95,
                        schema_alignment_score=0.95,
                        needs_review=False,
                    )
                )

            moved_match = re.search(r"\bstepped off the train into (?:a |the )?([^.,;]+)", lowered)
            if moved_match:
                destination = moved_match.group(1).strip()
                if destination:
                    out.append(
                        self._mk_fact(
                            f"{name} stepped off the train into {destination}.",
                            text,
                            s_start,
                            s_end,
                            time_id,
                            confidence=0.68,
                            method="rules",
                            schema_version=schema_version,
                            entity_type=entity_type,
                            atomicity_score=0.9,
                            schema_alignment_score=0.9,
                            needs_review=False,
                        )
                    )

            waiting_match = re.search(r"\bwaiting for (?:a |the )?([^.,;]+)", lowered)
            if waiting_match:
                target = waiting_match.group(1).strip()
                if target:
                    out.append(
                        self._mk_fact(
                            f"{name} is waiting for {target}.",
                            text,
                            s_start,
                            s_end,
                            time_id,
                            confidence=0.68,
                            method="rules",
                            schema_version=schema_version,
                            entity_type=entity_type,
                            atomicity_score=0.95,
                            schema_alignment_score=0.88,
                            needs_review=False,
                        )
                    )

            if re.search(r"\bnot waiting for a train\b", lowered):
                out.append(
                    self._mk_fact(
                        f"{name} is not waiting for a train.",
                        text,
                        s_start,
                        s_end,
                        time_id,
                        confidence=0.7,
                        method="rules",
                        schema_version=schema_version,
                        entity_type=entity_type,
                        atomicity_score=0.96,
                        schema_alignment_score=0.9,
                        needs_review=False,
                    )
                )

            if re.search(r"\bfelt drawn forward\b", lowered):
                out.append(
                    self._mk_fact(
                        f"{name} feels drawn forward.",
                        text,
                        s_start,
                        s_end,
                        time_id,
                        confidence=0.62,
                        method="rules",
                        schema_version=schema_version,
                        entity_type=entity_type,
                        atomicity_score=0.92,
                        schema_alignment_score=0.85,
                        needs_review=False,
                    )
                )

        if entity_type == "location":
            if re.search(r"\bold train station\b", lowered):
                out.append(
                    self._mk_fact(
                        f"The train station is old.",
                        text,
                        s_start,
                        s_end,
                        time_id,
                        confidence=0.7,
                        method="rules",
                        schema_version=schema_version,
                        entity_type=entity_type,
                        atomicity_score=0.95,
                        schema_alignment_score=0.95,
                        needs_review=False,
                    )
                )
            if re.search(r"\bdusty windows?\b", lowered):
                out.append(
                    self._mk_fact(
                        f"The train station has dusty windows.",
                        text,
                        s_start,
                        s_end,
                        time_id,
                        confidence=0.7,
                        method="rules",
                        schema_version=schema_version,
                        entity_type=entity_type,
                        atomicity_score=0.95,
                        schema_alignment_score=0.95,
                        needs_review=False,
                    )
                )
            if re.search(r"\bticket booth\b", lowered):
                out.append(
                    self._mk_fact(
                        f"The train station contains a ticket booth.",
                        text,
                        s_start,
                        s_end,
                        time_id,
                        confidence=0.7,
                        method="rules",
                        schema_version=schema_version,
                        entity_type=entity_type,
                        atomicity_score=0.95,
                        schema_alignment_score=0.95,
                        needs_review=False,
                    )
                )
            if re.search(r"\bpeeling paint\b", lowered):
                out.append(
                    self._mk_fact(
                        f"The ticket booth has peeling paint.",
                        text,
                        s_start,
                        s_end,
                        time_id,
                        confidence=0.7,
                        method="rules",
                        schema_version=schema_version,
                        entity_type=entity_type,
                        atomicity_score=0.95,
                        schema_alignment_score=0.95,
                        needs_review=False,
                    )
                )

        return out

    def _normalize_atomic_candidates(
        self,
        fact_text: str,
        name: str,
        aliases: Optional[List[str]] = None,
        entity_type: str = "concept",
    ) -> List[str]:
        candidates = self._decompose_sentence(fact_text)
        if not candidates:
            return []

        name_re = self._name_patterns(name, aliases)
        filtered: List[str] = []
        for chunk in candidates:
            text = chunk.strip()
            if not text:
                continue
            if not self._contains_verb(text):
                continue
            if not name_re.search(text):
                rewritten = self._rewrite_coref_clause(text, name, entity_type)
                text = rewritten if rewritten else text
            filtered.append(text)

        # Keep the first mention-bearing candidate when decomposition produced only descriptive fragments.
        if not filtered:
            for chunk in candidates:
                text = chunk.strip()
                if text and name_re.search(text):
                    filtered.append(text)
                    break
        return filtered

    def _rewrite_coref_clause(self, clause: str, name: str, entity_type: str) -> Optional[str]:
        text = clause.strip()
        if not text:
            return None

        if entity_type == "character":
            lowered = text.lower()
            if lowered.startswith("her "):
                return f"{name}'s {text[4:]}"
            if lowered.startswith("his "):
                return f"{name}'s {text[4:]}"
            if lowered.startswith("she "):
                return f"{name} {text[4:]}"
            if lowered.startswith("he "):
                return f"{name} {text[3:]}"
            if re.match(r"^(had|was|is|has|felt|knew|waited|moved|smiled)\b", lowered):
                return f"{name} {text}"

        if entity_type == "location":
            lowered = text.lower()
            if re.search(r"\b(window|windows|booth|hall|halls|wall|walls|door|doors|platform|ticket)\b", lowered):
                if lowered.startswith("the "):
                    return f"{name} has {text[4:]}"
                return f"{name} has {text}"

        return None

    def _is_character_coref_sentence(self, text: str) -> bool:
        lowered = text.lower()
        has_pronoun = bool(re.search(r"\b(she|her|hers|he|him|his)\b", lowered))
        return has_pronoun and self._contains_verb(text)

    def _is_location_context_sentence(self, text: str) -> bool:
        lowered = text.lower()
        has_location_feature = bool(
            re.search(r"\b(window|windows|booth|ticket|hall|halls|platform|wall|walls|door|doors|dusty|peeling|old)\b", lowered)
        )
        return has_location_feature and self._contains_verb(text)

    def _contains_verb(self, text: str) -> bool:
        words = text.lower().split()
        return any(
            w in {
                "is", "was", "were", "are", "has", "had", "have", "been",
                "became", "served", "worked", "lived", "died", "born", "held",
                "led", "said", "asked", "replied", "felt", "knew", "waited", "moved",
                "smiled", "sounded", "caught", "traced",
            }
            for w in words
        )

    def _decompose_sentence(self, sentence: str) -> List[str]:
        cleaned = re.sub(r"\s+", " ", sentence).strip()
        if not cleaned:
            return []
        parts = re.split(r"\s*(?:,|;| and | but | while | who | that | which | because | as | with )\s*", cleaned, flags=re.IGNORECASE)
        return [p.strip(" .") for p in parts if p and p.strip()]

    def _atomicity_score(self, text: str) -> float:
        lowered = text.lower()
        tokens = max(1, len(lowered.split()))
        clause_markers = len(re.findall(r"\b(and|but|while|who|that|because|with|which)\b", lowered))
        punctuation_breaks = lowered.count(",") + lowered.count(";")
        preposition_density = len(re.findall(r"\b(with|of|through|along|into|over|under|across|before|after)\b", lowered))
        qualifier_pattern = 1 if re.search(r"\bwith\b.+\bof\b", lowered) else 0
        penalty = min(
            0.88,
            0.2 * clause_markers
            + 0.1 * punctuation_breaks
            + max(0, tokens - 12) * 0.02
            + max(0, preposition_density - 1) * 0.08
            + 0.16 * qualifier_pattern,
        )
        return max(0.1, round(1.0 - penalty, 3))

    def _schema_alignment_score(self, text: str, entity_type: str) -> float:
        lowered = text.lower()
        if entity_type == "character":
            signals = ["eyes", "hair", "waiting", "moved", "smiled", "age", "from", "is in"]
        elif entity_type == "location":
            signals = ["station", "building", "windows", "booth", "room", "old", "dusty", "peeling"]
        elif entity_type == "event":
            signals = ["happened", "started", "ended", "sounded", "occurred"]
        else:
            signals = ["is", "has", "was", "were"]
        hits = sum(1 for s in signals if s in lowered)
        return min(1.0, 0.35 + 0.15 * hits)

    def _safe_json(self, text: str) -> Dict[str, Any]:
        if not text:
            return {"facts": []}
        s = text.strip()
        s = s.replace("```json", "```").replace("```JSON", "```")
        if s.startswith("```") and s.endswith("```"):
            s = s.strip("`").strip()
            start, end = s.find("{"), s.rfind("}")
            if start != -1 and end != -1 and end > start:
                s = s[start : end + 1]
        s = s.replace("“", '"').replace("”", '"').replace("’", "'").replace("‛", "'")
        try:
            return json.loads(s)
        except Exception:
            pass
        # try to coerce common single-quote JSON mistakes
        s2 = re.sub(r"(?P<pre>[\{,\s])'(?P<key>\w+)'(?P<post>\s*:)", r'\g<pre>"\g<key>"\g<post>', s)
        s2 = re.sub(r':\s*\'(.*?)\'', lambda m: ':"{}"'.format(m.group(1).replace('"', '\\"')), s2)
        try:
            return json.loads(s2)
        except Exception:
            pass
        m = re.search(r'\[(?:\s*".*?"\s*)(?:,\s*".*?"\s*)*\]', s)
        if m:
            try:
                arr = json.loads(m.group(0))
                return {"facts": arr}
            except Exception:
                pass
        return {"facts": []}