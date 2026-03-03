from __future__ import annotations
import re
import json
import logging
from typing import Dict, List, Any, Optional, Tuple, Callable

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

            aliases = ent.get("aliases") or []
            mention_sents = self._find_mention_sentences(name, sentences, aliases)

            facts: List[dict] = []
            for start, end, sent_text in mention_sents:
                llm_facts = await self._llm_extract_facts_for_sentence(name, sent_text)
                for sf in llm_facts:
                    facts.append(
                        self._mk_fact(
                            sf,
                            sent_text,
                            start,
                            end,
                            time_id,
                            confidence=0.80,
                            method="llm",
                        )
                    )
                if self.rules_fallback and not llm_facts:
                    facts.extend(
                        self._facts_from_sentence_rules(
                            name, (start, end, sent_text), time_id, aliases
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
    async def _llm_extract_facts_for_sentence(self, name: str, sentence: str) -> List[str]:
        if not self.llm or not self.use_llm:
            return []
        prompt = (
            "You extract facts strictly from the given sentence.\n"
            f"Target entity: {name}\n"
            'Return JSON ONLY with EXACTLY this schema:\n{"facts": ["<fact-1>", "<fact-2>"]}\n'
            "Rules:\n"
            "- Include 0–3 facts explicitly supported by THIS sentence.\n"
            "- No external knowledge.\n"
            "- Use DOUBLE quotes. Do NOT use single quotes.\n"
            "- Do NOT include markdown/code fences or any extra text.\n"
            '- If no facts, return {"facts": []} exactly.\n\n'
            f"Sentence: {sentence}\n"
            'Example output: {"facts": ["Ludwig van Beethoven was a German composer.", "His early period lasted until 1802."]}'
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
        self, name: str, sentences: List[Sentence], aliases: Optional[List[str]] = None
    ) -> List[Sentence]:
        name_re = self._name_patterns(name, aliases)
        return [s for s in sentences if name_re.search(s[2])]

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
    ) -> Dict[str, Any]:
        return {
            "fact": fact,
            "sourceText": source_text,
            "evidence": {"timeId": time_id, "start": s_start, "end": s_end},
            "confidence": round(float(confidence), 3),
            "method": method,
        }

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