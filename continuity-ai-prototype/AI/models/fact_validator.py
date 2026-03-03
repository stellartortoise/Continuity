"""
Fact validation using Natural Language Inference (NLI) with DeBERTa.

This module uses microsoft/deberta-large-mnli to cross-check facts for:
- Contradictions: Facts that contradict each other
- Entailment: Facts that support/imply each other
- Neutral: Facts that are independent
"""
from __future__ import annotations
import logging
from typing import List, Dict, Any, Tuple
import torch
from transformers import pipeline
from config.settings import MODELS_CACHE_DIR

logger = logging.getLogger(__name__)


class FactValidator:
    """
    Validates facts using Natural Language Inference (NLI).
    
    Uses microsoft/deberta-large-mnli to detect contradictions and entailments
    between extracted facts.
    """
    
    def __init__(self, model_name: str = "microsoft/deberta-large-mnli", threshold: float = 0.7):
        """
        Initialize the fact validator.

        Args:
            model_name: HuggingFace model name for NLI (default: microsoft/deberta-large-mnli)
            threshold: Confidence threshold for contradiction/entailment detection (0.0-1.0)
        """
        self.model_name = model_name
        self.threshold = threshold
        logger.info(f"Loading NLI model: {model_name} (cache: {MODELS_CACHE_DIR})")

        try:
            # Load model and tokenizer with cache_dir, then pass to pipeline
            from transformers import AutoModelForSequenceClassification, AutoTokenizer

            model = AutoModelForSequenceClassification.from_pretrained(
                model_name,
                cache_dir=MODELS_CACHE_DIR
            )
            tokenizer = AutoTokenizer.from_pretrained(
                model_name,
                cache_dir=MODELS_CACHE_DIR
            )

            # DeBERTa MNLI is a text-classification model for NLI
            self.nli_pipeline = pipeline(
                "text-classification",
                model=model,
                tokenizer=tokenizer,
                device=0 if torch.cuda.is_available() else -1,
                top_k=None,  # Return all labels with scores
            )
            logger.info("[OK] NLI model loaded from cache")
        except Exception as e:
            logger.error(f"Failed to load NLI model: {e}")
            raise

        # Label mapping for MNLI models
        self.label_map = {
            "CONTRADICTION": "contradiction",
            "ENTAILMENT": "entailment",
            "NEUTRAL": "neutral",
        }
    
    def check_contradiction(self, fact1: str, fact2: str) -> Tuple[bool, float]:
        """
        Check if two facts contradict each other.

        Args:
            fact1: First fact statement (premise)
            fact2: Second fact statement (hypothesis)

        Returns:
            Tuple of (is_contradiction, confidence_score)
        """
        try:
            # NLI format: premise [SEP] hypothesis
            # Check if fact1 contradicts fact2
            input_text = f"{fact1} [SEP] {fact2}"
            results = self.nli_pipeline(input_text)

            # Find contradiction score
            contradiction_score = 0.0
            for result in results[0]:
                label = self.label_map.get(result["label"].upper(), result["label"].lower())
                if label == "contradiction":
                    contradiction_score = result["score"]
                    break

            is_contradiction = contradiction_score >= self.threshold
            return is_contradiction, contradiction_score

        except Exception as e:
            logger.warning(f"Error checking contradiction: {e}")
            return False, 0.0
    
    def check_entailment(self, fact1: str, fact2: str) -> Tuple[bool, float]:
        """
        Check if fact1 entails (supports/implies) fact2.

        Args:
            fact1: First fact statement (premise)
            fact2: Second fact statement (hypothesis)

        Returns:
            Tuple of (is_entailment, confidence_score)
        """
        try:
            # NLI format: premise [SEP] hypothesis
            input_text = f"{fact1} [SEP] {fact2}"
            results = self.nli_pipeline(input_text)

            # Find entailment score
            entailment_score = 0.0
            for result in results[0]:
                label = self.label_map.get(result["label"].upper(), result["label"].lower())
                if label == "entailment":
                    entailment_score = result["score"]
                    break

            is_entailment = entailment_score >= self.threshold
            return is_entailment, entailment_score

        except Exception as e:
            logger.warning(f"Error checking entailment: {e}")
            return False, 0.0
    
    def validate_facts(self, facts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Cross-validate a list of facts, detecting contradictions and entailments.
        
        Args:
            facts: List of fact dictionaries with at least a "fact" key
            
        Returns:
            List of facts with added validation metadata:
            - contradicts: List of indices of contradicting facts
            - entails: List of indices of entailed facts
            - validation_confidence: Average confidence of relationships
        """
        if len(facts) <= 1:
            return facts
        
        validated_facts = []
        
        for i, fact_dict in enumerate(facts):
            fact_text = fact_dict.get("fact", "").strip()
            if not fact_text:
                validated_facts.append(fact_dict)
                continue
            
            contradictions = []
            entailments = []
            confidence_scores = []
            
            # Compare with all other facts
            for j, other_fact_dict in enumerate(facts):
                if i == j:
                    continue
                
                other_fact_text = other_fact_dict.get("fact", "").strip()
                if not other_fact_text:
                    continue
                
                # Check for contradiction
                is_contradiction, contradiction_score = self.check_contradiction(fact_text, other_fact_text)
                if is_contradiction:
                    contradictions.append(j)
                    confidence_scores.append(contradiction_score)
                    logger.info(f"Contradiction detected (score={contradiction_score:.3f}):\n  - {fact_text}\n  - {other_fact_text}")
                
                # Check for entailment
                is_entailment, entailment_score = self.check_entailment(fact_text, other_fact_text)
                if is_entailment:
                    entailments.append(j)
                    confidence_scores.append(entailment_score)
                    logger.debug(f"Entailment detected (score={entailment_score:.3f}):\n  - {fact_text} -> {other_fact_text}")
            
            # Add validation metadata
            validated_fact = fact_dict.copy()
            validated_fact["contradicts"] = contradictions
            validated_fact["entails"] = entailments
            validated_fact["validation_confidence"] = (
                sum(confidence_scores) / len(confidence_scores) if confidence_scores else 1.0
            )
            
            validated_facts.append(validated_fact)
        
        return validated_facts

