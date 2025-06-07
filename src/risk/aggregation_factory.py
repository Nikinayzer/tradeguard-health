from collections import defaultdict
from typing import List, Optional, Dict

from src.models.risk_models import AtomicPattern, CompositePattern, RiskCategory, RiskLevel, RiskRepost
import logging

logger = logging.getLogger(__name__)


class AggregationFactory:
    # def __init__(self):
    #     ...
    @staticmethod
    def calculate_risk_level(confidence: float) -> RiskLevel:
        if confidence >= 0.9:
            return RiskLevel.CRITICAL
        elif confidence >= 0.7:
            return RiskLevel.HIGH
        elif confidence >= 0.5:
            return RiskLevel.MEDIUM
        elif confidence > 0.0:
            return RiskLevel.LOW
        return RiskLevel.NONE

    @staticmethod
    def calculate_composite_confidence(patterns: List[AtomicPattern]) -> float:
        """
        Calculate confidence for a composite pattern based on its component patterns.
        
        Args:
            patterns: List of atomic patterns that form the composite
            
        Returns:
            Confidence value between 0 and 1, rounded to 2 decimal places
        """
        if not patterns:
            return 0.0
            
        # Calculate confidence as average of component severities
        confidence = sum(p.severity for p in patterns) / len(patterns)
        return round(confidence, 2)

    @staticmethod
    def calculate_aggregated_confidence(patterns: List[CompositePattern | AtomicPattern]) -> float:
        """
        Calculate aggregated confidence using weighted average method.
        If composite patterns exist, only use those. Otherwise, use atomic patterns.
        
        Args:
            patterns: List of patterns to aggregate, each with its category weight
            
        Returns:
            Aggregated confidence value between 0 and 1, rounded to 2 decimal places
        """
        if not patterns:
            return 0.0

        composite_patterns = [p for p in patterns if isinstance(p, CompositePattern)]
        atomic_patterns = [p for p in patterns if not isinstance(p, CompositePattern)]

        # If composite patterns, only use them (MAX confidence)
        if composite_patterns:
            composite_score = max(
                p.confidence * p.category_weights[p.category]
                for p in composite_patterns
            )
            return min(1.0, round(composite_score, 2))
        
        # If no composite patterns, use atomic patterns
        if atomic_patterns:
            atomic_score = sum(p.severity * p.category_weights[p.category]
                             for p in atomic_patterns) / len(atomic_patterns)
            return min(1.0, round(atomic_score, 2))
        
        return 0.0

    @staticmethod
    def aggregate(
            patterns: List[AtomicPattern],
            composite_patterns: List[CompositePattern],
            user_id: int,
    ) -> RiskRepost:

        logger.info(f"[AggregationFactory] Starting aggregation for user {user_id}")
        logger.info(f"[AggregationFactory] Input patterns: {len(patterns)} atomic, {len(composite_patterns)} composite")

        atomic_patterns_number = len(patterns)
        composite_patterns_number = len(composite_patterns)
        consumed_patterns_number = sum(1 for p in patterns if p.consumed)

        category_to_patterns: Dict[RiskCategory, List[AtomicPattern | CompositePattern]] = defaultdict(list)

        for pattern in composite_patterns:
            logger.info(f"[AggregationFactory] Processing composite pattern: {pattern.pattern_id}")
            for category, weight in pattern.category_weights.items():
                weighted_pattern = CompositePattern(**pattern.dict())
                category_to_patterns[category].append(weighted_pattern)
                logger.info(
                    f"[AggregationFactory] Added to category {category} with original confidence {weighted_pattern.confidence}")

        for pattern in patterns:
            if not pattern.is_composite and not pattern.consumed:
                logger.info(f"[AggregationFactory] Processing atomic pattern: {pattern.pattern_id}")
                for category, weight in pattern.category_weights.items():
                    weighted_pattern = AtomicPattern(**pattern.dict())
                    # Apply 50% factor to atomic patterns to give composites higher priority
                    weighted_pattern.severity *= weight * 0.5
                    category_to_patterns[category].append(weighted_pattern)
                    logger.info(f"[AggregationFactory] Added to category {category} with weight {weight * 0.5}")

        # Compute final category scores using weighted aggregation
        category_scores: Dict[RiskCategory, float] = {
            cat: AggregationFactory.calculate_aggregated_confidence(pats)
            for cat, pats in category_to_patterns.items()
        }
        logger.info(f"[AggregationFactory] Category scores: {category_scores}")

        if not category_scores:
            logger.info("[AggregationFactory] No category scores found, returning default report")
            return RiskRepost(
                user_id=user_id,
                top_risk_confidence=0.0,
                top_risk_type=RiskCategory.OVERCONFIDENCE,  # default fallback
                top_risk_level=RiskLevel.NONE,
                category_scores={},
                patterns=patterns,  # Include ALL original patterns for context
                composite_patterns=composite_patterns,
                atomic_patterns_number=atomic_patterns_number,
                composite_patterns_number=composite_patterns_number,
                consumed_patterns_number=consumed_patterns_number,
            )

        top_risk_type, top_confidence = max(category_scores.items(), key=lambda x: x[1])
        top_risk_level = AggregationFactory.calculate_risk_level(top_confidence)
        logger.info(
            f"[AggregationFactory] Top risk: {top_risk_type} at {top_risk_level} (confidence: {top_confidence})")

        return RiskRepost(
            user_id=user_id,
            top_risk_confidence=top_confidence,
            top_risk_type=top_risk_type,
            top_risk_level=top_risk_level,
            category_scores=category_scores,
            patterns=patterns,  # Include ALL original patterns for context
            composite_patterns=composite_patterns,
            atomic_patterns_number=atomic_patterns_number,
            composite_patterns_number=composite_patterns_number,
            consumed_patterns_number=consumed_patterns_number,
        )
