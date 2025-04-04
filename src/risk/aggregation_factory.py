from collections import defaultdict
from typing import List, Optional, Dict

from src.models.risk_models import Pattern, RiskCategory, RiskLevel, RiskRepost


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
    def calculate_aggregated_confidence(patterns: List[Pattern]) -> float:
        """Weight patterns by their confidence (more weight to stronger ones)."""
        if not patterns:
            return 0.0

        sorted_patterns = sorted(patterns, key=lambda p: p.confidence, reverse=True)
        weights = [1.0 - (i * 0.2) for i in range(min(5, len(sorted_patterns)))]
        weighted_sum = sum(p.confidence * w for p, w in zip(sorted_patterns, weights))
        return weighted_sum / sum(weights)

    @staticmethod
    def aggregate(
            patterns: List[Pattern],
            user_id: int,
            job_id: Optional[int] = None
    ) -> RiskRepost:

        category_to_patterns: Dict[RiskCategory, List[Pattern]] = defaultdict(list)

        # Apply weighted confidence per category
        for pattern in patterns:
            for category, weight in pattern.category_weights.items():
                weighted_pattern = Pattern(**pattern.dict())
                weighted_pattern.confidence *= weight
                category_to_patterns[category].append(weighted_pattern)

        # Compute final category scores using weighted aggregation
        category_scores: Dict[RiskCategory, float] = {
            cat: AggregationFactory.calculate_aggregated_confidence(pats)
            for cat, pats in category_to_patterns.items()
        }

        if not category_scores:
            return RiskRepost(
                user_id=user_id,
                job_id=job_id,
                top_risk_confidence=0.0,
                top_risk_type=RiskCategory.OVERTRADING,  # default fallback
                top_risk_level=RiskLevel.NONE,
                category_scores={},
                patterns=[],
                decay_params={
                    "initial_priority": 100,
                    "half_life_minutes": 60,
                    "min_priority": 10
                }
            )

        # Determine the dominant category
        top_risk_type, top_confidence = max(category_scores.items(), key=lambda x: x[1])
        top_risk_level = AggregationFactory.calculate_risk_level(top_confidence)

        return RiskRepost(
            user_id=user_id,
            job_id=job_id,
            top_risk_confidence=top_confidence,
            top_risk_type=top_risk_type,
            top_risk_level=top_risk_level,
            category_scores=category_scores,
            patterns=patterns,
            decay_params={
                "initial_priority": 100,
                "half_life_minutes": 60,
                "min_priority": 10
            }
        )
