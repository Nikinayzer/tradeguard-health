from collections import defaultdict
from typing import List, Optional, Dict

from src.models.risk_models import AtomicPattern, CompositePattern, RiskCategory, RiskLevel, RiskRepost


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

    # TODO Even though it works, I can't really justify use of noisy-or model with dependent factors. But in
    #  practice, works well.
    @staticmethod
    def calculate_aggregated_confidence(patterns: List[CompositePattern | AtomicPattern]) -> float:
        """
        Calculate aggregated confidence using Noisy-OR method.
        Noisy-OR is a probabilistic model for combining binary signals, which works with probability of no risk,
        then converts back to probability of risk at the end.
        EXAMPLE:
        Array of possibilities: (0.7 , 0.5)
        Formula: (1 - 0.7) × (1 - 0.5) = 0.15 => 1 - 0.15 = 0.85
        Noisy-OR treats each pattern as a partially reliable indicator of risk.
        With this approach:
        - Adding a new pattern will never decrease the overall confidence
        - Multiple weaker signals can combine to produce a stronger signal
        - A strong signal remains strong even with weaker additional signals
        
        Args:
            patterns: List of patterns to aggregate
            
        Returns:
            Aggregated confidence value between 0 and 1
        """
        if not patterns:
            return 0.0

        prob_no_risk = 1.0

        for pattern in patterns:
            if pattern.__class__ == CompositePattern:
                prob_no_risk *= (1.0 - pattern.confidence)
            else:
                prob_no_risk *= (1.0 - pattern.severity)

        return 1.0 - prob_no_risk

    @staticmethod
    def aggregate(
            patterns: List[AtomicPattern],
            composite_patterns: List[CompositePattern],
            user_id: int,
            job_id: Optional[int] = None
    ) -> RiskRepost:

        category_to_patterns: Dict[RiskCategory, List[AtomicPattern | CompositePattern]] = defaultdict(list)

        # Process composite patterns first (already boosted in composition logic)
        for pattern in composite_patterns:
            for category, weight in pattern.category_weights.items():
                weighted_pattern = CompositePattern(**pattern.dict())
                weighted_pattern.confidence *= weight  # Apply category weight, but no additional boost
                category_to_patterns[category].append(weighted_pattern)

        # Then process unconsumed atomic patterns with 50% weight
        for pattern in patterns:
            if not pattern.is_composite and not pattern.consumed:
                for category, weight in pattern.category_weights.items():
                    weighted_pattern = AtomicPattern(**pattern.dict())
                    # Apply 50% factor to atomic patterns to give composites higher priority
                    weighted_pattern.severity *= weight * 0.5
                    category_to_patterns[category].append(weighted_pattern)

        # Compute final category scores using weighted aggregation
        category_scores: Dict[RiskCategory, float] = {
            cat: AggregationFactory.calculate_aggregated_confidence(pats)
            for cat, pats in category_to_patterns.items()
        }

        # Count pattern types for metadata
        consumed_count = sum(1 for p in patterns if p.consumed)
        unconsumed_count = sum(1 for p in patterns if not p.consumed and not p.is_composite)
        awareness_patterns = [p for p in patterns if not p.consumed and not p.is_composite]

        # Create metadata
        metadata = {
            "pattern_stats": {
                "total_patterns": len(patterns),
                "composite_patterns": len(composite_patterns),
                "consumed_patterns": consumed_count,
                "awareness_patterns": unconsumed_count
            },
            "signal_source": "composite" if composite_patterns else "atomic",
            "primary_categories": list(category_scores.keys()) if category_scores else [],
            "awareness_signals": [
                {
                    "type": p.pattern_id,
                    "id": p.internal_id,
                    "confidence": p.confidence,
                    "category": max(p.category_weights.items(), key=lambda x: x[1])[0].value
                }
                for p in awareness_patterns[:5]  # Limit to top 5 for brevity
            ] if awareness_patterns else []
        }

        if not category_scores:
            return RiskRepost(
                user_id=user_id,
                job_id=job_id,
                top_risk_confidence=0.0,
                top_risk_type=RiskCategory.OVERTRADING,  # default fallback
                top_risk_level=RiskLevel.NONE,
                category_scores={},
                patterns=patterns,  # Include ALL original patterns for context
                composite_patterns=composite_patterns,
                metadata=metadata,
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
            patterns=patterns,  # Include ALL original patterns for context
            composite_patterns=composite_patterns,
            metadata=metadata,
            decay_params={
                "initial_priority": 100,
                "half_life_minutes": 60,
                "min_priority": 10
            }
        )
