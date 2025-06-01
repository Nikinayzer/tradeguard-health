from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Any, Tuple
from collections import defaultdict
from src.models.risk_models import AtomicPattern, CompositePattern, RiskCategory
from src.utils.log_util import get_logger
from src.risk.aggregation_factory import AggregationFactory

logger = get_logger()


class CompositePatternRule:
    """
    Rule definition for detecting a composite pattern from atomic patterns.
    
    Attributes:
        rule_id (str): Unique identifier for the rule
        pattern_requirements (Dict[str, str]): Pattern requirements where values can be:
            - "0": Optional pattern, can be consumed once if found
            - "0+": Optional pattern, can be consumed multiple times if found
            - "1": Required pattern, consumed once
            - "1+": Required pattern, can be consumed multiple times
            - "2": Required pattern, consumed twice
            - "2+": Required pattern, consumed twice and can consume more
            etc.
        sequence_matters (bool): Whether the patterns must appear in a specific sequence
        time_window_minutes (int): Time window in which patterns must occur
        category (RiskCategory): Primary risk category for this pattern
        message (str): Message template for the resulting composite pattern
    """

    def __init__(
            self,
            rule_id: str,
            pattern_requirements: Dict[str, str],
            category: RiskCategory,
            time_window_minutes: int = 1440,
            sequence_matters: bool = False,
            message: str = "Composite pattern detected",
            position_specific: bool = False
    ):
        self.rule_id = rule_id
        self.pattern_requirements = pattern_requirements
        self.sequence_matters = sequence_matters
        self.time_window_minutes = time_window_minutes
        self.category = category
        self.message = message
        self.position_specific = position_specific

        self._validate_rule()

    @property
    def pattern_ids(self) -> List[str]:
        """Get the list of pattern IDs from pattern_requirements."""
        return list(self.pattern_requirements.keys())

    @property
    def min_patterns_required(self) -> int:
        """Get the minimum number of patterns required based on pattern_requirements."""
        return sum(self._get_required_count(pid) for pid in self.pattern_requirements.keys())

    def _validate_rule(self) -> None:
        """Validate rule configuration and ensure consistency."""
        if not self.pattern_requirements:
            raise ValueError("pattern_requirements cannot be empty")

        for pattern_id, requirement in self.pattern_requirements.items():
            if not isinstance(requirement, str):
                raise ValueError(f"Pattern requirement for {pattern_id} must be a string")

            if not (requirement.isdigit() or
                    (requirement.endswith('+') and requirement[:-1].isdigit()) or
                    requirement == '0+'):
                raise ValueError(f"Invalid pattern requirement format for {pattern_id}: {requirement}")

            if requirement in ['0', '0+'] and all(req in ['0', '0+'] for req in self.pattern_requirements.values()):
                raise ValueError("At least one pattern must be required (not '0' or '0+')")

    def _get_required_count(self, pattern_id: str) -> int:
        """Get required count for a specific pattern type."""
        requirement = self.pattern_requirements.get(pattern_id, '0')
        if requirement in ['0', '0+']:
            return 0
        return int(requirement.rstrip('+'))

    def _is_greedy(self, pattern_id: str) -> bool:
        """Check if a pattern type can be consumed multiple times."""
        requirement = self.pattern_requirements.get(pattern_id, '0')
        return requirement.endswith('+')

    def get_required_count(self, pattern_id: str) -> int:
        """Get required count for a specific pattern type."""
        return self._get_required_count(pattern_id)

    def get_total_required_count(self) -> int:
        """Get total number of patterns required by this rule."""
        return self.min_patterns_required

    def can_consume_more(self, pattern_id: str, current_count: int) -> bool:
        """Check if more patterns of this type can be consumed."""
        if not self._is_greedy(pattern_id):
            return current_count < self._get_required_count(pattern_id)
        return True


class PatternCompositionEngine:
    """
    Engine for detecting composite patterns from atomic patterns based on rules.
    
    This engine processes atomic patterns and applies composition rules to detect
    higher-level behavioral patterns that may indicate cognitive biases.
    """

    def __init__(self):
        """Initialize the pattern composition engine with default rules."""
        self.rules: List[CompositePatternRule] = []
        self._initialize_default_rules()

    def _initialize_default_rules(self):
        """Initialize default composition rules for common biases."""
        # Overconfidence
        self.rules.append(CompositePatternRule(
            rule_id="extensive trading",
            pattern_requirements={
                "limit_daily_trades_count": "1",
                "limit_cooldown": "1+",
                "limit_single_job_amount": "0+"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60 * 24,
            sequence_matters=False,
            message="Multiple overtrading indicators detected. "
                    "This may indicate overconfidence or impulsive trading behavior. "
                    "Consider reviewing your trading strategy to avoid excessive trading activity."
        ))
        self.rules.append(CompositePatternRule(
            rule_id="overtrading",
            pattern_requirements={
                "limit_daily_trades_count": "1",
                "limit_cooldown": "1+",
                "limit_single_job_amount": "0+"
            },
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=60 * 24,
            sequence_matters=False,
            message="Multiple overtrading indicators detected. "
                    "This may indicate overconfidence or impulsive trading behavior. "
                    "Consider reviewing your trading strategy to avoid excessive trading activity."
        ))

        # Risk-seeking
        self.rules.append(CompositePatternRule(
            rule_id="risk_seeking",
            pattern_requirements={
                "position_long_holding_time": "1",
                "position_unrealized_pnl_threshold": "1"
            },
            category=RiskCategory.LOSS_BEHAVIOR,
            time_window_minutes=1440 * 7,
            position_specific=True,
            message="One or more long-held positions with significant unrealized PnL. "
                    "This may indicate risk-seeking behavior. "
                    "Consider reviewing your trading strategy "
                    "to ensure you are not holding onto positions with high risk for too long."
        ))
        # Risk-aversion
        self.rules.append(CompositePatternRule(
            rule_id="cutting_profits",
            pattern_requirements={
                "position_early_profit_exit": "3"
            },
            category=RiskCategory.LOSS_BEHAVIOR,
            time_window_minutes=1440 * 7,
            message=f"Multiple early profit exits detected. This may indicate risk-averse behavior. "
                    f"Consider reviewing your trading strategy to ensure you are not exiting profitable"
                    f" positions too early."
        ))
        # Overweighting small probabilities
        self.rules.append(CompositePatternRule(
            rule_id="position_size_equity_ratio",
            pattern_requirements={
                "position_low_liquidity": "1",
                "position_size_equity_ratio": "1"
            },
            category=RiskCategory.FOMO,
            position_specific=True,
            time_window_minutes=1440 * 7,
            message="High investment in illiquid coin found. Consider reviewing your portfolio allocation. "
        ))
        # Availability Heuristic
        # self.rules.append(CompositePatternRule(
        #     rule_id="availability_heuristic",
        #     pattern_requirements={
        #         "position_high_volatility": "1",
        #         # "position_news_spike": "1",
        #     },
        #     category=RiskCategory.FOMO,
        #     position_specific=True,
        #     time_window_minutes=1440 * 7,
        #     sequence_matters=False,
        #     message="Recent high and low prices with volume spikes detected. "
        #             "This may indicate availability heuristic bias. "
        #             "Consider reviewing your strategy to avoid making decisions based on recent price movements."
        # ))

    def add_rule(self, rule: CompositePatternRule):
        """Add a new composition rule to the engine."""
        self.rules.append(rule)
        logger.info(f"Added composite pattern rule: {rule.rule_id}")

    def process_patterns(self, patterns: List[AtomicPattern]) -> List[CompositePattern]:
        """
        Process a list of atomic patterns and detect composite patterns.
        
        Args:
            patterns: List of atomic patterns from evaluators
            
        Returns:
            List of composite patterns detected
        """
        if not patterns:
            logger.info("[PatternCompositionEngine] No patterns to process")
            return []

        logger.info(f"[PatternCompositionEngine] Processing {len(patterns)} patterns")
        logger.info(f"[PatternCompositionEngine] Available rules: {[r.rule_id for r in self.rules]}")

        composite_patterns = []

        patterns_by_id = self._index_patterns_by_id(patterns)
        logger.info(f"[PatternCompositionEngine] Pattern types found: {list(patterns_by_id.keys())}")

        for rule in self.rules:
            logger.info(f"[PatternCompositionEngine] Checking rule: {rule.rule_id}")
            logger.info(f"[PatternCompositionEngine] Rule requires patterns: {rule.pattern_requirements}")
            matching_combinations = self._match_rule(rule, patterns_by_id)
            if matching_combinations:
                logger.info(
                    f"[PatternCompositionEngine] Rule {rule.rule_id} matched with {len(matching_combinations)} combinations")
                for matching_patterns in matching_combinations:
                    composite_pattern = self._create_composite_pattern(rule, matching_patterns)
                    composite_patterns.append(composite_pattern)
                    logger.info(
                        f"[PatternCompositionEngine] Created composite pattern: {rule.rule_id} with confidence {composite_pattern.confidence:.2f}")
            else:
                logger.info(f"[PatternCompositionEngine] Rule {rule.rule_id} did not match")

        logger.info(f"[PatternCompositionEngine] Found {len(composite_patterns)} composite patterns")
        return composite_patterns

    def _index_patterns_by_id(self, patterns: List[AtomicPattern]) -> Dict[str, List[AtomicPattern]]:
        """Index patterns by their pattern_id for efficient lookup."""
        patterns_by_id = defaultdict(list)
        for pattern in patterns:
            patterns_by_id[pattern.pattern_id].append(pattern)
        return patterns_by_id

    def _match_rule(
            self,
            rule: CompositePatternRule,
            patterns_by_id: Dict[str, List[AtomicPattern]]
    ) -> List[List[AtomicPattern]]:
        """
        Check if a rule matches the available patterns based on their temporal relationships.
        
        Args:
            rule: The rule to check
            patterns_by_id: Dictionary of patterns indexed by pattern_id
            
        Returns:
            List of matching patterns if rule conditions are met, empty list otherwise
        """
        if not self._has_required_patterns(rule, patterns_by_id):
            return []

        recent_patterns = self._filter_by_recency(rule, patterns_by_id)

        if rule.position_specific:
            # Group patterns by position key
            patterns_by_position = defaultdict(list)
            for pattern in recent_patterns:
                if pattern.position_key:
                    patterns_by_position[pattern.position_key].append(pattern)
                else:
                    # If a pattern doesn't have a position key but rule is position-specific,
                    # we can't use it
                    continue

            # Try to match patterns for each position separately
            all_valid_combinations = []

            for position_key, position_patterns in patterns_by_position.items():
                # Create a new patterns_by_id for this position
                position_patterns_by_id = defaultdict(list)
                for pattern in position_patterns:
                    position_patterns_by_id[pattern.pattern_id].append(pattern)

                # Check if we have required patterns for this position
                if not self._has_required_patterns(rule, position_patterns_by_id):
                    continue

                if rule.sequence_matters:
                    valid_combinations = self._find_sequence_combinations(rule, position_patterns_by_id)
                else:
                    valid_combinations = self._find_window_combinations(rule, position_patterns)

                # If we found valid combinations, add the best one for this position
                if valid_combinations:
                    valid_combinations.sort(key=self._score_combination, reverse=True)
                    all_valid_combinations.append(valid_combinations[0])

            return all_valid_combinations

        else:
            recent_patterns_by_id = self._index_patterns_by_id(recent_patterns)
            if not self._has_required_patterns(rule, recent_patterns_by_id):
                return []

            if rule.sequence_matters:
                valid_combinations = self._find_sequence_combinations(rule, recent_patterns_by_id)
            else:
                valid_combinations = self._find_window_combinations(rule, recent_patterns)

            if valid_combinations:
                valid_combinations.sort(key=self._score_combination, reverse=True)
                return [valid_combinations[0]]

            return []

    def _has_required_patterns(self, rule: CompositePatternRule,
                               patterns_by_id: Dict[str, List[AtomicPattern]]) -> bool:
        """Check if we have enough patterns of each required type."""
        for pattern_id, required_count in rule.pattern_requirements.items():
            if required_count != '0':
                available_count = len(patterns_by_id.get(pattern_id, []))
                if available_count < int(required_count.rstrip('+')):
                    return False

        return True

    def _filter_by_recency(
            self,
            rule: CompositePatternRule,
            patterns_by_id: Dict[str, List[AtomicPattern]]
    ) -> List[AtomicPattern]:
        """
        Filter patterns to only include those recent enough for this rule.
        
        Args:
            rule: The rule containing time window information
            patterns_by_id: Dictionary of patterns indexed by pattern_id
            
        Returns:
            List of patterns that are recent enough to be considered
        """
        recent_patterns = []

        for pattern_list in patterns_by_id.values():
            for pattern in pattern_list:
                pattern_end = pattern.end_time or pattern.start_time

                if pattern_end.tzinfo is None:
                    pattern_end = pattern_end.replace(tzinfo=timezone.utc)

                current_time = datetime.now(timezone.utc)

                time_diff = (current_time - pattern_end).total_seconds() / 60
                if time_diff <= rule.time_window_minutes:
                    recent_patterns.append(pattern)

        return recent_patterns

    def _find_sequence_combinations(
            self,
            rule: CompositePatternRule,
            patterns_by_id: Dict[str, List[AtomicPattern]]
    ) -> List[List[AtomicPattern]]:
        """Find valid combinations for sequence-dependent rules."""
        valid_combinations = []

        # Group patterns by type for faster access
        patterns_by_type = {pid: patterns_by_id.get(pid, []) for pid in rule.pattern_requirements}

        # Consider potential starting points based on requirements
        start_pattern_id = next((pid for pid in rule.pattern_ids if rule.get_required_count(pid) > 0), None)
        if not start_pattern_id or start_pattern_id not in patterns_by_type:
            return []

        # Try each possible starting pattern
        for start_pattern in patterns_by_type[start_pattern_id]:
            combination = [start_pattern]
            current_end_time = start_pattern.end_time or start_pattern.start_time

            # Find all required patterns in sequence
            sequence_valid = True

            # Track what we've collected so far
            collected_counts = defaultdict(int)
            collected_counts[start_pattern_id] = 1

            # For each subsequent pattern in sequence
            for next_pattern_id in rule.pattern_ids[1:]:
                required_count = rule.get_required_count(next_pattern_id) - collected_counts[next_pattern_id]
                if required_count <= 0:
                    continue  # Already have enough of this type

                if next_pattern_id not in patterns_by_type:
                    sequence_valid = False
                    break

                # Find patterns of this type that follow in sequence
                valid_next_patterns = [
                    p for p in patterns_by_type[next_pattern_id]
                    if p.start_time >= current_end_time
                ]

                if not valid_next_patterns:
                    sequence_valid = False
                    break

                # Sort by distance to previous pattern
                valid_next_patterns.sort(key=lambda p: (p.start_time - current_end_time).total_seconds())

                # Take required number of patterns
                for i, next_pattern in enumerate(valid_next_patterns[:required_count]):
                    # Ensure time gap isn't too large
                    time_gap = (next_pattern.start_time - current_end_time).total_seconds() / 60
                    if time_gap > rule.time_window_minutes:
                        # If time gap too large, stop collecting
                        break

                    combination.append(next_pattern)
                    collected_counts[next_pattern_id] += 1

                    # Update current end time if this isn't the last pattern of this type
                    if i == required_count - 1:
                        current_end_time = next_pattern.end_time or next_pattern.start_time

            # Check if we collected enough of each required type
            sufficient = True
            for pattern_id, required_count in rule.pattern_requirements.items():
                if required_count != '0' and collected_counts[pattern_id] < int(required_count.rstrip('+')):
                    sufficient = False
                    break

            if sequence_valid and sufficient and len(combination) >= rule.min_patterns_required:
                valid_combinations.append(combination)

        return valid_combinations

    def _find_window_combinations(
            self,
            rule: CompositePatternRule,
            recent_patterns: List[AtomicPattern]
    ) -> List[List[AtomicPattern]]:
        """Find valid combinations by checking time relationships between patterns."""
        valid_combinations = []

        # Group patterns by type for efficient lookup
        patterns_by_type = defaultdict(list)
        for pattern in recent_patterns:
            patterns_by_type[pattern.pattern_id].append(pattern)

        # Get all required pattern types and their counts
        required_types = [(pid, count) for pid, count in rule.pattern_requirements.items()
                          if count not in ['0', '0+']]
        optional_types = [(pid, count) for pid, count in rule.pattern_requirements.items()
                          if count in ['0', '0+']]

        # Sort patterns by start time for each type
        for pattern_list in patterns_by_type.values():
            pattern_list.sort(key=lambda p: p.start_time)

        # Helper function to check if patterns are within time window of each other
        def patterns_within_window(patterns: List[AtomicPattern]) -> bool:
            if not patterns:
                return False

            # Check if patterns are within time window of each other
            for i, pattern1 in enumerate(patterns):
                pattern1_end = pattern1.end_time or pattern1.start_time

                # Check against all other patterns
                for j, pattern2 in enumerate(patterns):
                    if i == j:
                        continue

                    pattern2_end = pattern2.end_time or pattern2.start_time

                    # Calculate time difference in minutes
                    time_diff = abs((pattern1_end - pattern2_end).total_seconds() / 60)

                    # If any pattern is NOT within time window of another, the combination is invalid
                    if time_diff > rule.time_window_minutes:
                        return False

            return True

        # Helper function to find best combinations
        def find_combinations(current_combination: List[AtomicPattern],
                              remaining_required: List[Tuple[str, str]],
                              remaining_optional: List[Tuple[str, str]]) -> List[List[AtomicPattern]]:
            if not remaining_required and not remaining_optional:
                return [current_combination] if patterns_within_window(current_combination) else []

            combinations = []

            # Process required types first
            if remaining_required:
                pattern_type, required_count = remaining_required[0]
                available_patterns = patterns_by_type.get(pattern_type, [])
                min_required = int(required_count.rstrip('+'))
                is_greedy = required_count.endswith('+')

                if is_greedy:

                    for i in range(len(available_patterns)):
                        # Must take at least min_required patterns
                        if i + min_required > len(available_patterns):
                            break
                        new_combination = current_combination + available_patterns[i:]
                        if patterns_within_window(new_combination):
                            # Recursively find combinations with remaining types
                            combinations.extend(find_combinations(
                                new_combination,
                                remaining_required[1:],
                                remaining_optional
                            ))
                else:
                    for i in range(len(available_patterns) - min_required + 1):
                        new_combination = current_combination + available_patterns[i:i + min_required]
                        if patterns_within_window(new_combination):
                            # Recursively find combinations with remaining types
                            combinations.extend(find_combinations(
                                new_combination,
                                remaining_required[1:],
                                remaining_optional
                            ))
            else:
                pattern_type, required_count = remaining_optional[0]
                available_patterns = patterns_by_type.get(pattern_type, [])
                is_greedy = required_count == '0+'

                # Always try without this optional pattern
                combinations.extend(find_combinations(
                    current_combination,
                    [],
                    remaining_optional[1:]
                ))

                if is_greedy:
                    # For greedy optional patterns, try all available patterns
                    for i in range(len(available_patterns)):
                        new_combination = current_combination + available_patterns[i:]
                        if patterns_within_window(new_combination):
                            combinations.extend(find_combinations(
                                new_combination,
                                [],
                                remaining_optional[1:]
                            ))
                else:
                    # For non-greedy optional patterns, try taking just one
                    for pattern in available_patterns:
                        new_combination = current_combination + [pattern]
                        if patterns_within_window(new_combination):
                            combinations.extend(find_combinations(
                                new_combination,
                                [],
                                remaining_optional[1:]
                            ))

            return combinations

        valid_combinations = find_combinations([], required_types, optional_types)
        return [comb for comb in valid_combinations if len(comb) >= rule.min_patterns_required]

    def _score_combination(self, combination: List[AtomicPattern]) -> tuple:
        """Score a combination for ranking when multiple valid combinations exist."""
        # Use a tuple for lexicographic sorting
        # 1. More unique pattern types
        # 2. Most recent (by latest pattern end time)
        # 3. Highest average confidence
        unique_types = len(set(p.pattern_id for p in combination))
        latest_time = max((p.end_time or p.start_time) for p in combination).timestamp()
        avg_confidence = sum(p.severity for p in combination) / len(combination)

        return (unique_types, latest_time, avg_confidence)

    def _create_composite_pattern(self, rule: CompositePatternRule,
                                  matching_patterns: List[AtomicPattern]) -> CompositePattern:
        """
        Create a composite pattern from matched atomic patterns.
        
        Args:
            rule: The rule that matched
            matching_patterns: The atomic patterns that matched the rule
            
        Returns:
            A new Pattern object representing the composite pattern
        """
        for pattern in matching_patterns:
            pattern.consumed = True

        confidence = AggregationFactory.calculate_composite_confidence(matching_patterns)

        job_ids = set()
        for pattern in matching_patterns:
            if pattern.job_id:
                job_ids.update(pattern.job_id)

        start_times = [p.start_time for p in matching_patterns if p.start_time is not None]
        end_times = [p.end_time or p.start_time for p in matching_patterns if p.start_time is not None]

        composite_start_time = min(start_times) if start_times else None
        composite_end_time = max(end_times) if end_times else None

        details = {
            "components": [
                {
                    "internal_id": p.internal_id,
                    "pattern_id": p.pattern_id,
                    "severity": p.severity,
                }
                for p in matching_patterns
            ],
        }

        if rule.sequence_matters:
            details["sequence_dependent"] = True

        category_weights = {rule.category: 1.0}

        # secondary_categories = set()
        # for pattern in matching_patterns:
        #     if pattern.category_weights:
        #         secondary_categories.update(pattern.category_weights.keys())
        #
        # # Remove the primary category to avoid duplication
        # if rule.category in secondary_categories:
        #     secondary_categories.remove(rule.category)
        #
        # # Distribute remaining 0.3 weight among secondary categories
        # if secondary_categories:
        #     secondary_weight = 0.3 / len(secondary_categories)
        #     for category in secondary_categories:
        #         category_weights[category] = secondary_weight

        message = rule.message
        if len(matching_patterns) > 1:
            message += f" ({len(matching_patterns)} related patterns)"

        component_ids = [p.internal_id for p in matching_patterns]

        return CompositePattern(
            pattern_id=f"composite_{rule.rule_id}",
            job_id=list(job_ids) if job_ids else None,
            message=message,
            confidence=confidence,
            category_weights=category_weights,
            details=details,
            start_time=composite_start_time,
            end_time=composite_end_time,
            is_composite=True,
            component_patterns=component_ids
        )
