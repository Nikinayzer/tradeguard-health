from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any, Tuple
from collections import defaultdict
from src.models.risk_models import AtomicPattern, CompositePattern, RiskCategory
from src.utils.log_util import get_logger

logger = get_logger()


class CompositePatternRule:
    """
    Rule definition for detecting a composite pattern from atomic patterns.
    
    Attributes:
        rule_id (str): Unique identifier for the rule
        pattern_ids (List[str]): List of atomic pattern IDs this rule looks for
        sequence_matters (bool): Whether the patterns must appear in a specific sequence
        time_window_minutes (int): Time window in which patterns must occur
        category (RiskCategory): Primary risk category for this pattern
        confidence_boost (float): Boost applied to confidence if rule matches
        message (str): Message template for the resulting composite pattern
        pattern_requirements (Dict[str, int]): How many of each pattern type are required
        greedy_consumption (bool): Whether to consume all matching patterns or just required amount
    """

    def __init__(
            self,
            rule_id: str,
            pattern_ids: List[str],
            category: RiskCategory,
            time_window_minutes: int = 1440,
            sequence_matters: bool = False,
            confidence_boost: float = 0.1,
            message: str = "Composite pattern detected",
            pattern_requirements: Optional[Dict[str, int]] = None,
            greedy_consumption: bool = False
    ):
        self.rule_id = rule_id
        self.pattern_ids = pattern_ids
        self.sequence_matters = sequence_matters
        self.time_window_minutes = time_window_minutes
        self.category = category
        self.confidence_boost = confidence_boost
        self.message = message
        self.pattern_requirements = pattern_requirements or self._build_default_requirements()
        self.greedy_consumption = greedy_consumption

        self._validate_rule()

    @property
    def min_patterns_required(self) -> int:
        """Get the minimum number of patterns required based on pattern_requirements."""
        return sum(count for count in self.pattern_requirements.values() if count > 0)

    def _build_default_requirements(self) -> Dict[str, int]:
        """Build default pattern requirements (one of each pattern type)."""
        return {pattern_id: 1 for pattern_id in set(self.pattern_ids)}

    def _validate_rule(self) -> None:
        """Validate rule configuration and ensure consistency."""
        # Check if pattern_requirements are consistent with pattern_ids
        for pattern_id in self.pattern_requirements:
            if pattern_id not in self.pattern_ids and self.pattern_requirements[pattern_id] > 0:
                self.pattern_ids.append(pattern_id)

    def get_required_count(self, pattern_id: str) -> int:
        """Get required count for a specific pattern type."""
        return self.pattern_requirements.get(pattern_id, 0)

    def get_total_required_count(self) -> int:
        """Get total number of patterns required by this rule."""
        return self.min_patterns_required


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

        self.rules.append(CompositePatternRule(
            rule_id="overtrading",
            pattern_ids=["limit_daily_trades_count", "limit_cooldown", "limit_single_job_amount"],
            pattern_requirements={"limit_daily_trades_count": 1, "limit_cooldown": 1, "limit_single_job_amount": 1},
            category=RiskCategory.OVERCONFIDENCE,
            time_window_minutes=720,
            sequence_matters=False,
            confidence_boost=0.15,
            message="Multiple overtrading indicators detected"
        ))
        self.rules.append(CompositePatternRule(
            rule_id="loss_escalation",
            pattern_ids=["consecutive_loss", "position_size_increase"],
            pattern_requirements={"aaaaaaaaaa": 3},
            category=RiskCategory.SUNK_COST,
            time_window_minutes=1440,
            sequence_matters=True,
            confidence_boost=0.25,
            message="Loss followed by increasing position size"
        ))
        self.rules.append(CompositePatternRule(
            rule_id="cutting_profits",
            pattern_ids=["early_profit_exit"],
            pattern_requirements={"early_profit_exit": 3},
            category=RiskCategory.LOSS_BEHAVIOR,
            time_window_minutes=1440 * 7,
            confidence_boost=0.2,
            greedy_consumption=True,
            message=f"Multiple early profit exits detected"

        ))

    def add_rule(self, rule: CompositePatternRule):
        """Add a new composition rule to the engine."""
        self.rules.append(rule)
        logger.info(f"Added composite pattern rule: {rule.rule_id}")

    def process_patterns(self, patterns: List[AtomicPattern],
                         current_time: Optional[datetime] = None) -> List[CompositePattern]:
        """
        Process a list of atomic patterns and detect composite patterns.
        
        Args:
            patterns: List of atomic patterns from evaluators
            current_time: Current time (defaults to now)
            
        Returns:
            List of composite patterns detected
        """
        if not patterns:
            return []

        if current_time is None:
            current_time = datetime.now()

        composite_patterns = []

        patterns_by_id = self._index_patterns_by_id(patterns)

        for rule in self.rules:
            matching_patterns = self._match_rule(rule, patterns_by_id, current_time)
            if matching_patterns:
                composite_pattern = self._create_composite_pattern(rule, matching_patterns)
                composite_patterns.append(composite_pattern)
                logger.info(
                    f"Detected composite pattern: {rule.rule_id} with confidence {composite_pattern.confidence:.2f}")

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
            patterns_by_id: Dict[str, List[AtomicPattern]],
            current_time: datetime
    ) -> List[AtomicPattern]:
        """
        Check if a rule matches the available patterns based on their temporal relationships.
        
        Args:
            rule: The rule to check
            patterns_by_id: Dictionary of patterns indexed by pattern_id
            current_time: Current time for time window checks
            
        Returns:
            List of matching patterns if rule conditions are met, empty list otherwise
        """
        # Check if we have the required patterns
        if not self._has_required_patterns(rule, patterns_by_id):
            return []

        # Filter patterns by recency
        recent_patterns = self._filter_by_recency(rule, patterns_by_id, current_time)

        # Check if we still have required patterns after time filtering
        recent_patterns_by_id = self._index_patterns_by_id(recent_patterns)
        if not self._has_required_patterns(rule, recent_patterns_by_id):
            return []

        # Find valid combinations based on time windows
        valid_combinations = []

        # Use appropriate matching strategy based on rule type
        if rule.sequence_matters:
            valid_combinations = self._find_sequence_combinations(rule, recent_patterns_by_id)
        else:
            valid_combinations = self._find_window_combinations(rule, recent_patterns, current_time)

        # If we found valid combinations, select the best one
        if valid_combinations:
            valid_combinations.sort(key=self._score_combination, reverse=True)
            return valid_combinations[0]

        return []

    def _has_required_patterns(self, rule: CompositePatternRule, patterns_by_id: Dict[str, List[AtomicPattern]]) -> bool:
        """Check if we have enough patterns of each required type."""
        for pattern_id, required_count in rule.pattern_requirements.items():
            if required_count > 0:
                available_count = len(patterns_by_id.get(pattern_id, []))
                if available_count < required_count:
                    return False

        return True

    def _filter_by_recency(
            self,
            rule: CompositePatternRule,
            patterns_by_id: Dict[str, List[AtomicPattern]],
            current_time: datetime
    ) -> List[AtomicPattern]:
        """Filter patterns to only include those recent enough for this rule."""
        max_history_window = timedelta(minutes=rule.time_window_minutes * 4)  # 4x for flexibility
        earliest_history_cutoff = current_time - max_history_window

        recent_patterns = []

        for pattern_list in patterns_by_id.values():
            for pattern in pattern_list:
                p_start = pattern.start_time
                p_end = pattern.end_time or pattern.start_time

                # A pattern is "recent enough" if any part of it occurs after the cutoff
                if (p_end and p_end >= earliest_history_cutoff) or \
                        (pattern.end_time and p_start < earliest_history_cutoff and p_end >= earliest_history_cutoff):
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
                if required_count > 0 and collected_counts[pattern_id] < required_count:
                    sufficient = False
                    break

            if sequence_valid and sufficient and len(combination) >= rule.min_patterns_required:
                valid_combinations.append(combination)

        return valid_combinations

    def _find_window_combinations(
            self,
            rule: CompositePatternRule,
            recent_patterns: List[AtomicPattern],
            current_time: datetime
    ) -> List[List[AtomicPattern]]:
        """Find valid combinations by checking time relationships between patterns."""
        valid_combinations = []

        # Group patterns by type for efficient lookup
        patterns_by_type = defaultdict(list)
        for pattern in recent_patterns:
            patterns_by_type[pattern.pattern_id].append(pattern)

        # Get all required pattern types and their counts
        required_types = [(pid, count) for pid, count in rule.pattern_requirements.items() if count > 0]
        if not required_types:
            return valid_combinations

        # Sort patterns by start time for each type
        for pattern_list in patterns_by_type.values():
            pattern_list.sort(key=lambda p: p.start_time)

        # Helper function to check if patterns are within time window
        def patterns_within_window(patterns: List[AtomicPattern]) -> bool:
            if not patterns:
                return False

            # Find the earliest start and latest end
            starts = [p.start_time for p in patterns]
            ends = [p.end_time or p.start_time for p in patterns]

            earliest_start = min(starts)
            latest_end = max(ends)

            # Check if total span is within window
            return (latest_end - earliest_start).total_seconds() / 60 <= rule.time_window_minutes

        # Helper function to find best combinations
        def find_combinations(current_combination: List[AtomicPattern], remaining_types: List[Tuple[str, int]]) -> List[List[AtomicPattern]]:
            if not remaining_types:
                return [current_combination] if patterns_within_window(current_combination) else []

            pattern_type, required_count = remaining_types[0]
            available_patterns = patterns_by_type.get(pattern_type, [])

            combinations = []
            
            if rule.greedy_consumption:
                # For greedy consumption, try all available patterns
                for i in range(len(available_patterns)):
                    new_combination = current_combination + available_patterns[i:]
                    if patterns_within_window(new_combination):
                        # Recursively find combinations with remaining types
                        combinations.extend(find_combinations(new_combination, remaining_types[1:]))
            else:
                # For non-greedy consumption, only take required number of patterns
                for i in range(len(available_patterns) - required_count + 1):
                    new_combination = current_combination + available_patterns[i:i + required_count]
                    if patterns_within_window(new_combination):
                        # Recursively find combinations with remaining types
                        combinations.extend(find_combinations(new_combination, remaining_types[1:]))

            return combinations

        # Start with empty combination and all required types
        valid_combinations = find_combinations([], required_types)

        # Filter combinations that meet min_patterns_required
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

    def _create_composite_pattern(self, rule: CompositePatternRule, matching_patterns: List[AtomicPattern]) -> CompositePattern:
        """
        Create a composite pattern from matched atomic patterns.
        
        Args:
            rule: The rule that matched
            matching_patterns: The atomic patterns that matched the rule
            
        Returns:
            A new Pattern object representing the composite pattern
        """
        # Mark all component patterns as consumed
        for pattern in matching_patterns:
            pattern.consumed = True

        # Calculate the combined confidence
        base_confidence = sum(p.severity for p in matching_patterns) / len(matching_patterns)
        boosted_confidence = min(1.0, base_confidence + rule.confidence_boost)

        # Collect all job IDs
        job_ids = set()
        for pattern in matching_patterns:
            if pattern.job_id:
                job_ids.update(pattern.job_id)

        # Determine time boundaries of the composite pattern
        start_times = [p.start_time for p in matching_patterns if p.start_time is not None]
        end_times = [p.end_time or p.start_time for p in matching_patterns if p.start_time is not None]

        composite_start_time = min(start_times) if start_times else None
        composite_end_time = max(end_times) if end_times else None

        # Create a cleaner, more user-friendly details structure
        details = {
            # Component reference section
            "components": [
                {
                    "id": p.internal_id,
                    "pattern_type": p.pattern_id,
                    "severity": p.severity
                }
                for p in matching_patterns
            ],

            # Time information
            "time_span": {
                "duration_minutes": (composite_end_time - composite_start_time).total_seconds() / 60
                if composite_start_time and composite_end_time else None
            }
        }

        # Add sequence information only if relevant
        if rule.sequence_matters:
            details["sequence_dependent"] = True

        # Define category weights with primary category having highest weight
        category_weights = {rule.category: 0.7}

        # Add secondary categories with lower weights
        secondary_categories = set()
        for pattern in matching_patterns:
            if pattern.category_weights:
                secondary_categories.update(pattern.category_weights.keys())

        # Remove the primary category to avoid duplication
        if rule.category in secondary_categories:
            secondary_categories.remove(rule.category)

        # Distribute remaining 0.3 weight among secondary categories
        if secondary_categories:
            secondary_weight = 0.3 / len(secondary_categories)
            for category in secondary_categories:
                category_weights[category] = secondary_weight

        # Create a descriptive message that explains what the composite represents
        message = rule.message

        # Add component count if we have multiple components
        if len(matching_patterns) > 1:
            message += f" ({len(matching_patterns)} related patterns)"

        # Collect component pattern IDs
        component_ids = [p.internal_id for p in matching_patterns]

        # Create the composite pattern with time boundaries
        return CompositePattern(
            pattern_id=f"composite_{rule.rule_id}",
            job_id=list(job_ids) if job_ids else None,
            message=message,
            confidence=boosted_confidence,
            category_weights=category_weights,
            details=details,
            start_time=composite_start_time,
            end_time=composite_end_time,
            is_composite=True,
            component_patterns=component_ids
        )