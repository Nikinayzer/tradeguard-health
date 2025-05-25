"""
Risk Processor

Manages risk evaluation for jobs by running multiple evaluators independently.
Each evaluator analyzes specific aspects of risk and sends its own report.
"""
import json
import queue
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue
from threading import Thread
from typing import Dict, List, Any, Optional, Set, Union

from src.models.risk_models import (
    RiskCategory, RiskLevel, AtomicPattern, CompositePattern
)
from src.models import Job
from src.risk.aggregation_factory import AggregationFactory
from src.risk.evaluators import create_evaluators, BaseRiskEvaluator
from src.config.config import Config
from src.utils.log_util import get_logger
from src.state.state_manager import StateManager

logger = get_logger()


def _get_risk_level(confidence: float) -> RiskLevel:
    """Convert confidence to risk level."""
    if confidence >= 0.7:
        return RiskLevel.CRITICAL
    elif confidence >= 0.5:
        return RiskLevel.HIGH
    elif confidence >= 0.3:
        return RiskLevel.MEDIUM
    elif confidence > 0:
        return RiskLevel.LOW
    return RiskLevel.NONE


def _evaluate_in_thread(evaluator: BaseRiskEvaluator, user_id: int) -> List[AtomicPattern]:
    """Helper method to run a single evaluator in a thread"""
    return evaluator.evaluate(user_id)


class RiskProcessor:
    """
    Manages risk evaluation for jobs by running multiple evaluators independently.
    Each evaluator analyzes specific aspects of risk and sends its own report.
    """
    def __init__(self, state_manager: StateManager):
        """
        Initialize the risk processor with evaluators and state manager.
        
        Args:
            state_manager: The state manager instance for accessing job state
        """
        self.evaluators = create_evaluators(state_manager)
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.evaluation_queue = Queue(maxsize=1000)
        self.state_manager = state_manager
        self.kafka_handler = None
        
        # Initialize pattern composition engine
        from src.risk.pattern_composition import PatternCompositionEngine
        self.pattern_composition_engine = PatternCompositionEngine()

        self.presets = {  # todo make normal presets
        #    "limits_only": ["user_limits", "positions_evaluator"],
            "limits_only": ["user_limits"],
            "overtrading": ["overtrading_evaluator", "time_pattern_evaluator"],
            "daily_risk": ["portfolio_exposure_evaluator", "position_size_evaluator"],
            "all": [e.evaluator_id for e in self.evaluators.values()]
        }

        self.queue_thread = Thread(target=self._process_evaluations, daemon=True)
        self.queue_thread.start()

        logger.info(f"Risk processor initialized.")
        logger.debug(f"Available evaluators: {', '.join(self.evaluators.keys())}")

    def set_kafka_handler(self, kafka_handler) -> None:
        """Set the Kafka handler for publishing alerts."""
        self.kafka_handler = kafka_handler

    def run_preset(self, preset_name: str, user_id: int):
        """Run a predefined group of evaluators"""
        logger.info(f"[RiskProcessor] run_preset called: preset={preset_name}, user_id={user_id}")
        if preset_name not in self.presets:
            raise ValueError(f"Unknown preset: {preset_name}")
        evaluator_ids = self.presets[preset_name]
        return self.run_evaluators(evaluator_ids, user_id)

    def run_evaluators(self, evaluator_ids: List[str], user_id: int):
        """Run specific evaluators by ID"""
        try:
            self.evaluation_queue.put(
                (evaluator_ids, user_id),
                timeout=1
            )
        except queue.Full:
            logger.warning("Evaluation queue full, dropping request")

    def _process_evaluations(self):
        """Thread that processes evaluation requests"""
        logger.info("[RiskProcessor] Evaluation thread started")
        while True:
            try:
                evaluator_ids, user_id = self.evaluation_queue.get(timeout=1)
                logger.info(f"[RiskProcessor] Got evaluation job: {evaluator_ids} for user {user_id}")
                self.executor.submit(
                    self._run_evaluators_threaded,
                    evaluator_ids,
                    user_id,
                )
            except queue.Empty:
                continue

    def _run_evaluators_threaded(self,
                                 evaluator_ids: List[str],
                                 user_id: int
                                 ):
        """Run specified evaluators in parallel"""
        futures = {}
        all_patterns: List[AtomicPattern] = []

        logger.info(f"[RiskProcessor] Starting evaluation for user {user_id} with evaluators: {evaluator_ids}")
        logger.info(f"[RiskProcessor] Available evaluator keys: {list(self.evaluators.keys())}")

        for evaluator_id, evaluator in self.evaluators.items():
            logger.info(f"[RiskProcessor] Checking evaluator: {evaluator_id} with type {type(evaluator)}")
            if evaluator_id in evaluator_ids:
                logger.info(f"[RiskProcessor] Matched evaluator: {evaluator_id}")
                futures[evaluator_id] = self.executor.submit(
                    _evaluate_in_thread,
                    evaluator,
                    user_id,
                )

        for evaluator_id, future in futures.items():
            try:
                patterns = future.result(timeout=30)
                if patterns:
                    all_patterns.extend(patterns)
                    logger.info(f"[RiskProcessor] Evaluator {evaluator_id} returned {len(patterns)} patterns")
                    for pattern in patterns:
                        logger.debug(f"[RiskProcessor] Pattern from {evaluator_id}: {pattern.pattern_id} (severity: {pattern.severity})")
                else:
                    logger.info(f"[RiskProcessor] Evaluator {evaluator_id} returned NO patterns")
            except Exception as e:
                logger.error(f"Error in evaluator {evaluator_id}: {str(e)}")

        if all_patterns:
            logger.info(f"[RiskProcessor] Total patterns collected: {len(all_patterns)}")
            
            # Store atomic patterns - the storage will handle unique vs non-unique patterns
            self.state_manager.pattern_storage.store_patterns(user_id, all_patterns)
            logger.info(f"[RiskProcessor] Stored {len(all_patterns)} patterns in pattern storage")
            
            # Get patterns from storage to ensure we have all non-unique patterns
            stored_patterns = self.state_manager.pattern_storage.get_user_patterns(user_id)
            logger.info(f"[RiskProcessor] Retrieved {len(stored_patterns)} patterns from storage")
            
            # Log current state before composition
            try:
                logger.info("[RiskProcessor] Getting current pattern state...")
                unique_patterns = [p for p in stored_patterns if p.unique]
                non_unique_patterns = [p for p in stored_patterns if not p.unique]
                consumed_patterns = [p for p in stored_patterns if p.consumed]
                unconsumed_patterns = [p for p in stored_patterns if not p.consumed]
                
                logger.info(f"[RiskProcessor] Current pattern state for user {user_id}:")
                logger.info(f"  Total patterns: {len(stored_patterns)}")
                logger.info(f"  Unique patterns: {len(unique_patterns)}")
                logger.info(f"  Non-unique patterns: {len(non_unique_patterns)}")
                logger.info(f"  Consumed patterns: {len(consumed_patterns)}")
                logger.info(f"  Unconsumed patterns: {len(unconsumed_patterns)}")
                
                # Log pattern IDs for debugging
                logger.debug("Unique pattern IDs: " + ", ".join(p.pattern_id for p in unique_patterns))
                logger.debug("Non-unique pattern IDs: " + ", ".join(p.pattern_id for p in non_unique_patterns))
            except Exception as e:
                logger.error(f"[RiskProcessor] Error getting pattern state: {str(e)}", exc_info=True)
            
            # Apply pattern composition to detect composite patterns
            try:
                logger.info("[RiskProcessor] Starting pattern composition")
                logger.info(f"[RiskProcessor] Input patterns for composition: {[p.pattern_id for p in stored_patterns]}")
                composite_patterns = self.pattern_composition_engine.process_patterns(stored_patterns)
                if composite_patterns:
                    logger.info(f"[RiskProcessor] Detected {len(composite_patterns)} composite patterns")
                    for pattern in composite_patterns:
                        logger.info(f"[RiskProcessor] Composite pattern: {pattern.pattern_id} (confidence: {pattern.confidence})")
                        logger.info(f"[RiskProcessor] Composite pattern components: {pattern.component_patterns}")
                else:
                    logger.info("[RiskProcessor] No composite patterns detected")
                
                # Get unconsumed patterns for logging purposes only
                unconsumed_patterns = [p for p in stored_patterns if not p.consumed]
                logger.info(f"[RiskProcessor] {len(unconsumed_patterns)} unconsumed atomic patterns remain as awareness indicators")
                
                # Pass patterns from storage to maintain full context including non-unique patterns
                logger.info("[RiskProcessor] Starting pattern aggregation")
                logger.info(f"[RiskProcessor] Input for aggregation:")
                logger.info(f"  - Atomic patterns: {len(stored_patterns)}")
                logger.info(f"  - Composite patterns: {len(composite_patterns)}")
                report = AggregationFactory.aggregate(
                    stored_patterns,  # Use patterns from storage to include non-unique patterns
                    composite_patterns, # Composite patterns get priority
                    user_id,
                    job_id=None
                )
                
                logger.info(f"[RiskProcessor] Aggregation complete. Top risk: {report.top_risk_type} at {report.top_risk_level} (confidence: {report.top_risk_confidence})")
                logger.info(f"[RiskProcessor] Category scores: {report.category_scores}")
                
                if self.kafka_handler:
                    logger.info("[RiskProcessor] Preparing to send report to Kafka")
                    try:
                        json_string = report.model_dump_json()
                        message = json.loads(json_string)
                        self.kafka_handler.send_message(message)
                        logger.info(f"[RiskProcessor] Successfully sent report to Kafka. Report details:")
                        logger.info(f"  - Top risk: {report.top_risk_type} at {report.top_risk_level}")
                        logger.info(f"  - Confidence: {report.top_risk_confidence}")
                        logger.info(f"  - Categories: {list(report.category_scores.keys())}")
                        logger.info(f"  - Atomic patterns: {len(report.patterns)}")
                        logger.info(f"  - Composite patterns: {len(report.composite_patterns)}")
                    except Exception as e:
                        logger.error(f"[RiskProcessor] Error sending report to Kafka: {str(e)}", exc_info=True)
                else:
                    logger.warning("[RiskProcessor] No Kafka handler configured - report not sent")
                
            except Exception as e:
                logger.error(f"[RiskProcessor] Error in pattern composition/aggregation: {str(e)}", exc_info=True)
                # Fallback to original aggregation if composition fails
                logger.info("[RiskProcessor] Attempting fallback aggregation")
                try:
                    report = AggregationFactory.aggregate(
                        all_patterns,
                        [], # Empty list for composite_patterns in fallback case
                        user_id,
                        job_id=None
                    )
                    
                    if self.kafka_handler:
                        try:
                            json_string = report.model_dump_json()
                            message = json.loads(json_string)
                            self.kafka_handler.send_message(message)
                            logger.info(f"[RiskProcessor] Sent fallback report to Kafka. Report details:")
                            logger.info(f"  - Top risk: {report.top_risk_type} at {report.top_risk_level}")
                            logger.info(f"  - Confidence: {report.top_risk_confidence}")
                        except Exception as e:
                            logger.error(f"[RiskProcessor] Error sending fallback report to Kafka: {str(e)}", exc_info=True)
                except Exception as e:
                    logger.error(f"[RiskProcessor] Error in fallback aggregation: {str(e)}", exc_info=True)
        
        return all_patterns
