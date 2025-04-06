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
    RiskCategory, RiskLevel, Pattern,
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


def _evaluate_in_thread(evaluator, user_id: int, job_history: Dict[int, Job]):
    """Helper method to run a single evaluator in a thread"""
    return evaluator.evaluate(user_id, job_history)


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
        self.evaluators = create_evaluators()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.evaluation_queue = Queue(maxsize=1000)
        self.state_manager = state_manager
        self.kafka_handler = None
        
        # Initialize pattern composition engine
        from src.risk.pattern_composition import PatternCompositionEngine
        self.pattern_composition_engine = PatternCompositionEngine()

        self.presets = {  # todo make normal presets
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

    def run_preset(self, preset_name: str, user_id: int, job: Optional[Job] = None):
        """Run a predefined group of evaluators"""
        logger.info(f"[RiskProcessor] run_preset called: preset={preset_name}, user_id={user_id}")
        if preset_name not in self.presets:
            raise ValueError(f"Unknown preset: {preset_name}")
        evaluator_ids = self.presets[preset_name]

        job_history = self.state_manager.get_user_jobs(user_id, 24)
        logger.warning(job_history)
        return self.run_evaluators(evaluator_ids, user_id, job_history)

    def run_evaluators(self, evaluator_ids: List[str], user_id: int, job_history: Dict[int, Job]):
        """Run specific evaluators by ID"""
        try:
            self.evaluation_queue.put(
                (evaluator_ids, user_id, job_history),
                timeout=1
            )
        except queue.Full:
            logger.warning("Evaluation queue full, dropping request")

    def _process_evaluations(self):
        """Thread that processes evaluation requests"""
        logger.info("[RiskProcessor] Evaluation thread started")
        while True:
            try:
                evaluator_ids, user_id, job_history = self.evaluation_queue.get(timeout=1)
                logger.info(f"[RiskProcessor] Got evaluation job: {evaluator_ids} for user {user_id}")
                self.executor.submit(
                    self._run_evaluators_threaded,
                    evaluator_ids,
                    user_id,
                    job_history,
                )
            except queue.Empty:
                continue

    def _run_evaluators_threaded(self,
                                 evaluator_ids: List[str],
                                 user_id: int,
                                 job_history: Dict[int, Job]
                                 ):
        """Run specified evaluators in parallel"""
        futures = {}
        all_patterns: List[Pattern] = []

        logger.info(f"[RiskProcessor] Available evaluator keys: {list(self.evaluators.keys())}")
        logger.info(f"[RiskProcessor] Looking for evaluator IDs: {evaluator_ids}")

        for evaluator_id, evaluator in self.evaluators.items():
            logger.info(f"[RiskProcessor] Checking evaluator: {evaluator_id} with type {type(evaluator)}")
            if evaluator_id in evaluator_ids:
                logger.info(f"[RiskProcessor] Matched evaluator: {evaluator_id}")
                futures[evaluator_id] = self.executor.submit(
                    _evaluate_in_thread,
                    evaluator,
                    user_id,
                    job_history,
                )
        # for evaluator in self.evaluators:
        #     if evaluator.evaluator_id in evaluator_ids:
        #         futures[evaluator.evaluator_id] = self.executor.submit(
        #             _evaluate_in_thread,
        #             evaluator,
        #             user_id,
        #             job_history,
        #         )
        for evaluator_id, future in futures.items():
            try:
                patterns = future.result(timeout=30)
                if patterns:
                    #all_patterns[evaluator_id] = patterns
                    all_patterns.extend(patterns)
                    logger.info(f"[RiskProcessor] Evaluator {evaluator_id} returned {len(patterns)} patterns")
                else:
                    logger.info(f"[RiskProcessor] Evaluator {evaluator_id} returned NO patterns")
            except Exception as e:
                logger.error(f"Error in evaluator {evaluator_id}: {str(e)}")

        if all_patterns:
            # Apply pattern composition to detect composite patterns
            try:
                composite_patterns = self.pattern_composition_engine.process_patterns(all_patterns)
                if composite_patterns:
                    logger.info(f"[RiskProcessor] Detected {len(composite_patterns)} composite patterns")
                
                # Get unconsumed patterns for logging purposes only
                unconsumed_patterns = [p for p in all_patterns if not p.consumed]
                logger.info(f"[RiskProcessor] {len(unconsumed_patterns)} unconsumed atomic patterns remain as awareness indicators")
                
                # Pass ALL original patterns (both consumed and unconsumed) to maintain full context
                # The AggregationFactory will handle them differently based on their 'consumed' flag
                report = AggregationFactory.aggregate(
                    all_patterns,       # All original patterns (including consumed ones)
                    composite_patterns, # Composite patterns get priority
                    user_id,
                    job_id=None
                )
                
                if self.kafka_handler:
                    json_string = report.model_dump_json()
                    message = json.loads(json_string)
                    self.kafka_handler.send_message(message)
                    logger.info(f"[RiskProcessor] Sent report to Kafka.")
                logger.info(f"[RiskProcessor] Aggregated report: {report.top_risk_type} at {report.top_risk_level}")
            except Exception as e:
                logger.error(f"Error in pattern composition: {str(e)}")
                # Fallback to original aggregation if composition fails
                report = AggregationFactory.aggregate(
                    all_patterns,
                    [], # Empty list for composite_patterns in fallback case
                    user_id,
                    job_id=None
                )
                
                if self.kafka_handler:
                    json_string = report.model_dump_json()
                    message = json.loads(json_string)
                    self.kafka_handler.send_message(message)
                    logger.info(f"[RiskProcessor] Sent report to Kafka (fallback mode).")
        
        return all_patterns
