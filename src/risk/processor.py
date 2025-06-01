"""
Risk Processor

Manages risk evaluation for jobs by running multiple evaluators independently.
Each evaluator analyzes specific aspects of risk and sends its own report.
"""
import json
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Thread
from typing import List

from src.models.risk_models import (
    RiskLevel, AtomicPattern
)
from src.risk.aggregation_factory import AggregationFactory
from src.risk.evaluators import create_evaluators, BaseRiskEvaluator
from src.utils.log_util import get_logger
from src.state.state_manager import StateManager

logger = get_logger()


def _evaluate_in_thread(evaluator: BaseRiskEvaluator, user_id: int) -> List[AtomicPattern]:
    """Helper method to run a single evaluator in a thread"""
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        if asyncio.iscoroutinefunction(evaluator.evaluate):
            return loop.run_until_complete(evaluator.evaluate(user_id))
        else:
            return evaluator.evaluate(user_id)
    finally:
        loop.close()


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

        self.presets = {
            "default": ["user_limits", "positions_evaluator"],
            "positions_only": ["positions_evaluator"],
            "limits_only": ["user_limits"],
            "all": [e.evaluator_id for e in self.evaluators.values()]
        }

        self.queue_thread = Thread(target=self._process_evaluations, daemon=True)
        self.queue_thread.start()

        self.periodic_thread = Thread(target=self._run_periodic_evaluation, daemon=True)
        self.periodic_thread.start()

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

        for evaluator_id, evaluator in self.evaluators.items():
            if evaluator_id in evaluator_ids:
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

            patterns_by_key = {}
            for pattern in all_patterns:
                key = (pattern.pattern_id, pattern.position_key)
                if key not in patterns_by_key:
                    patterns_by_key[key] = pattern
                else:
                    if pattern.severity > patterns_by_key[key].severity:
                        patterns_by_key[key] = pattern

            deduplicated_patterns = list(patterns_by_key.values())
            logger.info(f"[RiskProcessor] Deduplicated patterns: {len(deduplicated_patterns)}")

            self.state_manager.pattern_storage.store_patterns(user_id, deduplicated_patterns)
            logger.info(f"[RiskProcessor] Stored {len(deduplicated_patterns)} patterns in pattern storage")

            stored_patterns = self.state_manager.pattern_storage.get_user_patterns(user_id)
            logger.info(f"[RiskProcessor] Retrieved {len(stored_patterns)} patterns from storage")

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

                logger.info("[RiskProcessor] Starting pattern aggregation")
                logger.info(f"[RiskProcessor] Input for aggregation:")
                logger.info(f"  - Atomic patterns: {len(stored_patterns)}")
                logger.info(f"  - Composite patterns: {len(composite_patterns)}")
                report = AggregationFactory.aggregate(
                    stored_patterns,
                    composite_patterns,
                    user_id,
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
                logger.info("[RiskProcessor] Attempting fallback aggregation")
                try:
                    report = AggregationFactory.aggregate(
                        all_patterns,
                        [],
                        user_id,
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

    def _run_periodic_evaluation(self):
        """Thread that runs periodic evaluation of all users' positions."""
        logger.info("[RiskProcessor] Periodic evaluation thread started")
        while True:
            try:
                all_positions = self.state_manager.position_storage.get_all_positions()
                for user_id in all_positions.keys():
                    self.run_preset("positions_only", user_id)

                time.sleep(20) # todo 60
            except Exception as e:
                logger.error(f"[RiskProcessor] Error in periodic evaluation: {str(e)}", exc_info=True)
                time.sleep(20)
