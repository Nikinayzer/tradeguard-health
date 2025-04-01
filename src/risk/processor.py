"""
Risk Processor

Manages risk evaluation for jobs by running multiple evaluators independently.
Each evaluator analyzes specific aspects of risk and sends its own report.
"""

import logging
import concurrent.futures
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Union

from src.models.risk_models import (
    RiskReport, RiskType, RiskLevel, Trigger,
    EarlyAlert, AggregatedAlert,
    SingleJobLimitTrigger, DailyTradesLimitTrigger,
    DailyVolumeLimitTrigger, TradeCooldownTrigger, ConcurrentJobsTrigger
)
from src.models import Job
from src.risk.evaluators import create_evaluators, BaseRiskEvaluator
from src.config.config import Config
from src.utils.log_util import get_logger
from src.state.state_manager import StateManager

logger = get_logger()


class RiskProcessor:
    """
    Manages risk evaluation for jobs by running multiple evaluators independently.
    Each evaluator analyzes specific aspects of risk and sends its own report.
    """

    # Risk type mapping for different categories
    RISK_TYPE_MAP = {
        "overtrading": RiskType.OVERTRADING,
        "fomo": RiskType.FOMO,
        "sunk_cost": RiskType.SUNK_COST,
        "position_size": RiskType.POSITION_SIZE,
        "time_pattern": RiskType.TIME_PATTERN,
        "portfolio_exposure": RiskType.PORTFOLIO_EXPOSURE,
        "market_volatility": RiskType.MARKET_VOLATILITY,
        "liquidity": RiskType.LIQUIDITY,
        "execution": RiskType.EXECUTION
    }

    def __init__(self, state_manager: StateManager):
        """
        Initialize the risk processor with evaluators and state manager.
        
        Args:
            state_manager: The state manager instance for accessing job state
        """
        # Create evaluators
        self.evaluators = create_evaluators()
        
        # Track enabled evaluators - by default, only user_limits is enabled
        self.enabled_evaluator_ids = set()
        if "user_limits" in self.evaluators:
            self.enabled_evaluator_ids.add("user_limits")
            logger.info("Enabled user_limits evaluator by default")
        
        self.notification_callback = None
        self.state_manager = state_manager
        self.kafka_handler = None  # Will be set by set_kafka_handler
        
        logger.info(f"Risk processor initialized with {len(self.evaluators)} evaluators, {len(self.enabled_evaluator_ids)} enabled")
        logger.debug(f"Available evaluators: {', '.join(self.evaluators.keys())}")
        logger.debug(f"Enabled evaluators: {', '.join(self.enabled_evaluator_ids)}")

    def set_notification_callback(self, callback: callable) -> None:
        """Set a callback function for risk notifications."""
        self.notification_callback = callback

    def set_kafka_handler(self, kafka_handler) -> None:
        """Set the Kafka handler for publishing alerts."""
        self.kafka_handler = kafka_handler
        
    def enable_evaluator(self, evaluator_id: str) -> None:
        """Enable a specific evaluator."""
        if evaluator_id in self.evaluators:
            self.enabled_evaluator_ids.add(evaluator_id)
            logger.info(f"Enabled evaluator: {evaluator_id}")
        else:
            logger.warning(f"Unknown evaluator ID: {evaluator_id}")
    
    def disable_evaluator(self, evaluator_id: str) -> None:
        """Disable a specific evaluator."""
        self.enabled_evaluator_ids.discard(evaluator_id)
        logger.info(f"Disabled evaluator: {evaluator_id}")
    
    def enable_all_evaluators(self) -> None:
        """Enable all available evaluators."""
        self.enabled_evaluator_ids = set(self.evaluators.keys())
        logger.info("Enabled all evaluators")
    
    def disable_all_evaluators(self) -> None:
        """Disable all evaluators."""
        self.enabled_evaluator_ids.clear()
        logger.info("Disabled all evaluators")
        
    def set_enabled_evaluators(self, evaluator_ids: List[str]) -> None:
        """Set which evaluators are enabled, disabling all others."""
        # First disable all
        self.disable_all_evaluators()
        
        # Then enable only the specified ones
        for evaluator_id in evaluator_ids:
            self.enable_evaluator(evaluator_id)
            
    def get_enabled_evaluators(self) -> Dict[str, BaseRiskEvaluator]:
        """Get dictionary of currently enabled evaluators."""
        return {
            evaluator_id: evaluator 
            for evaluator_id, evaluator in self.evaluators.items()
            if evaluator_id in self.enabled_evaluator_ids
        }
            
    def get_evaluator_status(self) -> Dict[str, bool]:
        """Get status of all evaluators (enabled/disabled)."""
        return {
            evaluator_id: evaluator_id in self.enabled_evaluator_ids
            for evaluator_id in self.evaluators
        }

    def process_job_threaded(self, job: Union[Job, Dict[str, Any]], user_id: int, max_workers: int = None) -> None:
        """
        Process a job using multiple threads for concurrent evaluation.
        
        Args:
            job: Current job (Job object or dictionary)
            user_id: The ID of the user
            max_workers: Maximum number of worker threads (default: None = auto)
        """
        # Ensure job is a Job object
        job_object = self._ensure_job_object(job)
        
        job_id = job_object.job_id
        logger.info(f"Processing job {job_id} for user {user_id} using threads")
        
        # Get user's job history from state manager
        user_jobs = self.state_manager.get_user_jobs(user_id)
        
        # Get enabled evaluators
        enabled_evaluators = self.get_enabled_evaluators()
        
        if not enabled_evaluators:
            logger.warning(f"No enabled evaluators to process job {job_id}")
            return
            
        # Dictionary to store all evaluation results
        all_results = {}
        
        # Process each enabled evaluator in a separate thread
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_evaluator = {
                executor.submit(
                    self._process_evaluator_threaded, 
                    evaluator_id, 
                    evaluator, 
                    user_id, 
                    job_object, 
                    user_jobs,
                    all_results
                ): evaluator_id
                for evaluator_id, evaluator in enabled_evaluators.items()
            }
            
            # Get results as they complete
            for future in concurrent.futures.as_completed(future_to_evaluator):
                evaluator_id = future_to_evaluator[future]
                try:
                    future.result()  # Will re-raise any exceptions from the thread
                    logger.debug(f"Completed threaded evaluation for {evaluator_id}")
                except Exception as e:
                    logger.error(f"Error in threaded evaluator {evaluator_id}: {str(e)}", exc_info=True)
        
        # After all evaluators complete, publish an aggregated report
        self._publish_aggregated_alert(job_object, user_id, all_results)

    def _process_evaluator_threaded(self, evaluator_id: str, evaluator: BaseRiskEvaluator, 
                                  user_id: int, job: Job, user_jobs: Dict[int, Job],
                                  all_results: Dict[str, List[Dict[str, Any]]]) -> None:
        """Process a single evaluator in a thread."""
        job_id = job.job_id
        try:
            # Run evaluator with current job history
            evidence = evaluator.evaluate(user_id, job, user_jobs)
            
            # Store results for aggregated report
            all_results[evaluator_id] = evidence
            
            # Create and send report
            report = self._create_report(
                job=job,
                user_id=user_id,
                evaluator_id=evaluator_id,
                evidence=evidence
            )
            
            # Check if this is a high priority alert that should be sent immediately
            risk_level = report.risk_level
            if risk_level in [RiskLevel.CRITICAL, RiskLevel.HIGH]:
                self._publish_early_alert(job, user_id, evaluator_id, report)
            
            # Send notification through callback
            self._send_report(report)
            logger.info(f"Sent threaded report from {evaluator_id} for job {job_id}")
        except Exception as e:
            logger.error(f"Error in threaded evaluator {evaluator_id}: {str(e)}")
            raise  # Re-raise to be caught by the executor

    def _ensure_job_object(self, job: Union[Job, Dict[str, Any]]) -> Job:
        """Ensure that we have a Job object, converting from dict if necessary."""
        if isinstance(job, Job):
            return job
        elif isinstance(job, dict):
            try:
                return Job.from_dict(job)
            except Exception as e:
                logger.error(f"Error converting job dictionary to Job object: {e}")
                raise
        else:
            error_msg = f"Expected Job object or dictionary, got {type(job)}"
            logger.error(error_msg)
            raise TypeError(error_msg)

    def _create_report(self, job: Job, user_id: int,
                       evaluator_id: str, evidence: List[Dict[str, Any]]) -> RiskReport:
        """Create a risk report from evaluator evidence."""
        # Get highest confidence evidence
        highest_evidence = max(evidence, key=lambda e: e.get("confidence", 0)) if evidence else {}
        confidence = highest_evidence.get("confidence", 0)
        category_id = highest_evidence.get("category_id")

        # Create triggers from evidence
        triggers = []
        for ev in evidence:
            if ev.get("confidence", 0) >= 0.4:
                reason = ev.get("data", {}).get("reason", "Unknown reason")
                trigger_type = self.RISK_TYPE_MAP.get(category_id, RiskType.OVERTRADING)
                
                # Create appropriate trigger details based on the reason
                trigger_details = None
                data = ev.get("data", {})
                
                if "Single job limit exceeded" in reason:
                    trigger_details = SingleJobLimitTrigger(
                        amount=data.get("amount", 0.0),
                        limit=data.get("limit", 0.0),
                        ratio=data.get("ratio", 0.0)
                    )
                elif "Daily trade limit exceeded" in reason:
                    trigger_details = DailyTradesLimitTrigger(
                        trade_count=data.get("trade_count", 0),
                        limit=data.get("limit", 0),
                        ratio=data.get("ratio", 0.0)
                    )
                elif "Daily volume limit exceeded" in reason:
                    trigger_details = DailyVolumeLimitTrigger(
                        daily_volume=data.get("daily_volume", 0.0),
                        limit=data.get("limit", 0.0),
                        ratio=data.get("ratio", 0.0)
                    )
                elif "Trading cooldown period violated" in reason:
                    trigger_details = TradeCooldownTrigger(
                        minutes_since_last_trade=data.get("minutes_since_last_trade", 0.0),
                        cooldown_minutes=data.get("cooldown_minutes", 0),
                        cooldown_remaining_minutes=data.get("cooldown_remaining_minutes", 0.0)
                    )
                elif "Concurrent jobs limit exceeded" in reason:
                    trigger_details = ConcurrentJobsTrigger(
                        open_jobs_count=data.get("open_jobs_count", 0),
                        limit=data.get("limit", 0),
                        ratio=data.get("ratio", 0.0)
                    )
                
                trigger = Trigger(
                    job_id=job.job_id,
                    message=reason,
                    type=trigger_type.value,
                    details=trigger_details
                )
                triggers.append(trigger)

        return RiskReport(
            user_id=user_id,
            job_id=job.job_id,
            risk_type=self.RISK_TYPE_MAP.get(category_id, RiskType.OVERTRADING),
            risk_level=self._get_risk_level(confidence),
            confidence=confidence,
            triggers=triggers,
            evidence=evidence,
            evaluator_id=evaluator_id
        )

    def _send_report(self, report: RiskReport) -> None:
        """Send report through notification callback."""
        if self.notification_callback:
            try:
                trigger_messages = [trigger.message for trigger in report.triggers] if report.triggers else []
                
                self.notification_callback(
                    report.user_id,
                    report.risk_level,
                    {
                        "job_id": report.job_id,
                        "risk_type": report.risk_type,
                        "triggers": trigger_messages,
                        "evaluator_id": report.evaluator_id
                    }
                )
            except Exception as e:
                logger.error(f"Error sending report: {str(e)}")

    def _publish_early_alert(self, job: Job, user_id: int, evaluator_id: str, report: RiskReport) -> None:
        """Publish an early alert for critical or high risk issues."""
        if not self.kafka_handler or not Config.KAFKA_RISK_NOTIFICATIONS_TOPIC:
            return
            
        try:
            # Format triggers with detailed information
            formatted_triggers = []
            for trigger in report.triggers:
                trigger_data = {
                    "message": trigger.message,
                    "type": trigger.type,
                }
                
                # Add detailed information if available
                if trigger.details:
                    trigger_data["details"] = trigger.details.model_dump() if trigger.details else {}
                
                formatted_triggers.append(trigger_data)
            
            # Create an early alert using the EarlyAlert model
            alert = EarlyAlert(
                timestamp=datetime.now().isoformat(),
                user_id=user_id,
                job_id=job.job_id,
                job_timestamp=job.timestamp,
                risk_category=report.risk_type.value,
                risk_level=report.risk_level.value,
                confidence=report.confidence,
                evaluator_id=evaluator_id,
                triggers=formatted_triggers,
                risk_signature=f"job:{job.job_id}:cat:{report.risk_type.value}:eval:{evaluator_id}",
                decay_params={
                    "initial_priority": 100,
                    "half_life_minutes": 60,
                    "min_priority": 10
                }
            )
            
            # Publish to Kafka
            self.kafka_handler.send_message(alert.model_dump())
            logger.info(f"Published early alert for job {job.job_id} from {evaluator_id}")
            
        except Exception as e:
            logger.error(f"Error publishing early alert: {str(e)}")

    def _publish_aggregated_alert(self, job: Job, user_id: int, 
                                 all_results: Dict[str, List[Dict[str, Any]]]) -> None:
        """Publish an aggregated alert with results from all evaluators."""
        if not self.kafka_handler or not Config.KAFKA_RISK_NOTIFICATIONS_TOPIC or not all_results:
            return
            
        try:
            # Create a list of all risks detected
            risks = []
            highest_level = RiskLevel.NONE
            highest_risk = None
            
            for evaluator_id, evidence_list in all_results.items():
                if not evidence_list:
                    continue
                    
                # Get highest confidence evidence from this evaluator
                highest_evidence = max(evidence_list, key=lambda e: e.get("confidence", 0))
                confidence = highest_evidence.get("confidence", 0)
                category_id = highest_evidence.get("category_id", "unknown")
                risk_type = self.RISK_TYPE_MAP.get(category_id, RiskType.OVERTRADING)
                
                # Create trigger messages with detailed information
                trigger_details = []
                for evidence in evidence_list:
                    if evidence.get("confidence", 0) >= 0.4:
                        data = evidence.get("data", {})
                        reason = data.get("reason", "Unknown reason")
                        
                        # Create a trigger with detailed information
                        trigger_data = {
                            "message": reason,
                            "type": risk_type.value,
                            "details": {}
                        }
                        
                        # Add specific details based on the trigger type
                        if "Single job limit exceeded" in reason and "amount" in data:
                            trigger_data["details"] = {
                                "amount": data.get("amount", 0.0),
                                "limit": data.get("limit", 0.0),
                                "ratio": data.get("ratio", 0.0)
                            }
                        elif "Daily trade limit exceeded" in reason and "trade_count" in data:
                            trigger_data["details"] = {
                                "trade_count": data.get("trade_count", 0),
                                "limit": data.get("limit", 0),
                                "ratio": data.get("ratio", 0.0)
                            }
                        elif "Daily volume limit exceeded" in reason and "daily_volume" in data:
                            trigger_data["details"] = {
                                "daily_volume": data.get("daily_volume", 0.0),
                                "limit": data.get("limit", 0.0),
                                "ratio": data.get("ratio", 0.0)
                            }
                        elif "Trading cooldown period violated" in reason:
                            trigger_data["details"] = {
                                "minutes_since_last_trade": data.get("minutes_since_last_trade", 0.0),
                                "cooldown_minutes": data.get("cooldown_minutes", 0),
                                "cooldown_remaining_minutes": data.get("cooldown_remaining_minutes", 0.0)
                            }
                        elif "Concurrent jobs limit exceeded" in reason:
                            trigger_data["details"] = {
                                "open_jobs_count": data.get("open_jobs_count", 0),
                                "limit": data.get("limit", 0),
                                "ratio": data.get("ratio", 0.0)
                            }
                        
                        trigger_details.append(trigger_data)
                
                # Skip if no triggers
                if not trigger_details:
                    continue
                
                # Get risk level
                risk_level = self._get_risk_level(confidence)
                
                # Track highest risk level
                if risk_level.value > highest_level.value:
                    highest_level = risk_level
                    highest_risk = {
                        "level": risk_level.value,
                        "category": risk_type.value,
                        "evaluator": evaluator_id
                    }
                
                # Add to risks list
                risks.append({
                    "category": risk_type.value,
                    "level": risk_level.value,
                    "confidence": confidence,
                    "evaluator_id": evaluator_id,
                    "triggers": trigger_details
                })
            
            # Skip if no risks detected
            if not risks:
                logger.info(f"No risks detected for job {job.job_id}, skipping aggregated alert")
                return
            
            # Create the aggregated alert using the AggregatedAlert model
            alert = AggregatedAlert(
                timestamp=datetime.now().isoformat(),
                user_id=user_id,
                job_id=job.job_id,
                job_timestamp=job.timestamp,
                evaluator_count=len(all_results),
                highest_risk=highest_risk,
                risks=risks,
                decay_params={
                    "initial_priority": 100,
                    "half_life_minutes": 60,
                    "min_priority": 10
                }
            )
            
            # Publish to Kafka
            self.kafka_handler.send_message(alert.model_dump())
            logger.info(f"Published aggregated alert for job {job.job_id} with {len(risks)} risks")
            
        except Exception as e:
            logger.error(f"Error publishing aggregated alert: {str(e)}")

    def _get_risk_level(self, confidence: float) -> RiskLevel:
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
