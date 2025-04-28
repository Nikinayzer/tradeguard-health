"""
Trade Guard Health Service
Main application entry point, initializes the service and starts processing messages from Kafka.
"""

import signal
import sys
import threading
import time
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

from src.config.config import Config
from src.handlers.kafka_handler import KafkaHandler, TopicConfig
from src.models import JobEvent, Created, Job, Position, Equity

from src.dashboard.web_dashboard import WebDashboard
from src.utils.log_util import setup_logging, get_logger
from src.risk.processor import RiskProcessor
from src.state.state_manager import StateManager

setup_logging()
logger = get_logger()


class TradeGuardHealth:
    def __init__(self):
        logger.info("Starting TradeGuard Health service...")

        if error := Config.validate():
            logger.error(f"Configuration error: {error}")
            raise ValueError(error)

        self.state_manager = StateManager()
        logger.info("State manager initialized")

        self.job_handler = KafkaHandler(
            Config.KAFKA_TOPIC_JOB_UPDATES,
            JobEvent,
            JobEvent.from_dict
        )
        self.position_handler = KafkaHandler(
            Config.KAFKA_TOPIC_POSITION_UPDATES,
            Position,
            Position.from_dict
        )
        self.equity_handler = KafkaHandler(
            Config.KAFKA_TOPIC_EQUITY,
            Equity,
            Equity.from_dict
        )
        self.risk_notification_handler = KafkaHandler(
            Config.KAFKA_TOPIC_RISK_NOTIFICATIONS,
            dict,
            lambda x: x
        )

        self.risk_processor = RiskProcessor(self.state_manager)
        self.risk_processor.set_kafka_handler(self.risk_notification_handler)
        logger.info("Risk processor initialized")

        self._initialize_state_from_kafka()

        self.web_dashboard = None
        self.web_dashboard_thread = None

        if Config.ENABLE_WEB_DASHBOARD:
            self._initialize_web_dashboard()
        self._update_dashboards()

    def _initialize_web_dashboard(self) -> None:
        """Initialize the web dashboard."""
        try:
            logger.info("Initializing web dashboard...")
            self.web_dashboard = WebDashboard()
            self.web_dashboard_thread = self.web_dashboard.start_server_in_thread()
            logger.info(f"Web dashboard started at http://localhost:{Config.DASHBOARD_PORT}")
        except Exception as e:
            logger.error(f"Failed to initialize web dashboard: {e}", exc_info=True)
            self.web_dashboard = None
            self.web_dashboard_thread = None

    def _update_dashboards(self) -> None:
        """Update all dashboards with the current state."""
        logger.debug("Updating dashboards...")
        if self.web_dashboard:
            try:
                positions_state = self.state_manager.get_positions_state()
                equity_state = self.state_manager.get_equity_state()

                self.web_dashboard.set_state_data(
                    self.state_manager.get_jobs_state(),
                    self.state_manager.get_dca_jobs(),
                    self.state_manager.get_liq_jobs(),
                    self.state_manager.get_job_to_user_map(),
                    positions_state,
                    equity_state
                )
            except Exception as e:
                logger.error(f"Error updating web dashboard: {e}", exc_info=True)

    def _process_job_event(self, event: JobEvent, is_historical: bool = False) -> None:
        """
        Process a single job event:
        1. Create or update job based on event
        2. Store job in state manager
        3. Trigger risk analysis if needed
        """
        try:
            # Create or update job based on event
            if isinstance(event.type, Created):
                job = Job.create_from_event(event)
                logger.info(f"Created new job {job.job_id} from {event.type} event")
            else:
                job = self.state_manager.get_job(event.job_id)
                if not job:
                    logger.warning(f"Received event for non-existent job: {event.job_id}")
                    return
                job.apply_event(event)
                logger.info(f"Updated job {job.job_id} with {event.type} event")

            self.state_manager.store_job(job)
            logger.debug(f"Stored job {job.job_id} in state manager")

            # Run risk analysis for non-historical events
            if not is_historical and isinstance(event.type, Created):
                self._update_dashboards()
                try:
                    user_id = job.user_id
                    self.risk_processor.run_preset("limits_only", user_id)
                except Exception as e:
                    logger.error(f"Error in risk processing for job {job.job_id}: {str(e)}", exc_info=True)

        except Exception as e:
            logger.error(f"Error processing event for job {event.job_id}: {e}", exc_info=True)

    def _process_position(self, position: Position) -> None:
        """
        Process a position update:
        1. Store position in state manager
        """
        try:
            self.state_manager.store_position(position)
            logger.info(f"Updated position for {position.symbol} on {position.venue} for user {position.user_id}")
            if self.web_dashboard:
                self._update_dashboards()
        except Exception as e:
            logger.error(f"Error processing position: {e}", exc_info=True)

    def _process_equity(self, equity: Equity) -> None:
        """
        Process an equity update:
        1. Store equity in state manager
        """
        try:
            self.state_manager.store_equity(equity)
            logger.info(f"Updated equity for {equity.equity_key}")
            if self.web_dashboard:
                self._update_dashboards()
        except Exception as e:
            logger.error(f"Error processing equity: {e}", exc_info=True)

    def _initialize_state_from_kafka(self) -> None:
        """
        Load initial job state by consuming historical events from the jobs topic.
        This ensures the service starts with a complete state without external dependencies.
        """
        logger.info("Initializing state from Kafka...")
        try:
            historical_events = self.job_handler.read_topic_from_beginning()
            count = 0
            for event in historical_events:
                self._process_job_event(event, is_historical=True)
                count += 1
                if count % 1000 == 0:
                    logger.info(f"Processed {count} historical events...")

            # Get final state counts
            jobs_state = self.state_manager.get_jobs_state()
            dca_jobs = self.state_manager.get_dca_jobs()
            liq_jobs = self.state_manager.get_liq_jobs()

            total_jobs = sum(len(jobs) for jobs in jobs_state.values())
            total_dca_jobs = sum(len(jobs) for jobs in dca_jobs.values())
            total_liq_jobs = sum(len(jobs) for jobs in liq_jobs.values())

            logger.info(f"State initialization complete. Processed {count} historical events.")
            logger.info(f"Loaded {total_jobs} jobs: {total_dca_jobs} DCA jobs, {total_liq_jobs} LIQ jobs")
            logger.info(jobs_state)

            logger.info("Initializing positions from Kafka...")
            try:
                historical_positions = self.position_handler.read_topic_from_beginning()
                position_count = 0
                for position in historical_positions:
                    self.state_manager.store_position(position)
                    position_count += 1
                    if position_count % 100 == 0:
                        logger.info(f"Processed {position_count} historical positions...")

                logger.info(f"Loaded {position_count} positions")
            except Exception as e:
                logger.error(f"Failed to initialize positions from Kafka: {e}", exc_info=True)
                logger.warning("Continuing with empty positions state...")

            logger.info("Initializing equity from Kafka...")
            try:
                historical_equities = self.equity_handler.read_topic_from_beginning()
                equity_count = 0
                for equity in historical_equities:
                    self.state_manager.store_equity(equity)
                    equity_count += 1
                    if equity_count % 100 == 0:
                        logger.info(f"Processed {equity_count} historical equities...")

                logger.info(f"Loaded {equity_count} equities")
            except Exception as e:
                logger.error(f"Failed to initialize positions from Kafka: {e}", exc_info=True)
                logger.warning("Continuing with empty positions state...")

        except Exception as e:
            logger.error(f"Failed to initialize state from Kafka: {e}", exc_info=True)
            logger.warning("Continuing with empty state...")

    def run(self) -> None:
        """Main application loop."""
        logger.info("TradeGuard Health service is running")

        # Setup signal handlers for graceful shutdown
        def signal_handler(sig, frame):
            logger.info("Shutdown signal received, stopping service...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            logger.info("Starting to process job messages...")
            job_thread = threading.Thread(
                target=self.job_handler.process_messages,
                args=(self._process_job_event,),
                daemon=True
            )
            job_thread.start()

            logger.info("Starting to process position messages...")
            position_thread = threading.Thread(
                target=self.position_handler.process_messages,
                args=(self._process_position,),
                daemon=True
            )
            position_thread.start()

            logger.info("Starting to process equity messages...")
            equity_thread = threading.Thread(
                target=self.equity_handler.process_messages,
                args=(self._process_equity,),
                daemon=True
            )
            equity_thread.start()

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the service and clean up resources."""
        logger.info("Stopping TradeGuard Health service...")

        # Close the Kafka handlers
        try:
            if hasattr(self, 'job_handler') and self.job_handler:
                self.job_handler.close()
                logger.info("Job Kafka handler closed")

            if hasattr(self, 'risk_notification_handler') and self.risk_notification_handler:
                self.risk_notification_handler.close()
                logger.info("Risk notification Kafka handler closed")

            if hasattr(self, 'position_handler') and self.position_handler:
                self.position_handler.close()
                logger.info("Position Kafka handler closed")
        except Exception as e:
            logger.error(f"Error closing Kafka handlers: {e}", exc_info=True)

        # Stop dashboards if running
        if self.web_dashboard:
            try:
                self.web_dashboard.stop_server()
                logger.info("Web dashboard stopped")
            except Exception as e:
                logger.error(f"Error stopping web dashboard: {e}", exc_info=True)

        logger.info("TradeGuard Health service stopped")


def main() -> None:
    """Application entry point."""
    load_dotenv()
    Config.validate()
    app = TradeGuardHealth()
    app.run()


if __name__ == "__main__":
    main()
