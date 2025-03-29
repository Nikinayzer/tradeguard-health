"""
Trade Guard Health Service
Main application entry point, initializes the service and starts processing messages from Kafka.
"""

import signal
import sys
from datetime import datetime
from dotenv import load_dotenv

from src.config.config import Config
from src.handlers.kafka_handler import KafkaHandler
from src.processors.job_processor import JobProcessor
from src.utils.log_util import setup_logging, get_logger

setup_logging()
logger = get_logger()


class TradeGuardHealth:
    def __init__(self):
        logger.info("Starting TradeGuard Health service...")

        if error := Config.validate():
            logger.error(f"Configuration error: {error}")
            raise ValueError(error)

        # Initialize internal state
        self.jobs_state = {}
        self.dca_jobs = {}
        self.liq_jobs = {}
        self.job_to_user_map = {}  # Mapping of job_id to user_id

        self.job_processor = JobProcessor()
        self.kafka_handler = KafkaHandler(self._handle_message)

        # Load historical events to build initial state
        self._initialize_state_from_kafka()

    def _update_job_to_user_mapping(self, event: dict) -> None:
        """
        Update the job-to-user mapping based on the event.
        If it's a 'Created' event with a user_id, add the mapping.
        If user_id is missing but job_id exists and is known, fill it in.
        """
        job_id = event.get('job_id')
        event_type = event.get('event_type')
        if event_type == 'Created' and job_id and 'user_id' in event:
            self.job_to_user_map[job_id] = event['user_id']
            logger.debug(f"Mapping updated: job {job_id} -> user {event['user_id']}")
        elif job_id and 'user_id' not in event and job_id in self.job_to_user_map:
            event['user_id'] = self.job_to_user_map[job_id]
            logger.debug(f"Added user_id {event['user_id']} to event for job {job_id}")

    def _process_job_event(self, event: dict, is_historical: bool = False) -> None:
        """
        Process a single job event:
          - Update the job-to-user mapping.
          - Update internal job state via the job processor.
        """
        try:
            self._update_job_to_user_mapping(event)
            self.job_processor.update_job_state(
                event,
                self.jobs_state,
                self.dca_jobs,
                self.liq_jobs,
                is_historical=is_historical
            )
        except Exception as e:
            job_id = event.get('job_id', 'unknown')
            logger.warning(f"Error processing event for job {job_id}: {e}", exc_info=True)

    def _initialize_state_from_kafka(self) -> None:
        """
        Load initial job state by consuming historical events from the jobs topic.
        This ensures the service starts with a complete state without external dependencies.
        """
        logger.info("Initializing state from Kafka...")
        try:
            historical_events = self.kafka_handler.read_topic_from_beginning(Config.KAFKA_JOBS_TOPIC)
            count = 0
            for event in historical_events:
                self._process_job_event(event, is_historical=True)
                count += 1
                if count % 1000 == 0:
                    logger.info(f"Processed {count} historical events...")

            total_jobs = sum(len(jobs) for jobs in self.jobs_state.values())
            total_dca_jobs = sum(len(jobs) for jobs in self.dca_jobs.values())
            total_liq_jobs = sum(len(jobs) for jobs in self.liq_jobs.values())

            logger.info(f"State initialization complete. Processed {count} historical events.")
            logger.info(f"Loaded {total_jobs} jobs, {total_dca_jobs} DCA jobs, {total_liq_jobs} LIQ jobs")

            for user_id, jobs in self.jobs_state.items():
                for job_id, job in jobs.items():
                    job_name = job.get('name', 'unknown').lower()
                    job_status = job.get('status', 'unknown')
                    logger.debug(f"Loaded job: id={job_id}, user={user_id}, name={job_name}, status={job_status}")

                    in_dca = user_id in self.dca_jobs and job_id in self.dca_jobs[user_id]
                    in_liq = user_id in self.liq_jobs and job_id in self.liq_jobs[user_id]

                    if job_name != 'dca' and in_dca:
                        logger.warning(f"Job {job_id} has name '{job_name}' but is in DCA collection")
                    elif job_name != 'liq' and in_liq:
                        logger.warning(f"Job {job_id} has name '{job_name}' but is in LIQ collection")

        except Exception as e:
            logger.error(f"Failed to initialize state from Kafka: {e}", exc_info=True)
            logger.warning("Continuing with empty state...")

    def _handle_message(self, message_data: dict) -> None:
        """Handle incoming Kafka messages."""
        try:
            job_id = message_data.get('job_id')
            event_type = message_data.get('event_type')
            logger.info(f"Received {event_type} event for job {job_id} at {datetime.now().isoformat()}")
            logger.debug(f"Message data: {message_data}")

            # Process job-to-user mapping and update state
            self._process_job_event(message_data)

            # Log state counts after update
            total_jobs = sum(len(jobs) for jobs in self.jobs_state.values())
            total_dca_jobs = sum(len(jobs) for jobs in self.dca_jobs.values())
            total_liq_jobs = sum(len(jobs) for jobs in self.liq_jobs.values())
            logger.debug(f"State after update: {total_jobs} total jobs, {total_dca_jobs} DCA jobs, {total_liq_jobs} LIQ jobs")

            # Process risk analysis only for 'Created' events
            if event_type == 'Created':
                logger.info(f"Triggering risk analysis for Created event (job_id: {job_id})")
                risk_report = self.job_processor.analyze_risk(
                    message_data,
                    self.jobs_state,
                    self.dca_jobs,
                    self.liq_jobs
                )
                if risk_report:
                    triggers = [t['message'] for t in risk_report.triggers]
                    logger.info(f"Risk report generated with {len(risk_report.triggers)} triggers: {triggers}")
                    self.kafka_handler.send_message(
                        Config.KAFKA_RISK_TOPIC,
                        risk_report.model_dump()
                    )
                    logger.info(f"Risk report sent to topic {Config.KAFKA_RISK_TOPIC}")
            else:
                logger.debug(f"Skipping risk analysis for {event_type} event (job_id: {job_id})")

        except Exception as e:
            logger.error(f"Error handling message for job {message_data.get('job_id', 'unknown')}: {e}", exc_info=True)

    def run(self) -> None:
        """Main application loop."""
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            logger.info("Starting to process messages...")
            self.kafka_handler.process_messages()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self._shutdown()

    def _signal_handler(self, sig, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {sig}. Shutting down...")
        self._shutdown()
        sys.exit(0)

    def _shutdown(self):
        """Clean shutdown procedure."""
        logger.info("Closing Kafka connections...")
        self.kafka_handler.close()
        logger.info("Service stopped")


def main() -> None:
    """Application entry point."""
    load_dotenv()
    Config.validate()
    app = TradeGuardHealth()
    app.run()


if __name__ == "__main__":
    main()
