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

from src.config.config import Config
from src.handlers.kafka_handler import KafkaHandler
from src.processors.job_processor import JobProcessor
from src.dashboard.web_dashboard import WebDashboard
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

        # Initialize job processor and kafka handler
        self.job_processor = JobProcessor()
        self.kafka_handler = KafkaHandler(self._handle_message)

        # Load historical events to build initial state
        self._initialize_state_from_kafka()

        # Initialize dashboards if enabled
        self.dashboard = None
        self.dashboard_thread = None
        self.web_dashboard = None
        self.web_dashboard_thread = None

        if Config.ENABLE_WEB_DASHBOARD:
            self._initialize_web_dashboard()
        # Update dashboards with initial state
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
        # Update terminal dashboard
        if self.dashboard:
            try:
                self.dashboard.set_state_data(
                    self.jobs_state,
                    self.dca_jobs,
                    self.liq_jobs,
                    self.job_to_user_map
                )
            except Exception as e:
                logger.error(f"Error updating terminal dashboard: {e}", exc_info=True)
                
        # Update web dashboard
        if self.web_dashboard:
            try:
                self.web_dashboard.set_state_data(
                    self.jobs_state,
                    self.dca_jobs,
                    self.liq_jobs,
                    self.job_to_user_map
                )
            except Exception as e:
                logger.error(f"Error updating web dashboard: {e}", exc_info=True)

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
                self.job_to_user_map,
                is_historical=is_historical
            )
            
            # Update dashboards periodically during historical loading to avoid too frequent updates
            if is_historical and event.get('job_id', 0) % 1000 == 0:
                self._update_dashboards()
                
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

            # Validate mappings after initialization
            self._validate_state_mappings()

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
            
    def _validate_state_mappings(self) -> None:
        """
        Validate the consistency of state mappings between jobs_state and job_to_user_map.
        Identifies and logs any discrepancies that could cause issues.
        """
        logger.info("Validating state mappings...")
        
        # Check 1: All jobs in job_to_user_map should exist in jobs_state
        missing_in_state = []
        for job_id, user_id in self.job_to_user_map.items():
            if user_id not in self.jobs_state or job_id not in self.jobs_state[user_id]:
                missing_in_state.append((job_id, user_id))
        
        if missing_in_state:
            logger.warning(f"Found {len(missing_in_state)} jobs in job_to_user_map but missing in jobs_state")
            for job_id, user_id in missing_in_state[:10]:  # Limit to first 10 for log readability
                logger.warning(f"Job {job_id} mapped to user {user_id} is missing in jobs_state")
        
        # Check 2: All jobs in jobs_state should be in job_to_user_map
        missing_in_map = []
        for user_id, jobs in self.jobs_state.items():
            for job_id in jobs:
                if job_id not in self.job_to_user_map:
                    missing_in_map.append((job_id, user_id))
        
        if missing_in_map:
            logger.warning(f"Found {len(missing_in_map)} jobs in jobs_state but missing in job_to_user_map")
            for job_id, user_id in missing_in_map[:10]:  # Limit to first 10 for log readability
                logger.warning(f"Job {job_id} for user {user_id} is missing in job_to_user_map")
                # Automatically fix this by adding to the map
                self.job_to_user_map[job_id] = user_id
                logger.info(f"Added missing mapping: job {job_id} -> user {user_id}")
        
        # Check 3: Verify user_id consistency between job_to_user_map and jobs_state
        inconsistent_mappings = []
        for user_id, jobs in self.jobs_state.items():
            for job_id in jobs:
                if job_id in self.job_to_user_map and self.job_to_user_map[job_id] != user_id:
                    inconsistent_mappings.append((job_id, user_id, self.job_to_user_map[job_id]))
        
        if inconsistent_mappings:
            logger.error(f"Found {len(inconsistent_mappings)} jobs with inconsistent user mappings")
            for job_id, state_user_id, map_user_id in inconsistent_mappings[:10]:
                logger.error(f"Job {job_id} has inconsistent mapping: {map_user_id} in map, {state_user_id} in state")
                
        logger.info(f"Mapping validation completed. job_to_user_map has {len(self.job_to_user_map)} entries.")
        logger.info(f"Total jobs in state: {sum(len(jobs) for jobs in self.jobs_state.values())}")
        
        # Return summary statistics
        return {
            "missing_in_state": len(missing_in_state),
            "missing_in_map": len(missing_in_map),
            "inconsistent": len(inconsistent_mappings),
            "total_mappings": len(self.job_to_user_map),
            "total_jobs": sum(len(jobs) for jobs in self.jobs_state.values())
        }

    def _handle_message(self, message_data: dict) -> None:
        """Handle incoming Kafka messages."""
        try:
            job_id = message_data.get('job_id')
            event_type = message_data.get('event_type')
            logger.info(f"Received {event_type} event for job {job_id} at {datetime.now().isoformat()}")
            logger.debug(f"Message data: {message_data}")

            # Process job-to-user mapping and update state
            self._process_job_event(message_data)
            
            # Update dashboards with new state
            self._update_dashboards()

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
            
            # Start periodic dashboard updaters
            self._start_dashboard_updaters()
                
            self.kafka_handler.process_messages()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self._shutdown()
    
    def _start_dashboard_updaters(self) -> None:
        """Start background threads that periodically update the dashboards."""
        def updater():
            while not hasattr(self, '_shutting_down') or not self._shutting_down:
                try:
                    self._update_dashboards()
                except Exception as e:
                    logger.error(f"Dashboard updater error: {e}")
                time.sleep(Config.DASHBOARD_REFRESH_RATE)
                
        threading.Thread(target=updater, daemon=True).start()
        logger.debug("Dashboard updater thread started")

    def _signal_handler(self, sig, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {sig}. Shutting down...")
        self._shutdown()
        sys.exit(0)

    def _shutdown(self):
        """Clean shutdown procedure."""
        logger.info("Closing Kafka connections...")
        self._shutting_down = True
        self.kafka_handler.close()
        
        if self.dashboard:
            logger.info("Stopping terminal dashboard...")
            self.dashboard.running = False
            if self.dashboard_thread and self.dashboard_thread.is_alive():
                self.dashboard_thread.join(timeout=1.0)
        
        # Web dashboard runs in a daemon thread, so we don't need to explicitly shut it down
        
        logger.info("Service stopped")


def main() -> None:
    """Application entry point."""
    load_dotenv()
    Config.validate()
    app = TradeGuardHealth()
    app.run()


if __name__ == "__main__":
    main()
