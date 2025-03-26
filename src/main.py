from dotenv import load_dotenv

from src.config.config import Config
from src.handlers.kafka_handler import KafkaHandler
from src.processors.job_processor import JobProcessor
from src.utils.logging import setup_logging

logger = setup_logging()

class TradeGuideHealth:
    def __init__(self):
        """Initialize the Trade Guide Health service"""
        logger.info("Starting Trade Guide Health service...")

        if error := Config.validate():
            logger.error(f"Configuration error: {error}")
            raise ValueError(error)

        self.job_processor = JobProcessor()
        self.kafka_handler = KafkaHandler(self._handle_message)

    def _handle_message(self, message_data: dict) -> None:
        """Handle incoming Kafka messages"""
        try:
            risk_report = self.job_processor.process_job(message_data)
            
            if risk_report:
                logger.info(f"Sending risk report to topic {Config.KAFKA_RISK_TOPIC}")
                self.kafka_handler.send_message(
                    Config.KAFKA_RISK_TOPIC,
                    risk_report.model_dump()
                )
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)

    def run(self) -> None:
        """Main application loop"""
        try:
            self.kafka_handler.process_messages()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.kafka_handler.close()
            logger.info("Service stopped")


def main() -> None:
    """Application entry point"""
    # Load environment variables
    load_dotenv()

    # Validate configuration
    Config.validate()

    # Start the service
    app = TradeGuideHealth()
    app.run()


if __name__ == "__main__":
    main()
