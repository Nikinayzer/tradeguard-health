import json
import logging
from typing import Callable, Dict, Any, Optional

from confluent_kafka import Consumer, Producer

from src.config.config import Config
from src.handlers.kafka_callbacks import delivery_report, connection_status_callback

logger = logging.getLogger('trade_guide_health')

class KafkaHandler:
    def __init__(self, message_handler: Callable[[Dict[str, Any]], None]):
        """Initialize Kafka handler with a message processing callback"""
        self.message_handler = message_handler
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self._setup_connections()

    def _setup_connections(self) -> None:
        """Setup Kafka consumer and producer connections"""
        logger.info(
            f"Connecting to Kafka at {Config.KAFKA_BOOTSTRAP_SERVERS}, subscribing to topic {Config.KAFKA_JOBS_TOPIC}")

        self.consumer = Consumer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'trade_guide_health_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'client.id': 'trade_guide_health_consumer',
            'stats_cb': connection_status_callback,
            'statistics.interval.ms': 1000
        })
        self.consumer.subscribe([Config.KAFKA_JOBS_TOPIC])

        self.producer = Producer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'trade_guide_health_producer',
            'stats_cb': connection_status_callback,
            'statistics.interval.ms': 1000
        })

    def send_message(self, topic: str, message: dict) -> None:
        """Send a message to a Kafka topic"""
        if not self.producer:
            logger.error("Producer not initialized")
            return

        try:
            self.producer.produce(
                topic,
                json.dumps(message).encode('utf-8'),
                callback=delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")

    def process_messages(self) -> None:
        """Process incoming Kafka messages"""
        if not self.consumer:
            logger.error("Consumer not initialized")
            return

        try:
            while True:
                msg = self.consumer.poll(1.0)  # Timeout of 1 second
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    self.message_handler(message_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding message: {e}")

        except KeyboardInterrupt:
            logger.info("Shutting down Kafka connections...")
        finally:
            self.close()

    def close(self) -> None:
        """Close Kafka connections"""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
        logger.info("Kafka connections closed") 