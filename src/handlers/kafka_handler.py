import json
import logging
from typing import Callable, Dict, Any, Optional, List, Iterator, Type, TypeVar, Generic
from datetime import datetime

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition

from src.config.config import Config
from src.handlers.kafka_callbacks import delivery_report, connection_status_callback
from src.models.job_events import JobEvent
from src.utils import log_util

logger = log_util.get_logger()

T = TypeVar('T')


class TopicConfig:
    """Configuration for a Kafka topic."""

    def __init__(self, topic: str, event_type: Type[T], deserializer: Callable[[Dict[str, Any]], T]):
        self.topic = topic
        self.event_type = event_type
        self.deserializer = deserializer


class KafkaHandler(Generic[T]):
    """
    Generic Kafka handler for a single topic and event type.
    
    Type Parameters:
        T: The type of event this handler processes
    """
    def __init__(self, topic: str, event_type: Type[T], deserializer: Callable[[Dict[str, Any]], T]):
        """
        Initialize Kafka handler for a specific topic and event type.
        
        Args:
            topic: The Kafka topic to handle
            event_type: The type of event this handler processes
            deserializer: Function to deserialize messages into events
        """
        self.topic = topic
        self.event_type = event_type
        self.deserializer = deserializer
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self._setup_connections()

    def _setup_connections(self) -> None:
        """Setup Kafka consumer and producer connections."""
        logger.info(
            f"Connecting to Kafka at {Config.KAFKA_BOOTSTRAP_SERVERS}, subscribing to topic {self.topic}"
        )
        
        self.consumer = Consumer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': Config.KAFKA_CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'client.id': f'tradeguard_health_{self.topic}_consumer',
            'stats_cb': connection_status_callback,
            'session.timeout.ms': 10000,
            'max.poll.interval.ms': 60000,
            'fetch.min.bytes': 1,
            'fetch.max.bytes': 52428800,
            'statistics.interval.ms': 30000
        })
        self.consumer.subscribe([self.topic])

        self.producer = Producer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': f'tradeguard_health_{self.topic}_producer',
            'stats_cb': connection_status_callback,
            'statistics.interval.ms': 1000
        })

    def _handle_consumer_error(self, msg):
        if msg.error():
            error_code = msg.error().code()
            if error_code == KafkaError._PARTITION_EOF:
                # End of partition event - not really an error
                return False
            elif error_code == KafkaError._TRANSPORT:
                # Not an error, but a transport event
                return False
            else:
                # Real error
                self.logger.error(f"Consumer error: {msg.error()}")
                return True
        return False

    def read_topic_from_beginning(self, max_messages: int = 1000000) -> Iterator[T]:
        """Read all messages from the beginning of the topic."""
        logger.info(f"Reading topic {self.topic} from beginning")
        
        # Create a separate consumer for historical reads
        historical_consumer = Consumer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f'tradeguard_health_historical_{id(self)}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'client.id': f'tradeguard_health_historical_{self.topic}_consumer'
        })

        count = 0
        try:
            # Get partitions once
            metadata = historical_consumer.list_topics(topic=self.topic, timeout=10.0)
            if self.topic not in metadata.topics:
                logger.warning(f"Topic {self.topic} not found")
                return

            topic_obj = metadata.topics[self.topic]
            partitions = topic_obj.partitions
            if not partitions:
                logger.warning(f"Topic {self.topic} has no partitions")
                return

            logger.info(f"Found {len(partitions)} partitions for topic {self.topic}")
            
            # Create TopicPartition objects and get watermarks
            topic_partitions = []
            for p_id in partitions:
                tp = TopicPartition(self.topic, p_id)
                low, high = historical_consumer.get_watermark_offsets(tp)
                if high > 0:  # Only include partitions with messages
                    tp.offset = low  # Set to earliest offset
                    topic_partitions.append(tp)
                    logger.info(f"Partition {p_id} has {high - low} messages (offset range: {low}-{high})")
            
            if not topic_partitions:
                logger.warning("No partitions have messages")
                return
                
            # Assign partitions with their starting offsets
            historical_consumer.assign(topic_partitions)
            logger.info(f"Assigned {len(topic_partitions)} partitions with messages")

            consecutive_empty_polls = 0
            max_empty_polls = 5  # Increased to ensure we read from all partitions
            remaining_partitions = set(tp.partition for tp in topic_partitions)

            while count < max_messages and consecutive_empty_polls < max_empty_polls and remaining_partitions:
                try:
                    msg = historical_consumer.poll(0.1)
                    if msg is None:
                        consecutive_empty_polls += 1
                        continue

                    consecutive_empty_polls = 0

                    if msg.error():
                        if msg.error().code() == KafkaError.PARTITION_EOF:
                            remaining_partitions.discard(msg.partition())
                            logger.debug(f"Reached end of partition {msg.partition()}, {len(remaining_partitions)} partitions remaining")
                            if not remaining_partitions:
                                logger.info("Reached end of all partitions")
                                break
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                        continue

                    message_data = _decode_and_parse_message(msg)
                    if message_data is not None:
                        try:
                            event = self.deserializer(message_data)
                            count += 1
                            yield event
                        except Exception as e:
                            logger.error(f"Error deserializing message: {e}")
                            continue

                except Exception as e:
                    logger.error(f"Unexpected error reading from Kafka: {e}")
                    consecutive_empty_polls += 1

        except KafkaException as e:
            logger.error(f"Kafka error during historical read: {e}")
        finally:
            try:
                historical_consumer.close()
                logger.info(f"Closed historical consumer after reading {count} messages")
            except Exception as e:
                logger.error(f"Error closing historical consumer: {e}")
            if count == 0:
                yield from []

    def send_message(self, message: dict) -> None:
        """Send a message to the Kafka topic."""
        if not self.producer:
            logger.error("Producer not initialized")
            return

        try:
            self.producer.produce(
                self.topic,
                json.dumps(message).encode('utf-8'),
                callback=delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")

    def process_messages(self, message_handler: Callable[[T], None]) -> None:
        """Process incoming Kafka messages."""
        if not self.consumer:
            logger.error("Consumer not initialized")
            return

        logger.info(f"Starting to consume messages from topic {self.topic}")
        message_count = 0
        error_count = 0
        last_log_time = datetime.now()
        empty_polls = 0
        max_empty_polls = 100

        try:
            while True:
                msg = self.consumer.poll(0.1)
                current_time = datetime.now()

                # Log stats every minute
                if (current_time - last_log_time).total_seconds() > 60:
                    logger.info(f"Kafka consumer stats: {message_count} messages processed, {error_count} errors")
                    last_log_time = current_time
                    message_count = 0  # Reset counters
                    error_count = 0

                if msg is None:
                    empty_polls += 1
                    if empty_polls >= max_empty_polls:
                        logger.debug("No messages received for a while")
                        empty_polls = 0
                    continue

                empty_polls = 0

                if self._handle_consumer_error(msg):
                    error_count += 1
                    continue

                try:
                    message_data = _decode_and_parse_message(msg)
                    if message_data is not None:
                        logger.info(f"!!!NEW MESSAGE FROM {self.topic}!!!")
                        try:
                            event = self.deserializer(message_data)
                            message_handler(event)
                            message_count += 1
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            error_count += 1

                except Exception as e:
                    logger.error(f"Error handling message: {e}")
                    error_count += 1

        except Exception as e:
            logger.error(f"Fatal error in message processing: {e}")
            raise

    def close(self) -> None:
        """Close Kafka connections."""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
        logger.info("Kafka connections closed")


def _reached_end_of_all_partitions(consumer: Consumer, partitions: List[TopicPartition]) -> bool:
    """Check if we've reached the end of all partitions."""
    try:
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(partition)
            current = consumer.position([partition])[0].offset
            if current < high:
                return False
        return True
    except Exception as e:
        logger.error(f"Error checking partition positions: {e}")
        return False


def _decode_and_parse_message(msg) -> Optional[Dict[str, Any]]:
    """
    Decode the raw message into a dictionary.
    
    Args:
        msg: The Kafka message to decode
    
    Returns:
        The decoded message as a dictionary or None if decoding fails
    """
    try:
        return json.loads(msg.value().decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding message: {e}")
        return None


def _seek_to_beginning(consumer: Consumer, partitions: List[TopicPartition]) -> None:
    """Seek to the beginning of all partitions."""
    try:
        for partition in partitions:
            consumer.seek(partition)
        logger.info("Seeking to beginning of all partitions")
    except Exception as e:
        logger.error(f"Error seeking to beginning of partitions: {e}")
