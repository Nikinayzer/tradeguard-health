import json
import logging
from typing import Callable, Dict, Any, Optional, List, Iterator
from datetime import datetime

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition

from src.config.config import Config
from src.handlers.kafka_callbacks import delivery_report, connection_status_callback
from src.models.job_events import JobEvent
from src.utils import log_util

logger = log_util.get_logger()


class KafkaHandler:
    def __init__(self, message_handler: Callable[[Dict[str, Any]], None]):
        """Initialize Kafka handler with a message processing callback."""
        self.message_handler = message_handler
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self._setup_connections()

    def _setup_connections(self) -> None:
        """Setup Kafka consumer and producer connections."""
        logger.info(
            f"Connecting to Kafka at {Config.KAFKA_BOOTSTRAP_SERVERS}, subscribing to topic {Config.KAFKA_JOBS_TOPIC}"
        )
        self.consumer = Consumer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': Config.KAFKA_CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'client.id': 'tradeguard_health_consumer',
            'stats_cb': connection_status_callback,
            'statistics.interval.ms': 1000
        })
        self.consumer.subscribe([Config.KAFKA_JOBS_TOPIC])

        self.producer = Producer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'tradeguard_health_producer',
            'stats_cb': connection_status_callback,
            'statistics.interval.ms': 1000
        })

    def _handle_consumer_error(self, msg) -> bool:
        """
        Handle consumer errors. Returns True if an error was handled and the caller
        should continue, or False if the error should break processing.
        """
        if msg.error():
            error_code = msg.error().code()
            if error_code == KafkaError._PARTITION_EOF:
                logger.debug(f"Reached end of partition {msg.partition()}")
            elif error_code == KafkaError._OFFSET_OUT_OF_RANGE:
                logger.debug(f"Offset out of range for partition {msg.partition()}")
                try:
                    tp = TopicPartition(msg.topic(), msg.partition())
                    low, high = self.consumer.get_watermark_offsets(tp)
                    if high > 0:
                        self.consumer.seek(tp, low)
                        logger.debug(f"Reset partition {msg.partition()} to offset {low}")
                    else:
                        logger.debug(f"Partition {msg.partition()} is empty")
                except Exception as e:
                    logger.warning(f"Could not reset offset for partition {msg.partition()}: {e}")
            else:
                logger.error(f"Consumer error: {msg.error()}")
            return True
        return False

    def read_topic_from_beginning(self, topic: str, max_messages: int = 1000000) -> Iterator[Dict[str, Any]]:
        """
        Read all messages from the beginning of a topic.

        Creates a temporary consumer for historical reads so as not to interfere with the main consumer.
        """
        logger.info(f"Creating temporary consumer to read topic {topic} from beginning")
        historical_consumer = Consumer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f'tradeguard_health_historical_{id(self)}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'client.id': 'tradeguard_health_historical_consumer'
        })

        count = 0
        try:
            metadata = historical_consumer.list_topics(topic=topic, timeout=10.0)
            if topic not in metadata.topics:
                logger.warning(f"Topic {topic} not found")
                return

            topic_obj = metadata.topics[topic]
            partitions = topic_obj.partitions
            if not partitions:
                logger.warning(f"Topic {topic} has no partitions")
                return

            logger.info(f"Found {len(partitions)} partitions for topic {topic}")
            topic_partitions = [TopicPartition(topic, p_id) for p_id in partitions]
            historical_consumer.assign(topic_partitions)
            _seek_to_beginning(historical_consumer, topic_partitions)

            consecutive_empty_polls = 0
            max_empty_polls = 5

            while count < max_messages and consecutive_empty_polls < max_empty_polls:
                try:
                    msg = historical_consumer.poll(1.0)
                    if msg is None:
                        consecutive_empty_polls += 1
                        if consecutive_empty_polls >= max_empty_polls:
                            logger.debug(f"No more messages after {consecutive_empty_polls} empty polls")
                            break
                        if _reached_end_of_all_partitions(historical_consumer, topic_partitions):
                            logger.debug(f"Reached end of all partitions in topic {topic}")
                            break
                        continue

                    # Reset counter when a message is received.
                    consecutive_empty_polls = 0

                    if self._handle_consumer_error(msg):
                        continue

                    message_data = _decode_and_parse_message(msg)
                    if message_data is not None:
                        count += 1
                        yield message_data

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
                logger.error(f"Error closing consumer: {e}")
            if count == 0:
                yield from []

    def send_message(self, topic: str, message: dict) -> None:
        """Send a message to a Kafka topic."""
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
        """Process incoming Kafka messages."""
        if not self.consumer:
            logger.error("Consumer not initialized")
            return

        logger.info(f"Starting to consume messages from topic {Config.KAFKA_JOBS_TOPIC}")
        message_count = 0
        error_count = 0
        last_log_time = datetime.now()

        try:
            while True:
                msg = self.consumer.poll(1.0)
                current_time = datetime.now()
                if (current_time - last_log_time).total_seconds() > 60:
                    logger.info(f"Kafka consumer stats: {message_count} messages processed, {error_count} errors")
                    last_log_time = current_time

                if msg is None:
                    continue

                if self._handle_consumer_error(msg):
                    error_count += 1
                    continue

                try:
                    raw_data = json.loads(msg.value().decode('utf-8'))
                    job_id = raw_data.get('job_id', 'unknown')
                    event_type = raw_data.get('event_type', 'unknown')
                    user_id = raw_data.get('user_id', 'unknown')
                    logger.debug(
                        f"Received message from partition {msg.partition()}, offset {msg.offset()}: "
                        f"job_id={job_id}, event={event_type}"
                    )

                    try:
                        job_event = JobEvent.from_dict(raw_data)
                        message_data = job_event.to_dict()
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Error parsing job event (job_id={job_id}): {e}. Using raw data instead.")
                        logger.debug(f"Raw event data: {raw_data}")
                        message_data = raw_data

                    logger.debug(f"Processing job event: job_id={job_id}, event={event_type}, user_id={user_id}")
                    self.message_handler(message_data)
                    message_count += 1

                except json.JSONDecodeError as e:
                    error_count += 1
                    logger.error(f"Error decoding message: {e}")
                    try:
                        raw_message = msg.value().decode('utf-8')
                    except UnicodeDecodeError as e:
                        logger.error(f"Failed to decode message due to {e}. Raw message: {msg.value()[:200]}...")
                    else:
                        logger.debug(f"Decoded message: {raw_message[:200]}...")

        except KeyboardInterrupt:
            logger.info("Shutting down Kafka connections...")
        except Exception as e:
            logger.exception(f"Unexpected error in Kafka consumer: {e}")
        finally:
            logger.info(f"Closing Kafka consumer. Processed {message_count} messages with {error_count} errors")
            self.close()

    def close(self) -> None:
        """Close Kafka connections."""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
        logger.info("Kafka connections closed")


def _reached_end_of_all_partitions(consumer: Consumer, topic_partitions: List[TopicPartition]) -> bool:
    """
    Check if the consumer's current positions have reached the high watermark for all partitions.

    Returns:
        True if all partitions are at their high watermark (or empty), False otherwise.
        In case of an error, logs the error and returns True to avoid blocking processing.
    """
    try:
        positions = consumer.position(topic_partitions)
        if not positions:
            logger.debug("No positions returned, assuming end of partitions")
            return True

        # Create a mapping: partition number -> position
        pos_map = {p.partition: p for p in positions}

        for tp in topic_partitions:
            position = pos_map.get(tp.partition)
            if position is None:
                logger.debug("No position found for partition %s, skipping", tp.partition)
                continue

            low, high = consumer.get_watermark_offsets(tp)
            logger.debug("Partition %s: position=%s, high=%s", tp.partition, repr(position), repr(high))

            # If the partition isn't empty and the position is behind the high watermark, we're not at the end.
            if high != 0 and position < high:
                logger.debug("Partition %s not at end: position=%s, high=%s", tp.partition, position, high)
                return False

        return True

    except Exception as e:
        # logger.warning("Error checking end of partitions: %s", e) #suspend this error message, probs kafka library issue
        return True


def _decode_and_parse_message(msg) -> Optional[Dict[str, Any]]:
    """
    Decode the raw message, attempt to deserialize with JobEvent model,
    and fall back to raw dict if needed.
    """
    try:
        raw_data = json.loads(msg.value().decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding historical message: {e}")
        return None

    try:
        # Attempt to deserialize with JobEvent model.
        job_event = JobEvent.from_dict(raw_data)
        message_data = job_event.to_dict()
    except (ValueError, KeyError) as e:
        logger.warning(f"Error parsing job event: {e}. Using raw data instead.")
        message_data = raw_data

    return message_data


def _seek_to_beginning(consumer: Consumer, topic_partitions: List[TopicPartition]) -> None:
    """Seek to the beginning of each partition."""
    for tp in topic_partitions:
        try:
            low, high = consumer.get_watermark_offsets(tp)
            if high == 0:
                logger.debug(f"Partition {tp.partition} is empty (high={high})")
                continue
            logger.debug(f"Partition {tp.partition} watermarks: low={low}, high={high}")
            try:
                consumer.seek(tp)
            except Exception as e1:
                logger.warning(f"Could not seek partition {tp.partition} without offset: {e1}")
                try:
                    consumer.seek(TopicPartition(tp.topic, tp.partition, low))
                except Exception as e2:
                    logger.warning(f"Could not seek partition {tp.partition} with offset: {e2}")
        except Exception as e:
            logger.warning(f"Could not seek partition {tp.partition}: {e}")
    logger.info("Seeking to beginning of all partitions")
