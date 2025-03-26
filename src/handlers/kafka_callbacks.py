import logging
from typing import Any, Dict

from confluent_kafka import Message

logger = logging.getLogger('trade_guide_health')

def delivery_report(err: Any, msg: Message) -> None:
    """Callback for message delivery reports"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def connection_status_callback(stats: Dict[str, Any]) -> None:
    """Callback for connection status changes"""
    try:
        if 'connection_status' in stats:
            status = stats['connection_status']
            if status == 'connected':
                logger.info('Connected to Kafka')
            elif status == 'disconnected':
                logger.warning('Lost connection to Kafka')
            elif status == 'connecting':
                logger.info('Connecting to Kafka...')
    except Exception as e:
        logger.error(f"Error in connection status callback: {e}") 