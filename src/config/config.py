import os
from typing import Optional, Dict
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Loging
    RS_LOG: str = os.getenv('RS_LOG', 'INFO').upper()
    KAFKA_LOG: str = os.getenv('KAFKA_LOG', 'INFO').upper()
    RS_LOG_JOB_PROCESSOR: str = os.getenv('RS_LOG_JOB_PROCESSOR', 'INFO').upper()

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
    KAFKA_CONSUMER_GROUP: str = os.getenv('KAFKA_CONSUMER_GROUP', 'tradeguard-health')
    KAFKA_TOPIC_JOB_UPDATES: str = os.getenv('KAFKA_TOPIC_JOB_UPDATES', 'job-updates')
    KAFKA_TOPIC_RISK_NOTIFICATIONS: str = os.getenv('KAFKA_TOPIC_RISK_NOTIFICATIONS', 'risk-notifications')
    KAFKA_TOPIC_POSITION_UPDATES: str = os.getenv('KAFKA_TOPIC_POSITION_UPDATES', 'position-updates')
    KAFKA_TOPIC_ORDER_FLOW: str = os.getenv('KAFKA_TOPIC_ORDER_FLOW', 'order-flow')
    KAFKA_TOPIC_EQUITY: str = os.getenv('KAFKA_TOPIC_EQUITY', 'equity')

    # API
    BFF_BASE_URL: str = os.getenv('BFF_BASE_URL', 'http://localhost:8080/api')
    API_USER_LIMITS_PATH: str = '/internal/users/{user_id}/limits'

    # Web Dashboard
    ENABLE_WEB_DASHBOARD: bool = os.getenv('ENABLE_WEB_DASHBOARD', 'true').lower() in ('true', 'yes', '1')
    DASHBOARD_REFRESH_RATE: int = int(os.getenv('DASHBOARD_REFRESH_RATE', '1'))
    DASHBOARD_HOST: str = os.getenv('DASHBOARD_HOST', '127.0.0.1')
    DASHBOARD_PORT: int = int(os.getenv('DASHBOARD_PORT', '42069'))

    @classmethod
    def get_user_limits_url(cls, user_id: int) -> str:
        """Get the full URL for user limits API endpoint"""
        return f"{cls.BFF_BASE_URL}{cls.API_USER_LIMITS_PATH.format(user_id=user_id)}"

    @classmethod
    def validate(cls) -> Optional[str]:
        """Validate configuration and return error message if invalid"""
        if not cls.RS_LOG:
            return "RS_LOG is not set"
        if not cls.KAFKA_LOG:
            return "KAFKA_LOG is not set"
        if not cls.RS_LOG_JOB_PROCESSOR:
            return "RS_LOG_JOB_PROCESSOR is not set"
        if not cls.BFF_BASE_URL:
            return "BFF_BASE_URL is not set"
        if not cls.API_USER_LIMITS_PATH:
            return "API_USER_LIMITS_PATH is not set"
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            return "KAFKA_BOOTSTRAP_SERVERS is not set"
        if not cls.KAFKA_CONSUMER_GROUP:
            return "KAFKA_CONSUMER_GROUP is not set"
        if not cls.KAFKA_TOPIC_JOB_UPDATES:
            return "KAFKA_TOPIC_JOB_UPDATES is not set"
        if not cls.KAFKA_TOPIC_RISK_NOTIFICATIONS:
            return "KAFKA_TOPIC_RISK_NOTIFICATIONS is not set"

        return None
