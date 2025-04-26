import os
from typing import Optional, Dict
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Loging
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO').upper()
    LOG_LEVEL_KAFKA = os.getenv('LOG_LEVEL_KAFKA', 'INFO').upper()
    LOG_LEVEL_JOB_PROCESSOR = os.getenv('LOG_LEVEL_JOB_PROCESSOR', 'INFO').upper()

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
    KAFKA_CONSUMER_GROUP: str = os.getenv('KAFKA_CONSUMER_GROUP', 'tradeguard-health')
    KAFKA_JOB_UPDATES_TOPIC: str = os.getenv('KAFKA_JOB_UPDATES_TOPIC', 'job-updates')
    KAFKA_RISK_NOTIFICATIONS_TOPIC: str = os.getenv('KAFKA_RISK_NOTIFICATIONS_TOPIC', 'risk-notifications')
    KAFKA_POSITIONS_TOPIC: str = os.getenv('KAFKA_POSITIONS_TOPIC', 'positions')

    # API
    API_BASE_URL: str = os.getenv('API_BASE_URL', 'http://localhost:8080')
    API_USER_LIMITS_PATH: str = os.getenv('API_USER_LIMITS_PATH', '/api/users/{user_id}/limits')

    # Web Dashboard
    ENABLE_WEB_DASHBOARD: bool = os.getenv('ENABLE_WEB_DASHBOARD', 'true').lower() in ('true', 'yes', '1')
    DASHBOARD_REFRESH_RATE: int = int(os.getenv('DASHBOARD_REFRESH_RATE', '2'))
    DASHBOARD_HOST: str = os.getenv('DASHBOARD_HOST', '0.0.0.0')
    DASHBOARD_PORT: int = int(os.getenv('DASHBOARD_PORT', '8081'))

    @classmethod
    def get_user_limits_url(cls, user_id: int) -> str:
        """Get the full URL for user limits API endpoint"""
        return f"{cls.API_BASE_URL}{cls.API_USER_LIMITS_PATH.format(user_id=user_id)}"

    @classmethod
    def validate(cls) -> Optional[str]:
        """Validate configuration and return error message if invalid"""
        if not cls.LOG_LEVEL:
            return "LOG_LEVEL is not set"
        if not cls.LOG_LEVEL_KAFKA:
            return "LOG_LEVEL_KAFKA is not set"
        if not cls.LOG_LEVEL_JOB_PROCESSOR:
            return "LOG_LEVEL_JOB_PROCESSOR is not set"
        if not cls.API_BASE_URL:
            return "API_BASE_URL is not set"
        if not cls.API_USER_LIMITS_PATH:
            return "API_USER_LIMITS_PATH is not set"
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            return "KAFKA_BOOTSTRAP_SERVERS is not set"
        if not cls.KAFKA_CONSUMER_GROUP:
            return "KAFKA_CONSUMER_GROUP is not set"
        if not cls.KAFKA_JOB_UPDATES_TOPIC:
            return "KAFKA_JOB_UPDATES_TOPIC is not set"
        if not cls.KAFKA_RISK_NOTIFICATIONS_TOPIC:
            return "KAFKA_RISK_NOTIFICATIONS_TOPIC is not set"
        if not cls.KAFKA_POSITIONS_TOPIC:
            return "KAFKA_POSITIONS_TOPIC is not set"

        return None
