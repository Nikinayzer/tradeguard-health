import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Kafka
    # todo maybe these fallbacks is redundant
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_JOBS_TOPIC: str = os.getenv('KAFKA_JOBS_TOPIC', 'jobs')
    KAFKA_RISK_TOPIC: str = os.getenv('KAFKA_RISK_TOPIC', 'risk_patterns')

    # API
    API_BASE_URL: str = os.getenv('API_BASE_URL', 'http://localhost:8000')
    API_USER_LIMITS_PATH: str = os.getenv('API_USER_LIMITS_PATH', '/api/users/{user_id}/limits')

    @classmethod
    def get_user_limits_url(cls, user_id: int) -> str:
        """Get the full URL for user limits API endpoint"""
        return f"{cls.API_BASE_URL}{cls.API_USER_LIMITS_PATH.format(user_id=user_id)}"

    @classmethod
    def validate(cls) -> Optional[str]:
        """Validate configuration and return error message if invalid"""
        if not cls.API_BASE_URL:
            return "API_BASE_URL is not set"
        if not cls.API_USER_LIMITS_PATH:
            return "API_USER_LIMITS_PATH is not set"
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            return "KAFKA_BOOTSTRAP_SERVERS is not set"
        if not cls.KAFKA_JOBS_TOPIC:
            return "KAFKA_JOBS_TOPIC is not set"
        if not cls.KAFKA_RISK_TOPIC:
            return "KAFKA_RISK_TOPIC is not set"
        return None
