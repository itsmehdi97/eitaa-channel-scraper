from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    QUERY_API_URL: str
    MESSAGE_FETCH_INTERVAL: int
    CHANNEL_REFRESH_INTERVAL: int

    INFO_CONTAINER_SELECTOR: str
    MESSAGE_CONTAINER_SELECTOR: str

    MONGO_HOST: str
    MONGO_PORT: str
    MONGO_USER: str
    MONGO_PASSWORD: str
    CHANNELS_COLLECTION: str
    MESSAGES_COLLECTION: str
    USERS_COLLECTION: str

    CELERY_BROKER_URL: str
    REDBEAT_REDIS_URL: str
    CELERY_TASK_RATE_LIMIT: str

    RABBITMQ_URL: str
    CHANNELS_QUEUE: str
    MESSAGES_QUEUE: str
    USERS_QUEUE: str

    LOG_LEVEL: str

    class Config:
        env_file = ".env"

    @property
    def db_uri(self) -> str:
        return f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}@{self.MONGO_HOST}:{self.MONGO_PORT}/"


@lru_cache
def get_settings() -> Settings:
    return Settings()
