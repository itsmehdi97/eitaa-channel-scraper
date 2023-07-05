from logging import getLogger

import requests
from requests.adapters import HTTPAdapter, Retry
from celery import Celery
from celery import Task

import pika
from pika.channel import Channel

from core.config import get_settings
from db.tasks import connect_to_db
from db import MongoClient
from crawler import JSONMessageScraper
from adapters import MongoChannScheduleRepository, RabbitConnection


logger = getLogger(__name__)

settings = get_settings()


class CustomTask(Task):
    _db: MongoClient = None
    _http: requests.Session = None
    _rabbit_conn: pika.BaseConnection = None
    _rabbit_chann: Channel = None

    @property
    def repository(self) -> MongoClient:
        if self._db is None:
            self._db = connect_to_db()
        return MongoChannScheduleRepository(self._db)

    @property
    def http_session(self) -> requests.Session:
        if self._http is None:
            retries = Retry(total=5, backoff_factor=0.5)
            s = requests.Session()
            s.mount('https://eitaa.com/', HTTPAdapter(max_retries=retries))
            s.headers.update(
                {
                    # "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0",
                    'content-type': 'application/json',
                    'accept': '*/*',
                }
            )
            self._http = s
        return self._http

    def _get_scraper(self) -> JSONMessageScraper:
        return JSONMessageScraper()

    @property
    def rabbit_channel(self) -> Channel:
        if self._rabbit_conn is None:
            conn = RabbitConnection(conn_url=settings.RABBITMQ_URL)
            conn.bind_queue(
                queue=settings.CHANNELS_QUEUE,
                exchange=settings.CHANNELS_QUEUE,
                routing_key=settings.CHANNELS_QUEUE,
            )
            conn.bind_queue(
                queue=settings.MESSAGES_QUEUE,
                exchange=settings.MESSAGES_QUEUE,
                routing_key=settings.MESSAGES_QUEUE,
            )
            conn.bind_queue(
                queue=settings.USERS_QUEUE,
                exchange=settings.USERS_QUEUE,
                routing_key=settings.USERS_QUEUE,
            )
            self._rabbit_conn = conn

        if self._rabbit_chann is None or self._rabbit_chann.is_closed:
            logger.info("creating rabbit channel")
            try:
                self._rabbit_chann = self._rabbit_conn.get_channel()
            except Exception:
                logger.info("restarting rabbitmq connection")
                conn = RabbitConnection(conn_url=settings.RABBITMQ_URL)
                self._rabbit_conn = conn
                self._rabbit_chann = self._rabbit_conn.get_channel()

        return self._rabbit_chann


app = Celery(__name__, task_cls="worker.celery.CustomTask")
app.conf.broker_url = settings.CELERY_BROKER_URL

app.autodiscover_tasks(packages=["worker"], related_name="tasks")

app.conf.task_annotations = {
    'worker.tasks.get_message_page': {'rate_limit': settings.CELERY_TASK_RATE_LIMIT},
}

app.conf.redbeat_redis_url = settings.REDBEAT_REDIS_URL
