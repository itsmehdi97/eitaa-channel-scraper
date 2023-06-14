import requests
from celery import Celery
from celery import Task

from core.config import get_settings
from db.tasks import connect_to_db
from db import MongoClient
from crawler import ChannelCrawler
from crawler import MessageScraper
from adapters import MongoRepository


settings = get_settings()


class CustomTask(Task):
    _db : MongoClient = None
    _http: requests.Session = None

    @property
    def repository(self) -> MongoClient:
        if self._db is None:
            self._db = connect_to_db()
        return MongoRepository(self._db)

    @property
    def http_session(self) -> requests.Session:
        if self._http is None:
            self._http = requests.Session()
            self._http.headers.update({
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0",
            })
        return self._http

    def _get_scraper(self) -> MessageScraper:
        return MessageScraper()


app = Celery(__name__, task_cls="worker.celery.CustomTask")
app.conf.broker_url = settings.CELERY_BROKER_URL

app.autodiscover_tasks(packages=['worker'], related_name='tasks')
