import logging

from pymongo import MongoClient

from core.config import get_settings


logger = logging.getLogger(__name__)


def connect_to_db(app=None) -> MongoClient:
    settings = get_settings()

    try:
        client = MongoClient(settings.db_uri)
        if app:
            app.state._db = client

        return client

    except Exception as e:
        logger.warn("--- DB CONNECTION ERROR ---")
        logger.warn(e)
        logger.warn("--- DB CONNECTION ERROR ---")
        raise


def teardown_db_conn(app) -> None:
    try:
        app.state._db.close()

    except Exception as e:
        logger.warn("--- DB DISCONNECT ERROR ---")
        logger.warn(e)
        logger.warn("--- DB DISCONNECT ERROR ---")
        raise
