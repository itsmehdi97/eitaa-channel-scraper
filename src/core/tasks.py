from typing import Callable
from fastapi import FastAPI

from db.tasks import connect_to_db, teardown_db_conn


def create_start_app_handler(app: FastAPI) -> Callable:
    async def start_app() -> None:
        connect_to_db(app=app)

    return start_app


def create_stop_app_handler(app: FastAPI) -> Callable:
    async def stop_app() -> None:
        await teardown_db_conn(app)

    return stop_app
