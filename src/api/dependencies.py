from fastapi import Depends
from starlette.requests import Request

from db import MongoClient
from adapters import MongoRepository


def get_db(request: Request):
    return request.app.state._db


def get_repo(
    db_client: MongoClient = Depends(get_db)
) -> MongoRepository:
    return MongoRepository(db_client)
