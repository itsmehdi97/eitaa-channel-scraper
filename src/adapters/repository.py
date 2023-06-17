import abc
from datetime import datetime
from typing import List

from pymongo import MongoClient

import schemas
import exceptions
from core.config import get_settings


SETTINGS = get_settings()


class BaseChannScheduleRepository(abc.ABC):
    @abc.abstractmethod
    def create(self, chann_schedule: schemas.ChannelSchedule) -> schemas.ChannelSchedule:
        raise NotImplementedError

    def update(self, **kwargs) -> None:
        raise NotImplementedError

    # def update_offset(self, channel_name: str, offset: int) -> None:
    #     raise NotImplementedError

    def get(self, kwargs: dict) -> schemas.ChannelSchedule | None:
        raise NotImplementedError

    def create(self, chann_schedule: schemas.ChannelSchedule) -> schemas.ChannelSchedule:
        raise NotImplementedError

class ConsoleRepository(BaseChannScheduleRepository):
    def create_channel(self, channel_name: str,channel_info: str) -> None:
        print(channel_name)
        print(channel_info)
        print("###############")

    def add_msg_to_channel(self, channel_name: str, msgs: List[str]) -> None:
        print(channel_name)
        print(msgs)
        print("###############")


class MongoChannScheduleRepository(BaseChannScheduleRepository):
    def __init__(self, client: MongoClient) -> None:
        self._db = client["eitaa"]

    # def update_offset(self, channel_name: str, offset: int) -> None:
    #     self._db[SETTINGS.CHANNELS_COLLECTION].update_one(
    #         {"name": channel_name},
    #         {"$set": {"offset": offset, "updated_at": datetime.utcnow()} },
    #         upsert=True
    #     )

    def update(self, channel_name: str, **kwargs) -> None:
        if self.get({"channel_name": channel_name}) is None:
            raise exceptions.NotFound(f"Channel not found: {channel_name}")

        kwargs['updated_at'] = datetime.utcnow()

        self._db[SETTINGS.CHANNELS_COLLECTION].update_one(
            {"channel_name": channel_name},
            {"$set": kwargs}
        )
    
    def get(self, kwargs: dict) -> schemas.ChannelSchedule | None:
        d = self._db[SETTINGS.CHANNELS_COLLECTION].find_one(kwargs)
        if d:
            return schemas.ChannelSchedule(**d)

    def create(self, chann_schedule: schemas.ChannelSchedule) ->  schemas.ChannelSchedule:
        chann_schedule.created_at = datetime.utcnow()

        self._db[SETTINGS.CHANNELS_COLLECTION].insert_one(chann_schedule.dict())

    def add_msg_to_channel(self, channel_name: str, msgs: List[schemas.Message]) -> None:
        self._db[SETTINGS.MESSAGES_COLLECTION].insert_many(
            [{**msg.dict(), "channel": channel_name} for msg in msgs]
        )
