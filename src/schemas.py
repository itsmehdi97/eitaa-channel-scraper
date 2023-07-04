from typing import Optional, Dict
from datetime import datetime

from pydantic import BaseModel

from core.config import get_settings


SETTINGS = get_settings()


class Peer(BaseModel):
    access_hash: Optional[int]


class PeerChannel(Peer):
    channel_id: int


class PeerUser(Peer):
    user_id: int


class Method(BaseModel):
    name: str
    params: Optional[Dict]

    @property
    def args(self):
        return [self.name, self.params, {}]


class ChannelSchedule(BaseModel):
    channel_id: int
    access_hash: Optional[int]
    refresh_interval: Optional[int] = SETTINGS.CHANNEL_REFRESH_INTERVAL
    offset: Optional[int] = 1
    pts: Optional[int]

    running: Optional[bool] = True
    error: Optional[str]

    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    # TODO: remove these fields
    title: Optional[str]
    username: Optional[str]
    participants_count: Optional[int]
    about: Optional[str]  


class Channel(BaseModel):
    channel_id: int
    access_hash: Optional[int]
    title: Optional[str]
    username: Optional[str]
    participants_count: Optional[int]
    about: Optional[str]


class Message(BaseModel):
    id: int
    message: Optional[str]
    date: int
    views: Optional[int]
    forwards: Optional[int]
    channel_id: int
    from_peer: Optional[dict]
    fwd_from: Optional[dict]


class User(BaseModel):
    id: int
    access_hash: Optional[int]
    first_name: Optional[str]
    last_name: Optional[str]
    username: Optional[str]
    phone: Optional[str]
