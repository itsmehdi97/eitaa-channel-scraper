from typing import Optional, Dict
from datetime import datetime

from pydantic import BaseModel

from core.config import get_settings


SETTINGS = get_settings()


class Peer(BaseModel):
    access_hash: int


class PeerChannel(Peer):
    channel_id: int


class Method(BaseModel):
    name: str
    params: Optional[Dict]

    @property
    def args(self):
        return [self.name, self.params, {}]


class ChannelSchedule(BaseModel):
    channel_id: int
    access_hash: int
    refresh_interval: Optional[int] = SETTINGS.CHANNEL_REFRESH_INTERVAL
    offset: Optional[int] = 1
    pts: Optional[int]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    # TODO: remove these fields
    title: Optional[str]
    username: Optional[str]
    participants_count: Optional[int]
    about: Optional[str]  


class Channel(BaseModel):
    channel_id: Optional[int]
    title: str
    username: str
    participants_count: int
    about: str


class Message(BaseModel):
    id: str
    text: Optional[str]
    img_url: Optional[str]
    vid_url: Optional[str]
    vid_duration: Optional[str]
    num_views: Optional[int]
    timestamp: datetime
