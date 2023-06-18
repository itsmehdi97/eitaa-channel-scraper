from typing import Optional
from datetime import datetime

from pydantic import BaseModel

from core.config import get_settings


SETTINGS = get_settings()


class ChannelSchedule(BaseModel):
    channel_name: str
    channel_id: Optional[str]
    refresh_interval: Optional[int] = SETTINGS.CHANNEL_REFRESH_INTERVAL
    offset: Optional[int] = 1
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    info: Optional[str]  # TODO: remove this field


class Channel(BaseModel):
    id: Optional[str]
    faname: str
    username: str
    num_follower: int
    num_img: int
    num_vid: int
    num_file: int
    info: str
    img_url: str


class Message(BaseModel):
    id: str
    text: Optional[str]
    img_url: Optional[str]
    vid_url: Optional[str]
    vid_duration: Optional[str]
    num_views: Optional[int]
    timestamp: datetime
