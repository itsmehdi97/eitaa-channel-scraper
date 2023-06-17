from pydantic import BaseModel


class ChannelSchedule(BaseModel):
    channel_name: str
