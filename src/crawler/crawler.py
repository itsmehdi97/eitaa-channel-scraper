import json
import time
from typing import Tuple, List
from logging import getLogger

from requests import Session
from pika.channel import Channel

import schemas
from core.config import get_settings
from crawler.scraper import MessageScraper
from adapters import MongoChannScheduleRepository, RabbitConnection


logger = getLogger(__name__)

SETTINGS = get_settings()


class ChannelCrawler:
    """
    scrolls through pages in the given channel and fetches data.
    """

    def __init__(
        self,
        channel_name: str,
        *,
        http_agent: Session,
        scraper: MessageScraper,
        repository: MongoChannScheduleRepository,
        rabbit_channel: Channel
    ) -> None:
        self.channel_name = channel_name
        self.channel_id = None
        self.channel_url = f"https://{SETTINGS.EITAA_DOMAIN}/{channel_name}"
        self._http_agent = http_agent
        self._scraper = scraper
        self._repository = repository
        self._rabbit_channel = rabbit_channel

    def start(self) -> int:
        msg_offset, channel = self.get_channel_info()

        if self._channel_updated(channel):
            self.publish(
                channel,
                SETTINGS.CHANNELS_QUEUE)

            self._repository.update(self.channel_name, **channel.dict())

        if self.get_prev_run_offset and self.get_prev_run_offset >= msg_offset:
            logger.info(
                f"No new message on channel `{self.channel_name}` since last run"
            )
            return msg_offset

        _prev_offset = self.get_prev_run_offset

        self._repository.update(self.channel_name, offset=msg_offset)

        from worker.tasks import get_message_page
        get_message_page.apply_async(kwargs={
            'channel_name': self.channel_name,
            'start_offset':msg_offset,
            'end_offset':_prev_offset
        })

        return msg_offset

    @property
    def get_prev_run_offset(self) -> int | None:
        channel = self._repository.get({"channel_name": self.channel_name})
        if channel:
            return channel.offset or 1

    def update_channel_offset(self, offset: int) -> None:
        channel = self._repository.get({"channel_name": self.channel_name})
        current_offset = channel.offset
        if offset > current_offset:
            self._repository.update(self.channel_name, offset=offset)

    def get_channel_info(self) -> Tuple[int, schemas.Channel]:
        chnl_text = self._fetch_channel()
        return self._scraper.extract_channel_info(chnl_text)

    def _fetch_channel(self) -> str:
        resp = self._http_agent.get(self.channel_url)
        return resp.text

    def _channel_updated(self, channel: schemas.Channel) -> bool:
        if old_channel := self._repository.get({"channel_name": channel.username}):
            channel = channel.dict()
            old_channel = old_channel.dict()
            if 'updated_at' in channel:
                del channel['updated_at']
            if 'updated_at' in old_channel:
                del old_channel['updated_at']
            if not old_channel == channel:
                return True
            return False
        return True

    def publish(self, data: schemas.BaseModel, queue: str) -> None:
        RabbitConnection.publish(
            json.dumps(data.json(), ensure_ascii=False),
            channel=self._rabbit_channel,
            exchange=queue,
            routing_key=queue
        )


class MessageCrawler(ChannelCrawler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def start(self, offset: int) -> int:
        next_page_offest, messages = self.get_msg_page(offset)

        if messages:
            self._repository.add_msg_to_channel(self.channel_name, messages)
            for msg in messages:
                self.publish(
                    msg,
                    queue=SETTINGS.MESSAGES_QUEUE
            )

        return next_page_offest
        
    def get_msg_page(self, offset: int) -> Tuple[int, List[schemas.Message]]:
        msg_text = self._fetch_msg_page(offset)
        return self._scraper.extarct_messages(msg_text)

    def _fetch_msg_page(self, offset: int) -> str:
        resp = self._http_agent.get(f"{self.channel_url}?before={offset}")
        return resp.text
