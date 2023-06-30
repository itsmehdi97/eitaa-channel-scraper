import json
from typing import Tuple, List
from logging import getLogger

from requests import Session
from pika.channel import Channel as PikaChannel

import schemas
from core.config import get_settings
from crawler.scraper import JSONMessageScraper
from adapters import MongoChannScheduleRepository, RabbitConnection


logger = getLogger(__name__)

SETTINGS = get_settings()


class ChannelCrawler:
    """
    scrolls through pages in the given channel and fetches data.
    """

    def __init__(
        self,
        peer_channel: schemas.PeerChannel,
        *,
        http_agent: Session,
        scraper: JSONMessageScraper,
        repository: MongoChannScheduleRepository,
        rabbit_channel: PikaChannel
    ) -> None:
        self.peer_channel = peer_channel
        self.query_api_url = SETTINGS.QUERY_API_URL
        self._http_agent = http_agent
        self._scraper = scraper
        self._repository = repository
        self._rabbit_channel = rabbit_channel

    def start(self) -> int:
        pts, channel = self.get_channel_info()
        if not pts:
            raise ValueError("Invalid info response for")

        chan_sched = self._repository.get({"id": self.peer_channel.channel_id})
        if chan_sched.pts:
            if pts <= chan_sched.pts:
                logger.info(
                    f"Channel`{self.peer_channel.channel_id}` has not changed since last run"
                )
                return chan_sched.offset
        else:
            self._repository.update(self.peer_channel.channel_id, pts=pts)

        if self._channel_info_updated(channel):
            self.publish(
                channel,
                SETTINGS.CHANNELS_QUEUE)

            self._repository.update(self.peer_channel.channel_id, **channel.dict())

        msg_offset = self.get_channel_offset()

        if self.get_prev_run_offset and self.get_prev_run_offset >= msg_offset:
            logger.info(
                f"No new message on channel `{self.peer_channel.channel_id}` since last run"
            )
            return msg_offset

        _prev_offset = self.get_prev_run_offset

        self._repository.update(self.peer_channel.channel_id, offset=msg_offset)

        from worker.tasks import get_message_page
        get_message_page.apply_async(kwargs={
            'peer_channel': {
                'id': self.peer_channel.channel_id,
                'access_hash':self.peer_channel.access_hash
            },
            'start_offset':msg_offset,
            'end_offset':_prev_offset
        })

        return msg_offset

    @property
    def get_prev_run_offset(self) -> int | None:
        channel = self._repository.get({"id": self.peer_channel.channel_id})
        if channel:
            return channel.offset or 1

    def get_channel_offset(self) -> int:
        method = schemas.Method(
            name="messages.getHistory",
            params={
                "add_offset": 0,
                "hash": 0,
                "limit": 1,
                "max_id": 0,
                "min_id": 0,
                "offset_date": 0,
                "peer": {
                    "_": "inputPeerChannel",
                    **self.peer_channel.dict(),
                }
            },
        )
        resp = self._http_agent.post(self.query_api_url, data=json.dumps({'args': method.args, "username": "989044600776"}))
        return resp.json()['messages'][0]['id']

    def update_channel_offset(self, offset: int) -> None:
        channel = self._repository.get({"channel_name": self.channel_name})
        current_offset = channel.offset
        if offset > current_offset:
            self._repository.update(self.channel_name, offset=offset)

    def get_channel_info(self) -> Tuple[int, schemas.Channel]:
        channel_data = self._fetch_channel()
        return self._scraper.extract_channel_info(channel_data)

    def _fetch_channel(self) -> str:
        method = schemas.Method(
            name="channels.getFullChannel",
            params={
                "channel": {
                    "_":  "inputChannel",
                    **self.peer_channel.dict(),
                }
            }
        )
        resp = self._http_agent.post(
            self.query_api_url,
            data=json.dumps({'args': method.args, "username": "989044600776"}))
        return resp.json()

    def _channel_info_updated(self, channel: schemas.Channel) -> bool:
        if old_channel := self._repository.get({"id": channel.id}):
            return old_channel.title != channel.title or\
                old_channel.username != channel.username or\
                old_channel.participants_count != channel.participants_count or\
                old_channel.about != channel.about
        
        return True

    def publish(self, data: schemas.BaseModel, queue: str) -> None:
        # RabbitConnection.publish(
        #     json.dumps(data.json(), ensure_ascii=False),
        #     channel=self._rabbit_channel,
        #     exchange=queue,
        #     routing_key=queue
        # )
        pass


class MessageCrawler(ChannelCrawler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def start(self, offset: int) -> int:
        next_page_offest, messages = self.get_msg_page(offset)

        if messages:
            self._repository.add_msg_to_channel(self.peer_channel.channel_id, messages)
            for msg in messages:
                self.publish(
                    msg,
                    queue=SETTINGS.MESSAGES_QUEUE
            )

        return next_page_offest
        
    def get_msg_page(self, offset: int) -> Tuple[int, List[schemas.Message]]:
        history_data = self._fetch_msg_page(offset)
        return self._scraper.extarct_messages(history_data)

    def _fetch_msg_page(self, offset: int) -> str:
        method = schemas.Method(
            name="messages.getHistory",
            params={
                "add_offset": 0,
                "hash": 0,
                "limit": 100,
                "max_id": 0,
                "min_id": 0,
                "offset_date": 0,
                "offset_id": offset,
                "peer": {
                    "_": "inputPeerChannel",
                    **self.peer_channel.dict(),
                }
            },
        )
        resp = self._http_agent.post(
            self.query_api_url, 
            data=json.dumps({'args': method.args, "username": "989044600776"}))
        return resp.json()
