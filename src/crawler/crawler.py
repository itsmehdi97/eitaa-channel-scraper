import json
from typing import Tuple, List
from logging import getLogger

from requests import Session
from pika.channel import Channel as PikaChannel
from pika.exceptions import AMQPConnectionError, AMQPChannelError

import schemas
from core.config import get_settings
from crawler.scraper import JSONMessageScraper
from adapters import MongoChannScheduleRepository, RabbitConnection
from crawler import exceptions


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

        if self._channel_info_updated(channel):
            self.publish_many(
                [channel],
                SETTINGS.CHANNELS_QUEUE)

            self._repository.update(self.peer_channel.channel_id, **channel.dict())

        chan_sched = self._repository.get({"channel_id": self.peer_channel.channel_id})
        if chan_sched.pts:
            if pts <= chan_sched.pts:
                logger.info(
                    f"Channel `{self.peer_channel.channel_id}` has not changed since last run"
                )
                return chan_sched.offset
        else:
            self._repository.update(self.peer_channel.channel_id, pts=pts)

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
            'peer_channel': self.peer_channel.dict(),
            'start_offset':msg_offset,
            'end_offset':_prev_offset
        })

        return msg_offset

    @property
    def get_prev_run_offset(self) -> int | None:
        channel = self._repository.get({"channel_id": self.peer_channel.channel_id})
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
        resp = self._http_agent.post(
            self.query_api_url,
            data=json.dumps({'args': method.args, "username": "989215988724"})).json()

        if resp.get('_') == 'error':
            raise exceptions.ChannelFetchException(resp.get("text"))

        return resp['messages'][0]['id']

    def update_channel_offset(self, offset: int) -> None:
        channel = self._repository.get({"channel_id": self.peer_channel.channel_id})
        current_offset = channel.offset
        if offset > current_offset:
            self._repository.update(self.peer_channel.channel_id, offset=offset)

    def get_channel_info(self) -> Tuple[int, schemas.Channel]:
        channel_data = self._fetch_channel()
        return self._scraper.extract_channel_info(channel_data)

    def stop(self, error: str=None) -> None:
        from worker.celery import app as celery_app
        from redbeat import RedBeatSchedulerEntry

        entry = RedBeatSchedulerEntry.from_key(f"redbeat:crawl-{self.peer_channel.channel_id}", app=celery_app)
        entry.delete()

        self._repository.update(self.peer_channel.channel_id, running=False, error=error)

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
            data=json.dumps({'args': method.args, "username": "989215988724"})).json()

        if resp.get('_') == 'error':
            raise exceptions.ChannelFetchException(resp.get("text"))

        return resp

    def _channel_info_updated(self, channel: schemas.Channel) -> bool:
        if old_channel := self._repository.get({"channel_id": self.peer_channel.channel_id}):
            return old_channel.title != channel.title or\
                old_channel.username != channel.username or\
                old_channel.participants_count != channel.participants_count or\
                old_channel.about != channel.about
        
        return True
    
    def _publish(self, data: schemas.BaseModel, queue: str) -> None:
        RabbitConnection.publish(
            json.dumps(data.json(), ensure_ascii=False),
            channel=self._rabbit_channel,
            exchange=queue,
            routing_key=queue
        )
    
    def publish_many(self, data: List[schemas.BaseModel], queue: str) -> None:
        from worker.tasks import publish_many

        num_published = 0
        try:
            for item in data:
                self._publish(item, queue)
                num_published += 1

        except (AMQPConnectionError, AMQPChannelError):
            logger.info(f'Connection to broker failed. published count: {num_published}')
            publish_many.apply_async(kwargs={
                'data': list(map(lambda item: item.json(), data[num_published:])),
                'queue': queue
            })


class MessageCrawler(ChannelCrawler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def start(self, offset: int, end_offset: int) -> Tuple[int, bool]:
        next_page_offest, messages, channels, users = self.get_msg_page(offset)


        self.publish_many(
            list(filter(lambda m: m.id >= end_offset, messages)), SETTINGS.MESSAGES_QUEUE)
        self.publish_many(channels, SETTINGS.CHANNELS_QUEUE)
        self.publish_many(users, SETTINGS.USERS_QUEUE)

        return next_page_offest, False
        
    def get_msg_page(self, offset: int) -> Tuple[int, List[schemas.Message], List[schemas.Channel], List[schemas.User]]:
        history_data = self._fetch_msg_page(offset)
        return self._scraper.extract_entities(history_data)

    def _fetch_msg_page(self, offset: int) -> str:
        method = schemas.Method(
            name="messages.getHistory",
            params={
                "add_offset": 0,
                "hash": 0,
                "limit": 40,
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
            data=json.dumps({'args': method.args, "username": "989215988724"})).json()

        if resp.get('_') == 'error':
            raise exceptions.ChannelFetchException(resp.get("text"))

        return resp
