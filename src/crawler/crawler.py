import time
from typing import Tuple, List
from logging import getLogger

from requests import Session

import schemas
from core.config import get_settings
from crawler.scraper import MessageScraper
from adapters import MongoChannScheduleRepository


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
    ) -> None:
        self.channel_name = channel_name
        self.channel_id = None
        self.channel_url = f"https://{SETTINGS.EITAA_DOMAIN}/{channel_name}"
        self._http_agent = http_agent
        self._scraper = scraper
        self._repository = repository

    def start(self) -> int:
        msg_offset, channel = self.get_channel_info()

        if self.get_prev_run_offset and self.get_prev_run_offset >= msg_offset:
            logger.info(
                f"No new message on channel `{self.channel_name}` since last run"
            )
            return msg_offset

        _prev_offset = self.get_prev_run_offset

        self._repository.update(self.channel_name, offset=msg_offset, **channel.dict())

        current_offset = msg_offset
        while True:
            time.sleep(SETTINGS.MESSAGE_FETCH_INTERVAL / 1000)
            next_page_offset, messages = self.get_msg_page(current_offset)
            if messages:
                self._repository.add_msg_to_channel(self.channel_name, messages)

            logger.debug(f"next page offset for channel `{self.channel_name}`: {next_page_offset}")
            if (
                not next_page_offset
                or next_page_offset == 1
                or (
                    _prev_offset
                    and next_page_offset <= _prev_offset
                )
            ):
                logger.info(
                    f"All messages on channel `{self.channel_name}` collected from beginning to offset {msg_offset}"
                )
                break

            current_offset = next_page_offset

        # self._repository.update(self.channel_name, offset=msg_offset)

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

    def get_msg_page(self, offset: int) -> Tuple[int, List[str]]:
        msg_text = self._fetch_msg_page(offset)
        return self._scraper.extarct_messages(msg_text)

    def get_channel_info(self) -> Tuple[int, schemas.Channel]:
        chnl_text = self._fetch_channel()
        return self._scraper.extract_channel_info(chnl_text)

    def _fetch_msg_page(self, offset: int) -> str:
        resp = self._http_agent.get(f"{self.channel_url}?before={offset}")
        return resp.text

    def _fetch_channel(self) -> str:
        resp = self._http_agent.get(self.channel_url)
        return resp.text
