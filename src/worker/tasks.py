from logging import getLogger

import schemas
from worker.celery import app
from core.config import get_settings
from crawler import ChannelCrawler, MessageCrawler


logger = getLogger(__name__)

settings = get_settings()


@app.task(bind=True)
def refresh_channel(self, *, peer_channel: dict) -> None:  # type: ignore
    crawler = ChannelCrawler(
        peer_channel=schemas.PeerChannel(**peer_channel),
        http_agent=self.http_session,
        scraper=self._get_scraper(),
        repository=self.repository,
        rabbit_channel=self.rabbit_channel
    )

    crawler.start()


@app.task(bind=True)
def get_message_page(self, *, peer_channel: dict, start_offset: int, end_offset: int) -> None:
    if not start_offset:
        logger.info(f"start offset for channel `{peer_channel['channel_id']}`: None")
        return

    if start_offset <= end_offset or start_offset == 1:
        # logger.info(f"start offset for channel `{channel_name}`: None")?
        return

    crawler = MessageCrawler(
        peer_channel=schemas.PeerChannel(**peer_channel),
        http_agent=self.http_session,
        scraper=self._get_scraper(),
        repository=self.repository,
        rabbit_channel=self.rabbit_channel
    )

    next_start_offset = crawler.start(start_offset, end_offset)

    if next_start_offset and \
        next_start_offset >= end_offset and next_start_offset != 1:
        get_message_page.apply_async(kwargs={
            'peer_channel': peer_channel,
            'start_offset': next_start_offset,
            'end_offset': end_offset
        })
