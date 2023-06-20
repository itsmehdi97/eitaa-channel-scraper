from logging import getLogger

from worker.celery import app
from core.config import get_settings
from crawler import ChannelCrawler, MessageCrawler


logger = getLogger(__name__)

settings = get_settings()


@app.task(bind=True)
def refresh_channel(self, *, channel_name: str) -> None:  # type: ignore
    crawler = ChannelCrawler(
        channel_name,
        http_agent=self.http_session,
        scraper=self._get_scraper(),
        repository=self.repository,
        rabbit_channel=self.rabbit_channel
    )

    crawler.start()


@app.task(bind=True)
def get_message_page(self, *, channel_name: str, start_offset: int, end_offset: int) -> None:
    if not start_offset:
        logger.info(f"start offset for channel `{channel_name}`: None")
        return

    if start_offset <= end_offset or start_offset == 1:
        # logger.info(f"start offset for channel `{channel_name}`: None")?
        return

    crawler = MessageCrawler(
        channel_name,
        http_agent=self.http_session,
        scraper=self._get_scraper(),
        repository=self.repository,
        rabbit_channel=self.rabbit_channel
    )

    next_start_offset = crawler.start(start_offset)

    if next_start_offset and \
        next_start_offset >= end_offset and next_start_offset != 1:
        get_message_page.apply_async(kwargs={
            'channel_name': channel_name,
            'start_offset': next_start_offset,
            'end_offset': end_offset
        })
