import random
from logging import getLogger

import schemas
from worker.celery import app
from core.config import get_settings
from crawler import ChannelCrawler, MessageCrawler
from crawler import exceptions as exc


logger = getLogger(__name__)

settings = get_settings()


@app.task(bind=True,
    retry_jitter=True,
    retry_backoff=1,
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 10})
def refresh_channel(self, *, peer_channel: dict) -> None:  # type: ignore
    crawler = ChannelCrawler(
        peer_channel=schemas.PeerChannel(**peer_channel),
        http_agent=self.http_session,
        scraper=self._get_scraper(),
        repository=self.repository,
        rabbit_channel=self.rabbit_channel
    )

    try:
        crawler.start()

    except exc.ChannelFetchException as e:
        err_text = str(e)
        crawler.stop(error=err_text)

        logger.info(f"Channel `{peer_channel['channel_id']}` stopped: {err_text}")


@app.task(bind=True,
    retry_jitter=True,
    retry_backoff=1,
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 20})
def get_message_page(self, *, peer_channel: dict, start_offset: int, end_offset: int) -> None:
    if not start_offset:
        logger.info(f"start offset for channel `{peer_channel['channel_id']}`: None")
        return

    if start_offset <= end_offset or start_offset == 1:
        return

    crawler = MessageCrawler(
        peer_channel=schemas.PeerChannel(**peer_channel),
        http_agent=self.http_session,
        scraper=self._get_scraper(),
        repository=self.repository,
        rabbit_channel=self.rabbit_channel
    )

    next_start_offset, corrupted = crawler.start(start_offset, end_offset)
    if corrupted:
        self.retry(kwargs={
            'peer_channel': peer_channel,
            'start_offset': next_start_offset,
            'end_offset': end_offset
        }, max_retries=20, countdown=int(random.uniform(1, 2) * self.request.retries))

    if next_start_offset and \
        next_start_offset >= end_offset and next_start_offset != 1:
        get_message_page.apply_async(kwargs={
            'peer_channel': peer_channel,
            'start_offset': next_start_offset,
            'end_offset': end_offset
        })
