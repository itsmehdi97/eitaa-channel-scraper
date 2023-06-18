from worker.celery import app
from core.config import get_settings
from crawler import ChannelCrawler


settings = get_settings()


@app.task(bind=True)
def refresh_channel(self, *, channel_name: str) -> None:  # type: ignore
    crawler = ChannelCrawler(
        channel_name,
        http_agent=self.http_session,
        scraper=self._get_scraper(),
        repository=self.repository,
    )

    crawler.start()
