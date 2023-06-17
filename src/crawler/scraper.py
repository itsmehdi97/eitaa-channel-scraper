from typing import Tuple, List
from datetime import datetime

from bs4 import BeautifulSoup

import schemas
from core.config import get_settings


SETTINGS = get_settings()


class MessageScraper:
    """
    extracts data from html.
    """

    def extract_channel_info(self, channel_text: str) -> Tuple[int | None, str]:
        offset = None

        soup = BeautifulSoup(channel_text, "html.parser")
        tag = soup.find("link", attrs={"rel": "canonical"})
        if tag:
            offset = int(tag.attrs["href"].split("=")[-1])

        info_div_tag_txt = soup.select_one(SETTINGS.INFO_CONTAINER_SELECTOR)

        return offset, str(info_div_tag_txt)

    def extarct_messages(self, messages_text: str) -> Tuple[int | None, List[str]]:
        messages_text = (
            messages_text[1:-1].replace("\\r", "").replace("\\n", "").replace("\\", "")
        )

        soup = BeautifulSoup(messages_text, "html.parser")
        message_wraps = soup.select('.etme_widget_message_wrap')
        messages = []
        for wrap in message_wraps:
            msg_text = None
            text_container = wrap.select_one('.etme_widget_message_text')
            if text_container:
                msg_text = text_container.get_text()

            num_views = None
            views_container = wrap.select_one('.etme_widget_message_views')
            if views_container:
                num_views = int(views_container.get_text().strip())

            messages.append(
                schemas.Message(
                    id=int(wrap.attrs['id']),
                    text=msg_text,
                    num_views=num_views,
                    timestamp=datetime.strptime(
                        wrap.select('time')[-1].attrs['datetime'],
                        '%Y-%m-%dT%H:%M:%S+00:00')
                )
            )

        offset = None
        if messages:
            offset = int(messages[0].id)

        return offset, messages
