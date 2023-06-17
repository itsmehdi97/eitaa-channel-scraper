from typing import Tuple, List
from datetime import datetime

from bs4 import BeautifulSoup

import schemas
from core.config import get_settings


SETTINGS = get_settings()


class MessageScraper:
    """
    extracts entitites from html.
    """

    def extract_channel_info(self, channel_text: str) -> Tuple[int | None, schemas.Channel]:
        offset = None

        soup = BeautifulSoup(channel_text, "html.parser")
        tag = soup.find("link", attrs={"rel": "canonical"})
        if tag:
            offset = int(tag.attrs["href"].split("=")[-1])
        
        stats = self._parse_channel_stats(soup)

        info = None
        if info_container := soup.select_one(".etme_channel_info_description"):
            info = info_container.get_text()

        return offset, schemas.Channel(
            faname=soup.select_one('.etme_channel_info_header_title').get_text(),
            username=soup.select_one('.etme_channel_info_header_username').select_one('a').get_text()[1:],
            info=info,
            img_url=f"eitaa.com/{soup.select_one('img').attrs['src']}",
            **stats
        )

    def extarct_messages(self, messages_text: str) -> Tuple[int | None, List[str]]:
        messages_text = (  # TODO
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

    def _parse_number(self, numstr: str):
        factors = {
            'هزار': 1000,
            'میلیون': 1000000,
        }

        for key, val in factors.items():
            if key in numstr:
                return float(numstr.replace(key, '')) * val

        return float(numstr)

    def _parse_channel_stats(self, soup: BeautifulSoup) -> dict:
        keys = {
            'دنبال\u200cکننده': 'num_follower',
            'عکس': 'num_img',
            'ویدیو': 'num_vid',
            'فایل': 'num_file'
        }

        nums = map(lambda tag: self._parse_number(tag.get_text().strip()), soup.select('.counter_value'))
        names = map(lambda tag: keys[tag.get_text().strip()], soup.select('.counter_type'))

        return dict(zip(names, nums))