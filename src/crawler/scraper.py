from typing import Tuple, List

from bs4 import BeautifulSoup

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
        messages = soup.select(SETTINGS.MESSAGE_CONTAINER_SELECTOR)

        offset = None
        if messages:
            offset = int(messages[0].attrs["id"])

        return offset, list(map(lambda x: str(x), messages))
