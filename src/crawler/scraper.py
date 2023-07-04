from typing import Tuple, List
from datetime import datetime

from bs4 import BeautifulSoup

import schemas
from core.config import get_settings
from .exceptions import InvalidHTML


SETTINGS = get_settings()


class HTMLMessageScraper:
    """
    extracts entitites from html.
    """

    def extract_channel_info(
        self, channel_text: str
    ) -> Tuple[int | None, schemas.Channel]:
        offset = None

        soup = BeautifulSoup(channel_text, "html.parser")
        tag = soup.find("link", attrs={"rel": "canonical"})
        if tag:
            offset = int(tag.attrs["href"].split("=")[-1])
        else:
            raise InvalidHTML("Html received from channel is not valid")

        stats = self._parse_channel_stats(soup)

        info_container = soup.select_one(".etme_channel_info")

        info = None
        if desc := info_container.select_one(".etme_channel_info_description"):
            info = desc.get_text()

        return offset, schemas.Channel(
            faname=info_container.select_one(
                ".etme_channel_info_header_title"
            ).get_text(),
            username=info_container.select_one(".etme_channel_info_header_username")
            .select_one("a")
            .get_text()[1:],
            info=info,
            img_url=f"eitaa.com{info_container.select_one('img').attrs['src']}",
            **stats,
        )

    def extarct_messages(self, messages_text: str) -> Tuple[int | None, List[str]]:
        messages_text = (  # TODO
            messages_text[1:-1].replace("\\r", "").replace("\\n", "").replace("\\", "")
        )

        soup = BeautifulSoup(messages_text, "html.parser")
        message_wraps = soup.select(".etme_widget_message_wrap")
        messages = []
        for wrap in message_wraps:
            msg_text = None
            text_container = wrap.select_one(".etme_widget_message_text")
            if text_container:
                msg_text = text_container.get_text()

            num_views = None
            views_container = wrap.select_one(".etme_widget_message_views")
            if views_container:
                num_views = self._parse_number(views_container.get_text().strip())

            img_url = None
            if img := wrap.select_one('.etme_widget_message_photo_wrap'):
                s = img.attrs['style']
                img_url = "eitaa.com" + s[s.find("url('")+5:s.find("')")]

            vid_url = None
            vid_duration = None
            if vid := wrap.select_one('video.etme_widget_message_video'):
                vid_url = "eitaa.com" + vid.attrs['src']
                if v := wrap.select_one('time.message_video_duration'):
                    vid_duration = v.get_text()

            messages.append(
                schemas.Message(
                    id=int(wrap.attrs["id"]),
                    text=msg_text,
                    num_views=num_views,
                    img_url=img_url,
                    vid_url=vid_url,
                    vid_duration=vid_duration,
                    timestamp=datetime.strptime(
                        wrap.select("time")[-1].attrs["datetime"],
                        "%Y-%m-%dT%H:%M:%S+00:00",
                    ),
                )
            )

        offset = None
        if messages:
            offset = int(messages[0].id)

        return offset, messages

    def _parse_number(self, numstr: str):
        factors = {
            "هزار": 1000,
            "میلیون": 1000000,
        }

        for key, val in factors.items():
            if key in numstr:
                return float(numstr.replace(key, "")) * val

        return float(numstr)

    def _parse_channel_stats(self, soup: BeautifulSoup) -> dict:
        keys = {
            "دنبال\u200cکننده": "num_follower",
            "عکس": "num_img",
            "ویدیو": "num_vid",
            "فایل": "num_file",
        }

        nums = map(
            lambda tag: self._parse_number(tag.get_text().strip()),
            soup.select(".counter_value"),
        )
        names = map(
            lambda tag: keys[tag.get_text().strip()], soup.select(".counter_type")
        )

        return dict(zip(names, nums))


class JSONMessageScraper:
    """
    extracts entitites from json.
    """

    def extract_channel_info(
        self, channel_data: dict
    ) -> Tuple[int | None, schemas.Channel]:
        
        full_chat: dict = channel_data["full_chat"]
        chat: dict = channel_data["chats"][0]

        pts = full_chat.get("pts")
        
        return pts, schemas.Channel(
            channel_id=chat["id"],
            title=chat.get("title"),
            username=chat.get("username"),
            about=full_chat.get("about"),
            participants_count=full_chat.get("participants_count"),
        )

    def extract_entities(self, history_data: dict) -> Tuple[int | None, List[schemas.Message], List[schemas.Channel], List[schemas.User]]:
        offset = None
        messages = history_data.get('messages', [])
        chats = history_data.get('chats', [])
        users = history_data.get('users', [])
        if messages:
            offset = messages[-1]['id']

        parsed_msgs = []
        for msg in messages:
            parsed_msgs.append(schemas.Message(
                id=msg['id'],
                message=msg.get('message'),
                date=msg.get('date'),
                views=msg.get('views'),
                forwards=msg.get('forwards'),
                channel_id=msg.get('peer_id').get('channel_id'),
                from_peer=self._get_from_peer(msg.get('from_id')),
                fwd_from=self._get_fwd_info(msg.get('fwd_from'))
            ))

        parsed_chats = []
        for chat in chats:
            parsed_chats.append(schemas.Channel(
                channel_id=chat.get('id'),
                access_hash=chat.get('access_hash'),
                title=chat.get('title'),
                username=chat.get('username'),
                participants_count=chat.get('participants_count')
            ))

        parsed_users = []
        for user in users:
            parsed_users.append(schemas.User(
                id=user.get('id'),
                access_hash=user.get('access_hash'),
                first_name=user.get('first_name'),
                last_name=user.get('last_name'),
                username=user.get('username'),
                phone=user.get('phone')
            ))
        
        return offset, parsed_msgs, parsed_chats, parsed_users

    def _get_from_peer(self, from_id) -> dict | None:
        if not from_id:
            return
        rv = {'peer_type': from_id.get('_')}

        if xid := from_id.get('channel_id'):
            rv['channel_id'] = xid

        if xid := from_id.get('user_id'):
            rv['user_id'] = xid

        return rv

    def _get_fwd_info(self, fwd_from) -> dict | None:
        if not fwd_from:
            return

        rv = {'date': fwd_from.get('date')}
        if channe_post_id := fwd_from.get('channel_post'):
            rv['channe_post_id'] = channe_post_id
        
        rv['from_peer'] = self._get_from_peer(fwd_from.get('from_id'))
        return rv
                