import re
import json
import uuid

from app.plugins.syncmusiclist.utils import sub_str, change_str
from app.utils.http import RequestUtils


class QQMusicApi(object):
    """
    QQ音乐获取信息的api
    """
    def __init__(self):
        self.req = None
        self.cookie = ""
        self.headers = {}
        self.set_session()

    def set_cookie(self, cookie):
        self.cookie = cookie

    def set_headers(self):
        self.headers = {
            "authority": "u6.y.qq.com",
            "User-Agent": "QQ音乐/73222 CFNetwork/1406.0.2 Darwin/22.4.0".encode("utf-8"),
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh-Hans;q=0.9",
            "Referer": "http://y.qq.com",
            "Content-Type": "application/json; charset=UTF-8",
            "Cookie": self.cookie,
        }

    def set_session(self):
        self.set_headers()
        self.req = RequestUtils(headers=self.headers)
        return self.req

    def get_playlist_by_id(self, play_id):
        """
        通过歌单id获取歌单列表
        """
        _uuid = uuid.uuid1().__str__()
        url = "https://u.y.qq.com/cgi-bin/musicu.fcg"
        payload = {
            "getMusicPlaylist": {
                "module": "music.srfDissInfo.aiDissInfo",
                "method": "uniform_get_Dissinfo",
                "param": {
                    "disstid": int(play_id),
                    "userinfo": 1,
                    "tag": 1,
                    "is_pc": 1,
                    "guid": _uuid,
                },
            },
            "comm": {
                "g_tk": 0,
                "uin": "",
                "format": "json",
                "ct": 6,
                "cv": 80600,
                "platform": "wk_v17",
                "uid": "",
                "guid": _uuid,
            },
        }
        data = json.dumps(payload, ensure_ascii=False)
        res_json = self.req.post(url=url, data=data).json()
        playlist = res_json.get("getMusicPlaylist")
        if playlist["code"] == 0:
            lst = playlist.get('data', {}).get('songlist', [])
        else:
            lst = []
        list_clear = []
        for i in lst:
            # 正则处理
            name = sub_str(i.get('name'))
            # 多歌手处理
            ars = [change_str(ar.get('name')) for ar in i.get('singer')]
            list_clear.append([name, ars])
        return list_clear


if __name__ == '__main__':
    qq = QQMusicApi()
    res = qq.get_playlist_by_id('7039481526')
    print(res)

