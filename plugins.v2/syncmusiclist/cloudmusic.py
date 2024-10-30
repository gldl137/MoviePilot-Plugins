
import re
import time


from app.log import logger
from app.plugins.syncmusiclist.netcloudmusic import NeteaseCloudMusicApi
from app.plugins.syncmusiclist.utils import change_str, sub_str


class CloudMusic(object):
    def __init__(self, path=None):
        self.music_api = NeteaseCloudMusicApi(path=path)  # 初始化API
        version_result = self.music_api.request("inner_version")
        logger.info(f'当前使用NeteaseCloudMusicApi版本号：{version_result["NeteaseCloudMusicApi"]}\n'
                    f'当前使用NeteaseCloudMusicApi_V8版本号：{version_result["NeteaseCloudMusicApi_V8"]}')

    def login_status(self):
        """账户登录状态"""
        try:
            if not self.music_api.cookie:
                return None
            response = self.music_api.request("/login/status")
            if response['data']['data']["code"] == 200:
                try:
                    nickname = response["data"]["data"]["profile"]["nickname"]
                    logger.info(f'当前登录账号：{nickname}')
                except:
                    nickname = None
                return nickname
            else:
                return None
        except:
            return None

    def captcha_sent(self, _phone):
        """验证码"""
        response = self.music_api.request("/captcha/sent", {"phone": f"{_phone}"})
        return response

    def login_cellphone(self, _phone, _captcha):
        """验证码登录"""
        response = self.music_api.request("/login/cellphone", {"phone": f"{_phone}", "captcha": f"{_captcha}"})
        return response


    def login(self, username, password):
        """
        登陆到网易云音乐
        调用登录接口后，会自动设置cookie，如果cookie失效，需要重新登录，登录过后api会在你的当前工作目录下创建cookie_storage文件保存你的cookie
        在下次调用运行程序时，他会判断cookie是否过期，没有过期就自动读取cookie_storage文件中的cookie。

        总的来说你不需要手动管理cookie，只需要调用登录接口，然后调用其他接口即可，cookie会自动设置，如果cookie过期，再次调用登录接口就好。
        更好的办法是，在cookie还没有失效之前使用refresh_login接口刷新cookie，这样就不需要重新登录了（建议在你每次启动软件时都刷新，当然频繁重启调试的时候另算）

        如果你想判断当前是否已经登录，if not netease_cloud_music_api.cookie 就可以了，或者调用/login/status接口

        """
        # 登录
        if not self.music_api.cookie:
            if username.isdigit():
                response = self.music_api.request("/login/cellphone",
                                                  {"phone": f"{username}", "password": f"{password}"})
            else:
                response = self.music_api.request("/login",
                                                  {"email": f"{username}", "password": f"{password}"})
            if response.get("code") == 200:
                logger.info("登录成功")
                if self.music_api.cookie:
                    logger.info("cookie已缓存")
            else:
                logger.error("登录失败")

    def signin(self):
        """网易云签到"""
        res = self.music_api.request("/daily_signin")
        return res

    def get_list_days(self, nums=5):
        """每日推荐歌单"""
        res = self.music_api.request("/recommend/resource")
        recommend = res.get('data', {}).get('recommend', [])
        res = [[i.get("id"), i.get("name")] for i in recommend[:nums] if i.get("id")]
        return res

    def get_song_daily(self):
        """每日推荐歌曲"""
        res = self.music_api.request("/recommend/songs")
        dailySongs = res['data']['data']['dailySongs']
        track_names = []
        for i in dailySongs:
            # 正则处理
            name = sub_str(i.get('name'))
            # 多歌手处理
            ars = [change_str(ar.get('name')) for ar in i.get('ar')]
            track_names.append([name, ars])
        return track_names


    def playlist(self, uid):
        """
        歌单
        Full request URI: http://music.163.com/api/playlist/detail?id=37880978&updateTime=-1
        GET http://music.163.com/api/playlist/detail
        必要参数：
            id：歌单ID
        """
        res = self.music_api.request(f"/playlist/track/all", {"id": f"{uid}"})
        return res.get('data', {}).get('songs')
    
    def songofplaylist(self, uid):
        """
        获取歌单中歌曲
        """
        tracks = []
        # 循环进行重试
        # 设置最大重试次数
        max_retry_times = 5
        # 当前重试次数
        retry_times = 0
        while retry_times < max_retry_times:
            try:
                tracks = self.playlist(uid)
                break  # 如果成功执行，跳出循环
            except Exception as e:
                logger.warn(f"第 {retry_times + 1} 次重试失败：获取歌单错误")
                retry_times += 1
                time.sleep(2)  # 添加延时
        track_names = []
        for i in tracks:
            # 正则处理
            name = sub_str(i.get('name'))
            # 多歌手处理
            ars = [change_str(ar.get('name')) for ar in i.get('ar')]
            track_names.append([name, ars])
        return track_names


if __name__ == '__main__':
    cm = CloudMusic()
    cm.signin()
    res_s = cm.get_song_daily()

    print(res_s)
    res_day = cm.get_list_days()

    res = cm.songofplaylist("365436873")
    print(res)


