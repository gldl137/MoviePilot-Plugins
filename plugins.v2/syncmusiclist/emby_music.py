from typing import List, Optional, Union, Dict, Generator, Tuple

from app.log import logger
from app.schemas import MediaType, MediaServerLibrary
from app.modules.emby import Emby
from app.utils.http import RequestUtils
from app.core.config import settings


class EmbyMusic(Emby):
    """API for Plex
        继承根项目, 实现音乐库相关的api
    """

    def __init__(self, host: str = None, apikey: str = None, play_host: str = None,
                 sync_libraries: list = None, **kwargs):
        Emby.__init__(self, host=host, apikey=apikey)
        self.default_user = self.get_default_user(settings.SUPERUSER)
        self.music_libraries = []
        self.music_playlists = []

    def get_default_user(self, user_name: str = None) -> Optional[Union[str, int]]:
        """
        获得管理员用户
        """
        if not self._host or not self._apikey:
            return None
        req_url = "%sUsers?api_key=%s" % (self._host, self._apikey)
        try:
            res = RequestUtils().get_res(req_url)
            if res:
                users = res.json()
                # 先查询是否有与当前用户名称匹配的
                if user_name:
                    for user in users:
                        if user.get("Name") == user_name:
                            return user.get("Name")
                # 查询管理员
                for user in users:
                    if user.get("Policy", {}).get("IsAdministrator"):
                        return user.get("Name")
            else:
                logger.error(f"Users 未获取到返回数据")
        except Exception as e:
            logger.error(f"连接Users出错：" + str(e))
        return None

    def __get_emby_librarys(self) -> List[dict]:
        """
        获取Emby媒体库列表
        """
        if not self._host or not self._apikey:
            return []
        req_url = f"{self._host}emby/Users/{self.user}/Views?api_key={self._apikey}"
        try:
            res = RequestUtils().get_res(req_url)
            if res:
                return res.json().get("Items")
            else:
                logger.error(f"EmbyUser/Views 未获取到返回数据")
                return []
        except Exception as e:
            logger.error(f"Emby连接User/Views 出错：" + str(e))
            return []

    def get_music_library(self):
        """
        获取媒体服务器所有音乐类型媒体库列表以及所有playlist
        """
        if not self._host or not self._apikey:
            return []
        emby_librarys = self.__get_emby_librarys() or []
        self.music_playlists = []
        self.music_libraries = []
        for library in emby_librarys:
            if library.get("CollectionType") == 'music':
                self.music_libraries.append(
                    MediaServerLibrary(
                        server="emby",
                        id=library.get("Id"),
                        name=library.get("Name"),
                        path=library.get("Path"),
                        type=library.get("CollectionType")
                    )
                )
            elif library.get("CollectionType") == 'playlists':
                url = f'{self._host}emby/Users/{self.user}/Items?ParentId={library.get("Id")}&api_key={self._apikey}'
                try:
                    res = self.get_data(url)
                    playlist = res.json().get("Items")
                except Exception as err:
                    logger.error(f"Emby获取播放列表失败: {err}")
                    playlist = []
                self.music_playlists += playlist
            else:
                continue
        return self.music_libraries

    def get_tracks_by_playlist(self, playlist_title):
        """获取播放列表的详细歌单"""
        playlist_id = None
        for i in self.music_playlists:
            if playlist_title == i.get('Name'):
                playlist_id = i.get('Id')
                break
        if not playlist_id:
            logger.warn(f"Emby媒体库中播放列表为:[{[i.get('Name') for i in self.music_playlists]}]\n 不存在: [{playlist_title}], 稍后会自动创建，如果失败请手动创建")
            return '', [], []
        url = f'{self._host}emby/Users/{self.user}/Items?ParentId={playlist_id}&api_key={self._apikey}'
        try:
            res = self.get_data(url)
            tracks = res.json().get("Items")
        except Exception as err:
            logger.error(f"Emby获取播放列表失败: {err}")
            tracks = []
        music_ids = [i.get('Id') for i in tracks if i.get('Type') == 'Audio']
        music_names = [i.get('Name') for i in tracks if i.get('Type') == 'Audio']
        return playlist_id, music_ids, music_names

    def create_playlist(self, name, ids, user=settings.SUPERUSER):
        """创建播放列表"""
        if name in [i.get('Name') for i in self.music_playlists]:
            logger.info(f"Emby歌单: [{name}] 已经存在，跳过创建")
        # Playlists
        url = f'{self._host}emby/Playlists?api_key={self._apikey}&userId={self.user}&Name={name}&Ids={ids}'
        try:
            res = self.post_data(
                url,
                headers={"Content-Type": "application/json"}
            )
            if res.status_code == 200:
                info = res.json()
                logger.info(f"Emby为用户{user}创建歌单成功:{info}")
                return True
            else:
                logger.error(f"Emby为用户{user}创建歌单失败:[{name}]")
                return False
        except Exception as e:
            logger.error(f"Emby为用户{user}创建歌单失败:[{name}]: {e}")
            return False

    def set_tracks_to_playlist(self, playlist_id, ids, user=settings.SUPERUSER):
        """添加歌曲到歌单中"""
        url = f'{self._host}emby/Playlists/{playlist_id}/Items?api_key={self._apikey}&userId={self.user}&Ids={ids}'
        try:
            res = self.post_data(
                url,
                headers={"Content-Type": "application/json"}
            )
            if res.status_code == 200:
                info = res.json()
                logger.info(f"Emby为用户{user}的歌单添加歌曲成功:[{info}]")
                return True
            else:
                logger.error(f"Emby为用户{user}的歌单添加歌曲失败")
                return False
        except Exception as e:
            logger.error(e)
            return False

    def search_music(self, name_singer, exact_match=True):
        """通过歌曲名在库中进行搜索"""
        # /emby/Users/{}/Items?Recursive=true&SearchTerm={}&api_key={}
        name = name_singer[0]
        singers = name_singer[1]
        url = f'{self._host}emby/Users/{self.user}/Items?Recursive=true&SearchTerm={name}&api_key={self._apikey}'
        try:
            res = self.get_data(url)
            count = res.json().get("TotalRecordCount")
            if count > 0:
                items = res.json().get("Items")
                if exact_match:
                    add_items = []
                    # 通过歌手过滤
                    for i in items:
                        arts = i.get("Artists", [])
                        if len(set(arts) & set(singers)) > 0:
                            # add
                            add_items = [i]
                            break
                        else:
                            str_arts = ' '.join(arts)
                            for singer in singers:
                                if singer in str_arts:
                                    # add
                                    add_items = [i]
                                    break
                            if add_items:
                                break
                else:
                    add_items = items

                ids_list = [i.get('Id') for i in add_items if i.get('Type') == 'Audio'][:1]
                name_list = [i.get('Name') for i in add_items if i.get('Type') == 'Audio'][:1]
            else:
                ids_list = []
                name_list = []
        except Exception as err:
            logger.error(err)
            return [], []
        return ids_list, name_list

    def mul_search_music(self, name_list, exact_match=True):
        logger.info(f"Emby中的播放列表为: {[i.get('Name') for i in self.music_playlists]}")
        all_list = []
        lack_list = []
        add_list = []
        for names in name_list:
            ids_list, name_list = self.search_music(names, exact_match)
            if len(ids_list) == 0:
                lack_list.append(names)
            else:
                add_list += name_list
                all_list += ids_list
        logger.info(f"Emby搜索库中获取歌曲[{len(add_list)}]首,列表为: {add_list}")
        logger.info(f"Emby库中未搜到歌曲[{len(lack_list)}]首,列表为: {lack_list}")
        return all_list


if __name__ == '__main__':
    em = EmbyMusic()
    emby_user = 'tzp'
    em.user = em.get_user(emby_user)
    em.get_music_library()
    media_playlist = "许嵩专版"
    t_tracks = [['通天大道宽又阔', ['崔京浩', '三叶草演唱组']], ['微微', ['傅如乔']], ['相思', ['毛阿敏']]]

    tracks = em.mul_search_music(t_tracks)
    playlist_id, music_ids, music_names = em.get_tracks_by_playlist(media_playlist)
    if playlist_id:
        ids = [i for i in tracks if i not in music_ids]
        em.set_tracks_to_playlist(playlist_id, ','.join(ids))
    else:
        em.create_playlist(media_playlist, ','.join(tracks))
    print(music_ids)


