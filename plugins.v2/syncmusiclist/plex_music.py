from urllib.parse import urlencode

from app.log import logger
from app.schemas import MediaServerLibrary
from app.modules.plex import Plex
from plexapi.myplex import MyPlexAccount
from plexapi.library import Hub


class PlexMusic(Plex):
    """API for Plex
        继承根项目, 实现音乐库相关的api
    """

    def __init__(self, host: str = None, token: str = None, play_host: str = None,
                 sync_libraries: list = None, **kwargs):
        Plex.__init__(self, host=host, token=token)
        self.music_libraries = []
        self.music_playlists = []
        self.music_names = []

    def get_user_name(self):
        account = MyPlexAccount(self._token)
        return account.username

    def get_music_library(self):
        """
        获取媒体服务器所有音乐类型媒体库列表
        """
        if not self._plex:
            return []
        try:
            _libraries = self._plex.library.sections()
        except Exception as err:
            logger.error(f"Plex获取媒体服务器所有媒体库列表出错：{str(err)}")
            return []
        for library in _libraries:
            match library.type:
                case "artist":
                    library_type = '音乐'
                case _:
                    continue
            self.music_names.append(library.title)
            self.music_libraries.append(
                MediaServerLibrary(
                    id=library.key,
                    name=library.title,
                    path=library.locations,
                    type=library_type
                )
            )
        return self.music_libraries

    def get_playlists(self):
        """获取播放列表"""
        try:
            playlists = self._plex.playlists()
        except:
            playlists = []
        for playlist in playlists:
            if playlist.isAudio:
                # music_playlists.append(
                #     {
                #         "name": playlist.title,
                #         "id": playlist.ratingKey,
                #         "url": playlist.thumbUrl,
                #     }
                # )
                self.music_playlists.append(playlist.title)
        return self.music_playlists

    def get_tracks_by_playlist(self, playlist_title):
        """获取播放列表的详细歌单"""
        if playlist_title not in self.music_playlists:
            logger.warn(f"Plex媒体库中播放列表为:{self.music_playlists}\n 不存在: {playlist_title}, 稍后会自动创建，如果失败请手动创建")
            return []
        playlist = self._plex.playlist(playlist_title)
        # 获取歌单中的歌曲
        tracks = playlist.items()
        music_names = [i.title for i in tracks]
        return music_names

    def create_playlist(self, title, items):
        """创建播放列表"""
        self._plex.createPlaylist(title=title, items=items, libtype='track')

    def set_tracks_to_playlist(self, playlist_title, tracks):
        """添加歌曲到歌单中"""
        playlist = self._plex.playlist(playlist_title)
        playlist.addItems(tracks)

    def search(self, query, mediatype=None, limit=None, sectionId=None):
        """ Returns a list of media items or filter categories from the resulting
            `Hub Search <https://www.plex.tv/blog/seek-plex-shall-find-leveling-web-app/>`_
            against all items in your Plex library. This searches genres, actors, directors,
            playlists, as well as all the obvious media titles. It performs spell-checking
            against your search terms (because KUROSAWA is hard to spell). It also provides
            contextual search results. So for example, if you search for 'Pernice', it’ll
            return 'Pernice Brothers' as the artist result, but we’ll also go ahead and
            return your most-listened to albums and tracks from the artist. If you type
            'Arnold' you’ll get a result for the actor, but also the most recently added
            movies he’s in.

            Parameters:
                query (str): Query to use when searching your library.
                mediatype (str, optional): Limit your search to the specified media type.
                    actor, album, artist, autotag, collection, director, episode, game, genre,
                    movie, photo, photoalbum, place, playlist, shared, show, tag, track
                limit (int, optional): Limit to the specified number of results per Hub.
                sectionId (int, optional): The section ID (key) of the library to search within.
        """
        results = []
        params = {
            'query': query,
            'X-Plex-Token': self._token,
            'includeCollections': 1,
            'includeExternalMedia': 1}
        if limit:
            params['limit'] = limit
        if sectionId:
            params['sectionId'] = sectionId


        key = f'/hubs/search?{urlencode(params)}'
        for hub in self._plex.fetchItems(key, Hub):
            if mediatype:
                if hub.type == mediatype:
                    return hub.items
            else:
                results += hub.items
        return results

    def search_music(self, name_singer, exact_match=True):
        """通过歌曲名在库中进行搜索"""
        name = name_singer[0]
        singers = name_singer[1]
        if len(self.music_libraries) == 1:
            search_res = self.search(name, mediatype='track', limit=20, sectionId=int(self.music_libraries[0].id))
        else:
            search_res = self.search(name, mediatype='track', limit=20)
        add_items = []
        if len(search_res) > 1:
            if exact_match:
                add_items = []
                bitrate = 0
                # 通过歌手过滤
                for i in search_res:
                    if i.grandparentTitle or i.grandparentTitle in singers:
                        # add
                        if i.media[0].bitrate or 1 > bitrate:
                            bitrate = i.media[0].bitrate or 1
                            add_items = [i]
                    else:
                        str_arts = i.originalTitle or i.grandparentTitle
                        for singer in singers:
                            if singer in str_arts:
                                # add
                                if i.media[0].bitrate > bitrate:
                                    bitrate = i.media[0].bitrate or 1
                                    add_items = [i]
            else:
                add_items = search_res[:1]
        return add_items


if __name__ == '__main__':
    pm = PlexMusic()
    ml = pm.get_user_name()
    pm.get_playlists()
    pm.get_tracks_by_playlist('经典华语')
    res = pm.search_music(['雨蝶', ['刘君']])
    print(res)
