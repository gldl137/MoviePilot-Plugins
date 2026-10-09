# -*- coding: utf-8 -*-
"""
整理刮削清理插件 (MoviePilot V3)
================================

场景：MoviePilot 重新整理（覆盖/重命名）一个媒体文件时，会先删除旧的整理文件，
再写入新命名的目标文件。旧文件同目录下的刮削文件（.nfo、图片、字幕等）因名称与
新文件对不上而成为「孤儿」，残留在媒体库中，导致媒体服务器读到错误/失效的元数据。

本插件在「整理完成」事件（EventType.TransferComplete）触发后，扫描目标目录，删除
那些不再归属于当前任何媒体文件的刮削文件，使刮削文件与整理后的文件保持一致。

实现说明：
- 仅删除「刮削文件后缀」配置中列出的后缀文件（默认 JPG、nfo），绝不删除目录、
  绝不删除实际媒体文件；不在列表中的后缀一律不动；
- 整目录级刮削图（poster.jpg / fanart.jpg / tvshow.nfo / season*-poster.jpg 等）
  默认受保护，不会被误删；
- 只有当目录中存在至少一个媒体文件时才会清理，空目录/纯刮削目录不会被动清理；
- 整理完成后自动清理目标目录（immediate 模式），也可通过命令对指定目录手动清理。
"""
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Tuple

# V3 规范：优先使用 app.sdk 稳定入口，旧宿主回退到兼容路径
try:
    from app.sdk.events import Event, eventmanager
    from app.sdk.logging import logger
    from app.sdk.plugin import _PluginBase
except ImportError:  # 兼容尚未提供 app.sdk 的旧宿主
    from app.core.event import Event, eventmanager
    from app.log import logger
    from app.plugins import _PluginBase

from app.schemas.types import EventType, MessageType

try:
    from app.chain.storage import StorageChain
except ImportError:  # 极旧宿主可能没有该链
    StorageChain = None

try:
    from app.schemas.file import FileItem
except ImportError:
    FileItem = None


class ScrapeReorgClean(_PluginBase):
    """
    整理刮削清理：重新整理后，删除与整理后文件对不上的孤儿刮削文件。
    """

    # 插件元信息
    plugin_name = "整理刮削清理"
    plugin_desc = (
        "重新整理（覆盖/重命名）媒体后，自动清理目标目录中不再匹配任何媒体文件的"
        "孤儿刮削文件（.nfo、图片、字幕等），避免残留失效元数据。"
    )
    plugin_icon = "mediasyncdel.png"
    plugin_version = "1.0.3"
    plugin_author = "gldl137"
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "scrapereorgclean_"
    plugin_order = 60
    auth_level = 1

    # 媒体文件后缀：只有这些后缀的文件才会被视为「媒体」，用于判断刮削文件归属
    MEDIA_EXTENSIONS = {
        # 视频
        ".strm", ".mkv", ".mp4", ".ts", ".avi", ".m2ts", ".iso", ".rmvb", ".wmv",
        ".mov", ".flv", ".m4v", ".mpg", ".mpeg", ".webm", ".3gp", ".m2v", ".vob",
        ".tp", ".trp", ".dat", ".bdmv",
        # 音频
        ".mp3", ".flac", ".aac", ".m4a", ".wav", ".ogg", ".opus", ".wma", ".dts",
        ".ac3", ".eac3", ".tak", ".ape", ".wv", ".mka", ".alac",
    }

    # 整目录级刮削文件的基础名（不带后缀），这些一律受保护，绝不删除
    PROTECTED_BARE = {
        "poster", "fanart", "banner", "clearart", "clearlogo", "logo",
        "disc", "discart", "thumb", "landscape", "backdrop", "tvshow",
        "folder", "movie",
    }

    # 配置项
    _enabled = False
    _clean_on_transfer = True
    _protect_folder_images = True
    _notify = False
    _scrap_extensions = "JPG, nfo"
    _scan_dirs = ""

    _scrap: List[str] = []
    _storage_chain: Any = None
    _lock = threading.Lock()

    # -----------------------------------------------------------
    # 生命周期
    # -----------------------------------------------------------
    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._clean_on_transfer = bool(config.get("clean_on_transfer", True))
        self._protect_folder_images = bool(config.get("protect_folder_images", True))
        self._notify = bool(config.get("notify"))
        self._scrap_extensions = config.get("scrap_extensions") or "JPG, nfo"
        self._scan_dirs = config.get("scan_dirs") or ""
        self._scrap = self._parse_custom_extensions(self._scrap_extensions)
        self._storage_chain = StorageChain() if StorageChain else None

        if self._enabled:
            logger.info(
                f"整理刮削清理已启用：整理后自动清理={self._clean_on_transfer}，"
                f"保护目录级图片={self._protect_folder_images}"
            )

    def get_state(self) -> bool:
        return self._enabled

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_page(self):
        pass

    def stop_service(self):
        pass

    # -----------------------------------------------------------
    # 事件订阅：整理完成
    # -----------------------------------------------------------
    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event) -> None:
        """
        整理完成后，扫描目标目录，清理孤儿刮削文件。
        """
        if not self._enabled or not self._clean_on_transfer:
            return
        if not event or not event.event_data:
            return

        diritem = self._get_diritem(event.event_data)
        if not diritem:
            logger.debug("整理刮削清理：无法从事件中获取目标目录，跳过")
            return

        try:
            deleted = self._clean_one_dir(diritem, do_recurse=False)
            if deleted and self._notify:
                self.post_message(
                    mtype=MessageType.SiteMessage,
                    title="🧹 整理刮削清理",
                    text=f"已清理 {deleted} 个孤儿刮削文件：{diritem.path}",
                )
        except Exception as e:
            logger.error(f"整理刮削清理失败：{e}")

    # -----------------------------------------------------------
    # 手动清理命令
    # -----------------------------------------------------------
    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/scrapeclean",
                "event": EventType.PluginAction,
                "desc": "清理目录中的孤儿刮削文件",
                "category": "插件",
                "data": {"action": "scrapeclean"},
            }
        ]

    def handle_command(self, event: Event):
        """处理 /scrapeclean 命令，对配置的目录递归清理。"""
        if not self._enabled:
            logger.warning("整理刮削清理插件未启用，忽略命令")
            return
        if not event or not event.event_data:
            return
        action = event.event_data.get("action")
        if action != "scrapeclean":
            return

        dirs = [d.strip() for d in self._scan_dirs.split("\n") if d.strip()]
        if not dirs:
            logger.warning("未配置扫描目录，无法执行手动清理")
            return

        total = 0
        for d in dirs:
            diritem = self._build_local_diritem(d)
            if not diritem:
                continue
            try:
                total += self._clean_one_dir(diritem, do_recurse=True)
            except Exception as e:
                logger.error(f"手动清理目录失败 {d}：{e}")
        logger.info(f"整理刮削清理：手动清理完成，共删除 {total} 个孤儿刮削文件")
        if total and self._notify:
            self.post_message(
                mtype=MessageType.SiteMessage,
                title="🧹 整理刮削清理",
                text=f"手动清理完成，共删除 {total} 个孤儿刮削文件",
            )

    # -----------------------------------------------------------
    # 目录清理核心逻辑
    # -----------------------------------------------------------
    def _clean_one_dir(self, diritem: Any, do_recurse: bool) -> int:
        """
        清理单个目录中的孤儿刮削文件（不删除目录本身）。
        返回删除（或试运行拟删除）的文件数量。
        """
        children = list(self._iter_dir(diritem))
        if not children:
            return 0

        # 计算当前目录中的媒体文件 stem 集合
        media_stems = {
            Path(name).stem
            for name, is_dir, storage, path in children
            if not is_dir and self._is_media(name)
        }

        # 目录中没有任何媒体文件时不清理，避免误删正在构建中的纯刮削目录
        if not media_stems:
            logger.debug(f"目录无媒体文件，跳过清理：{diritem.path}")
            return 0

        deleted = 0
        for name, is_dir, storage, path in children:
            if is_dir:
                if do_recurse:
                    sub = self._build_diritem(storage, path, name)
                    if sub:
                        deleted += self._clean_one_dir(sub, True)
                continue

            if not self._is_scrap(name):
                continue
            if not self._is_orphan(name, media_stems):
                continue

            logger.info(f"删除孤儿刮削文件：{path}")
            if self._delete_child(storage, path):
                deleted += 1

        return deleted

    # -----------------------------------------------------------
    # 刮削文件 / 孤儿判定
    # -----------------------------------------------------------
    def _is_media(self, name: str) -> bool:
        ext = os.path.splitext(name)[1].lower()
        return ext in self.MEDIA_EXTENSIONS

    def _is_scrap(self, name: str) -> bool:
        ext = os.path.splitext(name)[1].lower()
        return ext in self._scrap

    @staticmethod
    def _same_media_scrap_name(name: str, media_stem: str) -> bool:
        """
        刮削文件 name 是否属于媒体 media_stem：
        name 以 media_stem 开头，且紧跟的字符为非字母数字边界（或 name 仅为 media_stem+后缀）。
        避免 S01E01 误匹配 S01E010。
        """
        if not name.startswith(media_stem):
            return False
        if len(name) == len(media_stem):
            return True
        return not name[len(media_stem)].isalnum()

    def _is_orphan(self, name: str, media_stems: set) -> bool:
        """
        判断刮削文件 name 是否为孤儿（不属于目录中任何媒体文件）。
        返回 True 表示应删除。
        """
        # 整目录级刮削文件受保护
        if self._protect_folder_images:
            base = Path(name).stem.lower()
            if base in self.PROTECTED_BARE:
                return False
            # 季级图片：season01-poster.jpg 等
            if base.startswith("season"):
                return False

        # 不属于任何当前媒体文件 → 孤儿
        return not any(self._same_media_scrap_name(name, ms) for ms in media_stems)

    # -----------------------------------------------------------
    # 目录列举 / 删除（StorageChain 优先，本地回退 pathlib）
    # -----------------------------------------------------------
    def _iter_dir(self, diritem: Any) -> List[Tuple[str, bool, str, str]]:
        """
        列举目录子项，返回 [(name, is_dir, storage, path), ...]
        """
        storage = getattr(diritem, "storage", "local") or "local"
        path = getattr(diritem, "path", None)
        children: List[Tuple[str, bool, str, str]] = []

        sc = self._storage_chain
        raw = None
        if sc is not None:
            try:
                raw = sc.list_files(diritem)
            except Exception as e:
                logger.warning(f"列举目录失败，尝试本地回退：{e}")

        if raw:
            for c in raw:
                c_name = getattr(c, "name", None) or ""
                c_type = getattr(c, "type", None)
                c_storage = getattr(c, "storage", storage) or storage
                c_path = getattr(c, "path", None)
                if not c_name or not c_path:
                    continue
                children.append((c_name, c_type == "dir", c_storage, c_path))
            return children

        # 本地回退
        if storage == "local" and path:
            p = Path(path)
            if p.is_dir():
                for f in p.iterdir():
                    children.append((f.name, f.is_dir(), "local", str(f)))
        return children

    def _delete_child(self, storage: str, path: str) -> bool:
        """删除单个文件，返回是否成功。"""
        sc = self._storage_chain
        if sc is not None and FileItem is not None:
            try:
                item = FileItem(
                    storage=storage, path=path, type="file", name=Path(path).name
                )
                if sc.delete_file(item):
                    return True
            except Exception as e:
                logger.warning(f"通过存储链删除失败，尝试本地删除：{e}")
        if storage == "local":
            try:
                Path(path).unlink()
                return True
            except Exception as e:
                logger.error(f"删除文件失败：{path} - {e}")
        return False

    # -----------------------------------------------------------
    # 工具
    # -----------------------------------------------------------
    @staticmethod
    def _parse_custom_extensions(custom: str) -> List[str]:
        if not custom:
            return []
        exts: List[str] = []
        for item in custom.replace("，", ",").replace("\n", ",").split(","):
            e = item.strip().lower()
            if not e:
                continue
            if not e.startswith(".") and not e.startswith("-"):
                e = f".{e}"
            if e not in exts:
                exts.append(e)
        return exts

    def _get_diritem(self, event_data: Dict[str, Any]) -> Any:
        """从 TransferComplete 事件数据中提取目标目录 FileItem。"""
        ti = event_data.get("transferinfo")
        if not ti:
            return None

        diritem = self._attr_or_dict(ti, "target_diritem")
        if diritem is not None:
            return diritem

        # 回退：从目标文件推导其父目录
        item = self._attr_or_dict(ti, "target_item")
        if item is not None:
            storage = self._attr_or_dict(item, "storage") or "local"
            path = self._attr_or_dict(item, "path")
            if path:
                parent = str(Path(path).parent)
                return self._build_diritem(storage, parent, Path(parent).name)
        return None

    @staticmethod
    def _attr_or_dict(obj: Any, key: str) -> Any:
        """兼容对象与字典两种形态。"""
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    def _build_diritem(self, storage: str, path: str, name: str) -> Any:
        if FileItem is not None:
            return FileItem(storage=storage or "local", path=path, type="dir", name=name)
        # 极旧宿主无 FileItem 时，构造轻量对象
        class _Dir:
            def __init__(self, s, p, n):
                self.storage = s
                self.path = p
                self.name = n
                self.type = "dir"
        return _Dir(storage or "local", path, name)

    def _build_local_diritem(self, dir_path: str) -> Any:
        p = Path(dir_path)
        storage = "local"
        if not p.exists():
            logger.warning(f"扫描目录不存在：{dir_path}")
            return None
        return self._build_diritem(storage, str(p), p.name)

    # -----------------------------------------------------------
    # 配置页面
    # -----------------------------------------------------------
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {"component": "VSwitch", "props": {"model": "clean_on_transfer", "label": "整理后自动清理"}}
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {"component": "VSwitch", "props": {"model": "protect_folder_images", "label": "保护目录级图片"}}
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VTextarea",
                                "props": {
                                    "model": "scan_dirs",
                                    "label": "手动清理目录（命令 /scrapeclean 使用）",
                                    "rows": 3,
                                    "placeholder": "每行一个目录\n/STRM影视/影片/夸克网盘",
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VTextarea",
                                "props": {
                                    "model": "scrap_extensions",
                                    "label": "刮削文件后缀",
                                    "rows": 2,
                                    "placeholder": "每行或逗号分隔，例如：.json\n.bif",
                                    "hint": "仅删除此处列出的后缀文件（区分大小写已自动忽略），默认 JPG、nfo",
                                    "persistent-hint": True,
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "text": (
                                        "本插件仅在目录中仍存在媒体文件时才会清理孤儿刮削文件，"
                                        "不会删除目录、不会删除实际媒体文件，也不会删除 poster.jpg / "
                                        "fanart.jpg / tvshow.nfo / season*-poster.jpg 等目录级图片"
                                        "（可在「保护目录级图片」关闭，但请谨慎）。"
                                    ),
                                },
                            }
                        ],
                    },
                ],
            },
        ], {
            "enabled": False,
            "clean_on_transfer": True,
            "notify": False,
            "protect_folder_images": True,
            "scan_dirs": "",
            "scrap_extensions": "JPG, nfo",
        }
