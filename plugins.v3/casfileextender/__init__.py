"""
CAS视频扩展（CasFileExtender）

让 MoviePilot 把 .cas 当作视频文件识别：
1. 把 .cas 注入 settings.RMT_MEDIAEXT（影响实时读取扩展名的各处，如扫描、下载判断等）；
2. 刷新 TransferChain 单例缓存的 _media_exts / _allowed_exts（整理链按此缓存判定媒体类型）；
3. 刷新 Monitor 单例的 dispatcher.all_exts（目录监控按此过滤并触发整理）。

关键点：TransferChain 与 Monitor 的扩展名都是在实例 __init__ 时一次性算好并缓存的，
并不会实时读 settings，所以仅修改 settings.RMT_MEDIAEXT 或在类上打补丁都不够，
必须直接刷新这些已实例化对象的缓存列表。
"""
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.log import logger

try:  # 事件系统（整理重命名 Hook）
    from app.core.event import eventmanager, Event
    from app.schemas.types import ChainEventType
    _EVENT_OK = True
except ImportError:  # 兼容旧版宿主
    eventmanager = None  # type: ignore
    Event = None  # type: ignore
    ChainEventType = None  # type: ignore
    _EVENT_OK = False

try:  # 新版宿主
    from app.sdk.plugin.base import _PluginBase
except ImportError:
    try:
        from app.sdk.plugin import _PluginBase
    except ImportError:  # 兼容旧版宿主
        from app.plugins import _PluginBase  # type: ignore

__all__ = ["CasFileExtender"]

DEFAULT_EXT = "cas"

# 内层视频扩展名白名单：.cas 前若带这些扩展名，整理重命名时需保留
INNER_VIDEO_EXTS = {
    ".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mts", ".mov", ".wmv", ".flv",
    ".webm", ".rmvb", ".rm", ".mpg", ".mpeg", ".m4v", ".3gp", ".vob",
    ".iso", ".asf", ".f4v", ".ogv", ".divx", ".xvid",
}


def _load_settings():
    """兼容不同宿主版本的 settings 导入路径。"""
    try:
        from app.runtime.config import settings
        return settings
    except Exception:
        try:
            from app.core.config import settings  # type: ignore
            return settings
        except Exception:
            return None


def _inject_media_ext() -> bool:
    """
    把带点的 .cas 注入 settings.RMT_MEDIAEXT。

    白名单默认值为带点格式（.mp4/.mkv 等），且整理链 _is_media_file 判定使用
    f".{extension}" 拼点匹配，因此必须注入带点的 ".cas"。
    """
    settings = _load_settings()
    if settings is None:
        logger.warning("CAS视频扩展：未定位到 settings，白名单注入失败（宿主导入路径可能变更）")
        return False
    ext = f".{DEFAULT_EXT}"
    exts = getattr(settings, "RMT_MEDIAEXT", None)
    if not isinstance(exts, list):
        logger.warning(f"CAS视频扩展：RMT_MEDIAEXT 不是列表（实际 {type(exts)}），注入失败")
        return False
    if ext not in exts:
        exts.append(ext)
        logger.info(f"CAS视频扩展：已将 {ext} 注入视频白名单 RMT_MEDIAEXT")
    else:
        logger.info(f"CAS视频扩展：视频白名单已含 {ext}")
    return True


def _patch_transfer_chain() -> None:
    """
    刷新 TransferChain 单例缓存的媒体扩展名，使 .cas 被识别为视频文件。

    TransferChain 是 Singleton，其 _media_exts / _allowed_exts 在 __init__ 时一次性算好，
    不会实时读 settings，因此必须直接刷新实例缓存。
    """
    try:
        from app.chain.transfer import TransferChain
        tc = TransferChain()
        ext = f".{DEFAULT_EXT}"
        changed = False
        for attr in ("_media_exts", "_allowed_exts"):
            cur = list(getattr(tc, attr, []) or [])
            if ext not in cur:
                cur.append(ext)
                setattr(tc, attr, cur)
                changed = True
        if changed:
            logger.info("CAS视频扩展：已将 .cas 注入 TransferChain 媒体扩展名缓存")
        else:
            logger.info("CAS视频扩展：TransferChain 媒体扩展名缓存已含 .cas")
    except Exception as err:
        logger.warning(f"CAS视频扩展：刷新 TransferChain 失败：{err}")


def _refresh_native_monitor() -> bool:
    """
    刷新 Monitor 单例的 dispatcher.all_exts，使目录监控把 .cas 当作候选媒体触发整理。
    返回 True 表示刷新成功（Monitor 已就绪）。
    """
    try:
        from app.foundation.singleton import SingletonClass
        from app.monitor.monitor import Monitor
        monitor = SingletonClass.get_existing_instance(Monitor)
        if not monitor:
            return False
        dispatcher = getattr(monitor, "_dispatcher", None)
        if dispatcher is None:
            return False
        ext = f".{DEFAULT_EXT}"
        exts = list(getattr(dispatcher, "all_exts", []) or [])
        if ext not in exts:
            exts.append(ext)
            dispatcher.all_exts = exts
            logger.info("CAS视频扩展：已将 .cas 注入目录监控分发器扩展名")
        else:
            logger.info("CAS视频扩展：目录监控分发器扩展名已含 .cas")
        return True
    except Exception as err:
        logger.warning(f"CAS视频扩展：刷新分发器失败：{err}")
        return False


def _background_refresh_monitor() -> None:
    """
    后台周期刷新 Monitor 分发器扩展名，处理 Monitor 晚于插件启动的情况。
    最多重试 5 分钟，成功即结束。
    """
    for _ in range(150):
        if _refresh_native_monitor():
            return
        time.sleep(2)


class CasFileExtender(_PluginBase):
    # 插件元信息
    plugin_name = "CAS视频扩展"
    plugin_desc = "把 .cas 注入 MoviePilot 视频白名单，并刷新整理链与目录监控的扩展名缓存，使其把 .cas 当作视频文件识别（启用后无需重启）。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.27"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "casfileextender_"
    plugin_order = 27
    auth_level = 1

    _enabled: bool = False
    _keep_inner_ext: bool = True

    def init_plugin(self, config: dict = None):
        cfg = config or {}
        self._enabled = bool(cfg.get("enabled", False))
        self._keep_inner_ext = bool(cfg.get("keep_inner_ext", True))
        if not self._enabled:
            return
        # 启用时刷新各处扩展名缓存（.cas 识别是全局行为，但受启用开关控制）
        _inject_media_ext()
        _patch_transfer_chain()
        _refresh_native_monitor()
        # Monitor 可能晚于插件启动，后台重试直到刷新成功
        threading.Thread(target=_background_refresh_monitor, daemon=True).start()

    # ---------------- 宿主基类要求 ----------------

    def get_state(self) -> Any:
        """返回插件状态（宿主基类抽象方法）。"""
        return {"enabled": self._enabled, "keep_inner_ext": self._keep_inner_ext}

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """仅保留启用开关与「保留视频扩展名」开关。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "keep_inner_ext",
                                            "label": "保留视频扩展名",
                                            "hint": "整理重命名时把 .mkv.cas 的内层视频扩展名（如 .mkv）补回，避免被改成 .cas",
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ], {"enabled": False, "keep_inner_ext": True}

    def get_page(self) -> Optional[List[dict]]:
        """无自定义详情页（与系统命令屏蔽一致：点击卡片直接进设置）。"""
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    # ---------------- 整理重命名 Hook ----------------
    # 场景：源文件形如 `xxx.mkv.cas`，MoviePilot 整理时把扩展名算成 `.cas`，
    # 导致目标文件名丢掉内层 `.mkv`。这里在重命名渲染前后把扩展名补回成 `.mkv.cas`。

    @staticmethod
    def _cas_inner_video_ext(source_item: Any, source_path: Any) -> Optional[str]:
        """从 .cas 源文件名提取内层视频扩展名（如 `xxx.mkv.cas` -> `.mkv`）。"""
        name = ""
        if source_item is not None:
            name = getattr(source_item, "name", "") or ""
        if not name and source_path:
            name = Path(str(source_path)).name
        name = str(name or "").strip()
        if not name.lower().endswith(f".{DEFAULT_EXT}"):
            return None
        base = name[: -len(DEFAULT_EXT) - 1]  # 去掉末尾 .cas
        inner = Path(base).suffix.lower()
        if inner in INNER_VIDEO_EXTS:
            return inner
        return None

    if _EVENT_OK and getattr(ChainEventType, "TransferRenameBuild", None) is not None:

        @eventmanager.register(ChainEventType.TransferRenameBuild)
        def on_transfer_rename_build(self, event: Event) -> None:
            """渲染前把 rename_dict['fileExt'] 从 `.cas` 改为 `.mkv.cas`。"""
            if not self._enabled:
                return
            if not self._keep_inner_ext:
                return
            data = event.event_data
            if not data:
                return
            inner = self._cas_inner_video_ext(
                getattr(data, "source_item", None), getattr(data, "source_path", None)
            )
            if not inner:
                return
            rename_dict = getattr(data, "rename_dict", None)
            if not isinstance(rename_dict, dict):
                return
            old = str(rename_dict.get("fileExt") or "")
            new = f"{inner}.{DEFAULT_EXT}"
            if old.lower() != new.lower():
                rename_dict["fileExt"] = new
                logger.info(f"CAS视频扩展：重命名扩展名 {old or '空'} -> {new}")

    if _EVENT_OK and getattr(ChainEventType, "TransferRename", None) is not None:

        @eventmanager.register(ChainEventType.TransferRename)
        def on_transfer_rename(self, event: Event) -> None:
            """渲染后兜底：若最终文件名仍以 `.cas` 结尾，补回内层视频扩展名。"""
            if not self._enabled:
                return
            if not self._keep_inner_ext:
                return
            data = event.event_data
            if not data:
                return
            render_str = str(getattr(data, "render_str", "") or "")
            if not render_str.lower().endswith(f".{DEFAULT_EXT}"):
                return
            inner = self._cas_inner_video_ext(
                getattr(data, "source_item", None), getattr(data, "source_path", None)
            )
            if not inner:
                return
            if render_str.lower().endswith(f"{inner}.{DEFAULT_EXT}"):
                return  # 已是 .mkv.cas，无需再补
            new_str = render_str[: -len(DEFAULT_EXT) - 1] + f"{inner}.{DEFAULT_EXT}"
            data.updated = True
            data.updated_str = new_str
            data.source = "CAS视频扩展"
            logger.info(f"CAS视频扩展：修正目标文件名 {render_str} -> {new_str}")

    def stop_service(self):
        pass
