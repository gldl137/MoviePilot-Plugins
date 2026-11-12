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
from typing import Any, Dict, List, Optional, Tuple

from app.log import logger

try:  # 新版宿主
    from app.sdk.plugin.base import _PluginBase
except ImportError:
    try:
        from app.sdk.plugin import _PluginBase
    except ImportError:  # 兼容旧版宿主
        from app.plugins import _PluginBase  # type: ignore

# 事件相关（用于「存量补扫」命令），防御性导入以兼容不同宿主版本
try:
    from app.sdk.events import eventmanager  # type: ignore
except ImportError:
    try:
        from app.core.event import eventmanager  # type: ignore
    except ImportError:
        eventmanager = None  # type: ignore

try:
    from app.schemas.types import EventType  # type: ignore
except ImportError:
    try:
        from app.sdk.events import EventType  # type: ignore
    except ImportError:
        EventType = None  # type: ignore

__all__ = ["CasFileExtender"]

DEFAULT_EXT = "cas"


def _plugin_action_decorator(func):
    """
    兼容不同宿主版本的 PluginAction 事件注册：导入可用时才注册，
    否则原样返回函数，避免插件在旧宿主上因装饰器失败而无法加载。
    """
    if EventType is not None and eventmanager is not None:
        return eventmanager.register(EventType.PluginAction)(func)
    return func


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
    plugin_version = "1.24"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "casfileextender_"
    plugin_order = 27
    auth_level = 1

    _enabled: bool = False

    def init_plugin(self, config: dict = None):
        cfg = config or {}
        self._enabled = bool(cfg.get("enabled", False))
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
        return {"enabled": self._enabled}

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """仅保留一个启用开关。"""
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
                            }
                        ],
                    }
                ],
            }
        ], {"enabled": False}

    def get_page(self) -> Optional[List[dict]]:
        """无自定义详情页（与系统命令屏蔽一致：点击卡片直接进设置）。"""
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self) -> List[Dict[str, Any]]:
        if not self._enabled or EventType is None:
            return []
        return [{
            "cmd": "/cas_rescan",
            "event": EventType.PluginAction,
            "desc": "存量补扫CAS文件",
            "category": "插件",
            "data": {"action": "cas_rescan"}
        }]

    @_plugin_action_decorator
    def cas_rescan_action(self, event) -> None:
        """
        响应「存量补扫CAS文件」命令：主动遍历监控目录里已存在的 .cas 并提交整理。
        """
        if not self._enabled:
            return
        data = getattr(event, "event_data", None) or {}
        if data.get("action") != "cas_rescan":
            return
        logger.info("CAS视频扩展：收到存量补扫命令，启动后台补扫")
        threading.Thread(target=self._do_rescan, daemon=True).start()

    def _do_rescan(self) -> None:
        """
        遍历所有本地监控目录，把已存在的 .cas 文件提交整理链。
        依靠 dispatcher.handle_file 内部的历史去重与 TTL 去重，已整理过的不会重复处理。
        """
        from pathlib import Path

        ext = f".{DEFAULT_EXT}"
        # 1) 取监控目录
        try:
            from app.helper.directory import DirectoryHelper
        except ImportError:
            try:
                from app.utils.directory_helper import DirectoryHelper
            except ImportError:
                DirectoryHelper = None
        if DirectoryHelper is None:
            logger.warning("CAS视频扩展：未定位 DirectoryHelper，补扫终止")
            return
        try:
            dirs = DirectoryHelper().get_download_dirs() or []
        except Exception as err:
            logger.error(f"CAS视频扩展：读取监控目录失败：{err}")
            return
        if not dirs:
            logger.info("CAS视频扩展：未找到任何目录监控配置，补扫跳过")
            return

        # 2) 取 Monitor 分发器（优先走它，复用历史去重）
        dispatcher = None
        try:
            from app.foundation.singleton import SingletonClass
            from app.monitor.monitor import Monitor
            monitor = SingletonClass.get_existing_instance(Monitor)
            if monitor:
                dispatcher = getattr(monitor, "_dispatcher", None)
        except Exception:
            dispatcher = None

        scanned = 0
        submitted = 0
        for mon_dir in dirs:
            mon_path = Path(str(getattr(mon_dir, "download_path", "")))
            if not mon_path or not mon_path.exists():
                continue
            logger.info(f"CAS视频扩展：补扫目录 {mon_path}")
            for p in mon_path.rglob("*"):
                try:
                    if not p.is_file():
                        continue
                except OSError:
                    continue
                if p.suffix.casefold() != ext.casefold():
                    continue
                scanned += 1
                try:
                    st = p.stat()
                    fsize = st.st_size
                    fmtime = st.st_mtime
                except OSError:
                    continue
                try:
                    if dispatcher is not None:
                        ok = dispatcher.handle_file(
                            storage="local",
                            event_path=p,
                            file_size=fsize,
                            file_modify_time=fmtime,
                        )
                    else:
                        from app.chain.transfer import TransferChain
                        from app.schemas.types import FileItem
                        TransferChain().do_transfer(
                            fileitem=FileItem(
                                storage="local",
                                path=str(p),
                                type="file",
                                name=p.name,
                                basename=p.stem,
                                extension=p.suffix[1:],
                                size=fsize,
                                modify_time=fmtime,
                            ),
                            mtype=None,
                        )
                        ok = True
                    if ok:
                        submitted += 1
                        logger.info(f"CAS视频扩展：已提交补扫 {p}")
                except Exception as err:
                    logger.error(f"CAS视频扩展：补扫提交 {p} 失败：{err}")

        logger.info(f"CAS视频扩展：存量补扫完成，扫描 {scanned} 个 .cas，"
                    f"{submitted} 个进入整理链")

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def stop_service(self):
        pass
