"""
CAS视频扩展（CasFileExtender）

只做一件事：让 MoviePilot 原生目录监控把 .cas 当作视频文件识别。

做法：
1. 把 .cas 注入 settings.RMT_MEDIAEXT（视频白名单）；
2. 原地刷新原生目录监控分发器 TransferDispatcher 的 all_exts
   （共享实例属性，不重建实例、不影响已有监控线程）。

这样本地目录监控 / 远程轮询监控立即把 .cas 当视频识别、走整理链，
无需重启、不改核心配置。无自带扫描、无通知、无定时任务、无整理参数。
"""
from typing import Any, Dict, List, Optional, Tuple

from app.log import logger

try:  # 新版宿主
    from app.sdk.plugin.base import _PluginBase
except ImportError:
    try:
        from app.sdk.plugin import _PluginBase
    except ImportError:  # 兼容旧版宿主
        from app.plugins import _PluginBase  # type: ignore

__all__ = ["CasFileExtender"]

DEFAULT_EXT = "cas"


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

    白名单默认值为带点格式（.mp4/.mkv 等），且 _is_media_file 判定使用
    f".{extension}" 拼点匹配，因此必须注入带点的 ".cas"。
    """
    settings = _load_settings()
    if settings is None:
        return False
    ext = f".{DEFAULT_EXT}"
    exts = getattr(settings, "RMT_MEDIAEXT", None)
    if isinstance(exts, list):
        if ext not in exts:
            exts.append(ext)
        return True
    return False


# 模块加载即注入：宿主重新加载插件后白名单立即生效，不依赖是否重新初始化
_inject_media_ext()


class CasFileExtender(_PluginBase):
    # 插件元信息
    plugin_name = "CAS视频扩展"
    plugin_desc = "把 .cas 注入 MoviePilot 视频白名单并刷新原生目录监控，使其把 .cas 当作视频文件识别（无需重启）。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.2.0"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "casfileextender_"
    plugin_order = 27
    auth_level = 1

    _enabled: bool = False
    _refreshed: bool = False

    def init_plugin(self, config: dict = None):
        cfg = config or {}
        self._enabled = bool(cfg.get("enabled", False))
        self._refreshed = False
        if self._enabled:
            self._refresh()

    def _refresh(self) -> bool:
        """把 .cas 注入白名单并刷新原生监控分发器。"""
        try:
            _inject_media_ext()
            ok = self._refresh_native_monitor()
            self._refreshed = ok
            logger.info(
                f"CAS视频扩展：视频白名单已含 .{DEFAULT_EXT}，原生监控刷新{'成功' if ok else '失败'}"
            )
            return ok
        except Exception as err:
            logger.error(f"CAS视频扩展：刷新失败：{err}")
            return False

    @staticmethod
    def _refresh_native_monitor() -> bool:
        """原地刷新原生目录监控分发器的 all_exts（共享实例，不重建）。"""
        try:
            from app.foundation.singleton import SingletonClass
            from app.monitor.monitor import Monitor

            monitor = SingletonClass.get_existing_instance(Monitor)
            if not monitor:
                return False
            dispatcher = getattr(monitor, "_dispatcher", None)
            if dispatcher is None:
                return False
            exts = list(getattr(dispatcher, "all_exts", []) or [])
            ext = f".{DEFAULT_EXT}"  # 监控分发器扩展名同样带点
            if ext not in exts:
                exts.append(ext)
                dispatcher.all_exts = exts
            return True
        except Exception as err:
            logger.warning(f"CAS视频扩展：刷新原生监控失败：{err}")
            return False

    # ---------------- 宿主基类要求 ----------------

    def get_state(self) -> Any:
        """返回插件状态（宿主基类抽象方法）。"""
        return {"enabled": self._enabled, "refreshed": self._refreshed}

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
        """无自定义详情页。"""
        return None

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def stop_service(self):
        pass
