"""
CAS助手（casassistant）

集中管理 CAS 服务的连接配置（地址、API Key 等），并对外提供
get_cas_config() 读取接口，供其它插件统一获取，避免各插件重复填写 CAS 服务地址。

其它插件获取方式（任选其一）：
1. 调用方法（推荐，返回已剥离前缀的干净配置）：
   from app.runtime.extensions.plugin_manager import PluginManager
   cfg = PluginManager().run_plugin_method("casassistant", "get_cas_config")
2. 读取原始配置（带前缀 casassistant_）：
   cfg = PluginManager().get_plugin_config("casassistant")
"""
import threading
from typing import Any, Dict, List, Optional, Tuple

from app.log import logger

try:  # HTTP 探测连接状态
    import requests
except ImportError:  # 宿主通常自带 requests
    requests = None  # type: ignore

try:  # 新版宿主
    from app.sdk.plugin.base import _PluginBase
except ImportError:
    try:
        from app.sdk.plugin import _PluginBase
    except ImportError:
        from app.plugins import _PluginBase  # type: ignore


__all__ = ["casassistant"]

# 其它插件通过此 id 引用本插件：PluginManager.run_plugin_method(PLUGIN_ID, "get_cas_config")
PLUGIN_ID = "casassistant"

# 设置界面「其它插件如何调用」说明文本
CALL_HELP_TEXT = """其它插件无需各自填写 CAS 服务地址，直接读取本插件配置即可。

方式一（推荐）：调用方法，返回已剥离前缀的干净配置
  from app.runtime.extensions.plugin_manager import PluginManager

  cfg = PluginManager().run_plugin_method("casassistant", "get_cas_config")
  # cfg = {"enabled": bool, "host": str, "api_key": str}
  if cfg and cfg.get("enabled") and cfg.get("host"):
      host = cfg["host"]
      api_key = cfg["api_key"]
  else:
      # 未配置，回退到各插件自身填写
      pass

方式二：直接读取原始配置（键带前缀 casassistant_）
  raw = PluginManager().get_plugin_config("casassistant")
  # raw = {"casassistant_cas_host": ..., "casassistant_cas_api_key": ..., ...}
"""


class casassistant(_PluginBase):
    # 插件元信息
    plugin_name = "CAS助手"
    plugin_desc = "集中管理 CAS 服务的连接配置（地址、API Key），供其它插件统一读取，避免各插件重复填写 CAS 服务地址；启动时探测并在日志中显示连接状态。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.5"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "casassistant_"
    plugin_order = 28
    auth_level = 1

    # CAS 服务配置
    _cas_enabled: bool = False
    _cas_host: Optional[str] = None
    _cas_api_key: Optional[str] = None
    _cas_connected: Optional[bool] = None  # 最近一次连接探测结果

    def init_plugin(self, config: dict = None):
        cfg = config or {}
        self._cas_enabled = bool(cfg.get("enabled", False))
        self._cas_host = (cfg.get("cas_host", "") or "").strip()
        self._cas_api_key = (cfg.get("cas_api_key", "") or "").strip()
        logger.info("CAS助手：已加载配置（启用=%s，host=%s）",
                    self._cas_enabled, bool(self._cas_host))
        # 后台探测连接状态并写入日志，避免阻塞插件加载
        threading.Thread(target=self._check_and_log_connection, daemon=True).start()

    def _check_and_log_connection(self) -> None:
        """探测 CAS 服务连通性，并通过 /api/tasks 验证 API Key 是否有效（参考 CASTaskCleaner）。"""
        if not self._cas_enabled:
            logger.info("CAS助手：插件未启用，跳过连接检测")
            return
        host = (self._cas_host or "").strip()
        api_key = (self._cas_api_key or "").strip()
        if not host:
            logger.info("CAS助手：未配置 CAS 服务地址，跳过连接检测")
            return
        url = host if host.lower().startswith(("http://", "https://")) else f"http://{host}"
        if not url.endswith("/"):
            url += "/"
        if requests is None:
            logger.warning("CAS助手：当前环境缺少 requests，无法检测连接状态")
            return
        try:
            headers = {"x-api-key": api_key} if api_key else {}
            resp = requests.get(
                f"{url}api/tasks",
                headers=headers,
                params={"page": 1, "pageSize": 1},
                timeout=5,
                verify=False,
            )
            if resp.status_code == 200:
                self._cas_connected = True
                if api_key:
                    logger.info("CAS助手：CAS 服务连接正常且 API Key 鉴权通过（%s）", url)
                else:
                    logger.info("CAS助手：CAS 服务连通（%s），但未配置 API Key，未验证鉴权", url)
            else:
                self._cas_connected = False
                logger.warning("CAS助手：CAS 服务连接异常（%s）：HTTP %s%s",
                               url, resp.status_code,
                               (" - " + resp.text[:200] if resp.text else ""))
        except Exception as err:
            self._cas_connected = False
            logger.warning("CAS助手：CAS 服务连接异常（%s）：%s", url, err)

    # ---------------- 宿主基类要求 ----------------

    def get_state(self) -> Any:
        return {"enabled": self._cas_enabled, "connected": self._cas_connected}

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VCard",
                                        "props": {"variant": "flat", "class": "mb-2", "color": "surface"},
                                        "content": [
                                            {
                                                "component": "VCardItem",
                                                "content": [
                                                    {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "其它插件如何调用"}
                                                ]
                                            },
                                            {
                                                "component": "VCardText",
                                                "props": {
                                                    "style": "white-space: pre-wrap; font-size: 13px; line-height: 1.5;"
                                                },
                                                "text": CALL_HELP_TEXT
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用"},
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cas_host",
                                            "label": "CAS 服务地址",
                                            "placeholder": "http://127.0.0.1:19798",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cas_api_key",
                                            "label": "CAS API Key",
                                            "type": "password",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "cas_host": "",
            "cas_api_key": "",
        }

    def get_page(self) -> Optional[List[dict]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    # ---------------- 对外接口 ----------------

    def get_cas_config(self) -> Dict[str, Any]:
        """
        供其它插件调用：返回 CAS 服务连接配置（已剥离前缀）。

        返回：{
            "enabled": bool,
            "host": str,
            "api_key": str,
        }

        其它插件调用示例：
            from app.runtime.extensions.plugin_manager import PluginManager
            cfg = PluginManager().run_plugin_method("casassistant", "get_cas_config")
        """
        return {
            "enabled": self._cas_enabled,
            "host": self._cas_host or "",
            "api_key": self._cas_api_key or "",
        }

    def stop_service(self):
        pass
