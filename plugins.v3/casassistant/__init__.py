"""
CAS助手（casassistant）

集中管理 CAS 服务的连接配置（地址、API Key、路径映射等），并对外提供
get_cas_config() 读取接口，供其它插件统一获取，避免各插件重复填写 CAS 服务地址。

其它插件获取方式（任选其一）：
1. 调用方法（推荐，返回已剥离前缀的干净配置）：
   from app.runtime.extensions.plugin_manager import PluginManager
   cfg = PluginManager().run_plugin_method("casassistant", "get_cas_config")
2. 读取原始配置（带前缀 casassistant_）：
   cfg = PluginManager().get_plugin_config("casassistant")
"""
from typing import Any, Dict, List, Optional, Tuple

from app.log import logger

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


def _parse_path_mapping(config_str: str) -> List[Dict[str, str]]:
    """解析路径映射：每行 `local#account_id#account_name`（account_name 可选）。"""
    mappings: List[Dict[str, str]] = []
    if not config_str:
        return mappings
    for line in config_str.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split("#")]
        if len(parts) >= 2 and parts[0] and parts[1]:
            mappings.append({
                "path": parts[0],
                "account_id": parts[1],
                "account_name": parts[2] if len(parts) > 2 else parts[1],
            })
    return mappings


class casassistant(_PluginBase):
    # 插件元信息
    plugin_name = "CAS助手"
    plugin_desc = "集中管理 CAS 服务的连接配置（地址、API Key、路径映射等），供其它插件统一读取，避免各插件重复填写 CAS 服务地址。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.0"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "casassistant_"
    plugin_order = 28
    auth_level = 1

    # CAS 服务配置
    _cas_enabled: bool = False
    _cas_host: Optional[str] = None
    _cas_api_key: Optional[str] = None
    _cas_mappings: List[Dict[str, str]] = []

    def init_plugin(self, config: dict = None):
        cfg = config or {}
        self._cas_enabled = bool(cfg.get("enabled", False))
        self._cas_host = (cfg.get("cas_host", "") or "").strip()
        self._cas_api_key = (cfg.get("cas_api_key", "") or "").strip()
        self._cas_mappings = _parse_path_mapping(cfg.get("cas_path_mapping", ""))
        logger.info("CAS助手：已加载配置（启用=%s，host=%s，映射 %d 条）",
                    self._cas_enabled, bool(self._cas_host), len(self._cas_mappings))

    # ---------------- 宿主基类要求 ----------------

    def get_state(self) -> Any:
        return {"enabled": self._cas_enabled}

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
                                        "component": "VText",
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
                                        "component": "VText",
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
                                            "model": "cas_path_mapping",
                                            "label": "路径映射（可选，每行 local#account_id#account_name）",
                                            "rows": 3,
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ], {
            "enabled": False,
            "cas_host": "",
            "cas_api_key": "",
            "cas_path_mapping": "",
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
            "path_mapping": List[Dict[str, str]],
        }

        其它插件调用示例：
            from app.runtime.extensions.plugin_manager import PluginManager
            cfg = PluginManager().run_plugin_method("casassistant", "get_cas_config")
        """
        return {
            "enabled": self._cas_enabled,
            "host": self._cas_host or "",
            "api_key": self._cas_api_key or "",
            "path_mapping": self._cas_mappings,
        }

    def stop_service(self):
        pass
