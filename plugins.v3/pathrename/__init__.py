from typing import Any, Dict, List, Optional, Tuple

from app.plugins import _PluginBase

# V3 规范：新插件统一使用 app.sdk 稳定入口；旧宿主回退到兼容路径
try:
    from app.sdk.events import Event, eventmanager
    from app.sdk.logging import logger
except ImportError:  # 兼容尚未提供 app.sdk 的旧宿主
    from app.core.event import Event, eventmanager
    from app.log import logger

try:
    from app.schemas.types import ChainEventType
except ImportError:  # 兼容没有链式事件的版本：导入失败不让整模块挂掉
    ChainEventType = None


class PathRename(_PluginBase):
    """
    路径重命名

    根据源文件路径中的目录关键词，为整理重命名模板提供变量 pathtag。
    在重命名模板中填入 {{pathtag}}，即可在该位置输出命中的标识文字。
    例如源路径包含「CAS Pro」，pathtag 的值为「天翼网盘」。
    """

    # 插件名称
    plugin_name = "路径重命名"
    # 插件描述
    plugin_desc = "根据源文件路径中的目录关键词生成模板变量 pathtag，在整理重命名模板中填入 {{pathtag}} 即可在该位置追加网盘/来源标识文字。"
    # 插件图标
    plugin_icon = "link.png"
    # 插件版本
    plugin_version = "1.1.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "pathrename_"
    # 加载顺序
    plugin_order = 50
    # 可使用的用户级别
    auth_level = 1

    def __init__(self) -> None:
        super().__init__()
        self._enabled = False
        self._conversion_list: List[Tuple[str, str]] = []

    def init_plugin(self, config: dict = None) -> None:
        """
        初始化插件配置（可重复调用）
        """
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._conversion_list = self._parse_conversion_list(
            config.get("conversion_list") or ""
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面
        """
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
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
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
                                            "model": "conversion_list",
                                            "label": "转换列表",
                                            "rows": 6,
                                            "placeholder": "CAS Pro-天翼网盘\n夸克-夸克网盘",
                                            "hint": (
                                                "每行一条规则，格式「关键词-追加文字」"
                                                "（取第一个 - / = / : 作为分隔符）。"
                                                "整理时若源路径包含关键词，变量 pathtag 即为对应文字；"
                                                "多条命中时用空格拼接。"
                                            ),
                                            "persistent-hint": True,
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "density": "compact",
                                        },
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {"class": "text-body-2"},
                                                "text": (
                                                    "用法：本插件不直接改写文件名，只提供模板变量 {{pathtag}}。"
                                                    "到「设置 → 整理 → 重命名模板」中把 {{pathtag}} 放到想要的位置，"
                                                    "例如 {{title}} ({{year}}) {{pathtag}}{{ext}}，"
                                                    "整理后即为「海洋奇缘2 (2024) 天翼网盘.strm」。"
                                                    "仅基于「源文件路径」中的目录名匹配关键词；"
                                                    "未命中时 pathtag 为空。"
                                                ),
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "conversion_list": "CAS Pro-天翼网盘",
        }

    def get_page(self) -> Optional[List[dict]]:
        pass

    def stop_service(self) -> None:
        pass

    # ------------------------------------------------------------------ #
    # 规则解析
    # ------------------------------------------------------------------ #
    @staticmethod
    def _parse_conversion_list(raw: str) -> List[Tuple[str, str]]:
        """
        解析转换列表文本为 (关键词, 追加文字) 列表

        每行格式「关键词-追加文字」，按第一个出现的分隔符（- / = / : / ： / → / ->）切分
        """
        result: List[Tuple[str, str]] = []
        if not raw:
            return result
        delimiters = ("->", "→", "=", "：", ":", "-")
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            cut = -1
            used = "-"
            for d in delimiters:
                idx = line.find(d)
                if idx > 0:
                    cut = idx
                    used = d
                    break
            if cut <= 0:
                logger.warning("【路径重命名】转换规则缺少分隔符，已跳过：%s", line)
                continue
            keyword = line[:cut].strip()
            replacement = line[cut + len(used):].strip()
            if not keyword:
                continue
            result.append((keyword, replacement))
        return result

    # ------------------------------------------------------------------ #
    # 匹配
    # ------------------------------------------------------------------ #
    @classmethod
    def _match_tags(
        cls, source_path: str, conversion_list: List[Tuple[str, str]]
    ) -> List[str]:
        """
        根据源路径匹配出标识文字列表（保持配置顺序、去重）
        """
        if not source_path or not conversion_list:
            return []
        lower_path = source_path.casefold()
        tags: List[str] = []
        for keyword, replacement in conversion_list:
            if not keyword:
                continue
            if keyword.casefold() in lower_path and replacement:
                if replacement not in tags:
                    tags.append(replacement)
        return tags

    # ------------------------------------------------------------------ #
    # 事件处理
    # ------------------------------------------------------------------ #
    # 仅注册 TransferRenameBuild（渲染前写 rename_dict["pathtag"]，供模板 {{pathtag}} 引用）。
    # 事件名用 getattr 守卫，不同 MoviePilot 版本缺失该事件时不会导致整模块导入失败。
    if getattr(ChainEventType, "TransferRenameBuild", None) is not None:

        @eventmanager.register(ChainEventType.TransferRenameBuild)
        def on_transfer_rename_build(self, event: Event) -> None:
            """
            处理 TransferRenameBuild 事件，把命中的标识文字写入
            rename_dict["pathtag"]，供重命名模板用 {{pathtag}} 引用。
            """
            if not self._enabled:
                return
            data = event.event_data
            if not data:
                return
            source_path = getattr(data, "source_path", None)
            if not source_path or not str(source_path).strip():
                return
            tags = self._match_tags(str(source_path), self._conversion_list)
            if not tags:
                return
            rename_dict = getattr(data, "rename_dict", None)
            if isinstance(rename_dict, dict):
                rename_dict["pathtag"] = " ".join(tags)
                logger.info("【路径重命名】已写入 pathtag：%s", rename_dict["pathtag"])
