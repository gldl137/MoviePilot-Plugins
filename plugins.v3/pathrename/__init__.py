from typing import Any, Dict, List, Optional, Tuple

from app.plugins import _PluginBase
from app.core.event import Event, eventmanager
from app.log import logger
from app.schemas.types import ChainEventType


class PathRename(_PluginBase):
    """
    路径重命名

    根据源文件路径中的目录关键词，自动在整理后的文件名里追加网盘/来源标识文字。
    例如源路径包含「CAS Pro」，则把「天翼网盘」追加到文件名中。
    """

    # 插件名称
    plugin_name = "路径重命名"
    # 插件描述
    plugin_desc = "根据源文件路径中的目录关键词，自动在整理后的文件名中追加网盘/来源标识文字，例如路径含「CAS Pro」则在文件名追加「天翼网盘」。"
    # 插件图标
    plugin_icon = "link.png"
    # 插件版本
    plugin_version = "1.0.0"
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

    # 追加位置：扩展名前 / 文件名开头
    _POS_SUFFIX = "suffix"
    _POS_PREFIX = "prefix"

    def __init__(self) -> None:
        super().__init__()
        self._enabled = False
        self._conversion_list: List[Tuple[str, str]] = []
        self._insert_position = type(self)._POS_SUFFIX

    def init_plugin(self, config: dict = None) -> None:
        """
        初始化插件配置
        """
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        self._insert_position = (
            config.get("insert_position") or type(self)._POS_SUFFIX
        )
        self._conversion_list = self._parse_conversion_list(
            config.get("conversion_list") or ""
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面
        """
        cls = type(self)
        pos_items = [
            {
                "title": "扩展名前（如 名称 天翼网盘.strm）",
                "value": cls._POS_SUFFIX,
            },
            {
                "title": "文件名开头（如 天翼网盘 名称.strm）",
                "value": cls._POS_PREFIX,
            },
        ]
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "insert_position",
                                            "label": "追加位置",
                                            "items": pos_items,
                                            "hint": "追加文字相对于文件名主体与扩展名的位置",
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
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "conversion_list",
                                            "label": "转换列表",
                                            "rows": 6,
                                            "placeholder": "CAS Pro-天翼网盘\n夸克-夸克网盘",
                                            "hint": (
                                                "每行一条规则，格式「关键词-追加文字」"
                                                "（取第一个 - / = / : 作为分隔符）。"
                                                "整理时若源路径包含关键词，就在文件名追加对应文字；"
                                                "多条命中会按列表顺序依次追加。"
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
                                                    "说明：仅基于「源文件路径」中的目录名匹配关键词，"
                                                    "与文件名、媒体信息无关。例如源路径 "
                                                    "/STRM影视/网盘/CAS Pro/天翼云盘1/.../海洋奇缘2 (2024).strm "
                                                    "命中「CAS Pro」，则整理后的文件名变为"
                                                    "「海洋奇缘2 (2024) 天翼网盘.strm」。"
                                                    "已存在于文件名中的文字不会重复追加。"
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
            "insert_position": cls._POS_SUFFIX,
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
    # 匹配与改写
    # ------------------------------------------------------------------ #
    @classmethod
    def _match_tags(
        cls, source_path: str, conversion_list: List[Tuple[str, str]]
    ) -> List[str]:
        """
        根据源路径匹配出需要追加的文字列表（保持配置顺序、去重）
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

    @classmethod
    def _insert_tags(cls, name: str, tags: List[str], position: str) -> str:
        """
        把追加文字插入到文件名中（支持带目录分隔符的情况，只对最后一级文件名处理）
        """
        if not tags:
            return name
        combined = " ".join(tags)
        for sep in ("/", "\\"):
            if sep in name:
                dir_part, _, base = name.rpartition(sep)
                return dir_part + sep + cls._insert_into_name(base, combined, position)
        return cls._insert_into_name(name, combined, position)

    @classmethod
    def _insert_into_name(cls, name: str, combined: str, position: str) -> str:
        """
        在单个文件名上追加文字：stem + 追加文字 + 扩展名
        """
        stem, dot, ext = name.rpartition(".")
        if dot == "":
            stem, ext = name, ""
        if position == cls._POS_PREFIX:
            return f"{combined} {stem}{ext}"
        return f"{stem} {combined}{ext}"

    # ------------------------------------------------------------------ #
    # 事件处理
    # ------------------------------------------------------------------ #
    @eventmanager.register(ChainEventType.TransferRenameBuild)
    def on_transfer_rename_build(self, event: Event) -> None:
        """
        处理 TransferRenameBuild 事件（渲染前），把命中的标识文字写入
        rename_dict["pathtag"]，供重命名模板用 {{pathtag}} 引用。

        该事件在参考插件 ffprobenamingsupplement 中使用，必定存在，作为保底方案。
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

    # TransferRename（渲染后改写文件名，自动追加）仅在当前版本存在该事件时才注册，
    # 用 getattr 守卫，避免事件名缺失导致整个模块在类定义期导入失败。
    if getattr(ChainEventType, "TransferRename", None) is not None:

        @eventmanager.register(ChainEventType.TransferRename)
        def on_transfer_rename(self, event: Event) -> None:
            """
            处理 TransferRename 事件（渲染后），直接把标识文字追加到文件名。

            基于渲染后的字符串改写，可与其它只写 rename_dict 的插件（如 ffprobe 命名补充）
            共存；若已有其它插件改写过 updated_str，会在其基础上继续追加，不会覆盖。
            """
            if not self._enabled:
                return
            data = event.event_data
            if not data or not hasattr(data, "render_str"):
                return
            source_path = getattr(data, "source_path", None)
            if not source_path or not str(source_path).strip():
                logger.debug("【路径重命名】source_path 为空，跳过")
                return
            current = (
                data.updated_str
                if (data.updated and data.updated_str)
                else data.render_str
            )
            if not current:
                return

            tags = self._match_tags(str(source_path), self._conversion_list)
            if not tags:
                return

            # 去重：文件名中已经存在的文字不再追加（避免重复整理时叠加）
            tags = [t for t in tags if t and t not in current]
            if not tags:
                return

            new_str = self._insert_tags(current, tags, self._insert_position)
            if new_str != current:
                data.updated = True
                data.updated_str = new_str
                logger.info("【路径重命名】%s -> %s", current, new_str)
