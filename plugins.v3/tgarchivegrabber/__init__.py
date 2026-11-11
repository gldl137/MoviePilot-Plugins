# -*- coding: utf-8 -*-
"""
TG 群组 / 频道压缩包抓取插件（MoviePilot V3）

作者：gldl137
仓库：MoviePilot-Plugins（https://github.com/gldl137/MoviePilot-Plugins）

功能：
    - 监控指定 Telegram 群组（普通群、超级群）或频道的新消息，发现压缩包附件后自动下载解压。
    - 支持两种接入方式：Telethon 用户账号（推荐，可读取账号已加入的任意群组/频道，无大小限制）
      与 Telegram Bot API（需把 Bot 加入群组、频道设为管理员，并关闭隐私模式才能收到普通消息；
      官方 API 限制单文件 20MB）。
    - 支持 zip / tar / gz / 7z / rar 及常见分卷包，内置路径穿越与解压炸弹防护。
    - 记录已处理消息、失败重试、历史记录与通知，提供详情页日志和手动触发接口。

实现约定：
    - 只使用 V3 稳定入口（app.sdk.*），基类入口保留 app.plugins 兼容回退。
    - 采用插件内守护线程轮询（不依赖宿主 cron 服务，避免 register_plugin_service 的
      cron= 关键字兼容问题），线程可重复启停，stop_service 会释放后台资源。
    - 运行数据（去重集合、游标、历史、分卷待处理）一律写入插件数据目录，不写源码目录。
"""

import gzip
import re
import shutil
import subprocess
import tarfile
import threading
import time
import zipfile
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

try:
    # V3 稳定入口（与已上线插件 TgMediaCenter 一致）；顶层只依赖 app.sdk.*
    from app.sdk.plugin import _PluginBase
except ImportError:  # 兼容仅提供旧公开入口的宿主
    from app.plugins import _PluginBase  # type: ignore

try:
    from app.sdk.logging import logger
except ImportError:  # 宿主未提供 SDK 时用标准日志兜底，不引入 V2 旧路径
    import logging

    logger = logging.getLogger(__name__)

try:
    from app.sdk.config import settings
except ImportError:  # 无系统代理配置时仍可在插件配置页手动填写代理
    settings = None  # type: ignore

try:
    from app.schemas.types import NotificationType
except ImportError:
    try:
        from app.schemas import NotificationType  # type: ignore
    except ImportError:
        NotificationType = None  # type: ignore


PLUGIN_ID = "TgArchiveGrabber"
LOG_TAG = "[TG压缩包]"

MAX_LOG_LINES = 300
MAX_HISTORY = 200
MAX_PROCESSED = 5000
MAX_FAILURES = 500
DEFAULT_RETRY = 3
MIN_INTERVAL = 15

DEFAULT_EXTS = (
    ".zip,.rar,.7z,.tar,.gz,.tgz,.tar.gz,.bz2,.tbz2,.xz,.txz,"
    ".001,.part1.rar,.z01,.r00"
)

EXISTING_ACTIONS = {
    "skip": "跳过（已存在则不处理）",
    "overwrite": "覆盖（清空同名输出子目录）",
    "rename": "重命名（自动追加序号）",
}

MODES = {
    "user": "用户账号（Telethon，推荐）",
    "bot": "机器人（Bot API）",
}

# 分卷包：捕获基础名、卷号与后缀类型
_PART_RE = re.compile(
    r"^(?P<stem>.+?)(?:\.part(?P<part>\d+)\.rar|\.7z\.(?P<s7z>\d{3})|"
    r"\.z(?P<zip>\d{2})|\.r(?P<rar>\d{2})|\.(?P<num>\d{3}))$",
    re.IGNORECASE,
)


def _tf(model: str, label: str, hint: str = "", placeholder: str = "",
        ftype: str = "", md: int = 4) -> Dict[str, Any]:
    """构造一个配置页文本输入框。"""
    props: Dict[str, Any] = {"model": model, "label": label}
    if hint:
        props["hint"] = hint
    if placeholder:
        props["placeholder"] = placeholder
    if ftype:
        props["type"] = ftype
    return {
        "component": "VCol",
        "props": {"cols": 12, "md": md},
        "content": [{"component": "VTextField", "props": props}],
    }


def _sw(model: str, label: str, hint: str = "", md: int = 3) -> Dict[str, Any]:
    """构造一个配置页开关。"""
    props: Dict[str, Any] = {"model": model, "label": label}
    if hint:
        props["hint"] = hint
    return {
        "component": "VCol",
        "props": {"cols": 12, "md": md},
        "content": [{"component": "VSwitch", "props": props}],
    }


def _sel(model: str, label: str, items: Dict[str, str],
         hint: str = "", md: int = 4) -> Dict[str, Any]:
    """构造一个配置页下拉框。"""
    props: Dict[str, Any] = {
        "model": model,
        "label": label,
        "items": [{"title": text, "value": value} for value, text in items.items()],
    }
    if hint:
        props["hint"] = hint
    return {
        "component": "VCol",
        "props": {"cols": 12, "md": md},
        "content": [{"component": "VSelect", "props": props}],
    }


def _row(*cols: Dict[str, Any]) -> Dict[str, Any]:
    """构造配置页的一行。"""
    return {"component": "VRow", "content": list(cols)}


def _alert(text: str, atype: str = "info", css: str = "my-2") -> Dict[str, Any]:
    """构造配置页/详情页的提示条。"""
    return {
        "component": "VAlert",
        "props": {
            "type": atype,
            "variant": "tonal",
            "density": "compact",
            "class": css,
            "text": text,
        },
    }


def _human_size(size: Optional[int]) -> str:
    """把字节数格式化为易读文本。"""
    value = float(size or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
        value /= 1024
    return f"{value:.1f}TB"


def _split_list(raw: Any) -> List[str]:
    """把逗号/换行/分号分隔的字符串切成列表。"""
    if isinstance(raw, (list, tuple)):
        return [str(x).strip() for x in raw if str(x).strip()]
    return [x.strip() for x in re.split(r"[,，;\n]+", str(raw or "")) if x.strip()]


def _safe_name(name: str) -> str:
    """清理文件名，去掉路径与控制字符，避免路径穿越。"""
    cleaned = re.sub(r"[\\/:*?\"<>|\[\]\x00-\x1f]", "_", str(name or ""))
    cleaned = cleaned.strip(" .")
    return cleaned[:180] or "archive.bin"


def _archive_group(filename: str) -> Tuple[str, bool, bool]:
    """
    识别压缩包的分卷信息。

    :return: (分组基础名, 是否属于分卷包, 是否为主卷)
    """
    name = str(filename or "")
    match = _PART_RE.match(name)
    if match:
        stem = match.group("stem")
        part = match.group("part")
        s7z = match.group("s7z")
        num = match.group("num")
        is_main = bool((part and int(part) == 1) or (s7z and int(s7z) == 1)
                       or (num and int(num) == 1))
        return stem, True, is_main
    stem = re.sub(r"(?i)\.tar\.gz$|\.tar\.bz2$|\.tar\.xz$|\.tgz$|\.tbz2$|\.txz$", "", name)
    stem = re.sub(r"(?i)\.[^.]+$", "", stem)
    return stem, False, True


def _proxy_runtime_missing() -> bool:
    """检测 Telethon 的代理软依赖 python-socks 是否缺失。"""
    try:
        import python_socks  # noqa: F401
        return False
    except ImportError:
        return True


def _environment() -> Tuple[List[str], str]:
    """检查可选依赖与系统解压工具，返回 (缺失项, 提示文本)。"""
    missing: List[str] = []
    tips: List[str] = []
    try:
        import telethon  # noqa: F401
    except ImportError:
        missing.append("telethon（用户账号模式必需）")
        tips.append("请在插件目录声明的 pyproject.toml 依赖安装后重启 MoviePilot。")
    if _proxy_runtime_missing():
        tips.append(
            "缺少 python-socks[asyncio]：Telethon 会静默忽略代理直连 Telegram，"
            "表现为代理配了却连接超时，请安装依赖后重启。"
        )
    try:
        import pyzipper  # noqa: F401
    except ImportError:
        tips.append("AES 加密 ZIP 需要 pyzipper。")
    if not any(shutil.which(tool) for tool in ("unar", "unrar", "7z", "bsdtar")):
        tips.append(
            "RAR / 7z 包需要系统解压工具：RAR 用 unar / unrar / 7z / bsdtar，"
            ".7z 用 7z（p7zip-full）或 unar / bsdtar（官方 Docker 镜像内置 unar）。"
        )
    return missing, " ".join(tips)


class TgArchiveGrabber(_PluginBase):
    """监控 Telegram 群组 / 频道压缩包并自动下载解压。"""

    # ---- 插件元信息（必须写成字面量：宿主会静态解析类属性做加载校验） ----
    plugin_name = "TG群组压缩包抓取"
    plugin_desc = "监控 TG 群组 / 频道新消息中的压缩包，自动下载并解压到指定目录。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.5"
    plugin_author = "gldl137"
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "tgarchivegrabber_"
    plugin_order = 40
    auth_level = 1

    _default_config: Dict[str, Any] = {
        "enabled": False,
        "mode": "user",
        "channels": "",
        "api_id": "",
        "api_hash": "",
        "session_string": "",
        "proxy": "",
        "bot_token": "",
        "bot_api_base": "https://api.telegram.org",
        "download_dir": "/downloads/tgarchives",
        "extract_dir": "/downloads/tgarchives/extracted",
        "archive_ext": DEFAULT_EXTS,
        "keywords": "",
        "exclude_keywords": "",
        "senders": "",
        "min_size_mb": 0,
        "max_size_mb": 0,
        "max_files_per_round": 20,
        "scan_limit": 100,
        "interval": 60,
        "backfill": False,
        "backfill_count": 20,
        "extract_password": "",
        "per_message_dir": True,
        "existing_action": "skip",
        "delete_archive": False,
        "parts_wait_seconds": 120,
        "max_extract_mb": 4096,
        "inner_ext": "",
        "notify": True,
        "onlyonce": False,
        "clear_records": False,
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # 宿主实例化插件时可能携带额外参数，这里透传，避免 TypeError 导致加载失败
        super().__init__(*args, **kwargs)
        self._enabled = False
        self._config: Dict[str, Any] = dict(self._default_config)
        self._logs: deque = deque(maxlen=MAX_LOG_LINES)
        self._log_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._scan_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._trigger = threading.Event()
        self._scanning = False
        self._last_scan = ""
        self._last_result = ""
        self._pending_parts: Dict[str, Dict[str, Any]] = {}
        self._resolved_entities: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def init_plugin(self, config: Optional[dict] = None) -> None:
        """读取配置并启动守护线程，允许重复调用（配置保存/插件重载）。"""
        self.stop_service()
        self._stop_event.clear()
        self._config = {**self._default_config, **(config or {})}
        # 会话串属于自管状态：宿主 update_config 是整份覆盖，前端若不回传 password
        # 字段就可能把它清空，因此同步持久化到插件数据目录（参照 TgMediaCenter 的做法）。
        configured_session = str(self._config.get("session_string") or "").strip()
        if configured_session:
            self.save_data("tg_session_string", configured_session)
        # 配置/账号可能变化，实体缓存里的 access_hash 属于旧账号，需失效
        self._resolved_entities = {}
        self._enabled = bool(self._config.get("enabled"))
        self._pending_parts = self.get_data("pending_parts") or {}
        if not isinstance(self._pending_parts, dict):
            self._pending_parts = {}
        self._log(
            f"初始化完成：启用={self._enabled}，模式={self._cfg_str('mode', 'user')}，"
            f"监控会话数={len(self._channels())}，间隔={self._cfg_int('interval', 60, MIN_INTERVAL)}秒"
        )
        if self._enabled and self._proxy_url() and _proxy_runtime_missing():
            self._log(
                "已配置代理但缺少 python-socks[asyncio]：Telethon 会静默忽略代理直连，"
                "请安装依赖后重启，否则连接大概率超时", "warning"
            )
        if self._enabled:
            problem = self._validate()
            if problem:
                self._log(f"配置不完整，线程未启动：{problem}", "warning")
            else:
                self._ensure_worker()
        if self._config.get("onlyonce") or self._config.get("clear_records"):
            threading.Thread(target=self._handle_switches, daemon=True,
                             name="tgarchivegrabber-switch").start()

    def get_state(self) -> bool:
        """插件是否启用。"""
        return self._enabled

    def stop_service(self) -> None:
        """停止守护线程并释放后台资源，可重复调用。"""
        self._enabled = False
        self._stop_event.set()
        self._trigger.set()  # 唤醒间隔等待中的线程，使其尽快检查停止信号
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
            if thread.is_alive():
                self._log("守护线程未在 5 秒内退出（可能正在下载大文件），将在当前任务结束后停止", "warning")
        with self._state_lock:
            self._thread = None
        # 保留同一个 Event 实例：旧线程仍持有它，重建会让超时未退出的旧线程丢失停止信号
        self._trigger.clear()

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """不注册远程命令，避免不同宿主事件枚举差异导致加载失败。"""
        return []

    @staticmethod
    def get_service() -> List[Dict[str, Any]]:
        """
        不向宿主注册 cron 服务。

        本插件采用插件内守护线程轮询（可在配置页设置秒级间隔），避免宿主
        register_plugin_service 的 cron= 关键字与部分 APScheduler 版本不兼容。
        线程由 init_plugin / api_scan / watchdog 逻辑保证存活。
        """
        return []

    # ------------------------------------------------------------------
    # 配置页
    # ------------------------------------------------------------------
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回 Vuetify 配置页与默认配置模型。"""
        cfgs = [
            _alert(
                f"{self.plugin_name}：{self.plugin_desc} "
                "支持普通群组、超级群与频道；用户账号模式需 api_id / api_hash 与 StringSession"
                "（可用插件目录 tools/tg_login.py 生成，脚本会列出已加入群组的 ID）；"
                "Bot 模式需把机器人加入群组（频道设为管理员），并在 @BotFather 用 /setprivacy "
                "关闭隐私模式才能收到群组普通消息；官方 Bot API 只能下载 20MB 以内文件。"
            ),
            _row(
                _sw("enabled", "启用插件"),
                _sw("notify", "发送通知", "下载解压成功后发送 MoviePilot 消息"),
                _sw("onlyonce", "立即运行一次", "保存配置后立即扫描一轮", md=3),
                _sw("clear_records", "清空记录", "清空去重/游标/历史，文件将可重新处理", md=3),
            ),
            _row(
                _sel("mode", "接入方式", MODES, "推荐用户账号模式", md=4),
                _tf("channels", "监控群组 / 频道",
                    "一行或逗号分隔，如 @xxx、-1001234567890（超级群/频道）、-123456789（普通群）；"
                    "可用 tools/tg_login.py 查询 ID",
                    "@groupname", md=8),
            ),
            _row(
                _tf("api_id", "API ID（用户账号）", "my.telegram.org 申请", md=4),
                _tf("api_hash", "API Hash（用户账号）", "", "", "password", md=4),
                _tf("session_string", "StringSession（用户账号）", "tools/tg_login.py 生成后粘贴",
                    "", "password", md=4),
            ),
            _row(
                _tf("bot_token", "Bot Token（机器人模式）",
                    "从 @BotFather 获取；群组需 /setprivacy 关闭隐私模式", "", "password", md=6),
                _tf("bot_api_base", "Bot API 地址", "自建 Bot API Server 可解除 20MB 限制",
                    "https://api.telegram.org", md=6),
            ),
            _row(
                _tf("proxy", "代理地址", "留空则尝试读取 MoviePilot 系统代理；支持 http/socks5",
                    "socks5://127.0.0.1:7890", md=6),
                _tf("interval", "轮询间隔（秒）", "每轮扫描的最小间隔，最小 15 秒", "60", md=3),
                _tf("scan_limit", "单轮最多拉取消息", "防止首次启动时一次拉取过多历史消息", "100", md=3),
            ),
            _row(
                _tf("download_dir", "压缩包保存目录", "容器内的绝对路径", "/downloads/tgarchives", md=6),
                _tf("extract_dir", "解压输出目录", "容器内的绝对路径", "/downloads/tgarchives/extracted", md=6),
            ),
            _row(
                _tf("archive_ext", "压缩包后缀", "逗号分隔，留空使用默认值", DEFAULT_EXTS, md=12),
            ),
            _row(
                _tf("keywords", "包含关键词", "文件名或消息文本命中任一关键词才处理，留空不过滤", md=4),
                _tf("exclude_keywords", "排除关键词", "命中任一关键词则跳过", md=4),
                _tf("senders", "仅处理指定发送者",
                    "群组消息较多时可填用户 ID 或 @用户名（逗号分隔），留空处理全部",
                    "@someone", md=4),
            ),
            _row(
                _tf("min_size_mb", "最小文件（MB）", "0 表示不限", "0", md=4),
                _tf("max_size_mb", "最大文件（MB）", "0 表示不限", "0", md=4),
                _tf("max_files_per_round", "单轮最多处理压缩包", "避免一次轮询耗时过长", "20", md=4),
            ),
            _row(
                _tf("extract_password", "解压密码", "留空表示无密码，支持 7z/RAR/ZIP", "", "password", md=4),
                _sel("existing_action", "同名输出处理", EXISTING_ACTIONS, "", md=4),
                _tf("inner_ext", "仅保留的文件后缀", "解压后仅保留指定后缀，留空保留全部", ".mp4,.mkv,.srt", md=4),
            ),
            _row(
                _sw("per_message_dir", "每个包独立子目录", "输出目录下按压缩包名建子目录"),
                _sw("delete_archive", "完成后删除压缩包", "解压成功后删除已下载的压缩包"),
                _sw("backfill", "首次回溯历史消息", "启用后首轮处理最近 N 条消息", md=3),
            ),
            _row(
                _tf("backfill_count", "回溯条数", "首次启动只回溯最近 N 条消息", "20", md=4),
                _tf("parts_wait_seconds", "分卷等待（秒）", "收到主卷后等待其余分卷到齐再解压", "120", md=4),
                _tf("max_extract_mb", "单包解压上限（MB）", "防御解压炸弹，超限则丢弃本次解压结果", "4096", md=4),
            ),
        ]
        return [{"component": "VForm", "content": cfgs}], {**self._default_config, **self._config}

    # ------------------------------------------------------------------
    # 详情页
    # ------------------------------------------------------------------
    def get_page(self) -> List[dict]:
        """状态、操作按钮、日志与最近记录。"""
        prefix = self.__class__.__name__
        missing, tips = _environment()
        with self._log_lock:
            lines = list(self._logs)
        log_content = ([{"component": "div", "text": line} for line in lines]
                       or [{"component": "div",
                            "props": {"class": "text-medium-emphasis"},
                            "text": "暂无日志，点击「立即扫描」或「测试连接」后显示。"}])
        worker_alive = bool(self._thread and self._thread.is_alive())
        state_text = (
            f"插件状态：{'已启用' if self._enabled else '未启用'}；"
            f"守护线程：{'运行中' if worker_alive else '未运行'}；"
            f"当前任务：{'扫描中' if self._scanning else '空闲'}；"
            f"上次扫描：{self._last_scan or '尚未扫描'}"
        )
        dep_text = (f"依赖检查：缺失 {'、'.join(missing)}。{tips}" if missing
                    else "依赖检查：Telethon 已就绪。" + (f" {tips}" if tips else ""))

        def button(text: str, path: str, icon: str, color: str = "primary") -> Dict[str, Any]:
            return {
                "component": "VCol",
                "props": {"cols": 12, "md": 3},
                "content": [{
                    "component": "VBtn",
                    "props": {"color": color, "variant": "elevated", "block": True,
                              "prepend-icon": icon},
                    "text": text,
                    "events": {"click": {
                        "type": "request",
                        "api": f"plugin/{prefix}{path}",
                        "method": "POST",
                        "success": "已执行，详见下方日志",
                        "fail": "请求失败，请查看插件日志",
                    }},
                }],
            }

        return [
            {"component": "VCard", "props": {"variant": "flat"}, "content": [
                {"component": "VCardTitle", "props": {"class": "text-h6"}, "text": self.plugin_name},
                {"component": "VCardSubtitle", "props": {"class": "text-caption text-medium-emphasis"},
                 "text": f"v{self.plugin_version}　作者：{self.plugin_author}　仓库：MoviePilot-Plugins　{self.plugin_desc}"},
                {"component": "div", "props": {"class": "text-caption text-medium-emphasis px-4"},
                 "text": str(self.author_url)},
                {"component": "VCardText", "content": [
                    _alert(state_text, "success" if self._enabled and worker_alive else "warning", "mb-3"),
                    _alert(dep_text, "error" if missing else "info", "mb-3"),
                    _alert(f"群组/频道：{'、'.join(self._channels()) or '未配置'}；"
                           f"下载目录：{self._cfg_str('download_dir')}；"
                           f"解压目录：{self._cfg_str('extract_dir')}；"
                           f"最近结果：{self._last_result or '无'}", "info", "mb-3"),
                    _row(
                        button("立即扫描", "/scan", "mdi-play"),
                        button("测试连接", "/test", "mdi-connection"),
                        button("环境自检", "/diagnose", "mdi-stethoscope"),
                        button("清空日志", "/clear_logs", "mdi-delete", "error"),
                    ),
                ]},
            ]},
            {"component": "VCard", "props": {"variant": "flat", "class": "mt-4"}, "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "运行日志（最近 300 行）"},
                {"component": "VCardText", "props": {"style": "max-height:320px;overflow:auto"},
                 "content": log_content},
            ]},
            {"component": "VCard", "props": {"variant": "flat", "class": "mt-4"}, "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "处理记录"},
                {"component": "VCardText", "content": [{
                    "component": "VTable",
                    "props": {
                        "api": f"plugin/{prefix}/history",
                        "headers": [
                            {"title": "时间", "key": "time"},
                            {"title": "群组/频道", "key": "chat"},
                            {"title": "文件", "key": "filename"},
                            {"title": "大小", "key": "size_text"},
                            {"title": "状态", "key": "status"},
                            {"title": "说明", "key": "message"},
                        ],
                        "itemKey": "key",
                    },
                }]},
            ]},
        ]

    # ------------------------------------------------------------------
    # 插件 API
    # ------------------------------------------------------------------
    def get_api(self) -> List[Dict[str, Any]]:
        """注册详情页使用的后端接口。"""
        return [
            {"path": "/status", "endpoint": self.api_status, "methods": ["GET"],
             "auth": "bear", "summary": "查询运行状态"},
            {"path": "/scan", "endpoint": self.api_scan, "methods": ["POST"],
             "auth": "bear", "summary": "立即扫描一轮"},
            {"path": "/test", "endpoint": self.api_test, "methods": ["POST"],
             "auth": "bear", "summary": "测试 Telegram 连接与群组/频道可访问性"},
            {"path": "/diagnose", "endpoint": self.api_diagnose, "methods": ["POST"],
             "auth": "bear", "summary": "环境自检"},
            {"path": "/history", "endpoint": self.api_history, "methods": ["GET"],
             "auth": "bear", "summary": "处理记录（VTable 数据源）"},
            {"path": "/clear_records", "endpoint": self.api_clear_records, "methods": ["POST"],
             "auth": "bear", "summary": "清空去重/游标/历史记录"},
            {"path": "/clear_logs", "endpoint": self.api_clear_logs, "methods": ["POST"],
             "auth": "bear", "summary": "清空详情页日志"},
        ]

    @staticmethod
    def _result(success: bool, message: str, **extra: Any) -> Dict[str, Any]:
        """统一 JSON 响应。"""
        return {"success": success, "message": message, **extra}

    def api_status(self) -> Dict[str, Any]:
        """返回运行状态与最近日志。"""
        with self._log_lock:
            lines = list(self._logs)[-30:]
        return self._result(
            True, self._last_result or "空闲",
            enabled=self._enabled,
            running=self._scanning,
            worker=bool(self._thread and self._thread.is_alive()),
            last_scan=self._last_scan,
            logs=lines,
        )

    def api_scan(self) -> Dict[str, Any]:
        """唤醒守护线程立即扫描一轮；插件未启用时只执行一次，不常驻。"""
        problem = self._validate()
        if problem:
            self._log(f"扫描失败：{problem}", "warning")
            return self._result(False, problem)
        if self._enabled:
            self._ensure_worker()
            self._trigger.set()
            return self._result(True, "已请求立即扫描，请查看下方日志")
        self._stop_event.clear()
        threading.Thread(target=self._scan_once, daemon=True,
                         name="tgarchivegrabber-once").start()
        return self._result(True, "插件未启用，已启动单次扫描，请查看下方日志")

    def api_test(self) -> Dict[str, Any]:
        """在后台线程测试 Telegram 连接，避免阻塞 API 线程。"""
        problem = self._validate()
        if problem:
            self._log(f"连接测试失败：{problem}", "warning")
            return self._result(False, problem)
        box: Dict[str, Any] = {}
        done = threading.Event()

        def runner() -> None:
            try:
                box["result"] = self._test_connection()
            except Exception as err:  # noqa: BLE001
                box["result"] = self._result(False, f"连接测试异常：{err}")
            finally:
                done.set()

        threading.Thread(target=runner, daemon=True, name="tgarchivegrabber-test").start()
        if not done.wait(timeout=40):
            return self._result(False, "连接测试超时（40 秒），请检查网络或代理配置")
        result = box.get("result") or self._result(False, "连接测试无结果")
        self._log(f"连接测试：{result.get('message')}")
        for line in result.get("details") or []:
            self._log(f"  {line}")
        return result

    def api_diagnose(self) -> Dict[str, Any]:
        """把运行环境与配置检查结果写入日志面板。"""
        import sys

        missing, tips = _environment()
        tools = {name: shutil.which(name) for name in ("unar", "unrar", "7z", "bsdtar")}
        lines = [
            f"Python：{sys.version.split()[0]}，解释器：{sys.executable}",
            f"模式：{self._cfg_str('mode', 'user')}；群组/频道：{self._channels() or '未配置'}",
            f"发送者白名单：{_split_list(self._config.get('senders')) or '全部'}",
            f"代理：{self._proxy_url() or '未设置'}",
            f"代理软依赖 python-socks：{'缺失' if _proxy_runtime_missing() else '已安装'}",
            f"会话串：{'插件数据目录（已持久化）' if self.get_data('tg_session_string') else ('配置页' if self._cfg_str('session_string') else '未设置')}",
            f"系统解压工具：{tools}",
            f"缺失依赖：{missing or '无'}",
            f"提示：{tips or '无'}",
            f"配置校验：{self._validate() or '通过'}",
        ]
        for line in lines:
            self._log(f"[自检] {line}")
        return self._result(not missing, "环境自检完成，结果已写入日志", details=lines)

    def api_history(self) -> Dict[str, Any]:
        """VTable 数据源：{code:0, data:[...], total:N}。"""
        rows = self._load_history()
        return {"code": 0, "data": rows, "total": len(rows)}

    def api_clear_records(self) -> Dict[str, Any]:
        """清空已处理集合、游标与历史，使文件可重新处理。"""
        for key in ("processed", "cursors", "history", "failures", "pending_parts"):
            try:
                self.del_data(key)
            except Exception:  # noqa: BLE001
                pass
        self._pending_parts = {}
        self._log("已清空去重集合、会话游标、分卷待处理与历史记录")
        return self._result(True, "记录已清空")

    def api_clear_logs(self) -> Dict[str, Any]:
        """清空详情页日志。"""
        with self._log_lock:
            self._logs.clear()
        return self._result(True, "日志已清空")

    # ------------------------------------------------------------------
    # 通用配置与数据存取
    # ------------------------------------------------------------------
    def _cfg_str(self, key: str, default: str = "") -> str:
        """读取字符串配置。"""
        return str(self._config.get(key, default) or "").strip()

    def _cfg_int(self, key: str, default: int, minimum: int = 0) -> int:
        """读取整数配置并做下限保护。"""
        try:
            value = int(float(self._config.get(key, default)))
        except (TypeError, ValueError):
            value = default
        return max(minimum, value)

    def _cfg_bool(self, key: str, default: bool = False) -> bool:
        """读取布尔配置。"""
        return bool(self._config.get(key, default))

    def _session_string(self) -> str:
        """读取会话串：优先插件数据目录中的持久值，回退配置页填写的值。"""
        stored = str(self.get_data("tg_session_string") or "").strip()
        return stored or self._cfg_str("session_string")

    def _channels(self) -> List[str]:
        """返回配置的频道列表（规范化 @ 前缀）。"""
        chats: List[str] = []
        for raw in _split_list(self._config.get("channels")):
            chat = raw.strip().rstrip("/")
            if chat.startswith(("https://t.me/+", "t.me/+")):
                pass  # 邀请链接原样交给 Telethon 解析
            elif chat.startswith("https://t.me/"):
                chat = "@" + chat.split("https://t.me/", 1)[1]
            elif chat.startswith("t.me/"):
                chat = "@" + chat[5:]
            if chat and chat not in chats:
                chats.append(chat)
        return chats

    def _exts(self) -> Tuple[str, ...]:
        """返回允许处理的压缩包后缀元组。"""
        raw = self._cfg_str("archive_ext") or DEFAULT_EXTS
        exts = tuple(x.lower() if x.startswith(".") else "." + x.lower()
                     for x in _split_list(raw))
        return exts or tuple(_split_list(DEFAULT_EXTS))

    def _download_dir(self) -> Path:
        """压缩包下载目录。"""
        return Path(self._cfg_str("download_dir") or "/downloads/tgarchives")

    def _extract_dir(self) -> Path:
        """解压输出根目录。"""
        return Path(self._cfg_str("extract_dir") or "/downloads/tgarchives/extracted")

    def _validate(self) -> Optional[str]:
        """校验运行必需的凭据与目录，返回错误说明或 None。"""
        if not self._channels():
            return "未配置监控群组 / 频道"
        mode = self._cfg_str("mode", "user")
        if mode == "user":
            if not self._cfg_str("api_id") or not self._cfg_str("api_hash"):
                return "用户账号模式需要填写 API ID 与 API Hash"
            if not self._session_string():
                return "用户账号模式需要填写 StringSession（可用 tools/tg_login.py 生成）"
        elif mode == "bot":
            if not self._cfg_str("bot_token"):
                return "机器人模式需要填写 Bot Token"
        else:
            return f"未知接入方式：{mode}"
        if not self._cfg_str("download_dir") or not self._cfg_str("extract_dir"):
            return "需要填写压缩包保存目录与解压输出目录"
        return None

    def _log(self, text: str, level: str = "info") -> None:
        """同时写入详情页日志面板与宿主日志。"""
        line = f"{datetime.now().strftime('%m-%d %H:%M:%S')} {text}"
        with self._log_lock:
            self._logs.append(line)
        try:
            getattr(logger, level, logger.info)(f"{LOG_TAG} {text}")
        except Exception:  # noqa: BLE001
            pass

    def _load_list(self, key: str) -> List[Any]:
        """读取插件数据目录中的列表。"""
        data = self.get_data(key) or []
        return data if isinstance(data, list) else []

    def _load_dict(self, key: str) -> Dict[str, Any]:
        """读取插件数据目录中的字典。"""
        data = self.get_data(key) or {}
        return data if isinstance(data, dict) else {}

    def _load_processed(self) -> set:
        """已处理的消息 key 集合。"""
        return set(str(x) for x in self._load_list("processed"))

    def _save_processed(self, processed: set) -> None:
        """保存去重集合并限制长度，避免无限增长。"""
        items = list(processed)
        if len(items) > MAX_PROCESSED:
            items = items[-MAX_PROCESSED:]
        self.save_data("processed", items)

    def _load_cursors(self) -> Dict[str, int]:
        """读取每个频道（或 Bot）已处理到的消息游标。"""
        result: Dict[str, int] = {}
        for key, value in self._load_dict("cursors").items():
            try:
                result[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
        return result

    def _save_cursors(self, cursors: Dict[str, int]) -> None:
        """保存会话游标。"""
        self.save_data("cursors", cursors)

    def _load_history(self) -> List[Dict[str, Any]]:
        """最近处理记录（最新在前）。"""
        return [x for x in self._load_list("history") if isinstance(x, dict)]

    def _append_history(self, item: Dict[str, Any]) -> None:
        """追加一条处理记录。"""
        history = self._load_history()
        history.insert(0, item)
        self.save_data("history", history[:MAX_HISTORY])

    def _load_failures(self) -> Dict[str, int]:
        """读取消息失败次数。"""
        return {str(k): int(v) for k, v in self._load_dict("failures").items()
                if isinstance(v, (int, float))}

    def _notify(self, title: str, text: str) -> None:
        """通过宿主消息机制发送通知，兼容不同宿主的签名。"""
        if not self._cfg_bool("notify", True):
            return
        try:
            mtype = getattr(NotificationType, "Plugin", None) if NotificationType else None
            if mtype is not None:
                self.post_message(mtype=mtype, title=title, text=text)
            else:
                self.post_message(title=title, text=text)
        except TypeError:
            try:
                self.post_message(title=title, text=text)
            except Exception as err:  # noqa: BLE001
                self._log(f"发送通知失败：{err}", "warning")
        except Exception as err:  # noqa: BLE001
            self._log(f"发送通知失败：{err}", "warning")

    # ------------------------------------------------------------------
    # 守护线程与扫描调度
    # ------------------------------------------------------------------
    def _ensure_worker(self) -> None:
        """确保插件启用时存在且仅存在一个守护线程。"""
        with self._state_lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._worker_loop, name="tgarchivegrabber", daemon=True
            )
            self._thread.start()
            self._log("守护线程已启动")

    def _worker_loop(self) -> None:
        """按配置间隔轮询；异常只记日志，不让线程退出。"""
        current = threading.current_thread()
        while not self._stop_event.is_set() and self._thread is current:
            try:
                self._scan_once()
            except Exception as err:  # noqa: BLE001
                self._log(f"扫描出现异常：{err}", "error")
            interval = self._cfg_int("interval", 60, MIN_INTERVAL)
            self._trigger.wait(timeout=interval)
            self._trigger.clear()
        self._log("守护线程已退出")

    def _handle_switches(self) -> None:
        """处理「立即运行一次 / 清空记录」开关，触发后自动复位。"""
        try:
            if self._config.get("onlyonce"):
                self._log("检测到「立即运行一次」，开始扫描…")
                self._scan_once()
        except Exception as err:  # noqa: BLE001
            self._log(f"立即运行失败：{err}", "error")
        finally:
            if self._config.get("onlyonce"):
                self._config["onlyonce"] = False
                try:
                    self.update_config(self._config)
                except Exception:  # noqa: BLE001
                    pass
        try:
            if self._config.get("clear_records"):
                self.api_clear_records()
        finally:
            if self._config.get("clear_records"):
                self._config["clear_records"] = False
                try:
                    self.update_config(self._config)
                except Exception:  # noqa: BLE001
                    pass

    def _scan_once(self) -> None:
        """执行一轮完整扫描：拉取新消息 → 下载 → 解压 → 保存游标。"""
        if not self._scan_lock.acquire(blocking=False):
            self._log("上一轮扫描尚未结束，跳过本轮", "warning")
            return
        self._scanning = True
        cursors: Dict[str, int] = {}
        processed: set = set()
        stats: Dict[str, int] = {"handled": 0, "ok": 0, "fail": 0, "skipped": 0}
        try:
            problem = self._validate()
            if problem:
                self._log(f"跳过扫描：{problem}", "warning")
                return
            self._download_dir().mkdir(parents=True, exist_ok=True)
            self._extract_dir().mkdir(parents=True, exist_ok=True)
            cursors = self._load_cursors()
            processed = self._load_processed()
            failures = self._load_failures()
            self._last_scan = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            mode = self._cfg_str("mode", "user")
            try:
                if mode == "user":
                    cursors = self._scan_telethon(cursors, processed, failures, stats)
                else:
                    cursors = self._scan_bot(cursors, processed, failures, stats)
            except Exception as err:  # noqa: BLE001
                self._log(f"拉取群组/频道消息失败：{err}", "error")
            finally:
                self._save_cursors(cursors)
                self._save_processed(processed)
                trimmed = dict(list(failures.items())[-MAX_FAILURES:])
                self.save_data("failures", trimmed)
                self._flush_pending_parts()
            summary = (
                f"扫描完成：处理 {stats['handled']} 个压缩包，"
                f"成功 {stats['ok']}，失败 {stats['fail']}，跳过 {stats['skipped']}"
            )
            self._last_result = summary
            self._log(summary)
            if stats["ok"] or stats["fail"]:
                self._notify(
                    f"{self.plugin_name} 扫描完成",
                    f"成功 {stats['ok']} 个，失败 {stats['fail']} 个，"
                    f"输出目录：{self._extract_dir()}",
                )
        finally:
            self._scanning = False
            self._scan_lock.release()

    # ------------------------------------------------------------------
    # 网络代理
    # ------------------------------------------------------------------
    def _proxy_url(self) -> str:
        """返回最终使用的代理地址；未配置时尝试读取 MoviePilot 系统代理。"""
        raw = self._cfg_str("proxy")
        if raw:
            return raw
        try:
            system_proxy = getattr(settings, "PROXY", None) if settings else None
        except Exception:  # noqa: BLE001
            system_proxy = None
        if isinstance(system_proxy, dict):
            system_proxy = system_proxy.get("https") or system_proxy.get("http") or ""
        return str(system_proxy or "").strip() or ""

    def _telethon_proxy(self) -> Optional[tuple]:
        """把代理 URL 转成 Telethon 需要的 (type, host, port, ...) 元组。"""
        raw = self._proxy_url()
        if not raw:
            return None
        parsed = urlparse(raw if "://" in raw else "http://" + raw)
        scheme = (parsed.scheme or "").lower()
        if scheme.startswith("socks5"):
            ptype, default_port = "socks5", 1080
        elif scheme.startswith("socks4"):
            ptype, default_port = "socks4", 1080
        elif scheme.startswith("http"):
            ptype, default_port = "http", 8080
        else:
            self._log(f"不支持的代理协议：{scheme}，已忽略", "warning")
            return None
        if not parsed.hostname:
            return None
        proxy: List[Any] = [ptype, parsed.hostname, parsed.port or default_port]
        if parsed.username:
            proxy += [True, parsed.username, parsed.password or ""]
        return tuple(proxy)

    def _requests_proxies(self) -> Optional[Dict[str, str]]:
        """把代理 URL 转成 requests 的 proxies 字典。"""
        raw = self._proxy_url()
        return {"http": raw, "https": raw} if raw else None

    # ------------------------------------------------------------------
    # Telegram 接入：用户账号（Telethon）
    # ------------------------------------------------------------------
    def _resolve_entity(self, client: Any, chat: str) -> Any:
        """
        解析群组 / 频道实体并缓存结果。

        普通群 ID 可能是 ``-123456`` / ``123456``，超级群 / 频道是 ``-100123...``，
        依次尝试多种形式；StringSession 不保存 entity cache，全新会话直接用数字 ID
        解析会失败，因此最后按 TgMediaCenter 的 ``prime_updates`` 思路遍历对话列表
        预热并匹配。结果缓存下来，避免每轮轮询都重复预热触发限流。
        """
        cache: Dict[str, Any] = getattr(self, "_resolved_entities", None) or {}
        self._resolved_entities = cache
        cached = cache.get(chat)
        if cached is not None:
            return cached
        value = str(chat or "").strip()
        if re.fullmatch(r"-?\d+", value):
            number = int(value)
            candidates: List[Any] = [number]
            if number < 0 and not value.startswith("-100"):
                candidates.append(-number)
            elif number > 0:
                candidates.append(-number)
        else:
            candidates = [value]
        errors: List[str] = []
        for candidate in candidates:
            try:
                entity = client.get_entity(candidate)
                cache[chat] = entity
                return entity
            except Exception as err:  # noqa: BLE001 - 依次尝试其它 ID 形式
                errors.append(f"{candidate}: {err}")
        try:
            for dialog in client.iter_dialogs(limit=800):
                entity = dialog.entity
                entity_id = getattr(entity, "id", None)
                if entity_id is None:
                    continue
                raw_id = abs(int(entity_id))
                variants = {raw_id, -raw_id, int(f"-100{raw_id}")}
                username = getattr(entity, "username", None)
                for candidate in candidates:
                    if isinstance(candidate, int) and candidate in variants:
                        cache[chat] = entity
                        return entity
                    if (isinstance(candidate, str) and username
                            and candidate.lstrip("@").lower() == str(username).lower()):
                        cache[chat] = entity
                        return entity
        except Exception as err:  # noqa: BLE001 - 预热失败时给出可读错误
            errors.append(f"iter_dialogs: {err}")
        raise RuntimeError(
            f"无法解析群组/频道 {value}（请确认账号已加入该会话）："
            + " | ".join(errors)
        )

    @staticmethod
    def _latest_message_id(client: Any, entity: Any) -> int:
        """读取群组/频道最新消息 ID，兼容 Telethon 返回列表或单对象的不同版本。"""
        latest = client.get_messages(entity, limit=1)
        if isinstance(latest, (list, tuple)):
            return int(latest[0].id) if latest else 0
        return int(getattr(latest, "id", 0) or 0)

    def _scan_telethon(self, cursors: Dict[str, int], processed: set,
                       failures: Dict[str, int], stats: Dict[str, int]) -> Dict[str, int]:
        """用 Telethon 逐个群组/频道拉取新消息，下载闭包在连接上下文内执行。"""
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
        except ImportError:
            self._log("缺少 telethon 依赖，用户账号模式不可用（请安装插件声明的依赖）", "error")
            return cursors
        client = TelegramClient(
            StringSession(self._session_string()),
            int(self._cfg_str("api_id")),
            self._cfg_str("api_hash"),
            proxy=self._telethon_proxy(),
            connection_retries=2,
            retry_delay=1,
            timeout=30,
        )
        max_round = self._cfg_int("max_files_per_round", 20, 1)
        scan_limit = self._cfg_int("scan_limit", 100, 1)
        backfill = self._cfg_bool("backfill")
        backfill_count = self._cfg_int("backfill_count", 20, 0)
        with client:
            for chat in self._channels():
                if self._stop_event.is_set():
                    break
                ckey = f"user:{chat}"
                try:
                    entity = self._resolve_entity(client, chat)
                except Exception as err:  # noqa: BLE001
                    self._log(f"群组/频道 {chat} 无法访问：{err}", "warning")
                    continue
                cursor = cursors.get(ckey)
                if cursor is None:
                    latest_id = self._latest_message_id(client, entity)
                    if latest_id <= 0:
                        self._log(f"群组/频道 {chat} 无法读取最新消息，本轮跳过（避免误拉全量历史）",
                                  "warning")
                        continue
                    cursor = max(0, latest_id - backfill_count) if backfill else latest_id
                    self._log(
                        f"群组/频道 {chat} 初始化游标：{cursor}（最新消息 {latest_id}，"
                        f"{'回溯 ' + str(backfill_count) + ' 条' if backfill else '仅处理之后的新消息'}）"
                    )
                pulled = 0
                try:
                    messages = client.iter_messages(
                        entity, min_id=cursor, reverse=True, limit=scan_limit
                    )
                    for msg in messages:
                        if self._stop_event.is_set():
                            break
                        pulled += 1
                        item = self._build_telethon_item(client, msg, chat)
                        if item is None:
                            cursor = max(cursor, int(msg.id))
                            continue
                        if not self._match_archive(item):
                            cursor = max(cursor, item["sort_id"])
                            stats["skipped"] += 1
                            continue
                        if stats["handled"] >= max_round:
                            self._log(f"已达单轮处理上限 {max_round}，剩余消息留到下一轮")
                            break
                        stats["handled"] += 1
                        if self._process_item(item, processed, failures, stats):
                            cursor = max(cursor, item["sort_id"])
                        else:
                            self._log("该消息处理失败，保留游标下轮重试，继续处理后续消息")
                except Exception as err:  # noqa: BLE001
                    self._log(f"群组/频道 {chat} 遍历消息出错：{err}", "error")
                cursors[ckey] = cursor
                if pulled:
                    self._log(f"群组/频道 {chat} 本轮拉取 {pulled} 条新消息，游标={cursor}")
        return cursors

    def _build_telethon_item(self, client: Any, msg: Any, chat: str) -> Optional[Dict[str, Any]]:
        """把 Telethon 消息转换成统一的待处理项。"""
        name = ""
        size = 0
        try:
            file_obj = getattr(msg, "file", None)
            name = str(getattr(file_obj, "name", "") or "")
            size = int(getattr(file_obj, "size", 0) or 0)
        except Exception:  # noqa: BLE001
            pass
        if not name:
            try:
                for attr in getattr(getattr(msg, "document", None), "attributes", []) or []:
                    candidate = getattr(attr, "file_name", None)
                    if candidate:
                        name = str(candidate)
                        break
            except Exception:  # noqa: BLE001
                pass
        if not name or not getattr(msg, "document", None):
            return None
        sender_tokens: set = set()
        try:
            sender_id = getattr(msg, "sender_id", None)
            if sender_id:
                sender_tokens.add(str(sender_id))
        except Exception:  # noqa: BLE001
            pass
        try:
            sender = getattr(msg, "sender", None)
            username = getattr(sender, "username", None) if sender else None
            if username:
                sender_tokens.add("@" + str(username).lower())
        except Exception:  # noqa: BLE001 - 实体缓存缺失时忽略
            pass
        date = ""
        try:
            date = msg.date.strftime("%Y-%m-%d %H:%M:%S") if getattr(msg, "date", None) else ""
        except Exception:  # noqa: BLE001
            pass

        def download(target: Path, _msg: Any = msg, _client: Any = client) -> None:
            saved = _client.download_media(_msg, file=str(target))
            if not saved or not Path(str(saved)).exists():
                raise RuntimeError("Telethon 未返回有效的下载文件")

        return {
            "key": f"user:{chat}:{msg.id}",
            "sort_id": int(msg.id),
            "chat": chat,
            "msg_id": str(msg.id),
            "filename": name,
            "size": size,
            "caption": str(getattr(msg, "message", "") or ""),
            "date": date,
            "sender_tokens": sender_tokens,
            "download": download,
        }

    # ------------------------------------------------------------------
    # Telegram 接入：Bot API
    # ------------------------------------------------------------------
    def _bot_api(self, method: str, params: Optional[Dict[str, Any]] = None,
                 timeout: int = 30) -> Any:
        """调用 Telegram Bot API 并返回 result 字段。"""
        import requests

        base = (self._cfg_str("bot_api_base") or "https://api.telegram.org").rstrip("/")
        token = self._cfg_str("bot_token")
        response = requests.get(
            f"{base}/bot{token}/{method}", params=params or {}, timeout=timeout,
            proxies=self._requests_proxies(),
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Bot API {method} 返回错误：{data.get('description') or data}")
        return data.get("result")

    def _bot_chat_matches(self, post: Dict[str, Any]) -> bool:
        """Bot 模式下按群组/频道 ID、用户名或标题过滤更新。"""
        allowed = self._channels()
        if not allowed:
            return True
        chat = post.get("chat") or {}
        identifiers = {
            str(chat.get("id") or ""),
            str(chat.get("username") or "").lower(),
            str(chat.get("title") or "").lower(),
        }
        if chat.get("username"):
            identifiers.add("@" + str(chat["username"]).lower())
        for raw in allowed:
            value = raw.lower()
            if value in identifiers or value.lstrip("@") in identifiers:
                return True
            if re.fullmatch(r"-?\d+", value):
                chat_id = str(chat.get("id") or "")
                # 普通群 ID 常被写成 -123456 或 123456，忽略负号再比一次
                if value == chat_id or value.lstrip("-") == chat_id.lstrip("-"):
                    return True
        return False

    def _build_bot_item(self, update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """把 Bot API update 转换成统一的待处理项。"""
        post = (update.get("channel_post") or update.get("message")
                or update.get("edited_channel_post"))
        if not isinstance(post, dict) or not self._bot_chat_matches(post):
            return None
        doc = post.get("document")
        if not isinstance(doc, dict):
            return None
        name = str(doc.get("file_name") or "")
        file_id = doc.get("file_id")
        if not name or not file_id:
            return None
        chat_info = post.get("chat") or {}
        chat = str(chat_info.get("username") or chat_info.get("id") or "bot")
        sender_tokens: set = set()
        for info in (post.get("from") or {}, post.get("sender_chat") or {}):
            if not isinstance(info, dict):
                continue
            if info.get("id"):
                sender_tokens.add(str(info["id"]))
            if info.get("username"):
                sender_tokens.add("@" + str(info["username"]).lower())
        update_id = int(update.get("update_id") or 0)
        base = (self._cfg_str("bot_api_base") or "https://api.telegram.org").rstrip("/")
        token = self._cfg_str("bot_token")

        def download(target: Path, _file_id: str = str(file_id)) -> None:
            import requests

            try:
                info = self._bot_api("getFile", {"file_id": _file_id})
            except Exception as err:  # noqa: BLE001
                raise RuntimeError(
                    f"getFile 失败：{err}（官方 Bot API 限制单文件 20MB，"
                    "请改用用户账号模式或自建 Bot API Server）"
                ) from err
            file_path = (info or {}).get("file_path")
            if not file_path:
                raise RuntimeError("getFile 未返回 file_path")
            url = f"{base}/file/bot{token}/{file_path}"
            with requests.get(url, stream=True, timeout=(15, 180),
                              proxies=self._requests_proxies()) as response:
                response.raise_for_status()
                with open(target, "wb") as handle:
                    for chunk in response.iter_content(chunk_size=512 * 1024):
                        if chunk:
                            handle.write(chunk)

        return {
            "key": f"bot:{update_id}",
            "sort_id": update_id,
            "chat": chat,
            "msg_id": str(post.get("message_id") or update_id),
            "filename": name,
            "size": int(doc.get("file_size") or 0),
            "caption": str(post.get("caption") or post.get("text") or ""),
            "date": datetime.fromtimestamp(post.get("date") or time.time()).strftime(
                "%Y-%m-%d %H:%M:%S"),
            "sender_tokens": sender_tokens,
            "download": download,
        }

    def _scan_bot(self, cursors: Dict[str, int], processed: set,
                  failures: Dict[str, int], stats: Dict[str, int]) -> Dict[str, int]:
        """用 Bot API getUpdates 拉取新消息（Bot 需入群并关闭隐私模式）。"""
        import json

        ckey = "bot:updates"
        cursor = cursors.get(ckey)
        if cursor is None:
            try:
                last = self._bot_api("getUpdates", {"offset": -1, "limit": 1}) or []
                latest_id = int(last[-1]["update_id"]) if last else 0
            except Exception as err:  # noqa: BLE001
                self._log(f"Bot 初始化游标失败：{err}", "warning")
                latest_id = 0
            backfill_count = self._cfg_int("backfill_count", 20, 0)
            cursor = (max(0, latest_id - backfill_count)
                      if self._cfg_bool("backfill") else latest_id)
            self._log(f"Bot 初始化游标：{cursor}（最新 update {latest_id}）")
        params = {
            "offset": cursor + 1,
            "limit": 100,
            "timeout": 0,
            "allowed_updates": json.dumps(
                ["message", "channel_post", "edited_channel_post"]
            ),
        }
        updates = self._bot_api("getUpdates", params, timeout=45) or []
        max_round = self._cfg_int("max_files_per_round", 20, 1)
        for update in updates:
            if self._stop_event.is_set():
                break
            item = self._build_bot_item(update)
            sort_id = int(update.get("update_id") or 0)
            if item is None:
                cursor = max(cursor, sort_id)
                continue
            if not self._match_archive(item):
                cursor = max(cursor, sort_id)
                stats["skipped"] += 1
                continue
            if stats["handled"] >= max_round:
                self._log(f"已达单轮处理上限 {max_round}，剩余更新留到下一轮")
                break
            stats["handled"] += 1
            if self._process_item(item, processed, failures, stats):
                cursor = max(cursor, sort_id)
            else:
                self._log("该消息处理失败，保留游标下轮重试，继续处理后续更新")
        cursors[ckey] = cursor
        if updates:
            self._log(f"Bot 本轮拉取 {len(updates)} 条更新，游标={cursor}")
        return cursors

    def _test_connection(self) -> Dict[str, Any]:
        """测试凭据是否有效、群组/频道是否可访问，结果写入详情页日志。"""
        details: List[str] = []
        mode = self._cfg_str("mode", "user")
        failed = False
        if mode == "user":
            try:
                from telethon import TelegramClient
                from telethon.sessions import StringSession
            except ImportError:
                return self._result(False, "缺少 telethon 依赖", details=["请先安装插件声明的依赖"])
            client = TelegramClient(
                StringSession(self._session_string()),
                int(self._cfg_str("api_id")), self._cfg_str("api_hash"),
                proxy=self._telethon_proxy(), connection_retries=2,
                retry_delay=1, timeout=30,
            )
            try:
                with client:
                    me = client.get_me()
                    details.append(
                        f"登录账号：{getattr(me, 'first_name', '') or ''} "
                        f"(@{getattr(me, 'username', '') or '无用户名'})"
                    )
                    for chat in self._channels():
                        try:
                            entity = self._resolve_entity(client, chat)
                            title = getattr(entity, "title", None) or chat
                            details.append(
                                f"群组/频道 {chat} → {title}，最新消息 id="
                                f"{self._latest_message_id(client, entity)}"
                            )
                        except Exception as err:  # noqa: BLE001
                            failed = True
                            details.append(f"群组/频道 {chat} 访问失败：{err}")
                message = "连接测试完成" + (
                    "（部分群组/频道不可访问）" if failed else "，全部群组/频道可访问")
                return self._result(not failed, message, details=details)
            except Exception as err:  # noqa: BLE001
                return self._result(False, f"Telethon 连接失败：{err}", details=details)
        try:
            me = self._bot_api("getMe", {}) or {}
            details.append(f"Bot：@{me.get('username', '未知')} ({me.get('first_name', '')})")
            for chat in self._channels():
                try:
                    try:
                        info = self._bot_api("getChat", {"chat_id": chat}) or {}
                    except Exception:  # noqa: BLE001 - 普通群填写正数 ID 时补负号重试
                        if re.fullmatch(r"\d+", chat):
                            info = self._bot_api("getChat", {"chat_id": f"-{chat}"}) or {}
                        else:
                            raise
                    details.append(f"群组/频道 {chat} → {info.get('title') or info.get('username') or '可访问'}")
                except Exception as err:  # noqa: BLE001
                    failed = True
                    details.append(f"群组/频道 {chat} 访问失败：{err}")
            details.append(
                "提示：群组中 Bot 需在 @BotFather 用 /setprivacy 关闭隐私模式才能收到普通消息；"
                "频道需设为管理员才能收到 channel_post；官方 API 限制 20MB。"
            )
            details.append("注意：getChat 可访问不代表能收到消息，请确认 Bot 已入群且隐私模式已关闭。")
            return self._result(not failed, "Bot 连接测试完成", details=details)
        except Exception as err:  # noqa: BLE001
            return self._result(False, f"Bot 连接失败：{err}", details=details)

    # ------------------------------------------------------------------
    # 消息过滤与处理
    # ------------------------------------------------------------------
    def _sender_allowed(self, item: Dict[str, Any]) -> bool:
        """按发送者白名单过滤群组消息；消息缺少发送者信息时放行，避免误杀。"""
        allowed = [x.lower().lstrip("@")
                   for x in _split_list(self._config.get("senders"))]
        if not allowed:
            return True
        tokens = {str(token).lower().lstrip("@")
                  for token in (item.get("sender_tokens") or set()) if token}
        if not tokens:
            return True
        return any(value in tokens for value in allowed)

    def _match_archive(self, item: Dict[str, Any]) -> bool:
        """判断消息附件是否为需要处理的压缩包。"""
        if not self._sender_allowed(item):
            return False
        filename = str(item.get("filename") or "")
        lower = filename.lower()
        if not lower.endswith(self._exts()):
            return False
        size = int(item.get("size") or 0)
        min_size = self._cfg_int("min_size_mb", 0) * 1024 * 1024
        max_size = self._cfg_int("max_size_mb", 0) * 1024 * 1024
        if min_size and size and size < min_size:
            return False
        if max_size and size and size > max_size:
            return False
        text = f"{filename} {item.get('caption') or ''}".lower()
        keywords = [x.lower() for x in _split_list(self._config.get("keywords"))]
        if keywords and not any(x in text for x in keywords):
            return False
        excluded = [x.lower() for x in _split_list(self._config.get("exclude_keywords"))]
        if excluded and any(x in text for x in excluded):
            return False
        return True

    def _process_item(self, item: Dict[str, Any], processed: set,
                      failures: Dict[str, int], stats: Dict[str, int]) -> bool:
        """
        处理一条压缩包消息：下载、解压、记录。

        :return: True 表示游标可以推进；False 表示失败且需要下轮重试。
        """
        key = str(item["key"])
        if key in processed:
            return True
        filename = _safe_name(str(item["filename"]))
        stem, is_part, is_main = _archive_group(filename)
        target_dir = self._download_dir()
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            if not is_part:
                group_prefix = _safe_name(stem).lower() + "."
                siblings = [p for p in target_dir.iterdir()
                            if p.is_file()
                            and p.name.lower().startswith(group_prefix)
                            and p.name != filename
                            and p.name.lower().endswith(self._exts())]
                if siblings:
                    is_part, is_main = True, True
            target = target_dir / filename
            expected_size = int(item.get("size") or 0)
            reusable = (target.exists() and target.stat().st_size > 0
                        and (not expected_size or target.stat().st_size == expected_size))
            if not reusable:
                partial = target.with_suffix(target.suffix + ".part")
                if partial.exists():
                    partial.unlink()  # 清理上次崩溃残留，避免下载器自动改名
                self._log(f"下载 {item.get('chat')}/{filename}（{_human_size(item.get('size'))}）")
                item["download"](partial)
                if not partial.exists() or partial.stat().st_size == 0:
                    raise RuntimeError("下载完成但文件为空")
                if target.exists():
                    target.unlink()
                partial.replace(target)
            else:
                self._log(f"复用已下载文件 {target.name}")
            self._handle_archive(target, stem, is_part, is_main, item)
            processed.add(key)
            failures.pop(key, None)
            stats["ok"] += 1
            return True
        except Exception as err:  # noqa: BLE001
            count = int(failures.get(key, 0)) + 1
            failures[key] = count
            self._log(f"处理失败 {filename}（第 {count} 次）：{err}", "error")
            try:
                partial = target_dir / (filename + ".part")
                if partial.exists():
                    partial.unlink()
            except Exception:  # noqa: BLE001
                pass
            if count >= DEFAULT_RETRY:
                self._append_history({
                    "key": key, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "chat": item.get("chat", ""), "filename": filename,
                    "size_text": _human_size(item.get("size")), "status": "失败",
                    "message": f"重试 {count} 次后放弃：{err}",
                })
                processed.add(key)
                stats["fail"] += 1
                return True
            return False

    # ------------------------------------------------------------------
    # 压缩包处理（下载后的落盘、分卷聚合、解压提交）
    # ------------------------------------------------------------------
    @staticmethod
    def _find_main_volume(folder: Path, stem: str) -> Optional[Path]:
        """在下载目录中寻找同组已落盘的主卷文件。"""
        if not folder.exists():
            return None
        prefix = _safe_name(stem).lower() + "."
        for path in sorted(folder.iterdir()):
            if not path.is_file() or not path.name.lower().startswith(prefix):
                continue
            if path.name.lower().endswith(
                    (".part1.rar", ".7z.001", ".001", ".rar", ".zip")):
                return path
        return None

    def _handle_archive(self, archive: Path, stem: str, is_part: bool,
                        is_main: bool, item: Dict[str, Any]) -> None:
        """普通包立即解压；分卷包登记为待处理，等其余分卷到齐后统一解压。"""
        if not is_part:
            self._extract_one(archive, stem, item)
            return
        now = time.time()
        entry = self._pending_parts.get(stem) or {
            "dir": str(archive.parent),
            "main": "",
            "ts": now,
            "chat": item.get("chat", ""),
        }
        entry["dir"] = str(archive.parent)
        entry["chat"] = item.get("chat", entry.get("chat", ""))
        entry["ts"] = float(entry.get("ts") or now)
        if is_main:
            entry["main"] = str(archive)
            entry["ts"] = now
        elif not entry.get("main"):
            main = self._find_main_volume(Path(entry["dir"]), stem)
            if main:
                entry["main"] = str(main)
                self._log(f"分卷组 {stem} 找到已下载的主卷 {main.name}")
        self._pending_parts[stem] = entry
        self.save_data("pending_parts", self._pending_parts)
        group_prefix = _safe_name(stem).lower() + "."
        parts = [p.name for p in Path(entry["dir"]).iterdir()
                 if p.is_file() and p.name.lower().startswith(group_prefix)]
        self._log(
            f"分卷 {archive.name} 已就绪（主卷={'是' if is_main else '否'}，"
            f"当前同组 {len(parts)} 个文件），等待其余分卷…"
        )

    def _flush_pending_parts(self) -> None:
        """等待窗口结束后解压分卷包；失败记录后丢弃，避免无限重试。"""
        if not self._pending_parts:
            return
        wait = self._cfg_int("parts_wait_seconds", 120, 0)
        now = time.time()
        changed = False
        for stem, entry in list(self._pending_parts.items()):
            if not isinstance(entry, dict):
                self._pending_parts.pop(stem, None)
                changed = True
                continue
            if not entry.get("main"):
                if now - float(entry.get("ts") or now) > 3600:
                    self._log(f"分卷组 {stem} 始终未收到主卷，已丢弃待处理记录", "warning")
                    self._pending_parts.pop(stem, None)
                    changed = True
                continue
            if now - float(entry.get("ts") or now) < wait:
                continue
            main = Path(str(entry["main"]))
            parts_dir = Path(str(entry.get("dir") or main.parent))
            if not main.exists():
                self._pending_parts.pop(stem, None)
                changed = True
                continue
            item = {
                "key": f"parts:{stem}",
                "chat": entry.get("chat", ""),
                "filename": main.name,
                "size": main.stat().st_size,
            }
            try:
                self._log(f"分卷组 {stem} 等待结束，开始解压主卷 {main.name}")
                self._extract_one(main, stem, item)
                if self._cfg_bool("delete_archive"):
                    group_prefix = _safe_name(stem).lower() + "."
                    for part in parts_dir.iterdir():
                        if part.is_file() and part.name.lower().startswith(group_prefix):
                            try:
                                part.unlink()
                            except OSError:
                                pass
            except Exception as err:  # noqa: BLE001
                self._log(f"分卷组 {stem} 解压失败：{err}", "error")
                self._append_history({
                    "key": item["key"], "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "chat": item["chat"], "filename": main.name,
                    "size_text": _human_size(item["size"]), "status": "失败",
                    "message": f"分卷解压失败：{err}",
                })
            finally:
                self._pending_parts.pop(stem, None)
                changed = True
        if changed:
            self.save_data("pending_parts", self._pending_parts)

    def _extract_one(self, archive: Path, stem: str, item: Dict[str, Any]) -> None:
        """把单个压缩包解压到临时目录，校验后再提交到最终输出目录。"""
        root = self._extract_dir()
        root.mkdir(parents=True, exist_ok=True)
        action = self._cfg_str("existing_action", "skip") or "skip"
        if action not in EXISTING_ACTIONS:
            action = "skip"
        base = _safe_name(stem) if self._cfg_bool("per_message_dir", True) else ""
        final = root / base if base else root
        if base and final.exists() and any(final.iterdir()):
            if action == "skip":
                self._log(f"输出目录已存在，跳过解压：{final}")
                self._record(item, "跳过", f"输出目录已存在：{final}")
                return
            if action == "overwrite":
                self._log(f"覆盖同名输出目录：{final}", "warning")
                shutil.rmtree(final, ignore_errors=True)
            elif action == "rename":
                final = self._unique_dir(root, base)
        temp = root / f".tg_tmp_{_safe_name(stem)}_{int(time.time() * 1000)}"
        temp.mkdir(parents=True, exist_ok=True)
        try:
            count = self._extract_archive(archive, temp, self._cfg_str("extract_password"))
            kept, removed = self._apply_inner_filter(temp)
            final.mkdir(parents=True, exist_ok=True)
            self._commit_extract(temp, final, action)
            size_text = _human_size(item.get("size"))
            self._log(
                f"解压成功 {archive.name}（{size_text}）→ {final}，"
                f"文件 {count} 个，保留 {kept} 个"
                + (f"，过滤 {removed} 个" if removed else "")
            )
            self._record(item, "成功", f"解压 {kept} 个文件 → {final}")
            if self._cfg_bool("delete_archive") and not str(item.get("key", "")).startswith("parts:"):
                try:
                    archive.unlink()
                    self._log(f"已删除压缩包 {archive.name}")
                except OSError as err:
                    self._log(f"删除压缩包失败：{err}", "warning")
        finally:
            shutil.rmtree(temp, ignore_errors=True)

    def _record(self, item: Dict[str, Any], status: str, message: str) -> None:
        """写一条处理记录到历史面板。"""
        self._append_history({
            "key": str(item.get("key") or ""),
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "chat": str(item.get("chat") or ""),
            "filename": str(item.get("filename") or ""),
            "size_text": _human_size(item.get("size")),
            "status": status,
            "message": message,
        })

    @staticmethod
    def _unique_dir(root: Path, base: str) -> Path:
        """为同名输出目录生成带序号的新目录。"""
        for index in range(2, 1000):
            candidate = root / f"{base}_{index}"
            if not candidate.exists() or not any(candidate.iterdir()):
                return candidate
        return root / f"{base}_{int(time.time())}"

    @staticmethod
    def _unique_file(path: Path) -> Path:
        """为同名输出文件生成带序号的新路径。"""
        for index in range(2, 1000):
            candidate = path.with_name(f"{path.stem}_{index}{path.suffix}")
            if not candidate.exists():
                return candidate
        return path.with_name(f"{path.stem}_{int(time.time())}{path.suffix}")

    @staticmethod
    def _commit_extract(temp: Path, final: Path, action: str) -> None:
        """把临时解压结果移动到最终目录，按策略处理同名文件。"""
        for child in list(temp.iterdir()):
            target = final / child.name
            if target.exists():
                if action == "skip":
                    continue
                if action == "rename":
                    target = TgArchiveGrabber._unique_file(target)
                elif action == "overwrite":
                    if target.is_dir():
                        shutil.rmtree(target, ignore_errors=True)
                    else:
                        target.unlink()
            shutil.move(str(child), str(target))

    def _apply_inner_filter(self, dest: Path) -> Tuple[int, int]:
        """按配置仅保留指定后缀的文件，返回 (保留数, 过滤数)。"""
        raw = self._cfg_str("inner_ext")
        files = [p for p in dest.rglob("*") if p.is_file()]
        if not raw:
            return len(files), 0
        exts = tuple(x.lower() if x.startswith(".") else "." + x.lower()
                     for x in _split_list(raw))
        removed = 0
        for path in files:
            if not path.name.lower().endswith(exts):
                try:
                    path.unlink()
                    removed += 1
                except OSError:
                    pass
        for directory in sorted((p for p in dest.rglob("*") if p.is_dir()),
                                key=lambda p: len(p.parts), reverse=True):
            try:
                directory.rmdir()
            except OSError:
                pass
        return len(files) - removed, removed

    # ------------------------------------------------------------------
    # 解压实现
    # ------------------------------------------------------------------
    def _extract_archive(self, archive: Path, dest: Path, password: str) -> int:
        """按文件类型选择解压方式，返回包内文件数。"""
        name = archive.name.lower()
        if name.endswith(".zip"):
            siblings = [p for p in archive.parent.iterdir()
                        if p.is_file()
                        and re.fullmatch(re.escape(archive.stem) + r"\.z\d{2}",
                                         p.name, re.IGNORECASE)]
            if siblings:
                self._log(f"检测到 ZIP 分卷（{len(siblings)} 个附加卷），改用系统工具解压")
                return self._extract_external(archive, dest, password)
            return self._extract_zip(archive, dest, password)
        if name.endswith(".7z"):
            if not any(shutil.which(tool) for tool in ("7z", "unar", "bsdtar")):
                raise RuntimeError(
                    "解压 .7z 需要系统 7z 命令（p7zip-full）或 unar / bsdtar"
                )
            return self._extract_external(archive, dest, password)
        if tarfile.is_tarfile(archive):
            return self._extract_tar(archive, dest)
        if name.endswith(".gz"):
            return self._extract_gz(archive, dest)
        return self._extract_external(archive, dest, password)

    def _extract_zip(self, archive: Path, dest: Path, password: str) -> int:
        """标准库解压 ZIP，AES 加密包回退到 pyzipper。"""
        with zipfile.ZipFile(archive) as archive_file:
            members = [info for info in archive_file.infolist() if not info.is_dir()]
            self._check_members([m.filename for m in members],
                                [m.file_size for m in members])
            for info in members:
                self._assert_safe_path(dest, info.filename)
            try:
                archive_file.extractall(dest, pwd=password.encode("utf-8") if password else None)
            except (NotImplementedError, RuntimeError) as err:
                if not password and "encrypt" in str(err).lower():
                    raise RuntimeError("该 ZIP 已加密，请在配置页填写解压密码") from err
                if "compression" in str(err).lower() or "aes" in str(err).lower() \
                        or isinstance(err, NotImplementedError):
                    return self._extract_aes_zip(archive, dest, password)
                raise
        return len(members)

    def _extract_aes_zip(self, archive: Path, dest: Path, password: str) -> int:
        """用 pyzipper 解压 AES 加密 ZIP。"""
        try:
            import pyzipper
        except ImportError as err:
            raise RuntimeError("该 ZIP 使用 AES 加密，需要安装 pyzipper 依赖") from err
        if not password:
            raise RuntimeError("该 ZIP 使用 AES 加密，请在配置页填写解压密码")
        with pyzipper.AESZipFile(archive) as archive_file:
            members = [info for info in archive_file.infolist() if not info.is_dir()]
            self._check_members([m.filename for m in members],
                                [m.file_size for m in members])
            for info in members:
                self._assert_safe_path(dest, info.filename)
            archive_file.setpassword(password.encode("utf-8"))
            archive_file.extractall(dest)
        return len(members)

    def _extract_tar(self, archive: Path, dest: Path) -> int:
        """解压 tar / tar.gz / tar.bz2 / tar.xz。"""
        with tarfile.open(archive, "r:*") as tar:
            members = tar.getmembers()
            for member in members:
                self._assert_safe_path(dest, member.name)
                if member.issym() or member.islnk():
                    raise RuntimeError(f"包内包含符号链接，已拒绝解压：{member.name}")
            files = [m for m in members if m.isfile()]
            self._check_members([m.name for m in files], [m.size for m in files])
            try:
                tar.extractall(dest, filter="data")
            except TypeError:  # 兼容不带 filter 参数的旧版 Python
                tar.extractall(dest)
        return len(files)

    @staticmethod
    def _extract_gz(archive: Path, dest: Path) -> int:
        """解压单文件 .gz（非 tar）。"""
        target = dest / (_safe_name(archive.stem) or "output.bin")
        with gzip.open(archive, "rb") as source, open(target, "wb") as output:
            shutil.copyfileobj(source, output, length=1024 * 512)
        return 1

    def _extract_external(self, archive: Path, dest: Path, password: str) -> int:
        """调用系统工具解压 RAR / 分卷包 / 其它格式。"""
        dest.mkdir(parents=True, exist_ok=True)
        password = password or ""
        commands: List[List[str]] = []
        if shutil.which("7z"):
            command = ["7z", "x", "-y", "-bso0", "-bsp0"]
            if password:
                command.append(f"-p{password}")
            commands.append(command + [f"-o{dest.as_posix()}", archive.as_posix()])
        if shutil.which("unar"):
            command = ["unar", "-quiet", "-force-overwrite", "-output-directory", dest.as_posix()]
            if password:
                command += ["-password", password]
            commands.append(command + [archive.as_posix()])
        if shutil.which("unrar"):
            command = ["unrar", "x", "-o+", "-idq"]
            if password:
                command.append(f"-p{password}")
            commands.append(command + [archive.as_posix(), f"{dest.as_posix()}/"])
        if shutil.which("bsdtar"):
            commands.append(["bsdtar", "-xf", archive.as_posix(), "-C", dest.as_posix()])
        if not commands:
            raise RuntimeError(
                "未找到可用的解压工具（unar/unrar/7z/bsdtar），无法处理 RAR / 7z / 分卷包"
            )
        errors: List[str] = []
        for command in commands:
            try:
                result = subprocess.run(
                    command, check=False, text=True, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, timeout=900,
                )
                if result.returncode == 0:
                    self._validate_extracted(dest)
                    return self._count_files(dest)
                errors.append(f"{command[0]}: {(result.stdout or '')[-300:]}")
            except Exception as err:  # noqa: BLE001
                errors.append(f"{command[0]}: {err}")
        raise RuntimeError("解压失败：" + " | ".join(errors))

    # ------------------------------------------------------------------
    # 安全校验
    # ------------------------------------------------------------------
    @staticmethod
    def _assert_safe_path(dest: Path, name: str) -> None:
        """拒绝绝对路径、盘符路径与路径穿越（Zip Slip）。"""
        if not name or name.startswith(("/", "\\")) or "\x00" in name \
                or re.match(r"^[A-Za-z]:", name):
            raise RuntimeError(f"包内包含非法路径：{name}")
        root = dest.resolve()
        target = (dest / name).resolve()
        if target != root and root not in target.parents:
            raise RuntimeError(f"包内包含越界路径：{name}")

    def _check_members(self, names: List[str], sizes: List[Any]) -> None:
        """校验包内文件数量与解压后总体积，防御解压炸弹。"""
        if len(names) > 20000:
            raise RuntimeError(f"包内文件数过多（{len(names)}），已拒绝解压")
        limit = self._cfg_int("max_extract_mb", 4096, 1) * 1024 * 1024
        total = sum(int(size or 0) for size in sizes)
        if total > limit:
            raise RuntimeError(
                f"解压后体积 {_human_size(total)} 超过上限 {_human_size(limit)}，已拒绝解压"
            )

    def _validate_extracted(self, dest: Path) -> None:
        """外部命令解压后的兜底校验（无法预读包内清单）。"""
        files = [p for p in dest.rglob("*") if p.is_file()]
        limit = self._cfg_int("max_extract_mb", 4096, 1) * 1024 * 1024
        total = sum(p.stat().st_size for p in files)
        if len(files) > 20000 or total > limit:
            raise RuntimeError(
                f"解压结果异常（{len(files)} 个文件 / {_human_size(total)}），已丢弃本次解压"
            )

    @staticmethod
    def _count_files(dest: Path) -> int:
        """统计目录内的文件数。"""
        return len([p for p in dest.rglob("*") if p.is_file()])
