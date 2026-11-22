# -*- coding: utf-8 -*-
"""
TG 机器人压缩包提取插件（MoviePilot V3）

作者：gldl137
仓库：MoviePilot-Plugins（https://github.com/gldl137/MoviePilot-Plugins）

功能：
    - 基于 Telegram Bot API（只需 @BotFather 提供的 Token，无需开发者账号）：
      把群组/频道里的压缩包转发给机器人，机器人收到后立刻下载压缩包、
      解压到「下载目录下的 completed 子目录」（所有文件平铺，不保留包内文件夹结构），
      再删除压缩包（分卷包在所有分卷到齐并解压成功后统一删除）。
    - 支持 zip / tar / gz / 7z / rar 及常见分卷包，内置路径穿越与解压炸弹防护。
    - 记录已处理消息、失败重试、历史记录与通知，提供详情页日志和手动触发接口。

实现约定：
    - 只使用 V3 稳定入口（app.sdk.*），基类入口保留 app.plugins 兼容回退。
    - 采用 Telegram Bot API 长轮询（getUpdates timeout=30）：机器人挂在 Telegram 门口等，
      有转发消息立即返回并处理，无消息时连接挂起，不需要公网 HTTPS 地址。
    - 支持 http / socks5 代理（可手填或用 MoviePilot 系统代理），代理同时用于 API 与下载。
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

try:
    # V3 稳定入口（与已上线插件 TgMediaCenter 一致）；顶层只依赖 app.sdk.*
    from app.sdk.plugin import _PluginBase
except ImportError:  # 兼容仅提供旧公开入口的宿主
    from app.sdk.plugin.base import _PluginBase  # type: ignore

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
MIN_INTERVAL = 1
LONG_POLL_TIMEOUT = 30

DEFAULT_EXTS = (
    ".zip,.rar,.7z,.tar,.gz,.tgz,.tar.gz,.bz2,.tbz2,.xz,.txz,"
    ".001,.part1.rar,.z01,.r00,.cas"
)

CONFLICT_ACTIONS = {
    "skip": "跳过已有",
    "overwrite": "覆盖已有",
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


def _environment() -> Tuple[List[str], str]:
    """检查可选依赖与系统解压工具，返回 (缺失项, 提示文本)。"""
    missing: List[str] = []
    tips: List[str] = []
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
    """通过 Telegram 机器人接收转发的压缩包并自动下载解压。"""

    # ---- 插件元信息（必须写成字面量：宿主会静态解析类属性做加载校验） ----
    plugin_name = "TG机器人压缩包提取"
    plugin_desc = "把群里转发的压缩包交给 Telegram 机器人下载，解压文件平铺到下载目录下的 completed 子目录；同名可跳过/覆盖，解压后可删除原包，配置极简。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.5.6"
    plugin_author = "gldl137"
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "tgarchivegrabber_"
    plugin_order = 40
    auth_level = 1

    _default_config: Dict[str, Any] = {
        "enabled": False,
        "proxy": "",
        "bot_token": "",
        "bot_api_base": "https://api.telegram.org",
        "chat_id": "",
        "download_dir": "/downloads/tgarchives",
        "keywords": "",
        "exclude_keywords": "",
        "senders": "",
        "min_size_mb": 0,
        "max_size_mb": 0,
        "max_files_per_round": 20,
        "interval": 1,
        "backfill": False,
        "backfill_count": 20,
        "extract_password": "",
        "conflict": "skip",
        "delete_archive": True,
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
        self._skip_notified = False
        self._last_scan = ""
        self._last_result = ""
        self._pending_parts: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def init_plugin(self, config: Optional[dict] = None) -> None:
        """读取配置并启动守护线程，允许重复调用（配置保存/插件重载）。"""
        self.stop_service()
        self._stop_event.clear()
        self._config = {**self._default_config, **(config or {})}
        self._enabled = bool(self._config.get("enabled"))
        self._pending_parts = self.get_data("pending_parts") or {}
        if not isinstance(self._pending_parts, dict):
            self._pending_parts = {}
        self._log(
            f"初始化完成：启用={self._enabled}，长轮询模式（转发即时处理），"
            f"代理={self._proxy_url() or '未设置'}"
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
        """返回 Vuetify 配置页：只保留机器人转发解压的必需项。"""
        cfgs = [
            _alert(
                "把压缩包转发给机器人即可：实时接收 → 自动下载解压到「下载目录/completed」"
                "（平铺、不保留包内文件夹）→ 按设置处理原包。只需 @BotFather 的 Bot Token。"
                "单文件限 20MB，超限请拆分分卷或自建 Bot API Server。"
            ),
            _row(
                _sw("enabled", "启用插件", md=12),
            ),
            _row(
                _tf("bot_token", "Bot Token", "从 @BotFather 获取", "", "text", md=6),
                _tf("chat_id", "Chat ID（必填）",
                    "只处理这个用户转发的压缩包；给机器人发条消息后点「测试连接」可自动获取",
                    "", "text", md=6),
            ),
            _row(
                _tf("proxy", "代理地址",
                    "访问 Telegram 用；留空则用 MoviePilot 系统代理。支持 http / socks5",
                    "socks5://127.0.0.1:7890", md=12),
            ),
            _row(
                _tf("download_dir", "下载目录",
                    "容器内绝对路径；解压文件固定输出到该目录下的 completed 子目录",
                    "/downloads/tgarchives", md=12),
            ),
            _row(
                _sel("conflict", "同名文件", CONFLICT_ACTIONS, "", md=4),
                _sw("delete_archive", "解压后删除压缩包", "解压成功后删除原包（分卷包待全部到齐解压后统一删除）", md=4),
            ),
        ]
        return [{"component": "VForm", "content": cfgs}], {**self._default_config, **self._config}

    # ------------------------------------------------------------------
    # 详情页
    # ------------------------------------------------------------------
    def get_page(self) -> List[dict]:
        """只展示处理记录列表。"""
        history_rows: List[Dict[str, Any]] = []
        for it in self._load_history():
            status = str(it.get("status", ""))
            color = "success" if status == "成功" else ("error" if status == "失败" else "grey")
            history_rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "props": {"width": "190px", "class": "text-no-wrap"},
                     "text": str(it.get("time", ""))},
                    {"component": "td", "text": str(it.get("chat", ""))},
                    {"component": "td", "text": str(it.get("filename", ""))},
                    {"component": "td", "props": {"width": "90px"}, "content": [
                        {"component": "VChip", "props": {"color": color, "size": "small"},
                         "text": status or "未知"},
                    ]},
                    {"component": "td", "props": {"width": "80px"},
                     "text": str(it.get("size_text", ""))},
                    {"component": "td", "text": str(it.get("message", ""))},
                ],
            })
        if not history_rows:
            history_rows = [{
                "component": "tr",
                "content": [{"component": "td", "props": {"colspan": 6, "class": "text-center"},
                             "text": "暂无处理记录（转发压缩包并解压成功后会记录在此）"}],
            }]

        return [
            {"component": "VCard", "props": {"variant": "outlined"}, "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "处理记录"},
                {"component": "VCardText",
                 "text": f"共 {len(history_rows)} 条（最多保留 {MAX_HISTORY} 条，最新在上）。"},
            ]},
            {"component": "VCard", "props": {"variant": "outlined", "class": "mt-4"}, "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "记录列表"},
                {"component": "VCardText", "content": [{
                    "component": "VTable",
                    "props": {"density": "compact", "hover": True,
                              "fixed-header": True, "height": "600px"},
                    "content": [
                        {"component": "thead", "content": [
                            {"component": "tr", "content": [
                                {"component": "th", "props": {"width": "190px", "class": "text-no-wrap"},
                                 "text": "时间"},
                                {"component": "th", "text": "来源"},
                                {"component": "th", "text": "压缩包"},
                                {"component": "th", "props": {"width": "90px"}, "text": "状态"},
                                {"component": "th", "props": {"width": "80px"}, "text": "大小"},
                                {"component": "th", "text": "说明"},
                            ]}
                        ]},
                        {"component": "tbody", "content": history_rows},
                    ],
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
             "auth": "bear", "summary": "测试 Bot Token 连接"},
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
            f"机器人私聊转发：固定开启（转发给机器人的压缩包全部处理）",
            f"代理：{self._proxy_url() or '未设置'}",
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

    def _exts(self) -> Tuple[str, ...]:
        """返回允许处理的压缩包后缀元组（写死在代码中，配置页不再开放）。"""
        return tuple(x.lower() if x.startswith(".") else "." + x.lower()
                     for x in _split_list(DEFAULT_EXTS))

    def _download_dir(self) -> Path:
        """压缩包下载目录。"""
        return Path(self._cfg_str("download_dir") or "/downloads/tgarchives")

    def _extract_dir(self) -> Path:
        """解压输出目录：固定在下载目录下的 completed 子目录。"""
        return self._download_dir() / "completed"

    def _validate(self) -> Optional[str]:
        """校验运行必需的凭据与目录，返回错误说明或 None。"""
        if not self._cfg_str("bot_token"):
            return "需要填写 Bot Token（从 @BotFather 获取）"
        if not self._cfg_str("chat_id"):
            return "需要填写 Chat ID（只处理该用户转发的压缩包）"
        if not self._cfg_str("download_dir"):
            return "需要填写压缩包保存目录"
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
        """长轮询循环：getUpdates 挂起等待新消息，收到立即处理；异常退避降噪后重试。"""
        current = threading.current_thread()
        fail_count = 0
        while not self._stop_event.is_set() and self._thread is current:
            try:
                if self._scan_once():
                    fail_count = 0
                else:
                    # 上一轮扫描尚未结束（锁被占用），短暂等待后再试，避免空转刷屏
                    self._trigger.wait(timeout=1)
            except Exception as err:  # noqa: BLE001
                fail_count += 1
                # 连续失败只记第 1 次和之后每 10 次，避免网络抖动刷屏
                if fail_count == 1 or fail_count % 10 == 0:
                    self._log(f"连接异常（连续 {fail_count} 次）：{err}", "error")
                self._trigger.wait(timeout=3)
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

    def _scan_once(self) -> bool:
        """执行一轮完整扫描：拉取新消息 → 下载 → 解压 → 保存游标。

        返回 True 表示本轮真正执行了扫描；返回 False 表示上一轮扫描尚未结束而跳过。
        跳过告警每个竞争周期只记录一次，避免后台线程空转刷屏。
        """
        if not self._scan_lock.acquire(blocking=False):
            if not self._skip_notified:
                self._skip_notified = True
                self._log("上一轮扫描尚未结束，跳过本轮", "warning")
            return False
        self._skip_notified = False
        self._scanning = True
        cursors: Dict[str, int] = {}
        processed: set = set()
        stats: Dict[str, int] = {"handled": 0, "ok": 0, "fail": 0, "skipped": 0}
        try:
            problem = self._validate()
            if problem:
                self._log(f"跳过扫描：{problem}", "warning")
                return False
            self._download_dir().mkdir(parents=True, exist_ok=True)
            self._extract_dir().mkdir(parents=True, exist_ok=True)
            cursors = self._load_cursors()
            processed = self._load_processed()
            failures = self._load_failures()
            self._last_scan = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                cursors = self._scan_bot(cursors, processed, failures, stats)
            finally:
                self._save_cursors(cursors)
                self._save_processed(processed)
                trimmed = dict(list(failures.items())[-MAX_FAILURES:])
                self.save_data("failures", trimmed)
                self._flush_pending_parts()
            # 空轮不写日志不通知，避免长轮询循环刷屏；只有实际处理了压缩包才记录
            if stats["handled"] or stats["skipped"]:
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
            return True
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

    def _requests_proxies(self) -> Optional[Dict[str, str]]:
        """把代理 URL 转成 requests 的 proxies 字典；socks5 缺依赖时给出提示。"""
        raw = self._proxy_url()
        if not raw:
            return None
        if raw.lower().startswith("socks"):
            try:
                import socks  # noqa: F401
            except ImportError:
                self._log(
                    "socks5 代理缺少依赖 PySocks（pip install requests[socks]），"
                    "代理可能不生效，请改用 http 代理或安装依赖", "warning"
                )
        return {"http": raw, "https": raw}

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

    def _bot_reply(self, chat_id: Any, text: str) -> None:
        """给转发者回复处理结果；发送失败只记日志，不影响主流程。"""
        if not chat_id:
            return
        try:
            # 不设置 parse_mode，纯文本发送，避免文件名里的 _ * [ 等字符导致解析失败
            self._bot_api("sendMessage", {"chat_id": chat_id, "text": text}, timeout=15)
        except Exception as err:  # noqa: BLE001
            self._log(f"回复 Telegram 消息失败：{err}", "warning")

    def _bot_chat_matches(self, post: Dict[str, Any]) -> bool:
        """只处理指定 Chat ID 用户私聊转发的压缩包。"""
        chat = post.get("chat") or {}
        if str(chat.get("type") or "").lower() != "private":
            return False
        allowed = self._cfg_str("chat_id").strip().lstrip("@").lower()
        identifiers = {str(chat.get("id") or ""),
                       str(chat.get("username") or "").lower().lstrip("@")}
        return allowed in identifiers

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
        reply_chat_id = chat_info.get("id")
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
                    "请把压缩包拆成小于 20MB 的分卷转发，或自建 Bot API Server）"
                ) from err
            file_path = str((info or {}).get("file_path") or "")
            if not file_path:
                raise RuntimeError("getFile 未返回 file_path")
            # 官方云 API 返回相对路径；自建 Bot API Server 返回以 / 开头的绝对路径
            separator = "" if file_path.startswith("/") else "/"
            url = f"{base}/file/bot{token}{separator}{file_path}"
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
            "reply_chat_id": reply_chat_id,
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
            "timeout": LONG_POLL_TIMEOUT,
            "allowed_updates": json.dumps(["message"]),
        }
        # 长轮询：无消息时 Telegram 会挂住连接直到超时，有消息立刻返回
        updates = self._bot_api("getUpdates", params,
                                timeout=LONG_POLL_TIMEOUT + 10) or []
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
        """测试 Bot Token 是否有效，结果写入详情页日志。"""
        details: List[str] = []
        try:
            me = self._bot_api("getMe", {}) or {}
            details.append(f"Bot：@{me.get('username', '未知')} ({me.get('first_name', '')})")
            details.append("连接正常。把群里的压缩包转发给这个机器人即可自动下载解压。")
            # 列出最近私聊的 Chat ID，方便用户填写白名单
            try:
                updates = self._bot_api("getUpdates", {"limit": 100}) or []
                for update in updates:
                    post = update.get("message") or {}
                    chat = post.get("chat") or {}
                    if str(chat.get("type") or "").lower() == "private":
                        details.append(
                            f"你的 Chat ID：{chat.get('id')} "
                            f"(@{chat.get('username') or '无用户名'}) —— 复制这个数字填入配置"
                        )
                        break
            except Exception:  # noqa: BLE001 - 拉取失败不影响连接测试
                pass
            return self._result(True, "Bot 连接测试完成", details=details)
        except Exception as err:  # noqa: BLE001
            return self._result(False, f"Bot 连接失败：{err}", details=details)

    # ------------------------------------------------------------------
    # 消息过滤与处理
    # ------------------------------------------------------------------
    def _match_archive(self, item: Dict[str, Any]) -> bool:
        """转发的附件只要是配置的压缩包后缀就处理，不做其它过滤。"""
        filename = str(item.get("filename") or "")
        return filename.lower().endswith(self._exts())

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
                self._bot_reply(
                    item.get("reply_chat_id"),
                    f"📥 已收到 {filename}（{_human_size(item.get('size'))}），开始处理…"
                )
                item["download"](partial)
                if not partial.exists() or partial.stat().st_size == 0:
                    raise RuntimeError("下载完成但文件为空")
                if target.exists():
                    target.unlink()
                partial.replace(target)
            else:
                self._log(f"复用已下载文件 {target.name}")
            if filename.lower().endswith(".cas"):
                self._move_cas(target, item)
            else:
                self._handle_archive(target, stem, is_part, is_main, item)
            processed.add(key)
            failures.pop(key, None)
            stats["ok"] += 1
            return True
        except Exception as err:  # noqa: BLE001
            count = int(failures.get(key, 0)) + 1
            failures[key] = count
            self._log(f"处理失败 {filename}（第 {count} 次）：{err}", "error")
            self._bot_reply(
                item.get("reply_chat_id"),
                f"⚠️ {filename} 处理失败：{err}"
                + (f"（第 {count}/{DEFAULT_RETRY} 次，将自动重试）" if count < DEFAULT_RETRY else "")
            )
            try:
                partial = target_dir / (filename + ".part")
                if partial.exists():
                    partial.unlink()
            except Exception:  # noqa: BLE001
                pass
            if count >= DEFAULT_RETRY:
                self._bot_reply(
                    item.get("reply_chat_id"),
                    f"❌ {filename} 重试 {count} 次仍失败，已放弃：{err}"
                )
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
            "reply_chat_id": item.get("reply_chat_id"),
        }
        entry["dir"] = str(archive.parent)
        entry["chat"] = item.get("chat", entry.get("chat", ""))
        if not entry.get("reply_chat_id"):
            entry["reply_chat_id"] = item.get("reply_chat_id")
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
        self._bot_reply(
            item.get("reply_chat_id"),
            f"📥 已收到分卷 {archive.name}（同组共 {len(parts)} 个），"
            f"等待其余分卷到齐后统一解压…"
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
                    self._bot_reply(
                        entry.get("reply_chat_id"),
                        f"⏰ 分卷包 {stem} 等待 1 小时仍未收到主卷，已取消"
                    )
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
                "reply_chat_id": entry.get("reply_chat_id"),
                "filename": main.name,
                "size": main.stat().st_size,
            }
            try:
                self._log(f"分卷组 {stem} 等待结束，开始解压主卷 {main.name}")
                self._extract_one(main, stem, item)
                if self._cfg_bool("delete_archive", True):
                    # _extract_one 已删除主卷；这里清掉同组其余分卷
                    group_prefix = _safe_name(stem).lower() + "."
                    for part in parts_dir.iterdir():
                        if part.is_file() and part.name.lower().startswith(group_prefix):
                            try:
                                part.unlink()
                            except OSError:
                                pass
            except Exception as err:  # noqa: BLE001
                self._log(f"分卷组 {stem} 解压失败：{err}", "error")
                self._bot_reply(
                    item.get("reply_chat_id"),
                    f"❌ 分卷包 {main.name} 解压失败：{err}"
                )
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

    def _move_cas(self, archive: Path, item: Dict[str, Any]) -> None:
        """.cas 秒传文件不解压，直接移动到「下载目录/completed」。"""
        final = self._extract_dir()
        final.mkdir(parents=True, exist_ok=True)
        target = final / archive.name
        conflict = self._cfg_str("conflict", "skip") or "skip"
        if target.exists() and conflict != "overwrite":
            self._log(f"completed 已存在 {target.name}，跳过移动")
            self._record(item, "成功", f"completed 已存在同名文件，跳过 → {final}")
            self._bot_reply(
                item.get("reply_chat_id"),
                f"⏭️ {archive.name} completed 已存在同名文件，已跳过"
            )
            if self._cfg_bool("delete_archive", True):
                try:
                    archive.unlink()
                    self._log(f"已删除源文件 {archive.name}")
                except OSError as err:
                    self._log(f"删除源文件失败：{err}", "warning")
            return
        if target.exists():
            try:
                target.unlink()
            except OSError as err:
                raise RuntimeError(f"覆盖同名文件失败 {target.name}：{err}") from err
        shutil.move(str(archive), str(target))
        self._log(f"已移动 {archive.name} → {final}")
        self._record(item, "成功", f"已移动到 completed → {final}")
        self._bot_reply(
            item.get("reply_chat_id"),
            f"✅ {archive.name} 已移动到 {final}"
        )

    def _extract_one(self, archive: Path, stem: str, item: Dict[str, Any]) -> None:
        """解压到「下载目录/completed」：所有文件平铺、不建子目录；成功后删除压缩包。"""
        final = self._extract_dir()
        final.mkdir(parents=True, exist_ok=True)
        # 临时目录放在下载目录而非 completed，避免临时文件混入结果目录
        temp = self._download_dir() / f".tg_tmp_{_safe_name(stem)}_{int(time.time() * 1000)}"
        temp.mkdir(parents=True, exist_ok=True)
        try:
            count = self._extract_archive(archive, temp, self._cfg_str("extract_password"))
            kept, removed = self._apply_inner_filter(temp)
            conflict = self._cfg_str("conflict", "skip") or "skip"
            if conflict not in CONFLICT_ACTIONS:
                conflict = "skip"
            written, skipped = self._flatten_files(temp, final, conflict)
            size_text = _human_size(item.get("size"))
            self._log(
                f"解压成功 {archive.name}（{size_text}）→ {final}，"
                f"包内文件 {count} 个，保留 {kept} 个，落盘 {written} 个"
                + (f"，跳过同名 {skipped} 个" if skipped else "")
                + (f"，过滤 {removed} 个" if removed else "")
            )
            self._record(item, "成功", f"平铺解压 {written} 个文件 → {final}"
                         + (f"，跳过同名 {skipped} 个" if skipped else ""))
            self._bot_reply(
                item.get("reply_chat_id"),
                f"✅ {archive.name} 处理完成：解压 {written} 个文件 → {final}"
                + (f"（跳过同名 {skipped} 个）" if skipped else "")
            )
            if self._cfg_bool("delete_archive", True):
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
    def _flatten_files(temp: Path, final: Path, conflict: str) -> Tuple[int, int]:
        """
        把临时目录内的所有文件平铺移动到最终目录：不保留任何子目录层级，只写文件。
        同名文件按 conflict 处理：skip 跳过、overwrite 覆盖。返回 (落盘数, 跳过数)。
        """
        written = 0
        skipped = 0
        for source in sorted(p for p in temp.rglob("*") if p.is_file()):
            target = final / _safe_name(source.name)
            if target.exists():
                if conflict == "skip":
                    skipped += 1
                    continue
                try:
                    target.unlink()
                except OSError as err:
                    raise RuntimeError(f"覆盖同名文件失败 {target.name}：{err}") from err
            shutil.move(str(source), str(target))
            written += 1
        return written, skipped

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
