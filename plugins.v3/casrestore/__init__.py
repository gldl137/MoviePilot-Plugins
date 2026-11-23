"""
CAS恢复源文件（CasRestore）

监控目录中的 .cas 秒传文件（watchdog 事件驱动，新文件写入完成即自动恢复，不轮询），
调用 CAS 服务的 /api/cas-restore/restore 接口，在网盘直接恢复源文件。

特点：
1. CAS 服务地址 / API Key 只从 CAS助手（casassistant）插件读取，本插件不填写：
   from app.sdk.plugins import PluginManager
   cfg = PluginManager().run_plugin_method("casassistant", "get_cas_config")
   # cfg = {"enabled": bool, "host": str, "api_key": str}
2. 只处理 .cas 后缀文件；watchdog 不可用时回退 30 秒轮询兜底。
3. 网盘目录跟随本地结构：目标文件夹路径 + 本地相对子目录（manual 模式，CAS 自动建目录），
   恢复成功删除 .cas 后自动清空本地空目录（边界为监控目录本身，不向上越界）。
4. 请求一次只提交一条 .cas（files 数组仅一个元素）。

请求体（与 CAS 抓包一致）：
{
    "accountId": 3,
    "scope": "family",
    "familyId": "575994414",
    "directoryMode": "manual",
    "targetFolderId": "",
    "targetFolderPath": "家庭1/测试/子目录",
    "files": [{"localFileName": "xxx.cas", "content": "<cas 文件内容的 base64>"}]
}
"""
import base64
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.sdk.logging import logger

try:  # HTTP 请求
    import requests
except ImportError:  # 宿主通常自带 requests
    requests = None  # type: ignore

try:  # 目录事件监控（watchdog，MoviePilot 自带依赖）
    from watchdog.observers import Observer as _WD_Observer
    _WATCHDOG_OK = True
except ImportError:
    _WD_Observer = None  # type: ignore
    _WATCHDOG_OK = False

try:  # 读取 CAS助手 插件配置
    from app.sdk.plugins import PluginManager
except ImportError:  # 兼容不同宿主版本
    PluginManager = None  # type: ignore

try:  # 新版宿主
    from app.sdk.plugin.base import _PluginBase
except ImportError:
    try:
        from app.sdk.plugin import _PluginBase
    except ImportError:  # 兼容旧版宿主
        from app.sdk.plugin.base import _PluginBase  # type: ignore

try:
    from app.schemas.types import NotificationType
except ImportError:
    try:
        from app.schemas import NotificationType  # type: ignore
    except ImportError:
        NotificationType = None  # type: ignore

try:
    from app.sdk.config import settings
except ImportError:
    settings = None  # type: ignore

__all__ = ["CasRestore"]

LOG_TAG = "[CAS恢复]"
MAX_LOG_LINES = 300
MAX_PROCESSED = 5000
DEFAULT_RETRY = 3
POLL_FALLBACK_SECONDS = 30
RESTORE_TIMEOUT = 120

SCOPE_ACTIONS = {"family": "家庭空间", "personal": "个人空间"}

try:  # 内网自签证书时禁用告警
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:  # noqa: BLE001
    pass


class _CasEventHandler:
    """watchdog 事件处理器（duck-typing）：只响应文件写入完成（closed）与移入（moved）。"""

    def __init__(self, plugin: "CasRestore") -> None:
        self._plugin = plugin

    def on_closed(self, event: Any) -> None:
        if not event.is_directory:
            self._plugin._on_fs_event(event.src_path)

    def on_moved(self, event: Any) -> None:
        if not event.is_directory:
            self._plugin._on_fs_event(event.dest_path)


# ---------------- 配置页组件辅助 ----------------

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


def _sel_list(model: str, label: str, items: List[Dict[str, Any]],
              hint: str = "", md: int = 4) -> Dict[str, Any]:
    """构造一个下拉框（动态 {label, value} 列表自动转为 Vuetify 的 title/value）。"""
    normalized = [{"title": str(i.get("title") or i.get("label") or i.get("value", "")),
                   "value": i.get("value")}
                  for i in items if isinstance(i, dict)]
    props: Dict[str, Any] = {"model": model, "label": label, "items": normalized}
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


class CasRestore(_PluginBase):
    # 插件元信息
    plugin_name = "CAS恢复源文件"
    plugin_desc = ("扫描监控目录中的 .cas 秒传文件，调用 CAS 服务接口直接在网盘恢复源文件；"
                   "服务地址与 API Key 自动读取 CAS助手 插件配置，未配置时回退本插件填写。")
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.3.2"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "casrestore_"
    plugin_order = 29
    auth_level = 1

    _default_config = {
        "enabled": False,
        "watch_dir": "",
        "account_id": "",
        "scope": "family",
        "family_id": "",
        "target_folder_path": "",
        "notify": True,
        "onlyonce": False,
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

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def init_plugin(self, config: Optional[dict] = None) -> None:
        """读取配置并启动守护线程，允许重复调用（配置保存/插件重载）。"""
        self.stop_service()
        self._stop_event.clear()
        self._config = {**self._default_config, **(config or {})}
        # 兼容 v1.2.0 下拉 bug 存入的脏数据（整个选项对象或 [object Object] 字符串）
        for key in ("account_id", "family_id"):
            val = self._config.get(key)
            if isinstance(val, dict):
                val = val.get("value")
            text = str(val or "").strip()
            if text in ("[object Object]", "None", "undefined"):
                text = ""
            self._config[key] = text
        self._enabled = bool(self._config.get("enabled"))
        mode = "watchdog 事件监控" if _WATCHDOG_OK else "轮询兜底"
        self._log(f"初始化完成：启用={self._enabled}，监控方式={mode}，"
                  f"监控目录={self._cfg_str('watch_dir') or '未设置'}")
        if self._enabled:
            problem = self._validate()
            if problem:
                self._log(f"配置不完整，线程未启动：{problem}", "warning")
            else:
                self._ensure_worker()
        if self._config.get("onlyonce"):
            threading.Thread(target=self._handle_onlyonce, daemon=True,
                             name="casrestore-once").start()

    def get_state(self) -> bool:
        """插件是否启用。"""
        return self._enabled

    def stop_service(self) -> None:
        """停止守护线程，可重复调用。"""
        self._enabled = False
        self._stop_event.set()
        self._trigger.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
        with self._state_lock:
            self._thread = None
        self._trigger.clear()

    # ------------------------------------------------------------------
    # 宿主基类要求
    # ------------------------------------------------------------------
    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """不注册远程命令，避免不同宿主事件枚举差异导致加载失败。"""
        return []

    @staticmethod
    def get_service() -> List[Dict[str, Any]]:
        """不向宿主注册 cron 服务（部分宿主 APScheduler 版本不兼容 cron=），用内部线程轮询。"""
        return []

    # ------------------------------------------------------------------
    # 配置页
    # ------------------------------------------------------------------
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回 Vuetify 配置页；账号/家庭选项实时从 CAS 服务拉取供直接选择。"""
        accounts, account_err = self._fetch_accounts()
        account_items: List[Dict[str, Any]] = accounts
        # 当前已保存的值不在选项里时也展示出来，避免打开配置页看着是空的
        saved_account = self._cfg_str("account_id")
        if saved_account and all(i["value"] != saved_account for i in account_items):
            if saved_account.isdigit():
                # 真实 ID 但不在列表里：兜底展示，避免打开配置页看着是空的
                account_items = account_items + [{"label": f"ID {saved_account}（已保存）",
                                                  "value": saved_account}]
            else:
                # 非 ID 形态的脏值（如 v1.2.0 的 [object Object]）：直接清空
                saved_account = ""
                self._config["account_id"] = ""
        if not saved_account and account_items:
            # 与源项目一致：默认选中第一个账号
            saved_account = account_items[0]["value"]
            self._config["account_id"] = saved_account
        account_hint = (f"共 {len(accounts)} 个账号（实时读取 CAS 服务）"
                        if not account_err else f"拉取失败：{account_err}")

        family_items: List[Dict[str, Any]] = []
        family_hint = ("恢复位置为「个人空间」时无需选择家庭"
                       if self._cfg_str("scope") != "family"
                       else "先选择账号后再拉取家庭列表")
        if saved_account and self._cfg_str("scope") == "family":
            families, family_err = self._fetch_families(saved_account)
            family_items = families
            saved_family = self._cfg_str("family_id")
            if saved_family and all(i["value"] != saved_family for i in family_items):
                family_items = family_items + [{"label": f"ID {saved_family}（已保存）",
                                                "value": saved_family}]
            family_hint = (f"共 {len(families)} 个家庭（实时读取 CAS 服务）"
                           if not family_err else f"拉取失败：{family_err}")

        cfgs = [
            _alert(
                "监控目录有新 .cas 文件写入完成时自动恢复（watchdog 事件驱动，不轮询），"
                "调用 CAS 服务接口直接在网盘恢复源文件。CAS 服务地址与 API Key 直接读取"
                "「CAS助手」插件配置；账号/家庭直接从 CAS 服务下拉选择。网盘目录自动跟随"
                "本地子目录结构，恢复成功后删除 .cas 并清空本地空目录。每次请求只提交一条 .cas。"
            ),
            _row(
                _sw("enabled", "启用插件", md=12),
            ),
            _row(
                _tf("watch_dir", "监控目录",
                    "监控该目录（含子目录）下新增的 .cas 文件；容器内绝对路径",
                    "/downloads/tgcas", md=12),
            ),
            _row(
                _sel_list("account_id", "恢复账号", account_items,
                          hint=account_hint, md=4),
                _sel("scope", "恢复位置", SCOPE_ACTIONS, "", md=4),
                _sel_list("family_id", "家庭列表", family_items,
                          hint=family_hint, md=4),
            ),
            _row(
                _tf("target_folder_path", "目标文件夹路径",
                    "网盘基路径（手动输入）；本地子目录会自动拼接到其后（如 家庭1/测试）",
                    "家庭1/测试", md=12),
            ),
            _row(
                _sw("notify", "发送通知", "有恢复成功时发送 Message 通知", md=4),
                _sw("onlyonce", "立即运行一次", "保存后立即扫描一轮存量 .cas 并自动复位", md=4),
            ),
        ]
        return [{"component": "VForm", "content": cfgs}], {**self._default_config, **self._config}

    # ------------------------------------------------------------------
    # 详情页
    # ------------------------------------------------------------------
    def get_page(self) -> List[dict]:
        """展示恢复记录列表（带操作按钮，参考音乐整理插件的交互式详情页）。"""
        records = self.get_data("restore_records") or []
        if not isinstance(records, list):
            records = []
        success_count = sum(1 for r in records if isinstance(r, dict)
                            and r.get("status") == "成功")
        failed_count = len(records) - success_count

        rows: List[Dict[str, Any]] = []
        for it in records[:100]:
            status = str(it.get("status") or "")
            color = "success" if status == "成功" else "error"
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td",
                     "props": {"width": "170px", "class": "text-no-wrap"},
                     "text": str(it.get("time", ""))},
                    {"component": "td", "text": str(it.get("filename", ""))},
                    {"component": "td", "text": str(it.get("target", ""))},
                    {"component": "td", "props": {"width": "90px"}, "content": [
                        {"component": "VChip", "props": {"size": "small", "color": color},
                         "text": status or "未知"}]},
                    {"component": "td", "text": str(it.get("message", ""))},
                ],
            })
        if not rows:
            rows = [{"component": "tr", "content": [
                {"component": "td", "props": {"colspan": 5, "class": "text-center"},
                 "text": "暂无恢复记录（监控目录出现 .cas 后自动恢复，会记录在此）"}]}]

        apikey = getattr(settings, "API_TOKEN", "") if settings else ""
        buttons = [
            {"component": "VBtn",
             "props": {"color": "primary", "size": "small", "variant": "outlined"},
             "events": {"click": {"type": "request", "api": "plugin/CasRestore/scan",
                                  "method": "POST", "params": {"apikey": apikey},
                                  "success": "扫描已触发", "fail": "触发失败"}},
             "text": "立即扫描"},
            {"component": "VBtn",
             "props": {"color": "info", "size": "small", "variant": "outlined"},
             "events": {"click": {"type": "request", "api": "plugin/CasRestore/test",
                                  "method": "POST", "params": {"apikey": apikey},
                                  "success": "测试已执行", "fail": "测试失败"}},
             "text": "测试连接"},
            {"component": "VBtn",
             "props": {"color": "error", "size": "small", "variant": "outlined"},
             "events": {"click": {"type": "request",
                                  "api": "plugin/CasRestore/clear_records",
                                  "method": "POST", "params": {"apikey": apikey},
                                  "success": "清空成功", "fail": "清空失败"}},
             "text": "清空记录"},
        ]

        return [
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VCard", "props": {"variant": "outlined"}, "content": [
                        {"component": "VCardText", "content": [
                            {"component": "div", "props": {"class": "text-body-2"},
                             "text": f"状态：{'扫描中' if self._scanning else '空闲'}，"
                                     f"启用={self._enabled}，"
                                     f"上次扫描={self._last_scan or '从未'}，"
                                     f"上次结果={self._last_result or '无'}"}
                        ]},
                    ]},
                ]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VCard", "content": [
                        {"component": "VCardTitle",
                         "props": {"class": "d-flex align-center justify-space-between"},
                         "content": [
                            {"component": "div",
                             "text": f"恢复记录（共 {len(records)} 条  成功 {success_count} 条  "
                                     f"失败 {failed_count} 条）"},
                            {"component": "div",
                             "props": {"class": "d-flex align-center gap-2"},
                             "content": buttons},
                        ]},
                        {"component": "VCardText", "content": [
                            {"component": "VTable",
                             "props": {"density": "comfortable", "hover": True,
                                       "fixed-header": True, "height": "600px"},
                             "content": [
                                {"component": "thead", "content": [
                                    {"component": "tr", "content": [
                                        {"component": "th",
                                         "props": {"width": "170px",
                                                  "class": "text-no-wrap"},
                                         "text": "时间"},
                                        {"component": "th", "text": "本地文件"},
                                        {"component": "th", "text": "恢复到网盘"},
                                        {"component": "th",
                                         "props": {"width": "90px"}, "text": "状态"},
                                        {"component": "th", "text": "说明"},
                                    ]},
                                ]},
                                {"component": "tbody", "content": rows},
                             ]},
                        ]},
                    ]},
                ]},
            ]},
        ]

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------
    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/status", "endpoint": self.api_status, "methods": ["GET"],
             "auth": "bear", "summary": "运行状态"},
            {"path": "/scan", "endpoint": self.api_scan, "methods": ["POST"],
             "auth": "bear", "summary": "立即扫描一轮存量 .cas"},
            {"path": "/test", "endpoint": self.api_test, "methods": ["POST"],
             "auth": "bear", "summary": "测试 CAS 服务连接与鉴权"},
            {"path": "/clear_records", "endpoint": self.api_clear_records,
             "methods": ["POST"], "auth": "bear", "summary": "清空恢复记录"},
        ]

    def api_clear_records(self) -> Dict[str, Any]:
        """清空恢复记录列表。"""
        try:
            self.save_data("restore_records", [])
            self._log("已清空恢复记录")
            return {"success": True, "message": "已清空恢复记录"}
        except Exception as err:  # noqa: BLE001
            return {"success": False, "message": str(err)}

    def api_status(self) -> Dict[str, Any]:
        """返回运行状态与最近日志。"""
        with self._log_lock:
            lines = list(self._logs)[-30:]
        return {
            "success": True,
            "enabled": self._enabled,
            "running": self._scanning,
            "worker": bool(self._thread and self._thread.is_alive()),
            "last_scan": self._last_scan,
            "last_result": self._last_result,
            "logs": lines,
        }

    def api_scan(self) -> Dict[str, Any]:
        """立即扫描一轮存量 .cas（事件监控之外的手动触发，适合处理监控启动前的存量文件）。"""
        problem = self._validate()
        if problem:
            self._log(f"扫描失败：{problem}", "warning")
            return {"success": False, "message": problem}
        self._stop_event.clear()
        threading.Thread(target=self._scan_once, daemon=True,
                         name="casrestore-once").start()
        return {"success": True, "message": "已启动扫描，请查看日志"}

    def api_test(self) -> Dict[str, Any]:
        """测试 CAS 服务连接与鉴权，结果写入日志面板。"""
        host, api_key, source = self._resolve_cas()
        if not host:
            msg = "未获取到 CAS 服务地址（CAS助手 未启用且本插件未填写）"
            self._log(msg, "warning")
            return {"success": False, "message": msg}
        self._log(f"[测试] CAS 服务：{host}（来源：{source}），API Key：{'已配置' if api_key else '未配置'}")
        if requests is None:
            return {"success": False, "message": "当前环境缺少 requests 库"}
        try:
            session = self._new_session()
            headers = {"x-api-key": api_key} if api_key else {}
            resp = session.get(f"{host}/api/tasks", headers=headers,
                               params={"page": 1, "pageSize": 1}, timeout=10)
            if resp.status_code == 200:
                self._log("[测试] 连接正常且 API Key 鉴权通过")
            else:
                self._log(f"[测试] 连接异常：HTTP {resp.status_code} - {resp.text[:200]}", "warning")
                return {"success": False, "message": f"HTTP {resp.status_code}"}
            # 追加验证账号 / 家庭列表接口（与配置页下拉使用相同逻辑）
            accounts, account_err = self._fetch_accounts()
            if account_err:
                self._log(f"[测试] 账号列表拉取失败：{account_err}", "warning")
                return {"success": False, "message": f"账号列表拉取失败：{account_err}"}
            self._log(f"[测试] 账号列表正常（{len(accounts)} 个）")
            target = self._cfg_str("account_id") or (
                accounts[0]["value"] if accounts else "")
            if not target:
                return {"success": True, "message": "连接正常，但无可用账号"}
            fams, family_err = self._fetch_families(target)
            if family_err:
                self._log(f"[测试] 家庭列表拉取失败（账号 {target}）：{family_err}", "warning")
            else:
                self._log(f"[测试] 家庭列表正常（{len(fams)} 个，账号 {target}）")
            return {"success": True,
                    "message": f"CAS 服务连接正常（{source}），账号 {len(accounts)} 个"}
        except Exception as err:  # noqa: BLE001
            self._log(f"[测试] 连接失败：{err}", "warning")
            return {"success": False, "message": str(err)}

    # ------------------------------------------------------------------
    # 配置读取辅助
    # ------------------------------------------------------------------
    def _cfg_str(self, key: str, default: str = "") -> str:
        return str(self._config.get(key, default) or default).strip()

    def _cfg_int(self, key: str, default: int, minimum: int = 0) -> int:
        try:
            return max(minimum, int(float(self._config.get(key, default))))
        except (TypeError, ValueError):
            return default

    def _cfg_bool(self, key: str, default: bool = False) -> bool:
        return bool(self._config.get(key, default))

    def _validate(self) -> Optional[str]:
        """校验运行必需的配置，返回错误说明或 None。"""
        if not self._cfg_str("watch_dir"):
            return "需要填写监控目录（.cas 文件所在目录）"
        if not self._cfg_str("account_id"):
            return "需要填写账号 ID（CAS 请求体 accountId）"
        if self._cfg_str("scope") not in SCOPE_ACTIONS:
            return "空间类型只能是 family / personal"
        if self._cfg_str("scope") == "family" and not self._cfg_str("family_id"):
            return "空间类型为家庭空间时需要填写家庭 ID"
        if not self._cfg_str("target_folder_path"):
            return "需要填写目标文件夹路径（网盘基路径）"
        host, _, _ = self._resolve_cas()
        if not host:
            return "未获取到 CAS 服务地址，请先启用并配置「CAS助手」插件"
        return None

    # ------------------------------------------------------------------
    # 日志
    # ------------------------------------------------------------------
    def _log(self, text: str, level: str = "info") -> None:
        """同时写入详情页日志面板与宿主日志。"""
        line = f"{datetime.now().strftime('%m-%d %H:%M:%S')} {text}"
        with self._log_lock:
            self._logs.append(line)
        try:
            getattr(logger, level, logger.info)(f"{LOG_TAG} {text}")
        except Exception:  # noqa: BLE001
            pass

    def _notify(self, title: str, text: str) -> None:
        """发送 Message 通知；失败只记日志。"""
        if not self._cfg_bool("notify", True):
            return
        try:
            self.post_message(title=title, text=text)
        except Exception as err:  # noqa: BLE001
            self._log(f"发送通知失败：{err}", "warning")

    # ------------------------------------------------------------------
    # CAS 服务配置（优先 CAS助手 插件，回退本插件填写）
    # ------------------------------------------------------------------
    def _resolve_cas(self) -> Tuple[str, str, str]:
        """从 CAS助手 插件读取配置，返回 (host, api_key, 来源说明)；host 已规范化。"""
        host = ""
        api_key = ""
        source = "CAS助手 插件"
        if PluginManager is not None:
            try:
                cfg = PluginManager().run_plugin_method(
                    "casassistant", "get_cas_config") or {}
                if cfg.get("enabled") and cfg.get("host"):
                    host = str(cfg.get("host") or "").strip()
                    api_key = str(cfg.get("api_key") or "").strip()
            except Exception as err:  # noqa: BLE001
                self._log(f"读取 CAS助手 配置失败：{err}", "warning")
        host = self._normalize_host(host)
        return host, api_key, source

    def _fetch_accounts(self, timeout: int = 6) -> Tuple[List[Dict[str, Any]], str]:
        """
        从 CAS 服务拉取账号列表（GET /api/accounts）。

        返回 (items, error)：items 为 [{label, value}]；与源项目一致，
        只列个人账号（过滤掉带 familyId 的项）。
        """
        host, api_key, _ = self._resolve_cas()
        if not host:
            return [], "未获取到 CAS 服务地址（请先配置 CAS助手 插件）"
        if requests is None:
            return [], "当前环境缺少 requests 库"
        try:
            session = self._new_session()
            headers = {"x-api-key": api_key} if api_key else {}
            resp = session.get(f"{host}/api/accounts", headers=headers, timeout=timeout)
            if resp.status_code != 200:
                return [], f"HTTP {resp.status_code}: {resp.text[:150]}"
            data = resp.json()
            if not data.get("success"):
                return [], str(data.get("message") or data.get("error") or "接口返回失败")
            items: List[Dict[str, Any]] = []
            for acc in data.get("data") or []:
                if not isinstance(acc, dict) or acc.get("familyId"):
                    continue  # 与源项目一致：秒传恢复只列个人账号
                username = str(acc.get("username") or acc.get("id") or "")
                alias = str(acc.get("alias") or "")
                label = f"{username}（{alias}）" if alias else username
                items.append({"label": label, "value": str(acc.get("id"))})
            if not items:
                return [], "CAS 服务未返回个人账号"
            return items, ""
        except Exception as err:  # noqa: BLE001
            return [], str(err)

    def _fetch_families(self, account_id: str,
                        timeout: int = 6) -> Tuple[List[Dict[str, Any]], str]:
        """
        从 CAS 服务拉取指定账号的家庭列表（GET /api/accounts/{id}/families）。

        返回 (items, error)：items 为 [{label, value}]，value 为 familyId。
        """
        host, api_key, _ = self._resolve_cas()
        if not host:
            return [], "未获取到 CAS 服务地址（请先配置 CAS助手 插件）"
        if requests is None:
            return [], "当前环境缺少 requests 库"
        try:
            session = self._new_session()
            headers = {"x-api-key": api_key} if api_key else {}
            resp = session.get(f"{host}/api/accounts/{account_id}/families",
                               headers=headers, timeout=timeout)
            if resp.status_code != 200:
                return [], f"HTTP {resp.status_code}: {resp.text[:150]}"
            data = resp.json()
            if not data.get("success"):
                return [], str(data.get("message") or data.get("error") or "接口返回失败")
            items: List[Dict[str, Any]] = []
            for fam in data.get("data") or []:
                if not isinstance(fam, dict):
                    continue
                fid = str(fam.get("familyId") or fam.get("id") or "")
                remark = str(fam.get("remarkName") or fam.get("name") or "")
                label = f"{remark}（{fid}）" if remark else fid
                if fid:
                    items.append({"label": label, "value": fid})
            if not items:
                return [], f"该账号未获取到可用家庭（接口原始返回：{str(data)[:150]}）"
            return items, ""
        except Exception as err:  # noqa: BLE001
            return [], str(err)

    @staticmethod
    def _normalize_host(host: str) -> str:
        """补全 http(s):// 前缀并去掉尾部斜杠。"""
        host = (host or "").strip()
        if not host:
            return ""
        if not host.lower().startswith(("http://", "https://")):
            host = f"http://{host}"
        return host.rstrip("/")

    @staticmethod
    def _new_session():
        """CAS 服务通常是内网地址，禁用系统代理避免不可达。"""
        session = requests.Session()
        session.trust_env = False
        session.verify = False
        return session

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
                target=self._worker_loop, name="casrestore", daemon=True
            )
            self._thread.start()
            self._log("守护线程已启动")

    def _worker_loop(self) -> None:
        """调度入口：优先 watchdog 事件驱动（新 .cas 写入完成即恢复），不可用时回退轮询。"""
        if _WATCHDOG_OK:
            try:
                self._watch_loop()
                return
            except Exception as err:  # noqa: BLE001
                self._log(f"watchdog 监控异常，回退轮询：{err}", "warning")
        else:
            self._log("当前环境缺少 watchdog，回退轮询监控", "warning")
        self._poll_loop()

    def _watch_loop(self) -> None:
        """watchdog 事件循环：递归监听，新 .cas 写入完成 / 移入即恢复。"""
        watch_dir = self._cfg_str("watch_dir")
        observer = _WD_Observer()
        observer.schedule(_CasEventHandler(self), watch_dir, recursive=True)
        observer.start()
        self._log(f"已启动 watchdog 目录监控（事件驱动）：{watch_dir}")
        try:
            while not self._stop_event.is_set() and self._thread is threading.current_thread():
                self._stop_event.wait(timeout=1)
        finally:
            observer.stop()
            observer.join(timeout=5)
        self._log("watchdog 监控已退出")

    def _on_fs_event(self, full: str) -> None:
        """文件系统事件回调（watchdog 线程）：只处理 .cas 文件。"""
        if self._stop_event.is_set() or not full.lower().endswith(".cas"):
            return
        try:
            path = Path(full)
            if not path.is_file():
                return
            self._process_file(path)
        except Exception as err:  # noqa: BLE001
            self._log(f"处理 {Path(full).name} 异常：{err}", "error")

    def _poll_loop(self) -> None:
        """回退轮询循环（watchdog 不可用时）：固定 30 秒扫描一轮存量 .cas。"""
        current = threading.current_thread()
        while not self._stop_event.is_set() and self._thread is current:
            self._trigger.wait(timeout=POLL_FALLBACK_SECONDS)
            self._trigger.clear()
            if self._stop_event.is_set():
                break
            try:
                self._scan_once()
            except Exception as err:  # noqa: BLE001
                self._log(f"扫描异常：{err}", "error")
        self._log("守护线程已退出")

    def _handle_onlyonce(self) -> None:
        """处理「立即运行一次」开关，触发后自动复位。"""
        try:
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

    # ------------------------------------------------------------------
    # 扫描与恢复
    # ------------------------------------------------------------------
    @staticmethod
    def _file_key(path: Path) -> str:
        """文件唯一键：路径 + 大小 + 修改时间，内容变化后可重新恢复。"""
        try:
            stat = path.stat()
            return f"{path}|{stat.st_size}|{int(stat.st_mtime)}"
        except OSError:
            return str(path)

    def _scan_once(self) -> int:
        """扫描存量 .cas 并逐个恢复（手动触发 / 轮询兜底用）。返回成功数。"""
        if not self._scan_lock.acquire(blocking=False):
            self._log("上一轮扫描尚未结束，跳过本轮", "warning")
            return 0
        self._scanning = True
        ok = 0
        try:
            problem = self._validate()
            if problem:
                self._log(f"跳过扫描：{problem}", "warning")
                return 0
            watch_dir = Path(self._cfg_str("watch_dir"))
            if not watch_dir.is_dir():
                self._log(f"监控目录不存在：{watch_dir}", "warning")
                return 0
            todo = [p for p in sorted(watch_dir.rglob("*"))
                    if p.is_file() and p.suffix.lower() == ".cas"]
            if not todo:
                self._log("未发现待处理 .cas 文件")
                return 0
            self._log(f"发现 {len(todo)} 个 .cas 文件，开始逐个恢复")
            for path in todo:
                if self._stop_event.is_set():
                    break
                try:
                    if self._process_file(path):
                        ok += 1
                except Exception as err:  # noqa: BLE001
                    self._log(f"处理 {path.name} 异常：{err}", "error")
            summary = f"扫描完成：发现 {len(todo)} 个，恢复成功 {ok} 个"
            self._last_scan = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._last_result = summary
            self._log(summary)
            if ok:
                self._notify(f"{self.plugin_name} 恢复完成", summary)
            return ok
        finally:
            self._scanning = False
            self._scan_lock.release()

    def _process_file(self, path: Path) -> bool:
        """处理单个 .cas：恢复 → 删除 .cas → 清空本地空目录。返回是否成功。"""
        if not self._scan_lock.acquire(blocking=False):
            return False
        self._scanning = True
        try:
            problem = self._validate()
            if problem:
                self._log(f"跳过 {path.name}：{problem}", "warning")
                return False
            watch_dir = Path(self._cfg_str("watch_dir"))
            key = self._file_key(path)
            processed: Dict[str, Any] = self.get_data("processed") or {}
            if not isinstance(processed, dict):
                processed = {}
            if key in processed:
                return False
            failures: Dict[str, int] = self.get_data("failures") or {}
            if not isinstance(failures, dict):
                failures = {}
            if int(failures.get(key, 0)) >= DEFAULT_RETRY:
                return False

            host, api_key, _ = self._resolve_cas()
            success, reason = self._restore_one(host, api_key, path, watch_dir)
            self._last_scan = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if success:
                processed[key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                failures.pop(key, None)
                self.save_data("processed", dict(list(processed.items())[-MAX_PROCESSED:]))
                self.save_data("failures", failures)
                self._last_result = f"恢复成功：{path.name}"
                if self._cfg_bool("notify", True):
                    self._notify(
                        f"{self.plugin_name} 恢复成功",
                        f"{path.name} 已恢复到网盘：{self._cfg_str('target_folder_path')}",
                    )
                return True
            count = int(failures.get(key, 0)) + 1
            failures[key] = count
            self.save_data("failures", failures)
            self._last_result = f"恢复失败：{path.name}"
            if count >= DEFAULT_RETRY:
                processed[key] = f"失败放弃：{reason}"
                self.save_data("processed", dict(list(processed.items())[-MAX_PROCESSED:]))
                self._log(f"恢复失败 {path.name}（{count}/{DEFAULT_RETRY} 次，已放弃）：{reason}",
                          "error")
            else:
                self._log(f"恢复失败 {path.name}（{count}/{DEFAULT_RETRY} 次）：{reason}",
                          "warning")
            return False
        finally:
            self._scanning = False
            self._scan_lock.release()

    def _restore_one(self, host: str, api_key: str, path: Path,
                     watch_dir: Path) -> Tuple[bool, str]:
        """
        调用 /api/cas-restore/restore 恢复单个 .cas（一次一条）。

        网盘目录 = 目标基路径 + 本地相对子目录（manual 模式，CAS 自动创建缺失目录）。
        返回 (是否成功, 失败原因)。
        """
        try:
            rel = path.parent.resolve().relative_to(watch_dir.resolve()).as_posix()
        except (ValueError, OSError):
            rel = ""
        base = self._cfg_str("target_folder_path").strip().strip("/")
        target_path = f"{base}/{rel}" if (base and rel) else (base or rel)
        try:
            content = base64.b64encode(path.read_bytes()).decode("ascii")
        except OSError as err:
            self._append_record(path.name, target_path, "失败", f"读取失败：{err}")
            return False, f"读取失败：{err}"
        payload: Dict[str, Any] = {
            "accountId": int(float(self._cfg_str("account_id") or 0) or 0),
            "scope": self._cfg_str("scope"),
            "directoryMode": "manual",
            "targetFolderId": "",
            "targetFolderPath": target_path,
            "files": [{"localFileName": path.name, "content": content}],
        }
        if self._cfg_str("scope") == "family":
            payload["familyId"] = self._cfg_str("family_id")
        self._log(f"提交恢复：{path.name} → {target_path}")
        try:
            session = self._new_session()
            headers = {"x-api-key": api_key} if api_key else {}
            resp = session.post(f"{host}/api/cas-restore/restore",
                                json=payload, headers=headers, timeout=RESTORE_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as err:  # noqa: BLE001
            self._append_record(path.name, target_path, "失败", f"请求失败：{err}")
            return False, str(err)
        if not data.get("success"):
            message = str((data.get("data") or {}).get("message")
                          or data.get("message") or data)
            self._append_record(path.name, target_path, "失败", f"接口返回失败：{message}")
            return False, message
        rdata = data.get("data") or {}
        results = [r for r in (rdata.get("results") or []) if isinstance(r, dict)]
        result = results[0] if results else {}
        if not result.get("success"):
            reason = str(result.get("message") or "未知原因")
            self._append_record(path.name, target_path, "失败", reason)
            return False, reason
        mode_text = str(result.get("mode") or "")
        self._log(f"恢复成功：{path.name} → {result.get('fileName') or path.stem}"
                  f"（{mode_text}），网盘目录 {target_path}")
        self._append_record(path.name, target_path, "成功",
                            f"恢复为 {result.get('fileName') or path.stem}"
                            + (f"（模式 {mode_text}）" if mode_text else ""))
        # 删除本地 .cas 并清空空目录（边界为监控目录本身，不向上越界）
        try:
            path.unlink()
        except OSError as err:
            self._log(f"删除 .cas 失败 {path.name}：{err}", "warning")
        self._cleanup_empty_dirs(path.parent, watch_dir)
        return True, ""

    def _append_record(self, filename: str, target: str,
                       status: str, message: str) -> None:
        """追加一条恢复记录（详情页列表，最新在前，最多 200 条）。"""
        try:
            records = self.get_data("restore_records") or []
            if not isinstance(records, list):
                records = []
            records.insert(0, {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "filename": filename, "target": target,
                "status": status, "message": message,
            })
            self.save_data("restore_records", records[:200])
        except Exception as err:  # noqa: BLE001
            self._log(f"写入恢复记录失败：{err}", "warning")

    def _cleanup_empty_dirs(self, start: Path, watch_dir: Path) -> None:
        """删除 start 到监控目录之间的空目录（不删监控目录本身，不向上越界）。"""
        try:
            watch = watch_dir.resolve()
            current = start.resolve()
        except OSError:
            return
        while current != watch and watch in current.parents:
            try:
                current.rmdir()  # 仅空目录能删除，非空即停止
                self._log(f"已清空本地空目录：{current}")
            except OSError:
                return
            current = current.parent
