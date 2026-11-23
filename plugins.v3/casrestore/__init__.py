"""
CAS恢复源文件（CasRestore）

扫描监控目录中的 .cas 秒传文件，调用 CAS 服务的 /api/cas-restore/restore 接口，
在网盘直接恢复源文件（秒传，不占用本地下载带宽）。

CAS 服务地址与 API Key 优先读取 CAS助手（casassistant）插件配置：
    from app.sdk.plugins import PluginManager
    cfg = PluginManager().run_plugin_method("casassistant", "get_cas_config")
    # cfg = {"enabled": bool, "host": str, "api_key": str}
CAS助手 未配置时回退到本插件自身填写的 CAS 服务地址 / API Key。

请求体（与 CAS 抓包一致）：
{
    "accountId": 3,
    "scope": "family",
    "familyId": "575994414",
    "directoryMode": "select",
    "targetFolderId": "...",
    "targetFolderPath": "家庭1/测试",
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

__all__ = ["CasRestore"]

LOG_TAG = "[CAS恢复]"
MAX_LOG_LINES = 300
MAX_PROCESSED = 5000
DEFAULT_RETRY = 3
DEFAULT_BATCH = 10
RESTORE_TIMEOUT = 120

SCOPE_ACTIONS = {"family": "家庭空间", "personal": "个人空间"}
DIR_MODE_ACTIONS = {"select": "按目录 ID（select）", "manual": "按目录路径（manual）"}

try:  # 内网自签证书时禁用告警
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:  # noqa: BLE001
    pass


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
    plugin_version = "1.0.0"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "casrestore_"
    plugin_order = 29
    auth_level = 1

    _default_config = {
        "enabled": False,
        "watch_dir": "",
        "interval_seconds": 60,
        "batch_size": DEFAULT_BATCH,
        "delete_cas": True,
        "account_id": "",
        "scope": "family",
        "family_id": "",
        "directory_mode": "select",
        "target_folder_id": "",
        "target_folder_path": "",
        "cas_host": "",
        "cas_api_key": "",
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
        self._enabled = bool(self._config.get("enabled"))
        self._log(f"初始化完成：启用={self._enabled}，"
                  f"监控目录={self._cfg_str('watch_dir') or '未设置'}，"
                  f"间隔={self._cfg_int('interval_seconds', 60, 10)} 秒")
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
        """返回 Vuetify 配置页。"""
        cfgs = [
            _alert(
                "扫描监控目录中的 .cas 秒传文件，调用 CAS 服务接口直接在网盘恢复源文件"
                "（秒传，不占本地带宽）。CAS 服务地址优先读取「CAS助手」插件配置，"
                "也可在下方自行填写。恢复成功后可选删除本地 .cas 文件。"
            ),
            _row(
                _sw("enabled", "启用插件", md=12),
            ),
            _row(
                _tf("watch_dir", "监控目录",
                    "扫描该目录（含子目录）下的 .cas 文件；容器内绝对路径",
                    "/downloads/tgcas", md=12),
            ),
            _row(
                _tf("interval_seconds", "扫描间隔（秒）", "最小 10 秒", "60", "number", md=4),
                _tf("batch_size", "单次请求文件数", "一批 .cas 一起提交", "10", "number", md=4),
                _sw("delete_cas", "恢复成功后删除 .cas", "网盘恢复成功后删除本地 .cas 文件", md=4),
            ),
            _row(
                _tf("account_id", "账号 ID", "CAS 请求体 accountId", "3", "number", md=4),
                _sel("scope", "空间类型", SCOPE_ACTIONS, "", md=4),
                _tf("family_id", "家庭 ID", "空间类型为家庭空间时必填", "575994414", md=4),
            ),
            _row(
                _sel("directory_mode", "目录模式", DIR_MODE_ACTIONS, "", md=4),
                _tf("target_folder_id", "目标文件夹 ID", "目录模式 select 时必填", "", md=4),
                _tf("target_folder_path", "目标文件夹路径", "目录模式 manual 时必填，如 家庭1/测试",
                    "", md=4),
            ),
            _row(
                _tf("cas_host", "CAS 服务地址（可选）",
                    "留空自动读取 CAS助手 插件配置", "http://192.168.2.6:3005", md=6),
                _tf("cas_api_key", "CAS API Key（可选）",
                    "留空自动读取 CAS助手 插件配置", "", "text", md=6),
            ),
            _row(
                _sw("notify", "发送通知", "有恢复成功时发送 Message 通知", md=4),
                _sw("onlyonce", "立即运行一次", "保存后立即扫描一轮并自动复位", md=4),
            ),
        ]
        return [{"component": "VForm", "content": cfgs}], {**self._default_config, **self._config}

    # ------------------------------------------------------------------
    # 详情页
    # ------------------------------------------------------------------
    def get_page(self) -> List[dict]:
        """展示运行状态与最近日志。"""
        with self._log_lock:
            lines = list(self._logs)[-40:]
        log_content = [
            {"component": "pre", "props": {"class": "text-caption", "style": "white-space:pre-wrap"},
             "text": "\n".join(lines) or "暂无日志"}
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
                    {"component": "VCard", "props": {"variant": "outlined"}, "content": [
                        {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                         "text": "运行日志"},
                        {"component": "VCardText", "content": log_content},
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
             "auth": "bear", "summary": "运行状态与最近日志"},
            {"path": "/scan", "endpoint": self.api_scan, "methods": ["POST"],
             "auth": "bear", "summary": "唤醒守护线程立即扫描一轮"},
            {"path": "/test", "endpoint": self.api_test, "methods": ["POST"],
             "auth": "bear", "summary": "测试 CAS 服务连接与鉴权"},
        ]

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
        """唤醒守护线程立即扫描一轮；插件未启用时只执行一次，不常驻。"""
        problem = self._validate()
        if problem:
            self._log(f"扫描失败：{problem}", "warning")
            return {"success": False, "message": problem}
        if self._enabled:
            self._ensure_worker()
            self._trigger.set()
            return {"success": True, "message": "已请求立即扫描，请查看日志"}
        self._stop_event.clear()
        threading.Thread(target=self._scan_once, daemon=True,
                         name="casrestore-once").start()
        return {"success": True, "message": "插件未启用，已启动单次扫描，请查看日志"}

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
                return {"success": True, "message": f"CAS 服务连接正常（{source}）"}
            self._log(f"[测试] 连接异常：HTTP {resp.status_code} - {resp.text[:200]}", "warning")
            return {"success": False, "message": f"HTTP {resp.status_code}"}
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
        mode = self._cfg_str("directory_mode")
        if mode not in DIR_MODE_ACTIONS:
            return "目录模式只能是 select / manual"
        if mode == "select" and not self._cfg_str("target_folder_id"):
            return "目录模式 select 需要填写目标文件夹 ID"
        if mode == "manual" and not self._cfg_str("target_folder_path"):
            return "目录模式 manual 需要填写目标文件夹路径"
        host, _, _ = self._resolve_cas()
        if not host:
            return "未获取到 CAS 服务地址（CAS助手 未启用且本插件未填写）"
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
        """返回 (host, api_key, 来源说明)；host 已补全协议并去掉尾部斜杠。"""
        host = ""
        api_key = ""
        source = "本插件填写"
        if PluginManager is not None:
            try:
                cfg = PluginManager().run_plugin_method(
                    "casassistant", "get_cas_config") or {}
                if cfg.get("enabled") and cfg.get("host"):
                    host = str(cfg.get("host") or "").strip()
                    api_key = str(cfg.get("api_key") or "").strip()
                    source = "CAS助手 插件"
            except Exception as err:  # noqa: BLE001
                self._log(f"读取 CAS助手 配置失败，回退本插件填写：{err}", "warning")
        if not host:
            host = self._cfg_str("cas_host")
            api_key = api_key or self._cfg_str("cas_api_key")
        host = self._normalize_host(host)
        return host, api_key, source

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
        """轮询循环：按配置间隔扫描监控目录，trigger 唤醒立即扫描。"""
        current = threading.current_thread()
        while not self._stop_event.is_set() and self._thread is current:
            interval = self._cfg_int("interval_seconds", 60, 10)
            self._trigger.wait(timeout=interval)
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

    def _scan_once(self) -> bool:
        """执行一轮扫描：收集 .cas 文件 → 分批调用恢复接口 → 记录结果。"""
        if not self._scan_lock.acquire(blocking=False):
            self._log("上一轮扫描尚未结束，跳过本轮", "warning")
            return False
        self._scanning = True
        try:
            problem = self._validate()
            if problem:
                self._log(f"跳过扫描：{problem}", "warning")
                return False
            host, api_key, source = self._resolve_cas()
            watch_dir = Path(self._cfg_str("watch_dir"))
            if not watch_dir.is_dir():
                self._log(f"监控目录不存在：{watch_dir}", "warning")
                return False
            processed: Dict[str, Any] = self.get_data("processed") or {}
            if not isinstance(processed, dict):
                processed = {}
            failures: Dict[str, int] = self.get_data("failures") or {}
            if not isinstance(failures, dict):
                failures = {}

            # 收集待处理文件：跳过已处理；同轮内按文件名去重（接口按 localFileName 回结果）
            todo: List[Path] = []
            seen_names: Dict[str, Path] = {}
            renamed = 0
            for path in sorted(watch_dir.rglob("*")):
                if not path.is_file() or path.suffix.lower() != ".cas":
                    continue
                key = self._file_key(path)
                if key in processed:
                    continue
                if int(failures.get(key, 0)) >= DEFAULT_RETRY:
                    continue
                if path.name.lower() in seen_names:
                    renamed += 1
                    continue
                seen_names[path.name.lower()] = path
                todo.append(path)
            if renamed:
                self._log(f"本轮跳过 {renamed} 个同名 .cas（接口按文件名回结果，下轮再试）")
            if not todo:
                return False

            self._last_scan = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            batch = self._cfg_int("batch_size", DEFAULT_BATCH, 1)
            ok = 0
            failed = 0
            for start in range(0, len(todo), batch):
                if self._stop_event.is_set():
                    break
                chunk = todo[start:start + batch]
                if self._restore_batch(host, api_key, chunk, processed, failures):
                    ok += len(chunk)
                else:
                    failed += len(chunk)
            summary = (f"本轮发现 {len(todo)} 个 .cas，恢复成功 {ok}，失败 {failed}"
                       f"（CAS：{source}）")
            self._last_result = summary
            self._log(summary)
            trimmed = dict(list(processed.items())[-MAX_PROCESSED:])
            self.save_data("processed", trimmed)
            self.save_data("failures", dict(list(failures.items())[-1000:]))
            if ok:
                self._notify(f"{self.plugin_name} 恢复完成", summary)
            return ok > 0
        finally:
            self._scanning = False
            self._scan_lock.release()

    def _restore_batch(self, host: str, api_key: str, chunk: List[Path],
                       processed: Dict[str, Any], failures: Dict[str, int]) -> bool:
        """
        调用 /api/cas-restore/restore 恢复一批 .cas 文件。

        返回 True 表示本批全部成功；按文件粒度回写 processed / failures。
        """
        mode = self._cfg_str("directory_mode")
        payload: Dict[str, Any] = {
            "accountId": int(float(self._cfg_str("account_id") or 0) or 0),
            "scope": self._cfg_str("scope"),
            "directoryMode": mode,
            "targetFolderId": self._cfg_str("target_folder_id"),
            "targetFolderPath": self._cfg_str("target_folder_path"),
            "files": [],
        }
        if self._cfg_str("scope") == "family":
            payload["familyId"] = self._cfg_str("family_id")
        for path in chunk:
            try:
                content = base64.b64encode(path.read_bytes()).decode("ascii")
            except OSError as err:
                self._log(f"读取失败 {path.name}：{err}", "error")
                failures[self._file_key(path)] = DEFAULT_RETRY  # 直接放弃，避免死循环
                continue
            payload["files"].append({"localFileName": path.name, "content": content})
        if not payload["files"]:
            return False

        names = "、".join(p.name for p in chunk[:3]) + ("…" if len(chunk) > 3 else "")
        self._log(f"提交恢复 {len(chunk)} 个：{names}")
        try:
            session = self._new_session()
            headers = {"x-api-key": api_key} if api_key else {}
            resp = session.post(f"{host}/api/cas-restore/restore",
                                json=payload, headers=headers, timeout=RESTORE_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as err:  # noqa: BLE001
            self._log(f"恢复请求失败（{names}）：{err}", "error")
            for path in chunk:
                key = self._file_key(path)
                failures[key] = int(failures.get(key, 0)) + 1
            return False

        if not data.get("success"):
            message = str((data.get("data") or {}).get("message")
                          or data.get("message") or data)
            self._log(f"恢复接口返回失败（{names}）：{message}", "error")
            for path in chunk:
                key = self._file_key(path)
                failures[key] = int(failures.get(key, 0)) + 1
            return False

        rdata = data.get("data") or {}
        results = {str(r.get("localFileName") or ""): r
                   for r in (rdata.get("results") or []) if isinstance(r, dict)}
        all_ok = True
        for path in chunk:
            key = self._file_key(path)
            result = results.get(path.name) or {}
            if result.get("success"):
                processed[key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                failures.pop(key, None)
                self._log(f"恢复成功：{path.name} → {result.get('fileName') or path.stem}"
                          f"（{result.get('mode') or ''}）")
                if self._cfg_bool("delete_cas", True):
                    try:
                        path.unlink()
                        self._log(f"已删除 {path.name}")
                    except OSError as err:
                        self._log(f"删除 .cas 失败 {path.name}：{err}", "warning")
            else:
                all_ok = False
                count = int(failures.get(key, 0)) + 1
                failures[key] = count
                reason = str(result.get("message") or "未知原因")
                if count >= DEFAULT_RETRY:
                    self._log(f"恢复失败 {path.name}（第 {count} 次，已放弃）：{reason}", "error")
                    processed[key] = f"失败放弃：{reason}"
                else:
                    self._log(f"恢复失败 {path.name}（第 {count}/{DEFAULT_RETRY} 次）：{reason}",
                              "warning")
        return all_ok
