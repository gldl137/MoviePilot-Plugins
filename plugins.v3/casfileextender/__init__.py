"""
CAS 视频文件扩展插件（MoviePilot V3）

作者：gldl137
仓库：MoviePilot-Plugins（https://github.com/gldl137/MoviePilot-Plugins）

核心原理（已读 MoviePilot 3.0.0 源码确认）：
- MoviePilot 的目录监控分发器 TransferDispatcher（app/monitor/dispatcher.py）把媒体扩展名
  存为实例属性 self.all_exts，并在 is_monitor_file / is_transfer_candidate_path 中用它对
  文件做扩展名过滤；Monitor 只创建「一个」dispatcher 实例（app/monitor/monitor.py:83），
  并把它共享给 RemotePoller（monitor.py:87）。本地目录监控、远程轮询监控、补偿扫描全部
  经由 self._dispatcher.is_transfer_candidate_path() 判断扩展名。
- 问题：dispatcher 的 all_exts 在实例化时一次性快照 settings.RMT_MEDIAEXT，而 Monitor 的
  on_config_changed 只调 init() 不重建 dispatcher，所以运行期改 settings 对原生目录监控
  不生效（必须重启）。
- 突破口：dispatcher 是单实例、被多处共享引用。本插件在 init_plugin 时把 .cas 加入
  settings.RMT_MEDIAEXT，并【原地刷新这个共享 dispatcher 实例的 all_exts 属性】（不动实例、
  不破坏 poller/watcher 引用），于是原生本地 + 远程目录监控立即支持 .cas，无需重启。
"""

import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    # 新版宿主：base 模块（app.plugins._PluginBase 会映射到此处）
    from app.sdk.plugin.base import _PluginBase
except ImportError:
    try:
        from app.sdk.plugin import _PluginBase
    except ImportError:
        from app.plugins import _PluginBase  # type: ignore


# ------------------------------------------------------------------
# 配置页 / 详情页辅助（构造 Vuetify schema）
# ------------------------------------------------------------------
def _alert(text: str, atype: str = "info", css: str = "my-2") -> Dict[str, Any]:
    """构造提示条。"""
    return {
        "component": "VAlert",
        "props": {"type": atype, "variant": "tonal", "density": "compact",
                  "class": css, "text": text},
    }


def _row(*cols: Dict[str, Any]) -> Dict[str, Any]:
    """构造一行。"""
    return {"component": "VRow", "content": list(cols)}


def _sw(model: str, label: str, hint: str = "", md: int = 3) -> Dict[str, Any]:
    """构造开关。"""
    props: Dict[str, Any] = {"model": model, "label": label}
    if hint:
        props["hint"] = hint
    return {
        "component": "VCol",
        "props": {"cols": 12, "md": md},
        "content": [{"component": "VSwitch", "props": props}],
    }


def _tf(model: str, label: str, hint: str = "", placeholder: str = "",
        ftype: str = "", md: int = 4) -> Dict[str, Any]:
    """构造文本输入框。"""
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


def _sel(model: str, label: str, items: Dict[str, str],
         hint: str = "", md: int = 4) -> Dict[str, Any]:
    """构造下拉框。"""
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


def _result(success: bool, message: str, **extra: Any) -> Dict[str, Any]:
    """统一 JSON 响应。"""
    return {"success": success, "message": message, **extra}


class CasFileExtender(_PluginBase):
    # 元信息（字面量，避免宿主加载校验失败）
    plugin_name = "CAS视频扩展"
    plugin_desc = "把 .cas 等扩展名注入原生目录监控，使其立即识别整理为视频媒体"
    plugin_version = "1.0.1"
    plugin_author = "gldl137"
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"

    _default_config = {
        "enabled": False,
        "notify": False,
        "exts": "cas",
        "refresh_native_monitor": True,
        # 可选：自带扫描作为「监控启动前已存在的存量文件」补扫（原生监控只处理新增/修改）
        "monitor_dirs": "",
        "interval": 300,
        "transfer_type": "copy",
        "storage": "local",
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._config: Dict[str, Any] = {}
        self._enabled: bool = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._logs: deque = deque(maxlen=300)
        self._log_lock = threading.Lock()
        self._last_scan: Optional[str] = None

    # ------------------------------------------------------------------
    # 配置读取
    # ------------------------------------------------------------------
    def _cfg_str(self, key: str, default: str = "") -> str:
        return str(self._config.get(key, default) or default)

    def _cfg_int(self, key: str, default: int = 0, minimum: Optional[int] = None) -> int:
        try:
            value = int(self._config.get(key, default) or default)
        except (ValueError, TypeError):
            value = default
        if minimum is not None and value < minimum:
            value = minimum
        return value

    def _cfg_bool(self, key: str, default: bool = False) -> bool:
        return bool(self._config.get(key, default))

    def _exts(self) -> List[str]:
        return [f".{e.strip().lower().lstrip('.')}"
                for e in str(self._config.get("exts") or "cas").split(",") if e.strip()]

    # ------------------------------------------------------------------
    # 日志
    # ------------------------------------------------------------------
    def _log(self, text: str, level: str = "info") -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with self._log_lock:
            self._logs.append(f"[{ts}] [{level}] {text}")

    def _notify(self, title: str, text: str) -> None:
        if not self._cfg_bool("notify"):
            return
        try:
            self.send_message(title=title, text=text)
        except Exception:  # noqa: BLE001 - 通知失败不应阻断整理
            pass

    # ------------------------------------------------------------------
    # 扩展名注入（核心：让原生目录监控立即支持）
    # ------------------------------------------------------------------
    @staticmethod
    def _get_monitor() -> Optional[Any]:
        """安全获取已初始化的 Monitor 单例，不触发初始化。"""
        try:
            from app.monitor.monitor import Monitor
        except Exception:  # noqa: BLE001
            return None
        try:
            # 优先用 SingletonClass.get_existing_instance，避免插件初始化时
            # 意外创建 Monitor 而提前启动目录监控
            return Monitor.get_existing_instance()
        except Exception:  # noqa: BLE001
            try:
                return Monitor()  # 单例：已存在则复用，不重建
            except Exception:  # noqa: BLE001
                return None

    def _register_ext(self) -> None:
        """把目标扩展名加入白名单，并原地刷新原生目录监控分发器的扩展名集合。"""
        from app.runtime.config import settings  # 延迟导入，避免模块级依赖

        exts = self._exts()
        added = [e for e in exts if e not in settings.RMT_MEDIAEXT]
        for ext in added:
            settings.RMT_MEDIAEXT.append(ext)

        if not self._cfg_bool("refresh_native_monitor"):
            if added:
                self._log(f"已把 {added} 加入白名单 RMT_MEDIAEXT（未刷新原生监控）", "info")
            else:
                self._log(f"视频扩展名 {exts} 已在白名单中", "info")
            return

        # 刷新原生目录监控分发器的 all_exts 快照（共享实例，不重建，无副作用）
        try:
            monitor = self._get_monitor()
            if monitor is None:
                self._log("Monitor 尚未实例化，白名单已注入，将在其初始化时自动生效", "info")
            else:
                disp = getattr(monitor, "_dispatcher", None)
                if disp is None:
                    self._log("原生监控分发器尚未创建，白名单已注入，将在其初始化时自动生效", "info")
                else:
                    disp.all_exts = (settings.RMT_MEDIAEXT
                                     + settings.RMT_SUBEXT
                                     + settings.RMT_AUDIOEXT)
                    self._log(
                        f"已刷新原生目录监控扩展名集合，现含 {exts}"
                        f"（本地监控 + 远程轮询立即生效，无需重启）", "info")
        except Exception as err:  # noqa: BLE001
            self._log(f"刷新原生监控扩展名失败（白名单仍已注入）：{err}", "warning")

    def _refresh_native(self) -> Dict[str, Any]:
        """手动刷新原生监控扩展名集合（应对核心配置变更后）。"""
        from app.runtime.config import settings
        monitor = self._get_monitor()
        if monitor is None:
            return _result(False, "Monitor 尚未实例化，无法刷新（白名单已注入，将在其启动时生效）")
        disp = getattr(monitor, "_dispatcher", None)
        if disp is None:
            return _result(False, "原生监控分发器尚未创建，无法刷新")
        disp.all_exts = (settings.RMT_MEDIAEXT
                         + settings.RMT_SUBEXT
                         + settings.RMT_AUDIOEXT)
        self._log(f"已手动刷新原生目录监控扩展名集合，现含 {self._exts()}", "info")
        return _result(True, "原生目录监控扩展名集合已刷新", all_exts=list(disp.all_exts))

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def init_plugin(self, config: Optional[dict] = None) -> None:
        """读取配置、注入扩展名并（可选）启动后台扫描线程。"""
        self.stop_service()
        self._stop_event.clear()
        self._config = {**self._default_config, **(config or {})}
        self._enabled = self._cfg_bool("enabled")
        self._register_ext()
        self._log(
            f"初始化完成：启用={self._enabled}，扩展名={','.join(self._exts())}，"
            f"刷新原生监控={self._cfg_bool('refresh_native_monitor')}，"
            f"目录={self._cfg_str('monitor_dirs') or '未配置（仅靠原生监控）'}"
        )
        if self._enabled and self._cfg_str("monitor_dirs").strip():
            self._start_worker()

    def stop_service(self) -> None:
        """停止后台线程，可重复调用。"""
        self._enabled = False
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
            if thread.is_alive():
                self._log("守护线程未在 5 秒内退出，将在当前任务结束后停止", "warning")
        self._thread = None
        self._stop_event.clear()

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    @staticmethod
    def get_service() -> List[Dict[str, Any]]:
        # 采用插件内守护线程轮询，避免宿主 cron 服务兼容问题
        return []

    def _start_worker(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="casfileextender-loop")
        self._thread.start()

    def _loop(self) -> None:
        while self._enabled and not self._stop_event.is_set():
            try:
                self._scan_once()
            except Exception as err:  # noqa: BLE001
                self._log(f"扫描异常：{err}", "error")
            interval = self._cfg_int("interval", 300, 10)
            for _ in range(interval):
                if not self._enabled or self._stop_event.is_set():
                    break
                time.sleep(1)

    # ------------------------------------------------------------------
    # 可选：自带扫描（补扫原生监控启动前已存在的存量文件）
    # ------------------------------------------------------------------
    def _scan_once(self) -> None:
        """遍历监控目录，把目标扩展名文件送入整理链。"""
        from app.chain.transfer import TransferChain
        from app.schemas.workflow import FileItem

        exts = set(self._exts())
        dirs = [d.strip() for d in self._cfg_str("monitor_dirs").split(",") if d.strip()]
        if not exts or not dirs:
            return
        tc = TransferChain()
        processed = self.get_data("processed") or []
        processed_set = set(processed)
        count = 0
        for d in dirs:
            root = Path(d)
            if not root.exists():
                self._log(f"监控目录不存在：{d}", "warning")
                continue
            for p in root.rglob("*"):
                if not p.is_file():
                    continue
                if p.suffix.lower() not in exts:
                    continue
                try:
                    st = p.stat()
                except OSError:
                    continue
                key = f"{p.resolve()}:{st.st_size}:{int(st.st_mtime)}"
                if key in processed_set:
                    continue
                fileitem = FileItem(
                    storage=self._cfg_str("storage", "local"),
                    path=p.as_posix(),
                    type="file",
                    name=p.name,
                    basename=p.stem,
                    extension=p.suffix.lstrip(".").lower(),
                    size=st.st_size,
                    modify_time=st.st_mtime,
                )
                try:
                    tc.do_transfer(
                        fileitem=fileitem,
                        transfer_type=self._cfg_str("transfer_type", "copy"),
                    )
                    processed_set.add(key)
                    count += 1
                    self._log(f"已送入整理：{p.name}", "info")
                    self._notify(self.plugin_name, f"已整理：{p.name}")
                except Exception as err:  # noqa: BLE001
                    self._log(f"整理失败 {p.name}：{err}", "error")
        if processed_set:
            self.save_data("processed", list(processed_set)[-1000:])
        self._last_scan = time.strftime("%Y-%m-%d %H:%M:%S")
        if count:
            self._log(f"本轮共送入整理 {count} 个文件", "info")

    # ------------------------------------------------------------------
    # 配置页
    # ------------------------------------------------------------------
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回 Vuetify 配置页与默认配置模型。"""
        cfgs = [
            _alert(
                f"{self.plugin_name}：{self.plugin_desc}。"
                "本插件把 .cas 等扩展名加入 MoviePilot 视频白名单，并【原地刷新原生目录监控分发器的扩展名集合】，"
                "使本地目录监控与远程轮询监控立即识别 .cas 为视频媒体（无需重启、不改核心配置）。"
                "扩展名集合是共享实例，刷新的是其属性而非重建，不影响已有监控线程。",
                "success"),
            _row(
                _sw("enabled", "启用插件"),
                _sw("notify", "发送通知", "整理成功后发送 MoviePilot 消息"),
                _sw("refresh_native_monitor", "刷新原生监控", "开启后注入扩展名并立即让原生目录监控生效"),
            ),
            _row(
                _tf("exts", "注册的扩展名", "逗号分隔，不加点和前缀，如 cas,rmvb", "cas"),
            ),
            _alert(
                "可选：原生目录监控只处理「新增/修改」的文件，不处理监控启动前已存在的存量 .cas。"
                "如需补扫存量，可填下面的目录（递归扫描并送入整理链）。留空则仅依赖原生监控。",
                "info"),
            _row(
                _tf("monitor_dirs", "存量补扫目录", "容器内绝对路径，逗号分隔，递归扫描（可选，留空关闭）",
                    "/downloads/casfiles"),
            ),
            _row(
                _tf("interval", "补扫间隔（秒）", "最小 10 秒", "300", md=4),
                _sel("transfer_type", "转移模式",
                     {"copy": "复制", "move": "移动", "link": "硬链接",
                      "softlink": "软链接"}, "", md=4),
                _tf("storage", "存储标识", "本地目录一般填 local", "local", md=4),
            ),
        ]
        return [{"component": "VForm", "content": cfgs}], {**self._default_config, **self._config}

    # ------------------------------------------------------------------
    # 详情页
    # ------------------------------------------------------------------
    def get_page(self) -> List[dict]:
        """状态、操作按钮与日志。"""
        prefix = self.__class__.__name__
        with self._log_lock:
            lines = list(self._logs)
        log_content = ([{"component": "div", "text": line} for line in lines]
                       or [{"component": "div",
                            "props": {"class": "text-medium-emphasis"},
                            "text": "暂无日志，点击「立即刷新监控扩展名」或「存量补扫」后显示。"}])
        worker_alive = bool(self._thread and self._thread.is_alive())
        state_text = (
            f"插件状态：{'已启用' if self._enabled else '未启用'}；"
            f"原生监控刷新：{'开' if self._cfg_bool('refresh_native_monitor') else '关'}；"
            f"存量补扫线程：{'运行中' if worker_alive else '未运行'}；"
            f"上次补扫：{self._last_scan or '尚未扫描'}；"
            f"已注册扩展名：{','.join(self._exts())}"
        )

        def button(text: str, path: str, icon: str, color: str = "primary") -> Dict[str, Any]:
            return {
                "component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{
                    "component": "VBtn",
                    "props": {"color": color, "variant": "elevated", "block": True,
                              "prepend-icon": icon},
                    "text": text,
                    "events": {"click": {"type": "request",
                                        "api": f"plugin/{prefix}{path}", "method": "POST",
                                        "success": "已执行，详见下方日志",
                                        "fail": "请求失败，请查看插件日志"}},
                }],
            }

        return [
            {"component": "VCard", "props": {"variant": "flat"}, "content": [
                {"component": "VCardTitle", "props": {"class": "text-h6"}, "text": self.plugin_name},
                {"component": "VCardSubtitle",
                 "props": {"class": "text-caption text-medium-emphasis"},
                 "text": f"v{self.plugin_version}　作者：{self.plugin_author}　仓库：MoviePilot-Plugins　{self.plugin_desc}"},
                {"component": "div", "props": {"class": "text-caption text-medium-emphasis px-4"},
                 "text": self.author_url},
                {"component": "VCardText", "content": [
                    _alert(state_text,
                           "success" if (self._enabled and self._cfg_bool("refresh_native_monitor")) else "warning",
                           "mb-3"),
                    _row(
                        button("刷新监控扩展名", "/refresh", "mdi-refresh"),
                        button("存量补扫", "/scan", "mdi-play"),
                        button("环境自检", "/diagnose", "mdi-stethoscope"),
                        button("清空日志", "/clear_logs", "mdi-delete", "error"),
                    ),
                ]},
            ]},
            {"component": "VCard", "props": {"variant": "flat", "class": "mt-4"}, "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                 "text": "运行日志（最近 300 行）"},
                {"component": "VCardText", "props": {"style": "max-height:320px;overflow:auto"},
                 "content": log_content},
            ]},
        ]

    # ------------------------------------------------------------------
    # 插件 API
    # ------------------------------------------------------------------
    def get_state(self) -> Dict[str, Any]:
        """返回插件运行状态（满足宿主基类抽象方法要求）。"""
        with self._log_lock:
            lines = list(self._logs)
        return {
            "enabled": self._enabled,
            "refresh_native_monitor": self._cfg_bool("refresh_native_monitor"),
            "exts": self._exts(),
            "worker_alive": bool(self._thread and self._thread.is_alive()),
            "last_scan": self._last_scan,
            "logs": lines,
        }

    def get_api(self) -> List[Dict[str, Any]]:
        """注册详情页使用的后端接口。"""
        prefix = self.__class__.__name__
        return [
            {"path": "/status", "endpoint": self.api_status, "methods": ["GET"],
             "auth": "bear", "summary": "查询运行状态"},
            {"path": "/refresh", "endpoint": self.api_refresh, "methods": ["POST"],
             "auth": "bear", "summary": "立即刷新原生目录监控扩展名集合"},
            {"path": "/scan", "endpoint": self.api_scan, "methods": ["POST"],
             "auth": "bear", "summary": "存量补扫一轮"},
            {"path": "/diagnose", "endpoint": self.api_diagnose, "methods": ["POST"],
             "auth": "bear", "summary": "环境自检"},
            {"path": "/clear_logs", "endpoint": self.api_clear_logs, "methods": ["POST"],
             "auth": "bear", "summary": "清空日志"},
        ]

    def api_status(self) -> Dict[str, Any]:
        with self._log_lock:
            lines = list(self._logs)
        return _result(True, "ok", enabled=self._enabled,
                       refresh_native=self._cfg_bool("refresh_native_monitor"),
                       worker_alive=bool(self._thread and self._thread.is_alive()),
                       last_scan=self._last_scan, logs=lines)

    def api_refresh(self) -> Dict[str, Any]:
        return self._refresh_native()

    def api_scan(self) -> Dict[str, Any]:
        threading.Thread(target=self._scan_once, daemon=True, name="cas-scan").start()
        return _result(True, "已触发存量补扫，详见下方日志")

    def api_diagnose(self) -> Dict[str, Any]:
        from app.runtime.config import settings
        exts = self._exts()
        in_list = {ext: (ext in settings.RMT_MEDIAEXT) for ext in exts}
        monitor = self._get_monitor()
        disp = getattr(monitor, "_dispatcher", None) if monitor else None
        native_has = all(ext in (disp.all_exts if disp else []) for ext in exts) if disp else False
        return _result(True, "环境自检完成", exts=exts, in_whitelist=in_list,
                       mediaext=list(settings.RMT_MEDIAEXT),
                       native_monitor_has_ext=native_has)

    def api_clear_logs(self) -> Dict[str, Any]:
        with self._log_lock:
            self._logs.clear()
        return _result(True, "日志已清空")
