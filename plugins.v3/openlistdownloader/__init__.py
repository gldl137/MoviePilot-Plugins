# OpenList 自动下载插件
#
# 功能：定时扫描 OpenList（Alist 类网盘聚合工具）指定目录下的「新增文件」，
#       将文件的直链通过宿主 run_module 交给 aria2downloader 插件，由 Aria2 下载到本地。
#
# 设计要点：
#   - 仅下载「新出现」的文件（基于本地已处理记录去重，不重复提交）。
#   - 通过 run_module("download", ...) 复用 aria2downloader 插件，路径映射/记录逻辑一致。
#   - 不修改宿主源码。
#
# 注意：下载地址（直链）的拼接/签名方法由用户后续提供，统一在 _build_download_url() 中，
#       目前先用 OpenList 标准 /api/fs/get 返回的 raw_url。

import time
import json
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple

from app.core.config import settings
from app.plugins import _PluginBase
from app.runtime.log import logger
from app.core.metainfo import MetaInfo
from app.core.context import MediaInfo
from app.core.cache import TTLCache
from app.schemas.types import SystemConfigKey


_PLUGIN_NAME = "OpenList 自动下载"


class OpenListDownloader(_PluginBase):
    # ---- 插件元信息 ----
    plugin_name = _PLUGIN_NAME
    plugin_desc = "定时扫描 OpenList 指定目录的新增文件，通过 Aria2 自动下载到本地。"
    plugin_version = "1.0.0"
    plugin_author = "gldl137"
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_order = 20

    # 配置默认值
    _default_config = {
        "openlist_url": "",
        "openlist_token": "",
        "monitor_paths": "",
        "download_dir": "/downloads",
        "downloader": "Aria2",
        "cron_expression": "*/10 * * * *",
        "file_ext": "",
        "min_size": 0,
        "max_depth": 0,
        "max_files_per_round": 50,
        "enabled": False,
        "onlyonce": False,
        "clear_records": False,
        "notify": True,
        # 微信通知渠道（留空=使用 MP 默认通知机制；填写系统已配置的微信渠道名则通知发往该渠道）
        "wx_channel": "",
    }

    def __init__(self):
        super().__init__()
        self._enabled = False
        self._config: Dict[str, Any] = {}
        # 媒体识别结果缓存（避免重复识别 TMDB），TTL 24 小时
        self._media_cache = TTLCache(maxsize=1000, ttl=24 * 60 * 60)
        # 通知汇总线程防重入标志
        self._notify_thread_started = False
        # 通知待发队列读写锁（scan 主线程与通知 daemon 并发读写，避免丢通知）
        import threading as _t
        self._notify_lock = _t.Lock()

    # ------------------------------------------------------------------
    # 插件生命周期
    # ------------------------------------------------------------------
    def init_plugin(self, config: dict = None):
        config = config or {}
        self._config = {**self._default_config, **config}
        self._enabled = bool(self._config.get("enabled"))
        self.__update_config()
        # 启动连接自检（仅打印日志，不影响启用）
        if self._enabled:
            self._test_connection()
        # 开关类操作放到后台线程执行，避免阻塞插件加载（连接慢时尤其重要）
        if self._config.get("onlyonce") or self._config.get("clear_records"):
            import threading
            threading.Thread(target=self._handle_switches, daemon=True).start()
        # 启动通知汇总后台线程（15 秒窗口内同剧集合并成一条图文）
        # 防重入：多次调用 init_plugin（如保存配置）时不重复启动线程；
        # 若插件曾 stop_service（线程已退出），则重新启动
        if self._notify_thread_started and self._notify_stop:
            self._notify_thread_started = False
        self._notify_stop = False
        if not self._notify_thread_started:
            import threading
            threading.Thread(target=self._notify_daemon, daemon=True).start()
            self._notify_thread_started = True

    def _handle_switches(self):
        """后台处理 onlyonce / clear_records 开关（保存即触发，触发后自动关闭）。"""
        try:
            if self._config.get("onlyonce"):
                self.log_info("[OpenList] 检测到『立即运行一次』开关，开始扫描下载…")
                try:
                    self.scan_and_download()
                except Exception as e:  # noqa: BLE001
                    self.log_error(f"[OpenList] 立即运行一次出错: {e}")
                finally:
                    self._config["onlyonce"] = False
                    self.update_config(self._config)
            if self._config.get("clear_records"):
                self.log_info("[OpenList] 检测到『清空记录』开关，开始清空已处理记录与下载记录…")
                try:
                    processed_count = len(self._load_processed())
                    self.del_data("processed")
                    history_count = len(self.get_data("history") or [])
                    self.del_data("history")
                    self.log_info(
                        f"[OpenList] 已清空已处理记录（共 {processed_count} 条）与下载记录"
                        f"（共 {history_count} 条），文件将可重新被扫描")
                except Exception as e:  # noqa: BLE001
                    self.log_error(f"[OpenList] 清空记录出错: {e}")
                finally:
                    self._config["clear_records"] = False
                    self.update_config(self._config)
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 处理开关出错: {e}")

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        self._enabled = False
        self._notify_stop = True
        # 插件停止时把剩余待发通知立即发出，避免丢失
        try:
            pending = self._load_pending()
            for key, item in pending.items():
                self._flush_notify(key, item)
            if pending:
                self._save_pending({})
        except Exception:  # noqa: BLE001
            pass

    def __update_config(self):
        self.update_config(self._config)

    # ------------------------------------------------------------------
    # 渲染模式：Vue 远程组件（侧边栏全页面使用 ./AppPage）
    # ------------------------------------------------------------------
    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """
        声明插件使用 Vue 远程组件渲染，并指定构建产物目录。
        """
        return "vue", "dist/assets"

    def get_sidebar_nav(self) -> List[Dict[str, Any]]:
        """
        声明插件在主界面左侧导航栏中的全页入口。
        """
        return [
            {
                "nav_key": "main",
                "title": "下载记录",
                "icon": "mdi-download-box",
                "section": "organize",
                "permission": "manage",
                "order": 10,
            }
        ]

    # ------------------------------------------------------------------
    # 配置界面：vue 模式下设置页由前端 ./Config 远程组件渲染，
    # 故 get_form 返回 None + 默认值（与 guangyadisk 一致）。
    # ------------------------------------------------------------------
    def get_form(self) -> tuple:
        return None, self._default_config

    def get_page(self) -> Optional[List[dict]]:
        """
        Vue 渲染模式下不使用 Vuetify 页面定义，详情弹窗由前端 ./Page 远程组件渲染。
        """
        return None

    # ------------------------------------------------------------------
    # 定时任务
    # ------------------------------------------------------------------
    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        from apscheduler.triggers.cron import CronTrigger
        cron_expr = (self._config.get("cron_expression") or "").strip()
        if not cron_expr:
            cron_expr = "*/10 * * * *"
        try:
            trigger = CronTrigger.from_crontab(cron_expr)
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 定时表达式无效「{cron_expr}」：{e}，回退每10分钟")
            trigger = CronTrigger.from_crontab("*/10 * * * *")
        return [
            {
                "id": "openlist_scan",
                "name": "扫描 OpenList 新增文件",
                "trigger": trigger,
                "func": self.scan_and_download,
                "kwargs": {},
            }
        ]

    # ------------------------------------------------------------------
    # OpenList API 封装
    # ------------------------------------------------------------------
    def _resolve_token(self) -> str:
        """
        解析最终用于请求的 token：直接使用配置的 openlist_token。
        OpenList 要求 Authorization 头直接放 token（不带 Bearer 前缀）。
        """
        raw = (self._config.get("openlist_token") or "").strip()
        if raw.lower().startswith("bearer "):
            raw = raw[7:].strip()
        return raw

    def _test_connection(self) -> bool:
        """启动时验证 OpenList 连通性与鉴权是否有效。"""
        url = (self._config.get("openlist_url") or "").strip()
        if not url:
            self.log_warning("[OpenList] 连接测试：未配置 OpenList 地址")
            return False
        token = self._resolve_token()
        resp = self._openlist_post("/api/fs/list", {"path": "/", "refresh": False}, token)
        if resp and resp.get("code") == 200:
            self.log_info("[OpenList] 连接测试：成功（OpenList 可达，鉴权有效）")
            return True
        self.log_error(f"[OpenList] 连接测试：失败（响应：{resp}）")
        return False

    def get_api(self) -> List[Dict[str, Any]]:
        """注册自定义 API：立即运行扫描 / 清空记录。"""
        return [
            {
                "path": "/run",
                "endpoint": self.api_run,
                "methods": ["POST"],
                "summary": "立即运行",
                "description": "立即扫描 OpenList 并下载新增文件",
                "auth": "bear",
            },
            {
                "path": "/clear_records",
                "endpoint": self.api_clear_records,
                "methods": ["POST"],
                "summary": "删除记录",
                "description": "清空已处理文件记录，使文件可重新被扫描下载",
                "auth": "bear",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取状态",
                "description": "返回上次扫描时间等状态信息",
            },
            {
                "path": "/history",
                "endpoint": self.api_history,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取下载历史",
                "description": "返回已下载文件的历史记录（含封面）",
            },
            {
                "path": "/config",
                "endpoint": self.api_get_config,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取配置",
                "description": "返回当前插件配置（供前端设置页 ./Config 使用）",
            },
            {
                "path": "/config",
                "endpoint": self.api_update_config,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "更新配置",
                "description": "保存插件配置（供前端设置页 ./Config 使用）",
            },
            {
                "path": "/history/delete",
                "endpoint": self.api_history_delete,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "删除单条历史",
                "description": "按文件名删除一条下载历史记录",
            },
            {
                "path": "/redownload",
                "endpoint": self.api_redownload,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "重新下载",
                "description": "按文件名从历史记录重新提交下载到 Aria2",
            },
            {
                "path": "/channels",
                "endpoint": self.api_channels,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取微信通知渠道",
                "description": "返回 MP 系统已配置的企业微信通知渠道名称，供设置页选择",
            },
        ]

    def api_get_config(self) -> Dict[str, Any]:
        """GET /config：返回当前配置。"""
        return {
            "success": True,
            "config": self._config,
        }

    def api_channels(self) -> Dict[str, Any]:
        """GET /channels：返回 MP 系统已启用的通知渠道列表。

        列出所有已启用的通知渠道（不限类型），供设置页选择。
        企业微信(wechat) 凭证齐全时直发；其它类型走 MP 消息机制。
        """
        channels = []
        try:
            notifications = self.systemconfig.get(SystemConfigKey.Notifications) or []
            for n in notifications:
                if not n.get("enabled"):
                    continue
                name = n.get("name", "")
                if not name:
                    continue
                ctype = n.get("type", "")
                channels.append({"name": name, "type": ctype})
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 获取通知渠道失败: {e}")
        return {"success": True, "channels": channels}

    def api_update_config(self, config_payload: dict):
        """POST /config：保存配置。

        MP 框架将 POST 的 JSON body 作为 config_payload 传入。
        """
        try:
            config_payload = config_payload or {}
            self._config = {**self._default_config, **config_payload}
            self._enabled = bool(self._config.get("enabled"))
            self.update_config(self._config)
            # 保存配置后如需立即执行开关类操作，交由后台线程处理（不重复 init_plugin，避免重复启动线程）
            if self._config.get("onlyonce") or self._config.get("clear_records"):
                import threading
                threading.Thread(target=self._handle_switches, daemon=True).start()
            self.log_info("[OpenList] 配置已通过前端设置页更新")
            return {"success": True, "message": "配置已保存"}
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 保存配置失败: {e}")
            return {"success": False, "message": str(e)}

    def api_history(self) -> Dict[str, Any]:
        """GET /history：返回历史下载记录列表。"""
        history = self.get_data("history") or []
        if not isinstance(history, list):
            history = []
        return {
            "success": True,
            "count": len(history),
            "items": history,
        }

    def api_history_delete(self, payload: dict):
        """POST /history/delete：按文件名删除一条历史记录。"""
        try:
            payload = payload or {}
            name = (payload.get("name") or "").strip()
            if not name:
                return {"success": False, "message": "缺少文件名参数"}
            history = self.get_data("history") or []
            if not isinstance(history, list):
                history = []
            before = len(history)
            history = [h for h in history if (h.get("name") or "") != name]
            self.save_data("history", history)
            return {"success": True, "message": f"已删除 {before - len(history)} 条记录"}
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 删除历史出错: {e}")
            return {"success": False, "message": str(e)}

    def api_redownload(self, payload: dict):
        """POST /redownload：按文件名从历史记录重新提交下载到 Aria2。"""
        try:
            payload = payload or {}
            name = (payload.get("name") or "").strip()
            if not name:
                return {"success": False, "message": "缺少文件名参数"}
            history = self.get_data("history") or []
            if not isinstance(history, list):
                history = []
            record = next((h for h in history if (h.get("name") or "") == name), None)
            if not record:
                return {"success": False, "message": "未找到该记录"}
            url = (record.get("download_url") or "").strip()
            if not url:
                return {"success": False, "message": "该记录缺少下载链接，无法重新下载"}
            download_dir = (self._config.get("download_dir") or "").strip() or "/downloads"
            downloader = (self._config.get("downloader") or "Aria2").strip()
            ok = self._submit_to_aria2(url, download_dir, "", downloader, name)
            if ok:
                return {"success": True, "message": "已重新提交下载"}
            return {"success": False, "message": "重新提交下载失败"}
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 重新下载出错: {e}")
            return {"success": False, "message": str(e)}

    def api_run(self):
        """界面「立即运行」按钮回调：立即执行一次扫描下载。"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        try:
            self.scan_and_download()
            return {"success": True, "message": "已触发扫描，查看日志了解详情"}
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 立即运行出错: {e}")
            return {"success": False, "message": str(e)}

    def api_clear_records(self):
        """界面「删除记录」按钮回调：清空已处理记录与下载记录，使文件可重新扫描下载。"""
        try:
            processed_count = len(self._load_processed())
            self.del_data("processed")
            history_count = len(self.get_data("history") or [])
            self.del_data("history")
            self.log_info(
                f"[OpenList] 已清空已处理记录（共 {processed_count} 条）与下载记录"
                f"（共 {history_count} 条），文件将可重新被扫描")
            return {
                "success": True,
                "message": f"已删除 {processed_count} 条处理记录和 {history_count} 条下载记录",
            }
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 删除记录出错: {e}")
            return {"success": False, "message": str(e)}

    def api_status(self) -> Dict[str, Any]:
        """界面状态查询：返回上次扫描时间。"""
        last = self.get_data("last_scan") or {}
        return {
            "success": True,
            "last_scan": last.get("time", "") if last else "",
        }

    def _openlist_post(self, path: str, payload: dict, token: Optional[str] = None) -> Optional[dict]:
        base = (self._config.get("openlist_url") or "").strip()
        if not base:
            self.log_error("[OpenList] 未配置 OpenList 地址")
            return None
        url = f"{base.rstrip('/')}{path}"
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if token is None:
            token = self._resolve_token()
        if token:
            # OpenList 标准：Authorization 头直接放 token（不带 Bearer 前缀）
            token = token.strip()
            if token.lower().startswith("bearer "):
                token = token[7:].strip()
            headers["Authorization"] = token
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:  # noqa: BLE001
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:  # noqa: BLE001
                pass
            self.log_error(f"[OpenList] {path} HTTP {e.code}: {body}")
            return None
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] {path} 请求失败: {e}")
            return None

    def _list_dir(self, path: str, token: str, page=1, per_page=100, _sign=None) -> List[dict]:
        """列出 OpenList 目录下文件，自动翻页，返回 content 列表。"""
        payload = {"path": path, "refresh": False, "page": page, "per_page": per_page}
        if _sign:
            payload["sign"] = _sign
        resp = self._openlist_post("/api/fs/list", payload, token)
        if not resp or resp.get("code") != 200:
            return []
        data = resp.get("data") or {}
        content = data.get("content") or []
        # 翻页（OpenList 返回 total 与当前页内容数，自行判断是否还有下一页）
        total = data.get("total") or 0
        if total > page * per_page and content:
            # 跨页需保留上一页返回的 sign（如有）
            nxt_sign = data.get("sign") or _sign
            content += self._list_dir(path, token, page + 1, per_page, nxt_sign)
        return content

    # ------------------------------------------------------------------
    # 下载地址拼接
    # ------------------------------------------------------------------
    def _build_download_url(self, path: str, name: str, token: str) -> tuple:
        """
        根据 OpenList 文件路径构造交给 Aria2 的下载直链。

        直链规则（用户提供）：
            {openlist_url}/d/{文件路径}
        例：http://192.168.1.100:5244/d/夸克/影视/电视剧/国产剧/藏锋/藏锋.S01E02.mp4

        :param path:    OpenList 内的完整路径，如 /夸克/影视/.../藏锋.S01E02.mp4
        :param name:    文件名（预留）
        :param token:   OpenList token（用于生成 sign，见下方 TODO）
        :return: (download_url, cookie) —— cookie 用于直链鉴权（如有）
        """
        base = (self._config.get("openlist_url") or "").rstrip("/")
        if not base:
            return "", ""
        # 对路径部分做标准 UTF-8 编码（保留斜杠），使 Aria2/AriaNg 能正确解码显示中文
        from urllib.parse import quote as _quote
        encoded = _quote(path, safe="/")
        download_url = f"{base}/d{encoded}" if encoded.startswith("/") else f"{base}/d/{encoded}"
        self.log_info(f"[OpenList] 构造直链：{download_url}")

        # TODO(用户): 在此追加 sign 参数。
        # 例：sign = self._get_sign(path, token)
        #     download_url = f"{download_url}?sign={sign}"
        # 或如需鉴权 cookie：cookie = f"token={token}"
        cookie = ""
        return download_url, cookie

    # ------------------------------------------------------------------
    # 去重记录
    # ------------------------------------------------------------------
    def _load_processed(self) -> Set[str]:
        data = self.get_data("processed") or []
        if isinstance(data, dict):  # 兼容旧版 dict 格式
            data = list(data.keys())
        return set(data)

    def _save_processed(self, paths: Set[str]):
        # 控制体积：最多保留 5000 条（最近优先）
        if len(paths) > 5000:
            paths = set(list(paths)[-5000:])
        self.save_data("processed", list(paths))

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------
    def scan_and_download(self):
        if not self._enabled:
            return
        url = (self._config.get("openlist_url") or "").strip()
        downloader = (self._config.get("downloader") or "Aria2").strip()
        # 下载目录：配置为空时使用内置默认目录（必须传真实路径，不能传 None，
        # 否则 aria2downloader 会把 None 转成字符串 "None" 传给 Aria2 导致落盘错误）
        download_dir = (self._config.get("download_dir") or "").strip() or "/downloads"
        if not url:
            self.log_warning("[OpenList] 未配置地址，跳过扫描")
            return
        # 解析 token（支持直接 token 或用户名密码登录）
        token = self._resolve_token()
        if not token:
            self.log_warning("[OpenList] 未配置 Token 且未配置用户名密码，跳过扫描")
            return

        paths = [p.strip() for p in (self._config.get("monitor_paths") or "").splitlines() if p.strip()]
        if not paths:
            self.log_warning("[OpenList] 未配置监控路径，跳过扫描")
            return

        # 后缀 / 大小过滤
        exts = [e.strip().lower().lstrip(".") for e in (self._config.get("file_ext") or "").split(",") if e.strip()]
        min_size = float(self._config.get("min_size") or 0) * 1024 * 1024
        max_depth = int(self._config.get("max_depth") or 0)   # 0 = 仅当前层
        max_per_round = int(self._config.get("max_files_per_round") or 50)

        processed = self._load_processed()
        new_count = 0
        scanned = 0
        new_files: List[Dict[str, Any]] = []

        def _walk(base: str, depth: int):
            nonlocal new_count, scanned
            # 单轮上限：达到后停止扫描，避免一次提交过多
            if new_count >= max_per_round:
                return
            try:
                items = self._list_dir(base, token)
            except Exception as e:  # noqa: BLE001
                self.log_error(f"[OpenList] 列目录失败 {base}: {e}")
                return
            for it in items:
                if new_count >= max_per_round:
                    return
                if it.get("is_dir"):
                    # 递归：未限制深度或当前深度未超
                    if max_depth <= 0 or depth < max_depth:
                        _walk(it.get("path") or f"{base}/{it.get('name','')}", depth + 1)
                    continue
                name = it.get("name") or ""
                fpath = it.get("path") or f"{base}/{name}"
                scanned += 1
                # 去重：已处理过则跳过
                if fpath in processed:
                    continue
                # 后缀过滤
                if exts:
                    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                    if ext not in exts:
                        continue
                # 大小过滤
                size = int(it.get("size") or 0)
                if min_size and size < min_size:
                    continue
                # 构造下载直链（base/d/path，含 sign 拼接）
                download_url, cookie = self._build_download_url(fpath, name, token)
                if not download_url:
                    self.log_warning(f"[OpenList] 构造直链失败，跳过：{fpath}")
                    continue
                # 交给 aria2downloader 插件下载
                ok = self._submit_to_aria2(download_url, download_dir, cookie, downloader, name)
                if ok:
                    processed.add(fpath)
                    new_count += 1
                    new_files.append({
                        "name": name,
                        "size": size,
                        "fpath": fpath,
                        "download_url": download_url,
                    })
                    self.log_info(f"[OpenList] 已提交下载：{name}")
                else:
                    self.log_error(f"[OpenList] 提交下载失败：{name}")

        for base in paths:
            _walk(base, 1)

        if new_count:
            self._save_processed(processed)
            if self._config.get("notify", True):
                # 将本轮新增文件按媒体（TMDB id）入队，15 秒窗口内合并为一条图文
                for f in new_files:
                    self._enqueue_notify(
                        f["name"],
                        f.get("size"),
                        f.get("fpath"),
                        f.get("download_url"),
                    )
        else:
            self.log_info(f"[OpenList] 本轮无新增文件（扫描 {scanned} 个）")
        # 记录上次扫描统计（页面展示用）
        try:
            from datetime import datetime
            self.save_data("last_scan", {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "scanned": scanned,
                "new": new_count,
                "processed": len(processed),
            })
        except Exception:  # noqa: BLE001
            pass

    def _submit_to_aria2(self, url: str, download_dir: str, cookie: str,
                         downloader: str, name: str) -> bool:
        """通过宿主 run_module 调用 aria2downloader 的 download 方法。"""
        self.log_info(f"[OpenList] 发送下载地址：{url} -> dir={download_dir}")
        try:
            # download_dir 必须是真实路径字符串（不能传 None）
            result = self.chain.run_module(
            "download",
            content=url,
            download_dir=Path(download_dir),
            cookie=cookie,
            downloader=downloader,
            )
            self.log_info(f"[OpenList] run_module(download) 返回: {result!r}")
            if isinstance(result, tuple) and len(result) >= 4:
                # (downloader, gid, "Original", msg)
                if not result[1]:
                    self.log_error(f"[OpenList] Aria2 返回失败信息: {result[3]}")
                return bool(result[1])
            return bool(result)
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] run_module(download) 失败: {e}")
            return False

    def _recognize_media(self, name: str) -> dict:
        """根据文件名识别媒体信息，返回 {tmdb_id, title, year, type, poster, link}（识别失败字段为空）。

        使用系统缓存（TTLCache，24h）缓存识别成功的媒体信息，避免对相同文件重复请求 TMDB。
        """
        try:
            # 缓存命中直接返回
            if name in self._media_cache:
                return self._media_cache[name]
        except Exception:  # noqa: BLE001
            pass

        result = {"tmdb_id": "", "title": "", "year": "", "type": "", "season": "",
                  "poster": "", "link": ""}
        try:
            from app.chain.media import MediaChain
            meta = MetaInfo(name)
            if not meta.name:
                return result
            mediainfo: Optional[MediaInfo] = MediaChain().recognize_media(meta)
            if mediainfo:
                result["tmdb_id"] = str(getattr(mediainfo, "tmdb_id", "") or "")
                result["title"] = getattr(mediainfo, "title", "") or getattr(mediainfo, "name", "")
                result["year"] = str(getattr(mediainfo, "year", "") or "")
                mtype = getattr(mediainfo, "type", "") or ""
                result["type"] = "电影" if str(mtype).lower().startswith("movie") else (
                    "电视剧" if str(mtype).lower().startswith("tv") else "")
                # 季数：电视剧才有，供前端显示「第X季」
                season = getattr(mediainfo, "season", None)
                if season:
                    result["season"] = str(season)
                result["poster"] = getattr(mediainfo, "poster_path", "") or ""
                result["link"] = getattr(mediainfo, "detail_link", "") or ""
                # 仅缓存识别成功的结果
                if result["tmdb_id"]:
                    try:
                        self._media_cache[name] = result
                    except Exception:  # noqa: BLE001
                        pass
                self.log_info(f"[OpenList] 媒体识别成功：{result['title']} "
                              f"({result['year']}) 封面：{result['poster']}")
            else:
                self.log_info(f"[OpenList] 未识别到媒体信息：{name}")
            return result
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 媒体识别异常（{name}）：{e}")
            return result

    # ------------------------------------------------------------------
    # 日志辅助
    # ------------------------------------------------------------------
    def log_info(self, msg: str):
        logger.info(msg)

    def log_warning(self, msg: str):
        logger.warning(msg)

    def log_error(self, msg: str):
        logger.error(msg)

    @staticmethod
    def _human_size(size: float) -> str:
        """字节数转为人类可读大小（自动 KB/MB/GB）。"""
        try:
            size = float(size)
        except (TypeError, ValueError):
            return ""
        if size <= 0:
            return ""
        units = ["B", "KB", "MB", "GB", "TB"]
        idx = 0
        while size >= 1024 and idx < len(units) - 1:
            size /= 1024.0
            idx += 1
        return f"{size:.1f}{units[idx]}"

    # ------------------------------------------------------------------
    # 通知汇总（15 秒窗口内同电视剧合并为一条图文）
    # ------------------------------------------------------------------
    _NOTIFY_WINDOW = 15  # 秒：同一媒体在该窗口内的提交合并到一条通知

    def _load_pending(self) -> dict:
        return self.get_data("pending_notify") or {}

    def _save_pending(self, data: dict):
        self.save_data("pending_notify", data)

    def _enqueue_notify(self, fname: str, size: Optional[float] = None,
                        fpath: str = "", download_url: str = ""):
        """将单个文件加入通知汇总队列（按 TMDB id 合并，无 id 则用文件名）。"""
        try:
            info = self._recognize_media(fname)
            # 合并 key：有 tmdb_id 用 id，否则用首文件名保证不串剧
            key = info.get("tmdb_id") or f"name:{fname}"
            now = time.time()
            with self._notify_lock:
                pending = self._load_pending()
                item = pending.get(key)
                if not item:
                    item = {
                        "title": info.get("title") or "OpenList 自动下载",
                        "year": info.get("year") or "",
                        "type": info.get("type") or "",
                        "poster": info.get("poster") or "",
                        "link": info.get("link") or "",
                        "files": [],
                        "expire_at": now + self._NOTIFY_WINDOW,
                    }
                entry = {"name": fname, "size": size or 0}
                if not any(f["name"] == fname for f in item["files"]):
                    item["files"].append(entry)
                    # 写入历史记录（带封面+下载链接），用于侧边栏历史界面展示
                    self._add_history(info, fname, size or 0, now, download_url or "")
                # 每次有新文件都顺延窗口
                item["expire_at"] = now + self._NOTIFY_WINDOW
                # 用第一个识别到的媒体信息作为封面/标题（同剧一致）
                pending[key] = item
                self._save_pending(pending)
            self.log_info(f"[OpenList] 通知入队（{key}），当前 {len(item['files'])} 个文件，"
                          f"窗口至 {time.strftime('%H:%M:%S', time.localtime(item['expire_at']))}")
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 通知入队失败：{e}")

    def _add_history(self, info: dict, fname: str, size: float, ts: float,
                     download_url: str = ""):
        """记录一条历史（含封面+下载链接），供侧边栏历史界面展示。"""
        try:
            history = self.get_data("history") or []
            if not isinstance(history, list):
                history = []
            history.insert(0, {
                "title": info.get("title") or "",
                "year": info.get("year") or "",
                "poster": info.get("poster") or "",
                "link": info.get("link") or "",
                "name": fname,
                "download_url": download_url or "",
                "size": size or 0,
                "season": info.get("season") or "",
                "media_type": info.get("type") or "",
                "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)),
            })
            # 最多保留 100 条，超出后新的覆盖旧的
            self.save_data("history", history[:100])
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 写入历史失败：{e}")

    def _flush_notify(self, key: str, item: dict):
        """发出单条汇总图文通知。

        若配置了 wx_channel（系统微信渠道），则用该渠道直发企业微信；
        否则走 MP 默认通知机制（post_message）。
        """
        try:
            count = len(item.get("files") or [])
            media = item.get("title") or "OpenList 自动下载"
            if item.get("year"):
                media = f"{media} ({item['year']})"
            title = f"📥 已下载{count}个文件\n{media}"
            lines = []
            for f in item.get("files") or []:
                fname = f["name"] if isinstance(f, dict) else f
                fsize = f.get("size", 0) if isinstance(f, dict) else 0
                line = f"• {fname}"
                if fsize:
                    line += f"  {self._human_size(fsize)}"
                lines.append(line)
            text = f"已提交：\n" + "\n".join(lines)
            # 优先发送到选中的系统通知渠道
            wx_channel = (self._config.get("wx_channel") or "").strip()
            if wx_channel:
                ctype = self._get_mp_channel_type(wx_channel)
                if ctype == "wechat":
                    sent = self._send_wechat_channel(wx_channel, title, text, item.get("poster") or "")
                    if sent:
                        self.log_info(f"[OpenList] 已通过企业微信渠道「{wx_channel}」发送通知：{title}（{count} 个文件）")
                        return
                    self.log_warning(f"[OpenList] 企业微信渠道「{wx_channel}」发送失败，回退 MP 默认通知")
                else:
                    # 其它微信类渠道（如微信机器人）走 MP 消息机制
                    self._post_via_channel(ctype, title, text, item.get("poster") or "", item.get("link") or "")
                    self.log_info(f"[OpenList] 已通过渠道「{wx_channel}」({ctype}) 发送通知：{title}（{count} 个文件）")
                    return
            self.post_message(
                title=title,
                text=text,
                image=item.get("poster") or None,
                link=item.get("link") or None,
            )
            self.log_info(f"[OpenList] 已发送汇总通知：{title}（{count} 个文件）")
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 发送汇总通知失败：{e}")

    def _get_mp_wechat_config(self, channel_name: str) -> dict:
        """从 MP 系统通知配置中读取指定企业微信渠道的 config。"""
        try:
            notifications = self.systemconfig.get(SystemConfigKey.Notifications) or []
            for n in notifications:
                if (n.get("type") == "wechat"
                        and n.get("enabled")
                        and n.get("name") == channel_name):
                    return n.get("config") or {}
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 读取 MP 企业微信渠道配置失败: {e}")
        return {}

    def _get_mp_channel_type(self, channel_name: str) -> str:
        """返回指定系统通知渠道的类型（wechat / wechatclawbot / ...），找不到返回空。"""
        try:
            notifications = self.systemconfig.get(SystemConfigKey.Notifications) or []
            for n in notifications:
                if n.get("enabled") and n.get("name") == channel_name:
                    return n.get("type", "")
        except Exception:  # noqa: BLE001
            pass
        return ""

    def _post_via_channel(self, ctype: str, title: str, text: str, image: str, link: str) -> None:
        """对微信机器人等非企业微信渠道，走 MP 通用消息机制发送。

        ctype 为系统通知渠道类型字符串（如 wechatclawbot），映射到 NotificationChannel 枚举。
        注意：post_message 按渠道类型发送，会发到该类型下所有启用的渠道实例。
        """
        try:
            from app.schemas.types import NotificationChannel
            channel_map = {
                "wechatclawbot": NotificationChannel.WechatClawBot,
                "telegram": NotificationChannel.Telegram,
                "feishu": NotificationChannel.Feishu,
                "slack": NotificationChannel.Slack,
                "discord": NotificationChannel.Discord,
                "vocechat": NotificationChannel.VoceChat,
                "synologychat": NotificationChannel.SynologyChat,
                "qqbot": NotificationChannel.QQ,
            }
            channel = channel_map.get(ctype)
            if not channel:
                self.log_warning(f"[OpenList] 未知通知渠道类型「{ctype}」，回退 MP 默认通知")
                self.post_message(title=title, text=text, image=image or None, link=link or None)
                return
            self.post_message(channel=channel, title=title, text=text, image=image or None, link=link or None)
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 通过渠道类型「{ctype}」发送失败：{e}")
            self.post_message(title=title, text=text, image=image or None, link=link or None)

    def _send_wechat_channel(self, channel_name: str, title: str, text: str,
                             image: str = "") -> bool:
        """向指定的企业微信渠道直发一条图文/文本消息。

        从系统通知配置读取该渠道的 corpid/secret/agentid，调用企业微信 API 发送。
        """
        try:
            cfg = self._get_mp_wechat_config(channel_name)
            corpid = cfg.get("WECHAT_CORPID", "")
            secret = cfg.get("WECHAT_APP_SECRET", "")
            agentid = cfg.get("WECHAT_APP_ID", "")
            proxy = cfg.get("WECHAT_PROXY", "") or "https://qyapi.weixin.qq.com"
            if not (corpid and secret and agentid):
                self.log_error(f"[OpenList] 企业微信渠道「{channel_name}」凭证不完整，无法发送")
                return False
            import requests
            # 获取 access_token
            token = ""
            try:
                r = requests.get(
                    f"{proxy}/cgi-bin/gettoken",
                    params={"corpid": corpid, "corpsecret": secret},
                    timeout=10,
                    proxies=getattr(settings, 'PROXY', None),
                )
                data = r.json()
                if data.get("errcode", 0) == 0:
                    token = data["access_token"]
                else:
                    self.log_error(f"[OpenList] 获取企业微信 token 失败：{data}")
            except Exception as e:  # noqa: BLE001
                self.log_error(f"[OpenList] 获取企业微信 token 异常：{e}")
            if not token:
                return False
            # 组装消息：有图片用图文(news)，否则用文本(text)
            safe_title = title[:64]
            desc = text[:512]
            agentid_val = int(agentid) if str(agentid).isdigit() else agentid
            if image and image.startswith(("http://", "https://")):
                payload = {
                    "touser": cfg.get("WECHAT_ADMINS") or "@all",
                    "msgtype": "news",
                    "agentid": agentid_val,
                    "news": {"articles": [{
                        "title": safe_title,
                        "description": desc,
                        "url": "https://www.themoviedb.org/",
                        "picurl": image,
                    }]},
                }
            else:
                payload = {
                    "touser": cfg.get("WECHAT_ADMINS") or "@all",
                    "msgtype": "text",
                    "agentid": agentid_val,
                    "text": {"content": f"{safe_title}\n{desc}"},
                }
            resp = requests.post(
                f"{proxy}/cgi-bin/message/send?access_token={token}",
                json=payload,
                timeout=15,
                proxies=getattr(settings, 'PROXY', None),
            )
            data = resp.json()
            if data.get("errcode", 0) == 0:
                self.log_info(f"[OpenList] 企业微信「{channel_name}」发送成功")
                return True
            self.log_error(f"[OpenList] 企业微信「{channel_name}」发送失败：{data}")
            return False
        except Exception as e:  # noqa: BLE001
            self.log_error(f"[OpenList] 企业微信直发异常：{e}")
            return False

    def _notify_daemon(self):
        """后台线程：检查待发队列，窗口过期即发出并清理。"""
        while not getattr(self, "_notify_stop", False):
            try:
                # 收集已过期项
                expired = []
                with self._notify_lock:
                    pending = self._load_pending()
                    if pending:
                        now = time.time()
                        for key in list(pending.keys()):
                            if now >= pending[key].get("expire_at", 0):
                                expired.append((key, pending[key]))
                                del pending[key]
                        if expired:
                            self._save_pending(pending)
                # 锁外发通知，避免长时间持锁阻塞入队
                for key, item in expired:
                    self._flush_notify(key, item)
            except Exception as e:  # noqa: BLE001
                self.log_error(f"[OpenList] 通知汇总线程异常：{e}")
            time.sleep(2)
