"""
Aria2 下载器插件（MoviePilot V3，纯插件版，不改动宿主源码）

设计原则（参考 quarkdisk 插件把能力“加进系统”的方式）：
  1. init_plugin 时把下载器写入系统配置（SystemConfigKey.Downloaders），
     这样「下载器设置」里就会出现名为 Aria2 的下载器，用户可在 UI 启用/设为默认。
  2. 通过 get_module() 只注入 download 方法。Aria2 仅在用户触发下载的那一刻
     被宿主调用一次，把链接/种子经 JSON-RPC 发给 Aria2，随即结束——
     不注册 list_torrents / downloader_info 等会被宿主定时轮询的方法，避免刷屏。

下载记录：插件在自身界面提供「下载记录」面板。
  - 每次成功提交任务后，把任务元信息写入本地记录文件
    （data/plugins/aria2downloader/records.jsonl），即使 Aria2 已清理任务也可见。
  - 打开面板时，通过 get_api() 按需（非后台轮询）从 Aria2 拉取实时任务进度，
    并与本地历史记录合并展示，支持暂停/继续/删除。
"""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.request
from pathlib import Path
from typing import Any, Optional, Set, Union

from app.db.oper.systemconfig import SystemConfigOper
from app.db.oper.downloadhistory import DownloadHistoryOper
from app.db.session import get_session_factory
from app.plugins import _PluginBase
from app.runtime.log import logger
from app.schemas.types import SystemConfigKey, MediaType
from app.schemas.transfer import DownloaderTorrent
from app.schemas.types import TorrentStatus, TorrentQueryStatus, DownloadTaskState
from app.schemas.dashboard import DownloaderInfo
from app.sdk.string import StringUtils


# 本插件在系统配置里登记的下载器名称（在下载器设置中显示为可选项）
_DOWNLOADER_NAME = "Aria2"


# ---------------------------------------------------------------------------
# Aria2 JSON-RPC 客户端（仅用标准库，无第三方依赖）
# ---------------------------------------------------------------------------
class Aria2Client:
    def __init__(self, host: str = "http://127.0.0.1:6800/jsonrpc",
                 secret: str = "", timeout: int = 30):
        self._url = host.rstrip("/")
        if not self._url.endswith("/jsonrpc"):
            self._url = f"{self._url}/jsonrpc"
        self._secret = secret or ""
        self._timeout = timeout

    def _diag(self, msg: str) -> None:
        # 防御：部分调用链可能在 Aria2Client 上调用 _diag，统一降级为 debug 日志
        logger.debug(f"[Aria2Client] {msg}")

    def _call(self, method: str, params: Optional[list] = None) -> Any:
        if self._secret:
            params = [f"token:{self._secret}"] + (params or [])
        else:
            params = params or []
        payload = json.dumps({
            "jsonrpc": "2.0", "id": "mp-aria2",
            "method": f"aria2.{method}", "params": params,
        }).encode("utf-8")
        req = urllib.request.Request(
            self._url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:  # noqa: BLE001
            detail = ""
            try:
                detail = e.read().decode("utf-8", errors="replace")
            except Exception:  # noqa: BLE001
                pass
            raise RuntimeError(
                f"Aria2 HTTP {e.code}: {e.reason} | body={detail}"
            )
        if "error" in body:
            raise RuntimeError(f"Aria2 RPC 错误: {body['error']}")
        return body.get("result")

    def get_version(self) -> dict:
        return self._call("getVersion") or {}

    def is_reachable(self) -> bool:
        try:
            self.get_version()
            return True
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[Aria2Downloader] Aria2 连接失败: {e}")
            return False

    def add_uri(self, uri: str, download_dir: str = "",
                options: Optional[dict] = None) -> Optional[str]:
        opts = dict(options or {})
        if download_dir:
            opts["dir"] = download_dir
        logger.info(f"[Aria2Downloader] addUri 提交给 Aria2 的 URI={uri} opts={opts}")
        return self._call("addUri", [[uri], opts])

    def add_torrent(self, torrent_data: bytes, download_dir: str = "",
                    options: Optional[dict] = None) -> Optional[str]:
        opts = dict(options or {})
        if download_dir:
            opts["dir"] = download_dir
        b64 = base64.b64encode(torrent_data).decode("ascii")
        return self._call("addTorrent", [b64, [], opts])

    # ---- 查询与管理（按需调用，非后台轮询）----
    def tell_active(self, secret_keys: Optional[list] = None) -> list:
        return self._call("tellActive", [secret_keys or []]) or []

    def tell_wait(self, offset: int = 0, num: int = 1000,
                  secret_keys: Optional[list] = None) -> list:
        return self._call("tellWaiting", [offset, num, secret_keys or []]) or []

    def tell_stopped(self, offset: int = 0, num: int = 1000,
                     secret_keys: Optional[list] = None) -> list:
        return self._call("tellStopped", [offset, num, secret_keys or []]) or []

    def tell_status(self, gid: str, secret_keys: Optional[list] = None) -> dict:
        return self._call("tellStatus", [gid, secret_keys or []]) or {}

    def remove(self, gid: str) -> str:
        return self._call("remove", [gid]) or ""

    def force_remove(self, gid: str) -> str:
        return self._call("forceRemove", [gid]) or ""

    def pause(self, gid: str) -> str:
        return self._call("pause", [gid]) or ""

    def unpause(self, gid: str) -> str:
        return self._call("unpause", [gid]) or ""

    def get_global_stat(self) -> dict:
        return self._call("getGlobalStat") or {}


# ---------------------------------------------------------------------------
# 本地下载记录（追加写入 jsonl，仅插件内使用，不影响宿主源码/数据）
# ---------------------------------------------------------------------------
def _records_dir() -> Path:
    # 优先使用宿主提供的可写插件数据目录，避免相对路径 data 无写权限
    try:
        from app.core.config import settings
        p = settings.PLUGIN_DATA_PATH / "aria2downloader"
    except Exception:  # noqa: BLE001
        p = Path(os.environ.get("MP_DATA", "data")) / "plugins" / "aria2downloader"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _records_file() -> Path:
    return _records_dir() / "records.jsonl"


def _load_records(limit: int = 200) -> list:
    fp = _records_file()
    if not fp.exists():
        return []
    out = []
    try:
        with fp.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:  # noqa: BLE001
                    continue
    except Exception:  # noqa: BLE001
        return out
    # 最新在前
    out.reverse()
    return out[:limit]


def _append_record(rec: dict) -> None:
    try:
        with _records_file().open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:  # noqa: BLE001
        logger.debug(f"[Aria2Downloader] 写入下载记录失败: {e}")


def _dedup_append(rec: dict) -> None:
    """避免同一 gid 重复写入（Aria2 去重：同一任务只会成功返回一次 gid）。"""
    gid = rec.get("gid")
    if gid:
        for r in _load_records(limit=500):
            if r.get("gid") == gid:
                return
    _append_record(rec)


# ---------------------------------------------------------------------------
# 插件主类
# ---------------------------------------------------------------------------
class Aria2Downloader(_PluginBase):
    # ---- 元信息 ----
    plugin_name = "Aria2 下载器"
    plugin_desc = "纯发送型下载器：触发下载时把链接/种子发给 Aria2，并提供下载记录面板查看进度（不后台轮询）。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.3.0"
    plugin_author = "your-name"
    author_url = "https://github.com/your-name"
    plugin_config_prefix = "aria2_"
    plugin_order = 50
    auth_level = 1

    # ---- 运行时状态（由 init_plugin 填充）----
    _enabled = False
    _client = None
    _host = "http://127.0.0.1:6800/jsonrpc"
    _secret = ""
    _default_download_path = ""
    # list_torrents 短期缓存，避免宿主界面轮询/后台高频调用反复打 Aria2 RPC
    _torrent_cache: dict = {}
    _list_cache_ttl: int = 10

    def _diag(self, msg: str) -> None:
        logger.debug(f"[Aria2Downloader] {msg}")

    # ------------------------------------------------------------------
    # 把 Aria2 写进系统下载器配置（参考 quarkdisk 把存储写进系统配置的做法）
    # ------------------------------------------------------------------
    def _register_downloader(self) -> None:
        """确保 SystemConfigKey.Downloaders 中存在名为 Aria2 的下载器条目。"""
        try:
            configs: list = SystemConfigOper().get(SystemConfigKey.Downloaders) or []
            if not isinstance(configs, list):
                configs = []
            exists = any(
                isinstance(c, dict) and c.get("name") == _DOWNLOADER_NAME
                for c in configs
            )
            if exists:
                self._diag(f"系统下载器配置已存在 {_DOWNLOADER_NAME}，跳过注册")
                return
            entry = {
                "name": _DOWNLOADER_NAME,
                "type": "aria2",
                "default": False,
                "enabled": True,
                "config": {},
                "path_mapping": [],
            }
            configs.append(entry)
            SystemConfigOper().set(SystemConfigKey.Downloaders, configs)
            self._diag(f"已将 {_DOWNLOADER_NAME} 写入系统下载器配置")
        except Exception as e:  # noqa: BLE001
            self._diag(f"注册下载器到系统配置失败: {e}")

    def init_plugin(self, config: dict | None = None) -> None:
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._host = str(config.get("host") or "http://127.0.0.1:6800/jsonrpc")
        self._secret = str(config.get("secret") or "")
        self._default_download_path = str(config.get("download_path") or "")
        self._client = None
        self._diag(
            f"初始化完成：enabled={self._enabled}, rpc={self._host}, "
            f"默认目录={self._default_download_path or '无'}"
        )
        # 只要插件被加载（无论是否启用），都保证下载器出现在系统配置中，
        # 这样用户在「下载器设置」里能看到并可启用/设为默认。
        self._register_downloader()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> list:
        return []

    # ------------------------------------------------------------------
    # 下载记录 API（按需调用，非后台轮询）
    # ------------------------------------------------------------------
    def _status_label(self, code: str) -> str:
        mapping = {
            "active": "下载中", "waiting": "等待中", "paused": "已暂停",
            "error": "错误", "complete": "完成", "removed": "已移除",
            "submitted": "已提交", "offline": "离线",
        }
        return mapping.get(code, code or "-")

    def _build_records(self) -> list:
        """合并本地历史记录与 Aria2 实时状态，返回前端 VTable 数据。"""
        # 本地历史（最新在前）
        local = _load_records(limit=200)
        live_map: dict = {}
        if self._enabled:
            try:
                client = self._get_client()
                keys = ["gid", "status", "totalLength", "completedLength",
                        "downloadSpeed", "uploadSpeed", "files", "dir", "errorMessage"]
                for item in client.tell_active(keys) + client.tell_wait(0, 1000, keys) + client.tell_stopped(0, 1000, keys):
                    gid = item.get("gid")
                    if gid:
                        live_map[gid] = item
            except Exception as e:  # noqa: BLE001
                self._diag(f"拉取 Aria2 实时任务失败: {e}")

        rows = []
        for rec in local:
            gid = rec.get("gid")
            name = rec.get("name") or ""
            d = dict(rec)
            live = live_map.get(gid)
            if live:
                status_code = live.get("status") or "active"
                total = int(live.get("totalLength") or 0)
                completed = int(live.get("completedLength") or 0)
                pct = f"{(completed / total * 100):.1f}%" if total > 0 else "-"
                files = live.get("files") or []
                if not name and files:
                    name = (files[0].get("path") or "").rsplit("/", 1)[-1]
                d.update({
                    "status": status_code,
                    "status_text": self._status_label(status_code),
                    "name": name,
                    "total_length": total,
                    "completed_length": completed,
                    "progress": pct,
                    "speed": int(live.get("downloadSpeed") or 0),
                    "error": live.get("errorMessage") or "",
                    "live": True,
                })
            else:
                d.update({
                    "status": rec.get("status", "submitted"),
                    "status_text": self._status_label(rec.get("status", "submitted")),
                    "progress": "-",
                    "speed": 0,
                    "error": "",
                    "live": False,
                })
            rows.append(d)
        return rows

    def api_records(self):
        try:
            rows = self._build_records()
            return {"code": 0, "data": rows, "total": len(rows)}
        except Exception as e:  # noqa: BLE001
            return {"code": 1, "message": str(e), "data": []}

    def api_remove(self, gid: str = ""):
        if not gid:
            return {"success": False, "message": "缺少 gid"}
        try:
            client = self._get_client()
            client.force_remove(gid)
            return {"success": True, "message": f"已移除 {gid}"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def api_pause(self, gid: str = ""):
        if not gid:
            return {"success": False, "message": "缺少 gid"}
        try:
            client = self._get_client()
            client.pause(gid)
            return {"success": True, "message": f"已暂停 {gid}"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def api_unpause(self, gid: str = ""):
        if not gid:
            return {"success": False, "message": "缺少 gid"}
        try:
            client = self._get_client()
            client.unpause(gid)
            return {"success": True, "message": f"已继续 {gid}"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "message": str(e)}

    def get_api(self) -> list:
        return [
            {"path": "/records", "endpoint": self.api_records, "methods": ["GET"],
             "summary": "下载记录", "description": "获取 Aria2 下载记录与实时状态"},
            {"path": "/records/remove", "endpoint": self.api_remove, "methods": ["POST"],
             "summary": "移除任务", "description": "从 Aria2 移除下载任务", "auth": "bear"},
            {"path": "/records/pause", "endpoint": self.api_pause, "methods": ["POST"],
             "summary": "暂停任务", "description": "暂停 Aria2 下载任务", "auth": "bear"},
            {"path": "/records/unpause", "endpoint": self.api_unpause, "methods": ["POST"],
             "summary": "继续任务", "description": "继续 Aria2 下载任务", "auth": "bear"},
        ]

    def get_form(self) -> tuple:
        return [
            {"component": "VForm", "content": [
                {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}},
                {"component": "VTextField", "props": {
                    "model": "host", "label": "Aria2 RPC 地址",
                    "placeholder": "http://127.0.0.1:6800/jsonrpc"}},
                {"component": "VTextField", "props": {
                    "model": "secret", "label": "RPC Secret（Token）", "type": "password"}},
                {"component": "VTextField", "props": {
                    "model": "download_path", "label": "默认下载目录",
                    "placeholder": "/downloads"}},
            ]}
        ], {
            "enabled": False,
            "host": "http://127.0.0.1:6800/jsonrpc",
            "secret": "",
            "download_path": "",
        }

    def get_page(self) -> list:
        reach = self._client_reach()
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "success" if reach else "error",
                    "variant": "tonal",
                    "text": f"Aria2 下载器插件已{'启用' if self._enabled else '未启用'}。"
                            f"RPC {self._host} 连接：{'正常' if reach else '异常'}。"
                            f"下载器设置中已包含「{_DOWNLOADER_NAME}」，可在那里启用/设为默认。"
                            f"本插件仅在触发下载时把任务发送给 Aria2；下方为下载记录面板。",
                },
            },
            {
                "component": "VCard",
                "props": {"variant": "flat", "class": "mt-4", "color": "surface"},
                "content": [
                    {
                        "component": "VCardItem",
                        "content": [
                            {"component": "VCardTitle",
                             "props": {"class": "text-h6"}, "text": "下载记录"},
                        ],
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VTable",
                                "props": {
                                    "api": "plugin/aria2downloader/records",
                                    "headers": [
                                        {"title": "名称", "key": "name"},
                                        {"title": "状态", "key": "status_text"},
                                        {"title": "进度", "key": "progress"},
                                        {"title": "目录", "key": "download_dir"},
                                        {"title": "提交时间", "key": "submit_time"},
                                        {"title": "操作", "key": "actions", "sortable": False},
                                    ],
                                    "itemKey": "gid",
                                    "slots": {
                                        "item.actions": (
                                            '<div>'
                                            '<VBtn size="x-small" variant="text" color="primary" '
                                            'v-if="props.item.live && props.item.status===\'active\'" '
                                            '@click="aria2Pause(props.item.gid)">暂停</VBtn>'
                                            '<VBtn size="x-small" variant="text" color="primary" '
                                            'v-if="props.item.live && props.item.status===\'paused\'" '
                                            '@click="aria2Unpause(props.item.gid)">继续</VBtn>'
                                            '<VBtn size="x-small" variant="text" color="error" '
                                            'v-if="props.item.live" '
                                            '@click="aria2Remove(props.item.gid)">移除</VBtn>'
                                            '</div>'
                                        )
                                    },
                                    "script": (
                                        "function aria2Remove(gid){"
                                        "  window.MP.Api.post('plugin/aria2downloader/records/remove?gid='+encodeURIComponent(gid)).then(r=>{"
                                        "    if(r && r.success){ this.refreshTable && this.refreshTable(); }"
                                        "    else { window.MP.Message.error((r&&r.message)||'移除失败'); }"
                                        "  }); }"
                                        "function aria2Pause(gid){"
                                        "  window.MP.Api.post('plugin/aria2downloader/records/pause?gid='+encodeURIComponent(gid)).then(r=>{"
                                        "    if(r && r.success){ this.refreshTable && this.refreshTable(); }"
                                        "    else { window.MP.Message.error((r&&r.message)||'暂停失败'); }"
                                        "  }); }"
                                        "function aria2Unpause(gid){"
                                        "  window.MP.Api.post('plugin/aria2downloader/records/unpause?gid='+encodeURIComponent(gid)).then(r=>{"
                                        "    if(r && r.success){ this.refreshTable && this.refreshTable(); }"
                                        "    else { window.MP.Message.error((r&&r.message)||'继续失败'); }"
                                        "  }); }"
                                    ),
                                },
                            },
                        ],
                    },
                ],
            },
        ]

    def stop_service(self) -> None:
        self._enabled = False
        self._client = None

    # ---- 客户端 ----
    def _get_client(self) -> Aria2Client:
        if self._client is None:
            self._client = Aria2Client(host=self._host, secret=self._secret)
        return self._client

    def _client_reach(self) -> bool:
        try:
            return self._get_client().is_reachable()
        except Exception:
            return False

    def _should_handle(self, downloader: Optional[str]) -> bool:
        if not downloader:
            return False
        return downloader == _DOWNLOADER_NAME

    # ------------------------------------------------------------------
    # 下载器能力注入（get_module）
    # ------------------------------------------------------------------
    def get_module(self) -> dict:
        # 注册 download（触发下载时被动调用一次），
        # 以及 list_torrents / start_torrents / stop_torrents / remove_torrents：
        # 宿主下载管理界面会按需调用它们列出/控制 Aria2 任务（用户打开页面时才查询，
        # 属按需查询而非后台轮询）。
        if not self._enabled:
            return {}
        return {
            "download": self.download,
            "list_torrents": self.list_torrents,
            "start_torrents": self.start_torrents,
            "stop_torrents": self.stop_torrents,
            "remove_torrents": self.remove_torrents,
            "downloader_info": self.downloader_info,
            "transfer_completed": self.transfer_completed,
        }

    def download(self, content: Union[Path, str, bytes], download_dir: Path,
                 cookie: str, episodes: Set[int] = None, category: Optional[str] = None,
                 label: Optional[str] = None,
                 downloader: Optional[str] = None
                 ) -> Optional[tuple]:
        logger.info(f"[Aria2Downloader] download 被调用 downloader={downloader!r}")
        if not self._should_handle(downloader):
            return None
        logger.info(f"[Aria2Downloader] 收到下载地址 content={content} dir={download_dir}")
        client = self._get_client()
        dir_path = str(download_dir)
        options: dict = {}
        if cookie:
            options["header"] = [f"Cookie: {cookie}"]

        # 记录任务的展示名称（下载记录面板用）
        name = None
        try:
            if isinstance(content, bytes) or (isinstance(content, str) and content.endswith(".torrent")):
                if isinstance(content, str):
                    name = Path(content).name
                    with open(content, "rb") as f:
                        content = f.read()
                gid = client.add_torrent(content, download_dir=dir_path, options=options)
            elif isinstance(content, str) and (content.startswith("magnet:") or content.startswith("http") or content.startswith("ftp")):
                # 解析出友好文件名作为 Aria2 任务名/下载文件名（避免界面显示整条 URL）
                from urllib.parse import urlparse, unquote
                fname = unquote(urlparse(content).path.rsplit("/", 1)[-1]) or content
                options["out"] = fname
                name = fname
                gid = client.add_uri(content, download_dir=dir_path, options=options)
            else:
                return None, None, None, "Aria2 不支持的内容类型"
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Aria2Downloader] 添加下载失败: {e}")
            return None, None, None, f"Aria2 添加任务失败: {e}"

        if not gid:
            return None, None, None, "Aria2 添加任务失败"
        self._diag(f"添加成功 gid={gid}")

        # 写入本地下载记录（仅插件内，不影响宿主源码/数据）
        try:
            _dedup_append({
                "gid": gid,
                "name": name or "",
                "download_dir": dir_path,
                "category": category or "",
                "label": label or "",
                "submit_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "submitted",
            })
        except Exception as e:  # noqa: BLE001
            self._diag(f"记录下载任务失败: {e}")

        return downloader, gid, "Original", "Aria2 添加下载任务成功"

    # ------------------------------------------------------------------
    # 宿主下载管理界面所需的模块方法（按需查询 / 控制，非后台轮询）
    # ------------------------------------------------------------------
    def _normalize_status(self, raw: str) -> str:
        """把 Aria2 原生状态归一为宿主 DownloadTaskState。

        注意：paused/error 必须映射成 PAUSED，否则界面会把已暂停任务当成
        DOWNLOADING（显示「进行中」且只渲染暂停按钮），导致刷新后按钮与实际
        Aria2 状态错位、点击无效果。active/waiting 才是真实下载中。
        """
        mapping = {
            "active": DownloadTaskState.DOWNLOADING.value,
            "waiting": DownloadTaskState.DOWNLOADING.value,
            "paused": DownloadTaskState.PAUSED.value,
            "error": DownloadTaskState.PAUSED.value,
            "complete": DownloadTaskState.COMPLETED.value,
            "removed": DownloadTaskState.COMPLETED.value,
        }
        return mapping.get(raw, DownloadTaskState.DOWNLOADING.value)

    def _aria2_status_matches(self, raw: str, query) -> bool:
        """按宿主查询状态过滤 Aria2 任务。

        query 可能是 TorrentStatus（下载管理界面传入，值为「下载中」等中文）
        或 TorrentQueryStatus（值为 downloading/paused/...），这里统一按语义处理。
        """
        if query is None:
            return True
        q_val = query.value if hasattr(query, "value") else query
        if q_val in ("all", TorrentQueryStatus.ALL.value):
            return True
        # 下载中 / 「下载中」：所有未完成任务（含暂停）都算在下载列表里
        if q_val in ("downloading", "下载中"):
            return raw in ("active", "waiting", "paused")
        if q_val in ("paused", "已暂停"):
            return raw == "paused"
        if q_val in ("completed", "完成", "complete"):
            return raw in ("complete", "removed", "error")
        if q_val in ("transfer", "可转移", "可整理"):
            return raw in ("complete", "error")
        return True

    def _build_torrent(self, gid: str, raw_status: str, item: dict) -> DownloaderTorrent:
        total = int(item.get("totalLength") or 0)
        completed = int(item.get("completedLength") or 0)
        progress = (completed / total * 100) if total > 0 else 0.0
        files = item.get("files") or []
        # 优先取种子真实名称（bittorrent.info.name），否则取文件路径末段，避免显示 *.torrent
        btmeta = item.get("bittorrent") or {}
        info = btmeta.get("info") or {}
        name = info.get("name") or (files[0].get("path") or "").rsplit("/", 1)[-1] if files else gid
        if not name:
            name = gid
        dlspeed = int(item.get("downloadSpeed") or 0)
        upspeed = int(item.get("uploadSpeed") or 0)
        # 剩余时间估算
        left_time = None
        if dlspeed > 0 and total > completed:
            secs = (total - completed) / dlspeed
            left_time = self._human_seconds(secs)
        raw_path = files[0].get("path") if files else None
        raw_save_path = item.get("dir")
        # 应用下载器路径映射，把 Aria2 内部路径转成 MoviePilot 可访问路径
        path = self._normalize_return_path(raw_path)
        save_path = self._normalize_return_path(raw_save_path)
        return DownloaderTorrent(
            downloader=_DOWNLOADER_NAME,
            hash=gid,
            title=name,
            name=name,
            path=path,
            size=total,
            progress=round(progress, 1),
            state=self._normalize_status(raw_status),
            dlspeed=StringUtils.str_filesize(dlspeed) if dlspeed else None,
            upspeed=StringUtils.str_filesize(upspeed) if upspeed else None,
            left_time=left_time,
            save_path=save_path,
            content_path=path,
            category=save_path,
        )

    def _normalize_return_path(self, path: Optional[str]) -> Optional[str]:
        """
        将 Aria2 返回的下载器内部路径，按「下载器管理」里配置的 path_mapping
        映射为 MoviePilot 容器/进程可访问的存储路径。
        """
        if not path:
            return path
        try:
            configs = SystemConfigOper().get(SystemConfigKey.Downloaders) or []
            for conf in configs:
                if isinstance(conf, dict) and conf.get("name") == _DOWNLOADER_NAME:
                    for mapping in conf.get("path_mapping") or []:
                        if not isinstance(mapping, (list, tuple)) or len(mapping) < 2:
                            continue
                        storage_path, download_path = mapping[0], mapping[1]
                        mapped = self._replace_path_prefix(path, download_path, storage_path)
                        if mapped:
                            return mapped
                    break
        except Exception as e:  # noqa: BLE001
            self._diag(f"路径映射失败: {e}")
        return path

    @staticmethod
    def _replace_path_prefix(path: str, source: str, target: str) -> Optional[str]:
        """
        按完整路径段替换路径前缀，避免 /media 误匹配 /media2。
        与宿主 DownloaderBase 逻辑保持一致。
        """
        if not source or not source.strip() or not target or not target.strip():
            return None
        path_text = Path(path).as_posix()
        source_path = Path(source.strip()).as_posix()
        target_path = Path(target.strip()).as_posix()
        if path_text == source_path:
            return target_path
        source_prefix = f"{source_path.rstrip('/')}/"
        if path_text.startswith(source_prefix):
            suffix = path_text[len(source_prefix):]
            return (Path(target_path) / suffix).as_posix()
        return None

    @staticmethod
    def _human_seconds(secs: float) -> str:
        secs = int(secs)
        if secs < 0:
            secs = 0
        h, rem = divmod(secs, 3600)
        m, s = divmod(rem, 60)
        if h > 0:
            return f"{h}小时{m}分"
        if m > 0:
            return f"{m}分{s}秒"
        return f"{s}秒"

    def list_torrents(self, status: TorrentStatus = None,
                      hashs: Union[list, str] = None,
                      downloader: Optional[str] = None,
                      include_all_tags: bool = False
                      ) -> Optional[list]:
        """
        获取下载器任务列表（宿主下载管理界面调用）。
        :return: List[DownloaderTorrent]
        """
        if not self._enabled:
            logger.debug("[Aria2Downloader] list_torrents: 插件未启用，返回空")
            return []
        # 短期缓存：避免宿主界面轮询/后台高频调用反复打 Aria2 RPC（默认 60 秒）
        cache_key = (str(status), str(hashs) if hashs is not None else "", str(downloader))
        now = time.time()
        cached = self._torrent_cache.get(cache_key)
        if cached and now - cached[0] < self._list_cache_ttl:
            logger.debug(f"[Aria2Downloader] list_torrents 命中缓存 status={status}")
            return cached[1]
        logger.info(f"[Aria2Downloader] list_torrents 真实请求 Aria2 status={status} downloader={downloader}")
        try:
            client = self._get_client()
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Aria2Downloader] list_torrents 获取客户端失败: {e}")
            return []
        keys = ["gid", "status", "totalLength", "completedLength",
                "downloadSpeed", "uploadSpeed", "files", "dir", "bittorrent"]
        items: list = []
        # 三个列表分开拉取，单个失败不影响其它（尤其 tellActive 需要 secret 时）
        for fn_name, fn in (("tell_active", client.tell_active),
                            ("tell_wait", client.tell_wait),
                            ("tell_stopped", client.tell_stopped)):
            try:
                if fn_name == "tell_active":
                    items.extend(fn(keys))
                else:
                    items.extend(fn(0, 1000, keys))
            except Exception as e:  # noqa: BLE001
                logger.error(f"[Aria2Downloader] list_torrents {fn_name} 失败: {e}")
        logger.debug(f"[Aria2Downloader] list_torrents Aria2 返回原始任务数: {len(items)}")
        if not items:
            return []

        # 去重（同一 gid 可能同时出现在 active/paused 等不同查询）
        seen = set()
        deduped = []
        for it in items:
            g = it.get("gid")
            if g in seen:
                continue
            seen.add(g)
            deduped.append(it)

        # 已完成的任务写入下载历史（宿主下载历史界面读取），每个 gid 只写一次
        for it in deduped:
            if (it.get("status") or "active") == "complete":
                self._record_completed(it)
                # 转移成功后自动删除 Aria2 上的对应已完成任务：
                # complete 且映射后本地所有文件均已不存在（被 move/转移走）时，
                # 说明已成功转移，从 Aria2 移除任务（仅删任务，保留文件）。
                files = it.get("files") or []
                mapped = [self._normalize_return_path(f.get("path")) for f in files if f.get("path")]
                if mapped and all(p and not os.path.exists(p) for p in mapped):
                    try:
                        gid = it.get("gid")
                        client.force_remove(gid)
                        logger.info(f"[Aria2Downloader] 转移完成，已删除 Aria2 任务：{gid}")
                    except Exception as e:  # noqa: BLE001
                        self._diag(f"删除 Aria2 任务失败 {it.get('gid')}: {e}")

        # 按 hash 过滤
        if hashs:
            want = {hashs} if isinstance(hashs, str) else set(hashs)
            deduped = [it for it in deduped if it.get("gid") in want]

        # 按查询状态过滤（TorrentStatus 实际承载 TorrentQueryStatus 语义）
        query = status
        rows = []
        for it in deduped:
            raw = it.get("status") or "active"
            if query is not None and not self._aria2_status_matches(raw, query):
                continue
            rows.append(self._build_torrent(it.get("gid"), raw, it))
        logger.debug(f"[Aria2Downloader] list_torrents 过滤后返回 {len(rows)} 条")
        self._torrent_cache[cache_key] = (now, rows)
        return rows

    def _record_completed(self, item: dict) -> None:
        """把已完成的 Aria2 任务写入宿主 DownloadHistory（下载历史界面读取）。"""
        gid = item.get("gid")
        if not gid:
            return
        try:
            files = item.get("files") or []
            btmeta = item.get("bittorrent") or {}
            info = btmeta.get("info") or {}
            name = info.get("name") or (files[0].get("path") or "").rsplit("/", 1)[-1] or gid
            with get_session_factory()() as session:
                oper = DownloadHistoryOper(session)
                if oper.get_by_hash(gid):
                    return
                oper.add(
                    path=item.get("dir") or "",
                    type=MediaType.UNKNOWN.value,
                    title=name,
                    downloader=_DOWNLOADER_NAME,
                    download_hash=gid,
                    torrent_name=name,
                    date=time.strftime("%Y-%m-%d %H:%M:%S"),
                )
                session.commit()
            logger.info(f"[Aria2Downloader] 已写入下载历史: {name} ({gid})")
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Aria2Downloader] 写入下载历史失败: {e}")

    def start_torrents(self, hashs: Union[list, str],
                       downloader: Optional[str] = None) -> Optional[bool]:
        logger.info(f"[Aria2Downloader] start_torrents 被调用 hashs={hashs}")
        if not self._enabled:
            return None
        try:
            client = self._get_client()
            if isinstance(hashs, str):
                hashs = [hashs]
            for gid in hashs:
                # 预检真实状态：已在进行中则视为成功，避免对 active 重复 unpause
                try:
                    before = (client.tell_status(gid) or {}).get("status")
                except Exception:  # noqa: BLE001
                    before = None
                logger.info(f"[Aria2Downloader] 恢复前状态: gid={gid} status={before}")
                if before in ("active", "waiting"):
                    logger.info(f"[Aria2Downloader] 任务已是进行中，跳过 unpause: {gid}")
                    continue
                # 执行 unpause：部分 Aria2 构建在 unpause 已生效时仍可能返回
                # 错误码（如 gid 状态瞬时切换），故失败后再用 tell_status 确认
                # 是否已实际恢复，已恢复则视为成功（幂等）。
                unpause_ok = True
                try:
                    client.unpause(gid)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"[Aria2Downloader] unpause 异常（将二次确认）: {e}")
                    unpause_ok = False
                if not unpause_ok:
                    try:
                        now = (client.tell_status(gid) or {}).get("status")
                    except Exception:  # noqa: BLE001
                        now = None
                    logger.info(f"[Aria2Downloader] unpause 后二次确认状态: gid={gid} status={now}")
                    if now in ("active", "waiting"):
                        logger.info(f"[Aria2Downloader] unpause 实际已生效，视为成功: {gid}")
                        unpause_ok = True
                if not unpause_ok:
                    raise RuntimeError(f"unpause 失败且任务未恢复: {gid}")
            # 状态已变更，清空列表缓存让界面立即反映真实状态
            self._torrent_cache.clear()
            return True
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Aria2Downloader] start_torrents 失败: {e}")
            return False

    def stop_torrents(self, hashs: Union[list, str],
                      downloader: Optional[str] = None) -> Optional[bool]:
        logger.info(f"[Aria2Downloader] stop_torrents 被调用 hashs={hashs}")
        if not self._enabled:
            return None
        try:
            client = self._get_client()
            if isinstance(hashs, str):
                hashs = [hashs]
            for gid in hashs:
                # 幂等：已是 paused 则无需再 pause。
                try:
                    before = (client.tell_status(gid) or {}).get("status")
                except Exception:  # noqa: BLE001
                    before = None
                logger.info(f"[Aria2Downloader] 暂停前状态: gid={gid} status={before}")
                if before == "paused":
                    logger.info(f"[Aria2Downloader] 任务已是暂停，跳过 pause: {gid}")
                    continue
                client.pause(gid)
                try:
                    after = (client.tell_status(gid) or {}).get("status")
                except Exception:  # noqa: BLE001
                    after = None
                logger.info(f"[Aria2Downloader] 暂停后状态: gid={gid} status={after}")
            # 状态已变更，清空列表缓存让界面立即反映真实状态
            self._torrent_cache.clear()
            return True
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Aria2Downloader] stop_torrents 失败: {e}")
            return False

    def remove_torrents(self, hashs: Union[str, list], delete_file: Optional[bool] = True,
                        downloader: Optional[str] = None) -> Optional[bool]:
        """
        删除下载器任务（宿主下载管理界面删除按钮调用）。
        只移除 Aria2 任务记录（保留文件，避免误删网盘/已整理文件）。
        返回 True 表示成功（含任务已不存在的幂等情况）。
        """
        logger.info(f"[Aria2Downloader] remove_torrents 被调用 hashs={hashs} delete_file={delete_file}")
        if not self._enabled:
            return None
        try:
            client = self._get_client()
            if isinstance(hashs, str):
                hashs = [hashs]
            ok_all = True
            for gid in hashs:
                # 删除 Aria2 任务：先 remove，失败再 force_remove
                try:
                    client.remove(gid)
                except Exception:  # noqa: BLE001
                    client.force_remove(gid)
                logger.info(f"[Aria2Downloader] 已删除 Aria2 任务：{gid}")
            # 删除后清空列表缓存，避免界面还显示已删任务
            self._torrent_cache.clear()
            return ok_all
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Aria2Downloader] remove_torrents 失败: {e}")
            return False

    def downloader_info(self, downloader: Optional[str] = None) -> Optional[list]:
        """
        下载器监控信息（宿主仪表板「下载器监控」卡片调用）。
        :return: List[DownloaderInfo]
        """
        if not self._enabled:
            return []
        try:
            client = self._get_client()
            stat = client.get_global_stat() or {}
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Aria2Downloader] downloader_info 获取失败: {e}")
            return []
        try:
            dl_speed = int(stat.get("downloadSpeed") or 0)
            up_speed = int(stat.get("uploadSpeed") or 0)
            dl_size = int(stat.get("downloadLength") or 0)
            up_size = int(stat.get("uploadLength") or 0)
        except (TypeError, ValueError):
            dl_speed = up_speed = dl_size = up_size = 0
        return [DownloaderInfo(
            download_speed=dl_speed,
            upload_speed=up_speed,
            download_size=dl_size,
            upload_size=up_size,
        )]

    def transfer_completed(self, hashs: Union[str, list], downloader: Optional[str] = None) -> None:
        """
        整理完成后回调（宿主「目录整理 / 下载器监控」自动整理时调用）。
        Aria2 没有标签系统，这里用「移除已完成任务（保留文件）」作为已整理标记，
        避免任务长期留在可转移列表里、被重复整理。
        :param hashs: 任务 gid（或 gid 列表）
        :param downloader: 下载器名称（冗余参数，兼容宿主调用）
        """
        if not self._enabled:
            return
        if isinstance(hashs, str):
            hashs = [hashs]
        logger.info(f"[Aria2Downloader] 转移完成，准备删除对应下载任务：{hashs}")
        try:
            client = self._get_client()
            removed = []
            for gid in hashs:
                ok = False
                try:
                    # 已完成任务需先暂停才能 remove；失败则强制移除
                    try:
                        client.pause(gid)
                    except Exception:  # noqa: BLE001
                        pass
                    client.remove(gid)
                    ok = True
                except Exception:  # noqa: BLE001
                    try:
                        client.force_remove(gid)
                        ok = True
                    except Exception as e:  # noqa: BLE001
                        logger.debug(f"[Aria2Downloader] 删除任务 {gid} 失败: {e}")
                if ok:
                    removed.append(gid)
            logger.info(f"[Aria2Downloader] 已删除任务：{removed}")
        except Exception as e:  # noqa: BLE001
            logger.error(f"[Aria2Downloader] transfer_completed 失败: {e}")
