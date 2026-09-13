import concurrent.futures
import datetime
import json
import os
import re
import shutil
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from app import schemas
from app.chain.transfer import TransferChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import Event, eventmanager
from app.core.metainfo import MetaInfo
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.db.models.transferhistory import TransferHistory
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.downloader import DownloaderHelper
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import WebhookEventInfo
from app.schemas.types import (EventType, MediaImageType, MediaSource,
                               MediaType, NotificationType)
from app.utils.http import RequestUtils
from app.utils.system import SystemUtils
from apscheduler.schedulers.background import BackgroundScheduler


class ConfigParser:
    """配置解析器类，用于解析各种格式的配置字符串"""

    @staticmethod
    def parse_path_mapping(config_str: str, separator: str = "#") -> List[Dict]:
        """
        通用路径映射解析方法

        Args:
            config_str: 配置字符串
            separator: 分隔符

        Returns:
            List[Dict]: 解析后的映射列表
        """
        mappings = []
        if not config_str:
            return mappings

        for line in config_str.split("\n"):
            line = line.strip()
            if line and separator in line:
                parts = [p.strip() for p in line.split(separator)]
                if len(parts) >= 2:
                    mappings.append({
                        "path": parts[0],
                        "account_id": parts[1],
                        "account_name": parts[2] if len(parts) > 2 else parts[1]
                    })
        return mappings

    @staticmethod
    def parse_key_value_pairs(config_str: str, separator: str = "=") -> Dict[str, str]:
        """
        解析键值对配置

        Args:
            config_str: 配置字符串
            separator: 分隔符

        Returns:
            Dict[str, str]: 解析后的键值对字典
        """
        result = {}
        if not config_str:
            return result

        for line in config_str.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(separator, 1)
            if len(parts) == 2:
                result[parts[0].strip()] = parts[1].strip()
        return result

    @staticmethod
    def parse_list(config_str: str, separator: str = "#") -> List[str]:
        """
        解析列表配置

        Args:
            config_str: 配置字符串
            separator: 分隔符

        Returns:
            List[str]: 解析后的列表
        """
        if not config_str:
            return []
        return [item.strip() for item in config_str.split(separator) if item.strip()]


class embysyncdeletioncloud(_PluginBase):
    # 插件名称
    plugin_name = "Emby同步删除云盘"
    # 插件描述
    plugin_desc = "Emby媒体删除时自动删除本地和云盘文件。"
    # 插件图标
    plugin_icon = "mediasyncdel.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "embysyncdeletioncloud_"
    # 加载顺序
    plugin_order = 9
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler: Optional[BackgroundScheduler] = None
    _enabled = False
    _notify = False
    _del_source = False
    _del_history = False
    _local_library_path = None
    _mount_paths = {}  # 网盘路径
    _transferchain = None
    _downloader_helper = None
    _transferhis = None
    _downloadhis = None
    _mediaserver_helper = None
    _mediaserver = None
    _mediaservers = None

    # CAS 相关属性
    _cas_enabled: bool = False
    _cas_host: Optional[str] = None
    _cas_api_key: Optional[str] = None
    # CAS 删除任务开关
    _cas_delete_normal_task_enabled: bool = False
    _cas_delete_crystal_task_enabled: bool = False

    _cas_debug_log: bool = False
    _cas_session: requests.Session = None
    _cas_event_cache = {}
    _cas_thread_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
    _cas_mappings: List[Dict[str, str]] = []  # 存储CAS路径映射

    # OpenList 相关属性（独立直连，不依赖系统 StorageChain）
    _openlist_enabled: bool = False
    _openlist_host: Optional[str] = None
    _openlist_token_raw: Optional[str] = None  # 用户配置的管理令牌（直接作为 Authorization 使用）
    _openlist_path_mapping: List[Dict[str, str]] = []
    _openlist_token: Optional[str] = None  # 登录后缓存的 Authorization token
    _openlist_token_invalid: bool = False  # 令牌是否已失效（401），用于主动提示
    _openlist_session: requests.Session = None

    # 新增：记录已处理事件的集合和时间戳字典
    _handled_delete_events = set()
    _handled_delete_events_timestamps = {}  # 存储每个事件的时间戳

    # 新增：媒体类型识别缓存
    _media_type_cache = {}

    def init_plugin(self, config: dict = None):
        """
        初始化插件配置

        这是插件的主要初始化方法，负责：
        1. 初始化核心服务组件
        2. 初始化数据结构和缓存
        3. 创建网络会话和线程池
        4. 加载和解析配置参数
        5. 获取媒体服务器信息
        6. 测试外部服务连接

        Args:
            config (dict, optional): 插件配置字典，包含所有用户设置的参数
        """
        # ==================== 1. 初始化核心服务组件 ====================
        self._transferchain = TransferChain()  # 传输链服务 - 负责媒体文件的传输操作
        self._downloader_helper = DownloaderHelper()  # 下载器助手 - 管理下载器服务
        self._transferhis = TransferHistoryOper()  # 转移历史操作 - 管理转移历史记录
        self._downloadhis = DownloadHistoryOper()  # 下载历史操作 - 管理下载历史记录
        self._mediaserver_helper = MediaServerHelper()  # 媒体服务器助手 - 管理媒体服务器连接
        self._openlist_session = requests.Session()  # OpenList独立会话

        # ==================== 初始化时检查OpenList连接状态 ====================
        logger.info("插件初始化开始，OpenList改为独立直连，不再依赖系统StorageChain...")
        logger.info("插件初始化完成")

        # ==================== 2. 初始化数据结构和缓存 ====================
        self._handled_delete_events = set()  # 已处理事件集合 - 用于防止重复处理相同的删除事件
        self._mount_paths = {}  # 挂载路径映射 - 存储远程路径到本地路径的映射关系
        self._cas_mappings = []  # CAS路径映射 - 存储CAS服务的路径配置
        self._openlist_path_mapping = []  # OpenList路径映射 - 存储OpenList服务的路径配置

        # ==================== 3. 初始化网络会话 ====================
        self._cas_session = requests.Session()  # 创建CAS服务的网络会话
        self._cas_session.headers.update(
            {"User-Agent": "CASTaskCleaner/1.8"})  # 设置CAS User-Agent
        self._openlist_session = requests.Session()  # 创建OpenList服务的网络会话
        self._openlist_session.headers.update(
            {"User-Agent": "OpenListClient/1.0"})  # 设置OpenList User-Agent

        # ==================== 4. 初始化线程池 ====================
        self._cas_thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=5)  # 创建CAS线程池，最大工作线程数为5，用于并发处理删除任务
        logger.debug("CAS线程池初始化完成")  # 记录调试日志

        # ==================== 5. 加载配置参数 ====================
        if config:  # 如果有配置参数传入，则进行配置加载
            # -------------------- 5.1 加载核心设置 --------------------
            self._enabled = config.get("enabled")  # 插件启用状态
            self._notify = config.get("notify")  # 是否发送通知
            self._del_source = config.get("del_source")  # 是否删除源文件
            self._del_history = config.get("del_history")  # 是否删除历史记录
            self._local_library_path = config.get(
                "local_library_path")  # 本地媒体库路径映射配置
            logger.info(f"加载本地媒体库路径映射: {self._local_library_path}")
            self._mediaservers = config.get("mediaservers") or []  # 媒体服务器列表配置

            # -------------------- 5.2 加载CAS服务设置 --------------------
            self._cas_enabled = config.get("cas_enabled", False)  # CAS服务启用状态
            self._cas_host = config.get("cas_host", "")  # CAS服务主机地址
            self._cas_api_key = config.get("cas_api_key", "")  # CAS服务API密钥
            self._cas_debug_log = False  # cas_debug_log 使用默认值False
            self._cas_delete_normal_task_enabled = config.get(
                "cas_delete_normal_task_enabled", False)  # 普通任务删除功能开关
            self._cas_delete_crystal_task_enabled = config.get(
                "cas_delete_crystal_task_enabled", False)  # 玄晶任务删除功能开关

            # -------------------- 5.4 加载OpenList服务设置（独立直连） --------------------
            self._openlist_enabled = config.get(
                "openlist_enabled", False)  # OpenList服务启用状态
            self._openlist_host = (config.get("openlist_host", "") or "").strip()  # OpenList地址
            self._openlist_token_raw = (config.get("openlist_token", "") or "").strip()  # 管理令牌（仅支持令牌方式）

            # -------------------- 5.4.1 加载保护目录设置 --------------------
            self._protected_directories = ConfigParser.parse_list(
                config.get("protected_directories", "").strip()
            )
            logger.debug(f"加载保护目录: {self._protected_directories}")

            # -------------------- 5.5 解析CAS路径映射配置 --------------------
            cas_path_mapping = config.get("cas_path_mapping", "")
            self._cas_mappings = ConfigParser.parse_path_mapping(
                cas_path_mapping)
            for mapping in self._cas_mappings:
                logger.debug(
                    f"加载CAS路径映射: {mapping['path']} -> 账户ID {mapping['account_id']} 名称 {mapping['account_name']}")

            # -------------------- 5.6 解析OpenList路径映射配置 --------------------
            # 格式：本地前缀#云盘前缀（每行一对），示例：
            #   /STRM影视/网盘/天翼云盘完结/家庭4#/天翼家庭4/家庭4
            # 表示：transferhis真实路径中的'/STRM影视/网盘/天翼云盘完结/家庭4'
            #      替换为 alist 中的'/天翼家庭4/家庭4'
            openlist_path_mapping = config.get("openlist_path_mapping", "")
            self._openlist_path_mapping = []
            if openlist_path_mapping:
                for line_num, line in enumerate(openlist_path_mapping.split("\n"), start=1):
                    try:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        parts = [p.strip() for p in line.split("#") if p.strip()]
                        if len(parts) >= 2:
                            self._openlist_path_mapping.append({
                                "local_path": parts[0],
                                "cloud_path": parts[1]
                            })
                            logger.info(f"加载OpenList路径映射: {parts[0]} -> {parts[1]}")
                        else:
                            logger.warning(f"OpenList路径映射第{line_num}行格式错误(需 local#cloud): {line}")
                    except Exception as e:
                        logger.error(
                            f"解析OpenList路径映射第{line_num}行时出错: {str(e)}")

            # -------------------- 5.6.1 启动自动测试OpenList连接 --------------------
            if self._openlist_enabled:
                self._auto_test_openlist()

            # -------------------- 5.7 获取媒体服务器配置 --------------------
            if self._mediaservers:
                self._mediaserver = [self._mediaservers[0]]

            # -------------------- 5.9 清理历史记录（如果配置要求） --------------------
            if self._del_history:
                self.del_data(key="history")
                self._del_history = False
                config["del_history"] = False

            # -------------------- 5.10 初始化本地挂载路径映射 --------------------
            mount_paths = config.get("mount_paths", "")
            self._mount_paths = ConfigParser.parse_key_value_pairs(mount_paths)
            for remote_path, local_path in self._mount_paths.items():
                logger.debug(f"加载挂载路径映射: {remote_path} -> {local_path}")

            # -------------------- 5.11 更新插件配置 --------------------
            # 将挂载路径映射转换为配置字符串格式
            mount_paths_config = ""
            for remote_path, local_path in self._mount_paths.items():
                mount_paths_config += f"{remote_path}={local_path}\n"

            # 将保护目录集合转换为配置字符串格式（使用#符号分隔）
            protected_dirs_config = "#".join(
                sorted(self._protected_directories))

            self.update_config(  # 将当前配置保存到插件配置中
                {
                    "enabled": self._enabled,
                    "notify": self._notify,
                    "del_source": self._del_source,
                    "del_history": self._del_history,
                    "local_library_path": self._local_library_path,
                    "cas_path_mapping": cas_path_mapping,
                    "openlist_path_mapping": openlist_path_mapping,
                    "mediaservers": self._mediaserver,
                    "cas_enabled": self._cas_enabled,
                    "cas_host": self._cas_host,
                    "cas_api_key": self._cas_api_key,
                    "cas_debug_log": self._cas_debug_log,
                    "openlist_enabled": self._openlist_enabled,
                    "openlist_host": self._openlist_host,
                    "openlist_token": self._openlist_token_raw,
                    "mount_paths": mount_paths_config.strip(),  # 保存挂载路径配置
                    "protected_directories": protected_dirs_config,  # 保存保护目录配置
                    "music_mapping": "",  # 音乐路径映射已废弃，清空遗留配置

                    "cas_delete_normal_task_enabled": self._cas_delete_normal_task_enabled,
                    "cas_delete_crystal_task_enabled": self._cas_delete_crystal_task_enabled
                }
            )

        # ==================== 6. 获取媒体服务器信息 ====================
        # 媒体服务器配置已通过_mediaserver_helper获取，无需额外处理

        # ==================== 7. 测试外部服务连接 ====================
        if self._cas_enabled:  # 如果CAS服务已启用，测试CAS连接
            self._test_cas_connection()
        if self._openlist_enabled:  # 如果OpenList服务已启用，测试OpenList连接
            self._test_openlist_connection()

    def _safe_request(self, method: str, url: str, auth_required: bool = False, **kwargs) -> Optional[requests.Response]:
        """
        重试API请求。
        auth_required=True 时使用 OpenList 独立会话（携带 Authorization），
        否则使用 CAS 会话。
        """
        session = self._openlist_session if auth_required else self._cas_session
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = session.request(
                    method,
                    url,
                    timeout=10,
                    **kwargs
                )
                if self._cas_debug_log and not auth_required:
                    logger.debug(
                        f"请求成功: {method} {url} (尝试 {attempt+1}/{max_retries})")
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"请求失败: {method} {url} (尝试 {attempt+1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2)

        logger.error(f"所有请求尝试均失败: {method} {url}")
        return None

    def _test_cas_connection(self):
        """测试CAS服务连接（新版，变量名与用户要求一致）"""
        try:
            logger.debug("测试CAS服务连接...")
            headers = {"x-api-key": self._cas_api_key}
            res = self._safe_request(
                "GET",
                f"{self._cas_host.rstrip('/')}/api/tasks",
                headers=headers,
                params={"page": 1, "pageSize": 1}
            )

            if not res:
                logger.error("CAS服务连接失败: 请求无响应")
                return

            if res.status_code == 200:
                logger.info("CAS服务连接成功")
            else:
                logger.error(f"CAS服务连接失败，状态码: {res.status_code}")
                if res.text:
                    logger.error(f"错误响应: {res.text[:500]}")
        except Exception as e:
            logger.error(f"CAS服务连接异常: {str(e)}")

    def _openlist_login(self) -> bool:
        """
        连接 OpenList / alist 服务，使用管理令牌(access token)直接建立 Authorization。
        仅支持令牌方式：配置了 openlist_token 后直接用作 Authorization（无 Bearer 前缀），
        不发起登录请求。返回是否成功建立连接。
        """
        if not self._openlist_host:
            logger.error("OpenList地址未配置，无法连接")
            return False
        if not self._openlist_token_raw:
            logger.error("OpenList未配置管理令牌，无法连接（仅支持令牌方式）")
            return False
        self._openlist_token = self._openlist_token_raw
        self._openlist_session.headers.update({"Authorization": self._openlist_token})
        self._openlist_token_invalid = False
        logger.info("OpenList使用管理令牌连接成功")
        return True

    def _openlist_api(self, method: str, path: str, **kwargs):
        """
        通过独立的 OpenList 会话发起 API 请求。
        path 为 /api/... 完整路径（不含 host）。
        返回 (success: bool, response_or_error_msg)
        """
        if not self._openlist_host:
            return False, "OpenList地址未配置", None
        # 确保已建立连接（令牌方式）
        if not self._openlist_token:
            if not self._openlist_login():
                return False, "OpenList连接失败", None
        url = f"{self._openlist_host.rstrip('/')}{path}"
        res = self._safe_request(method, url, auth_required=True, **kwargs)
        # 令牌模式下管理令牌为静态值，401 表示令牌失效/权限不足，不再重试，直接返回失败
        if res is not None and res.status_code == 401:
            self._openlist_token_invalid = True
            logger.error("OpenList返回401，令牌可能失效或权限不足，请检查管理令牌是否正确/是否已过期")
        if res is None:
            return False, "OpenList请求无响应", None
        # 解析响应体 code（官方：code/message/data 结构）
        body_code = None
        try:
            body_code = res.json().get("code")
        except Exception:
            pass
        return True, res, body_code

    def _openlist_list_dir(self, path: str) -> Optional[list]:
        """
        列出 OpenList 目录内容，返回条目列表（含 name/is_dir）或 None。
        按官方规范 per_page 上限为 100，故分页拉取直到取完，避免大目录漏删/空目录误判。
        """
        all_items: List[dict] = []
        page = 1
        per_page = 100  # 官方 FsListRequest.per_page 最大 100
        while True:
            ok, res, body_code = self._openlist_api(
                "POST", "/api/fs/list",
                json={"path": path, "page": page, "per_page": per_page}
            )
            if not ok or res.status_code != 200:
                logger.error(f"OpenList列目录失败: {path} -> {res.status_code if res else ''} {res.text[:300] if res else ''}")
                return None
            # 业务成功以响应体 code==200 为准（官方 ApiResponse 结构）
            if body_code is not None and body_code != 200:
                logger.error(f"OpenList列目录业务失败: {path}, code={body_code}")
                return None
            try:
                data = res.json()
            except Exception:
                return None
            page_data = (data.get("data", {}) or {})
            content = page_data.get("content", []) or []
            all_items.extend(content)
            total = page_data.get("total", 0) or 0
            if len(all_items) >= total or not content:
                break
            page += 1
        return all_items

    def _openlist_get(self, path: str) -> Optional[dict]:
        """
        调用官方 /api/fs/get 获取单个文件/目录的信息。
        返回 data 字典（含 name/is_dir/size 等），不存在或失败时返回 None。
        用于在不列举整目录的情况下精确判断某路径是否存在、是否为目录。
        """
        ok, res, body_code = self._openlist_api(
            "POST", "/api/fs/get",
            json={"path": path}
        )
        if not ok or res.status_code != 200:
            logger.debug(f"OpenList获取信息失败: {path} -> {res.status_code if res else ''} {res.text[:200] if res else ''}")
            return None
        if body_code is not None and body_code != 200:
            logger.debug(f"OpenList获取信息业务失败: {path}, code={body_code}")
            return None
        try:
            data = res.json()
        except Exception:
            return None
        return data.get("data")

    def _openlist_search(self, keyword: str, parent: str = "/", max_results: int = 20) -> List[dict]:
        """
        调用官方 /api/fs/search 按关键词搜索文件/目录。
        当 Emby 的 Mount Path 与云盘真实路径映射不上时，可凭文件名反查真实路径。
        返回命中条目列表（data.content），失败时返回空列表。
        """
        ok, res, body_code = self._openlist_api(
            "POST", "/api/fs/search",
            json={"parent": parent, "keywords": keyword, "page": 1, "per_page": max_results}
        )
        if not ok or res.status_code != 200:
            logger.error(f"OpenList搜索失败: {keyword} -> {res.status_code if res else ''} {res.text[:300] if res else ''}")
            return []
        if body_code is not None and body_code != 200:
            logger.error(f"OpenList搜索业务失败: {keyword}, code={body_code}")
            return []
        try:
            data = res.json()
        except Exception:
            return []
        page_data = (data.get("data", {}) or {})
        return page_data.get("content", []) or []

    def _openlist_remove(self, dir_path: str, names: List[str]) -> bool:
        """
        调用 alist 的 /api/fs/remove 删除一个或多个条目。
        按官方规范 FsRemoveRequest: {dir: 父目录, names: [名称列表]}
        """
        ok, res, body_code = self._openlist_api(
            "POST", "/api/fs/remove",
            json={"dir": dir_path, "names": names}
        )
        if not ok or res.status_code != 200:
            logger.error(f"OpenList删除请求失败: {dir_path} / {names} -> {res.status_code if res else ''} {res.text[:300] if res else ''}")
            return False
        # 业务成功以响应体 code==200 为准（官方 ApiResponse 结构）
        if body_code is not None and body_code != 200:
            logger.error(f"OpenList删除业务失败: {dir_path} / {names}, code={body_code}, msg={res.text[:300] if res else ''}")
            return False
        return True

    def _openlist_remove_empty_directory(self, src_dir: str) -> bool:
        """
        调用官方 /api/fs/remove_empty_directory 递归清理 src_dir 下的空目录链。
        按规范只需传 {src_dir}，服务端会自动向上递归删除所有空目录。
        """
        ok, res, body_code = self._openlist_api(
            "POST", "/api/fs/remove_empty_directory",
            json={"path": src_dir}
        )
        if not ok or res.status_code != 200:
            logger.error(f"OpenList递归清理空目录请求失败: {src_dir} -> {res.status_code if res else ''} {res.text[:300] if res else ''}")
            return False
        if body_code is not None and body_code != 200:
            # code=500 通常表示起始目录本身非空（下面还有其它文件/目录），
            # 属预期行为，无需清理，降级为 DEBUG 避免刷 ERROR
            if body_code == 500:
                logger.debug(f"OpenList递归清理空目录跳过: {src_dir} 非空或含其它内容，无需清理 (code=500)")
                return True
            logger.error(f"OpenList递归清理空目录业务失败: {src_dir}, code={body_code}")
            return False
        logger.info(f"OpenList递归清理空目录成功: {src_dir}")
        return True

    def _test_openlist_connection(self):
        """测试OpenList服务连接 - 独立直连，不依赖系统StorageChain"""
        try:
            logger.debug("测试OpenList服务连接（独立直连）...")
            if not self._openlist_host:
                logger.error("OpenList服务地址未配置")
                return
            if self._openlist_login():
                content = self._openlist_list_dir("/")
                if content is not None:
                    logger.info("OpenList服务连接成功")
                else:
                    logger.warning("OpenList登录成功但列根目录失败，可能权限不足")
            else:
                logger.error("OpenList服务连接失败，请检查地址/账号/密码")
        except Exception as e:
            logger.error(f"OpenList服务连接异常: {str(e)}")

    # ==================== 接收EMBY删除通知 ====================
    @eventmanager.register(EventType.WebhookMessage)
    def sync_del_by_plugin(self, event):
        """
        接收EMBY删除通知，不能调用别的函数获取媒体信息，不然启动或出现重复任务
        该函数处理来自EMBY的媒体删除事件，根据事件信息执行相应的删除操作
        """
        if not self._enabled:
            return  # 如果插件未启用，直接返回不做处理

        media_suffix = None  # 媒体文件后缀
        media_storage = None  # 媒体存储位置

        event_data: WebhookEventInfo = event.event_data  # 获取事件数据
        event_type = event_data.event  # 获取事件类型

        # 神医助手深度删除标识
        if not event_type or str(event_type) != "deep.delete":
            return  # 如果不是深度删除事件，直接返回

        logger.debug(f"接收到删除通知事件: {event_data}")  # 记录调试信息

        # 第一步：构建唯一标识符，用于防止重复处理（只使用最基本的必要信息进行去重检查）
        media_name = event_data.item_name  # 媒体名称
        media_path = event_data.item_path  # 媒体路径
        media_type = event_data.media_type  # 媒体类型
        item_type = event_data.item_type  # 项目类型
        season_num = event_data.season_id  # 季数
        episode_num = event_data.episode_id  # 集数
        event_image_url = event_data.image_url or ""  # 提前定义event_image_url，如果不存在则为空字符串

        # 如果media_name为空，尝试从json_object中获取
        if not media_name and hasattr(event_data, 'json_object') and event_data.json_object:
            json_obj = event_data.json_object
            item_data = json_obj.get('Item', {})
            if item_data:
                media_name = item_data.get('Name')
                logger.debug(f"从json_object.Item.Name获取媒体名称: {media_name}")

        # 只使用event_data中直接提供的基本信息进行去重检查
        # media_path归一化：去掉末级文件名/目录名，取父目录作为去重基准，
        # 避免Emby对同一部电影/剧集同时推送"文件"和"目录"两条webhook导致重复处理。
        # 同名不同条目仍可通过media_name+媒体类型+季集号区分。
        dedupe_path = os.path.dirname(media_path) if media_path else media_path
        unique_key = f"{media_name}:{media_type or item_type}:{dedupe_path}:{season_num or ''}:{episode_num or ''}"

        # 清理过期的已处理事件记录（超过5分钟的记录）
        self._cleanup_expired_handled_events()

        if unique_key in self._handled_delete_events:  # 检查是否已处理过该事件
            logger.debug(f"跳过重复删除任务: {unique_key}")  # 记录调试信息
            return  # 如果是重复事件，直接返回

        # 验证必要参数
        if not media_name:  # 检查媒体名称是否存在
            logger.warn("接收到的删除通知缺少媒体名称，跳过处理")  # 记录警告信息
            return  # 如果没有媒体名称，直接返回

        if not media_path:  # 检查媒体路径是否存在
            logger.warn(f"媒体 {media_name} 的删除通知缺少媒体路径，跳过处理")  # 记录警告信息
            return  # 如果没有媒体路径，直接返回

        # 验证item_type：处理MOV、TV和AUD（音乐）类型
        if not item_type or item_type not in ["MOV", "TV", "AUD"]:
            logger.warn(
                f"跳过处理非媒体类型事件: item_type={item_type}, 媒体名称={media_name}")
            return

        # 使用基础信息判断媒体类型
        if media_type == "Episode":  # 如果是单集
            media_type_category = "episode"
        elif media_type == "Season":  # 如果是季
            media_type_category = "season"
        elif media_type == "Series":  # 如果是剧集
            media_type_category = "series"
        elif media_type == "Movie" or item_type == "MOV":  # 如果是电影
            media_type_category = "movie"
        elif media_type == "Audio" or item_type == "AUD":  # 如果是音乐
            media_type_category = "music"
        else:
            # 根据季集信息判断
            if season_num and episode_num:  # 如果有季数和集数
                media_type_category = "episode"
            elif season_num and not episode_num:  # 如果只有季数没有集数
                media_type_category = "season"
            elif item_type == "MOV":  # 如果项目类型是电影
                media_type_category = "movie"
            elif item_type == "AUD":  # 如果项目类型是音乐
                media_type_category = "music"
            else:  # 其他情况视为剧集
                media_type_category = "series"

        # 将媒体类型转换为中文显示
        media_type_chinese = {
            "movie": "电影",
            "series": "剧集",
            "season": "季",
            "episode": "集",
            "unknown": "未知"
        }.get(media_type_category, media_type_category)  # 获取中文媒体类型

        self._handled_delete_events.add(unique_key)  # 添加到已处理事件集合
        # 记录事件处理时间戳
        self._handled_delete_events_timestamps[unique_key] = time.time()
        logger.info(
            f"开始处理删除通知: {media_name} (媒体类型: {media_type_chinese})")  # 记录信息

        # 根据媒体类型设置标志
        is_single_episode = (media_type_category == "episode")  # 是否为单集
        is_season = (media_type_category == "season")  # 是否为季
        is_series = (media_type_category == "series")  # 是否为剧集

        # 提取媒体文件路径 获取文件的后缀
        media_suffix = Path(
            media_path).suffix if media_path else ""  # 获取媒体文件后缀

        # 匹配媒体存储模块
        if media_type_category == "music":  # 如果是音乐类型
            # 直接从事件数据中提取Mount Paths
            music_cloud_path = self.__get_music_mount_path(event_data)
            if music_cloud_path:
                media_storage = "music"  # 设置存储类型为音乐
                logger.debug(f"音乐Mount Paths提取成功: {music_cloud_path}")  # 记录信息
            else:
                logger.warning(f"音乐事件中未找到有效的Mount Paths: {media_path}")  # 记录警告
                return  # 如果提取失败，直接返回
        elif self._local_library_path:  # 如果是其他媒体类型且配置了本地媒体库路径
            status, matched_path = self.__get_local_media_path(
                media_path)  # 获取匹配的本地路径
            if status:  # 如果匹配成功
                media_storage = "local"  # 设置存储类型为本地
                logger.debug(
                    f"本地路径匹配成功: {media_path} -> 匹配路径: {matched_path}")  # 记录信息
            else:
                logger.warning(f"媒体路径 {media_path} 未在配置的本地媒体库路径中找到匹配项")  # 记录警告
                return  # 如果匹配失败，直接返回
        else:
            logger.warning("未配置本地媒体库路径映射，无法处理删除请求")  # 记录警告
            return  # 如果没有配置本地媒体库路径，直接返回

        # 统一获取TMDB ID（音乐类型直接返回None，不需要查询）
        if media_type_category == "music":
            tmdb_id = None
            logger.debug("音乐类型跳过TMDB ID查询")
        else:
            tmdb_id = self._get_tmdb_id(
                event_data, media_name, media_type_category)  # 获取TMDB ID

        try:
            # 调用内部同步删除方法
            self.__sync_del(
                media_type=media_type_category,  # 媒体类型
                media_name=media_name,  # 媒体名称
                media_path=media_path,  # 媒体路径
                tmdb_id=int(tmdb_id) if tmdb_id is not None else None,
                season_num=season_num,  # 季数
                episode_num=episode_num,  # 集数
                media_storage=media_storage,  # 媒体存储类型
                media_suffix=media_suffix,  # 媒体文件后缀
                event_image_url=event_image_url,  # 事件图片URL
                event_data=event_data  # 通知事件数据
            )
            logger.info(f"删除事件处理完成: {media_name}")  # 记录完成信息
        except Exception as e:  # 捕获异常
            logger.error(f"处理删除事件时发生异常: {str(e)}")  # 记录错误信息
            logger.error(traceback.format_exc())  # 记录异常堆栈

    def _get_event_unique_id(self, event_data) -> Optional[str]:
        """
        从事件数据中获取唯一标识符
        """
        if hasattr(event_data, 'item_id'):
            return event_data.item_id
        elif hasattr(event_data, 'id'):
            return event_data.id
        elif hasattr(event_data, 'json_object') and event_data.json_object:
            # 从json_object中提取唯一标识
            item = event_data.json_object.get("Item", {})
            return item.get("Id") or item.get("SeriesId") or item.get("SeasonId")
        return None

    # ==================== 获取TMDB ID ====================
    def _get_tmdb_id(self, event_data, media_name, media_type):
        """
        获取TMDB ID
        :param event_data: 事件数据
        :param media_name: 媒体名称
        :param media_type: 媒体类型
        :return: TMDB ID
        """
        # 1. 直接从event_data中获取tmdb_id，不区分媒体类型
        if event_data:
            # 检查event_data对象是否有tmdb_id属性
            if hasattr(event_data, 'tmdb_id') and event_data.tmdb_id:
                try:
                    tmdb_id = str(event_data.tmdb_id)
                    logger.debug(f"从event_data.tmdb_id直接获取到TMDB ID: {tmdb_id}")
                    return tmdb_id
                except Exception as e:
                    logger.warn(f"从event_data.tmdb_id获取TMDB ID时出错: {str(e)}")

            # 检查event_data是否为字典且包含tmdb_id键
            if isinstance(event_data, dict) and "tmdb_id" in event_data:
                try:
                    tmdb_id = str(event_data["tmdb_id"])
                    logger.debug(
                        f"从event_data['tmdb_id']直接获取到TMDB ID: {tmdb_id}")
                    return tmdb_id
                except Exception as e:
                    logger.warn(
                        f"从event_data['tmdb_id']获取TMDB ID时出错: {str(e)}")

            # 2. 从item_path文件路径中提取TMDB ID
            if hasattr(event_data, 'item_path') and event_data.item_path:
                try:
                    import re

                    # 匹配 {tmdbid=数字} 格式
                    match = re.search(r'\{tmdbid=(\d+)\}',
                                      event_data.item_path)
                    if match:
                        tmdb_id = match.group(1)
                        logger.debug(
                            f"从event_data.item_path提取到TMDB ID: {tmdb_id}")
                        return tmdb_id
                except Exception as e:
                    logger.warn(f"从event_data.item_path提取TMDB ID时出错: {str(e)}")

            # 3. 检查event_data.json_object中是否包含TMDB ID
            if hasattr(event_data, 'json_object') and event_data.json_object:
                try:
                    # 检查json_object中的Item对象
                    item = event_data.json_object.get("Item", {})
                    tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                    if tmdb_id:
                        logger.debug(
                            f"从event_data.json_object.Item获取到TMDB ID: {tmdb_id}")
                        return tmdb_id

                    # 检查json_object根级别
                    tmdb_id = event_data.json_object.get(
                        "ProviderIds", {}).get("Tmdb")
                    if tmdb_id:
                        logger.debug(
                            f"从event_data.json_object根级别获取到TMDB ID: {tmdb_id}")
                        return tmdb_id

                    # 检查其他可能的字段名
                    provider_ids = item.get("ProviderIds", {})
                    for key, value in provider_ids.items():
                        if key.lower() == "tmdb":
                            tmdb_id = str(value)
                            logger.debug(
                                f"从event_data.json_object.Item.ProviderIds.{key}获取到TMDB ID: {tmdb_id}")
                            return tmdb_id
                except Exception as e:
                    logger.warn(
                        f"从event_data.json_object获取TMDB ID时出错: {str(e)}")

        # 4. 如果无法直接从event_data获取，尝试通过_get_media_info方法获取
        logger.debug("无法从event_data中直接获取TMDB ID，尝试通过_get_media_info方法获取")
        media_info = self._get_media_info(media_name, None, media_type)
        if media_info and hasattr(media_info, "tmdb_id") and media_info.tmdb_id:
            tmdb_id = str(media_info.tmdb_id)
            logger.debug(f"通过_get_media_info获取到TMDB ID: {tmdb_id}")
            return tmdb_id

        # 如果以上方法都失败，记录日志并返回None
        logger.debug("无法获取TMDB ID")
        return None

    # ==================== 通用文件删除函数 ====================
    def _delete_file_safely(self, file_path: str, description: str = "文件") -> bool:
        """
        通用的安全删除函数

        Args:
            file_path: 要删除的文件路径
            description: 文件描述

        Returns:
            bool: 删除是否成功
        """
        if not file_path or not os.path.exists(file_path):
            logger.warning(f"{description}不存在: {file_path}")
            return False

        try:
            # 检查保护目录
            dir_name = os.path.basename(os.path.dirname(file_path))
            if dir_name in self._protected_directories:
                logger.error(f"拒绝删除受保护目录下的{description}: {file_path}")
                return False

            # 执行删除操作
            if os.path.isdir(file_path):
                import shutil
                shutil.rmtree(file_path)
            else:
                os.remove(file_path)

            logger.info(f"✅ {description}删除成功: {file_path}")
            return True
        except Exception as e:
            logger.error(f"❌ {description}删除失败 {file_path}: {str(e)}")
            return False

    # ==================== 保护目录的文件删除函数（向后兼容） ====================
    def __delete_file_with_protection(self, file_path, description="文件"):
        """
        安全的文件删除函数，包含保护目录检查（向后兼容）
        """
        return self._delete_file_safely(file_path, description)

    # ==================== 获取本地媒体目录路径 ====================
    def __get_local_media_path(self, media_path):
        """
        获取本地媒体目录路径
        """
        if not self._local_library_path:
            logger.warn("未配置本地媒体库路径映射")
            return False, None

        media_paths = self._local_library_path.split("\n")
        for path in media_paths:
            if not path or "#" not in path:
                continue

            path = path.strip()
            parts = path.split("#")
            if len(parts) < 2:
                continue

            library_path = parts[0].strip()
            local_path = parts[1].strip()

            if not library_path or not local_path:
                continue

            # 判断路径是否包含
            try:
                if not media_path or not library_path:
                    continue

                full = Path(media_path).parts
                prefix = Path(library_path).parts

                if len(prefix) > len(full):
                    continue

                if full[: len(prefix)] == prefix:
                    return True, [library_path, local_path]
            except Exception as e:
                logger.warn(f"路径匹配出错: {str(e)}")
                continue

        logger.debug(f"未找到匹配的本地媒体路径: {media_path}")
        return False, None

    # ==================== 从事件数据中提取音乐Mount Paths ====================
    def __get_music_mount_path(self, event_data):
        """
        从事件数据中提取音乐Mount Paths
        根据示例：Mount Paths在Description字段中，格式为：
        'Mount Paths:\nhttp://192.168.1.100:5244/d/天翼家庭4/家庭4/音乐/Taylor Swift&Sabrina Carpenter/The Life of a Showgirl (Explicit)/The Life of a Showgirl (Explicit) - Taylor Swift&Sabrina Carpenter.flac'
        需要返回文件路径：/天翼家庭4/家庭4/音乐/Taylor Swift&Sabrina Carpenter/The Life of a Showgirl (Explicit)/The Life of a Showgirl (Explicit) - Taylor Swift&Sabrina Carpenter.flac
        """
        if not event_data:
            logger.debug("事件数据为空，无法提取Mount Paths")
            return None

        try:
            # WebhookEventInfo是对象，不是字典，需要使用属性访问
            description = ""

            # 尝试从json_object属性获取Description
            if hasattr(event_data, 'json_object') and event_data.json_object:
                # json_object已经是字典对象，直接使用
                json_obj = event_data.json_object
                description = json_obj.get('Description', '')

            # 如果从json_object没获取到，尝试从其他属性获取
            if not description and hasattr(event_data, 'description'):
                description = event_data.description or ""

            if not description:
                logger.debug("事件数据中未找到Description字段")
                return None

            # 解析Mount Paths
            lines = description.split('\n')
            mount_paths_url = None
            for i, line in enumerate(lines):
                if line.strip() == 'Mount Paths:':
                    # 下一行就是URL
                    if i + 1 < len(lines):
                        mount_paths_url = lines[i + 1].strip()
                        break

            if not mount_paths_url:
                logger.debug("Description中未找到Mount Paths URL")
                return None

            if not mount_paths_url:
                logger.debug("Mount Paths URL为空")
                return None

            # 解析URL，提取文件路径（不是目录路径）
            # 示例：http://192.168.1.100:5244/d/天翼家庭4/家庭4/音乐/Taylor Swift&Sabrina Carpenter/The Life of a Showgirl (Explicit)/The Life of a Showgirl (Explicit) - Taylor Swift&Sabrina Carpenter.flac
            # 需要提取：/天翼家庭4/家庭4/音乐/Taylor Swift&Sabrina Carpenter/The Life of a Showgirl (Explicit)/The Life of a Showgirl (Explicit) - Taylor Swift&Sabrina Carpenter.flac

            # 分割URL路径
            url_parts = mount_paths_url.split('/d/')
            if len(url_parts) < 2:
                logger.debug(f"Mount Paths URL格式不正确: {mount_paths_url}")
                return None

            # 获取完整的文件路径（去掉/d/前缀）
            file_path = '/' + url_parts[1]

            logger.debug(
                f"从Mount Paths提取文件路径: {mount_paths_url} -> {file_path}")
            return file_path

        except Exception as e:
            error_msg = f"提取音乐Mount Paths失败: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return None

    # ==================== 简化媒体类型识别 ====================
    def _get_media_type_from_emby_notification(self, item_name: str, cloud_path: str, media_type: str = None,
                                               season_num: str = None, episode_num: str = None, item_type: str = None,
                                               event_data=None, caller: str = "unknown") -> str:
        """
        简化的媒体类型识别函数
        """
        # 检查缓存
        cached_result = self._get_cached_media_type_result(item_name, cloud_path, media_type, season_num,
                                                           episode_num, item_type, event_data, caller)
        if cached_result:
            return cached_result

        try:
            # 从事件数据提取参数
            media_type, season_num, episode_num, item_type = self._extract_media_params_from_event(
                media_type, season_num, episode_num, item_type, event_data, caller
            )

            # 简化的媒体类型判断逻辑
            result = self._determine_simple_media_type(
                item_name, media_type, season_num, episode_num, item_type, event_data)

            logger.debug(f"[{caller}] 媒体类型判断结果: {result} (title={item_name}, media_type={media_type}, "
                         f"item_type={item_type}, season={season_num}, episode={episode_num})")

            # 缓存结果
            self._cache_media_type_result(
                result, item_name, cloud_path, media_type, season_num, episode_num, item_type, event_data, caller)

            return result

        except Exception as e:
            logger.error(f"[{caller}] 判断媒体类型时发生异常: {str(e)}")
            return "unknown"

    def _determine_simple_media_type(self, title: str, media_type: str, season_num: str, episode_num: str,
                                     item_type: str, event_data) -> str:
        """
        简化的媒体类型判断逻辑
        """
        # 1. 根据季集信息优先判断
        if season_num and episode_num:
            return "episode"
        elif season_num and not episode_num:
            return "season"

        # 2. 根据item_type判断
        if item_type:
            if item_type in ["MOV", "Movie"]:
                return "movie"
            elif item_type in ["TV", "Series", "TvChannel"]:
                return "series"
            elif item_type == "AUD":
                return "music"
            elif item_type.lower() in ["episode", "series", "season"]:
                return item_type.lower()

        # 3. 根据media_type判断
        if media_type:
            media_lower = media_type.lower()
            if media_lower in ["movie", "mov"]:
                return "movie"
            elif media_lower in ["tv", "series"]:
                return "series"
            elif media_lower in ["episode"]:
                return "episode"
            elif media_lower in ["season"]:
                return "season"
            elif media_lower in ["audio", "music"]:
                return "music"

        # 4. 尝试通过TMDB信息获取
        return self._get_media_type_from_tmdb(title, event_data)

    def _get_media_type_from_tmdb(self, title: str, event_data) -> str:
        """
        通过TMDB信息获取媒体类型（备用方法）
        """
        try:
            tmdb_id = None
            if event_data:
                # 尝试从event_data获取tmdb_id
                if hasattr(event_data, 'tmdb_id') and event_data.tmdb_id:
                    tmdb_id = str(event_data.tmdb_id)

            if tmdb_id:
                media_info = self._get_media_info(title, tmdb_id, "unknown")
                if media_info:
                    if media_info.type == MediaType.MOVIE:
                        return "movie"
                    elif media_info.type == MediaType.TV:
                        return "series"
        except Exception:
            pass

        return "movie"  # 默认返回电影类型

    def _extract_media_params_from_event(self, media_type: str, season_num: str, episode_num: str, item_type: str, event_data, caller: str) -> Tuple[str, str, str, str]:
        """
        从事件数据中提取媒体参数
        """
        # 如果有event_data，优先从中提取所有参数，忽略外部传入的参数
        if event_data:
            if hasattr(event_data, 'item_type'):    # 提取媒体类型 电影还是电视剧
                item_type = event_data.item_type
            if hasattr(event_data, 'media_type'):   # 提取剧集类型  剧集、单集、季
                media_type = event_data.media_type
            if hasattr(event_data, 'season_id'):    # 提取季数
                season_num = event_data.season_id
            if hasattr(event_data, 'episode_id'):   # 提取集数
                episode_num = event_data.episode_id

        # 特殊处理：季删除时Emby可能错误设置season_id
        # 需要从JSON对象中提取正确的季号
        if (media_type and media_type.lower() in ["season", "季"] and
                event_data and hasattr(event_data, 'json_object') and event_data.json_object):

            try:
                logger.debug(
                    f"季删除特殊处理: 开始处理 media_type={media_type}, season_num={season_num}, episode_num={episode_num}")
                item = event_data.json_object.get("Item", {})
                # 从JSON对象中提取季号
                season_num_from_json = item.get(
                    "SeasonNumber") or item.get("IndexNumber")
                logger.debug(
                    f"季删除特殊处理: 从JSON对象提取到 SeasonNumber={item.get('SeasonNumber')}, IndexNumber={item.get('IndexNumber')}")

                if season_num_from_json is not None:
                    season_num = str(season_num_from_json)
                    # 清除错误的episode_id，因为这是季删除，不是集删除
                    if episode_num is not None:
                        episode_num = None
                        logger.debug(
                            f"季删除特殊处理成功: 提取季号={season_num}, 清除错误episode_id")
                    else:
                        logger.debug(f"季删除特殊处理成功: 提取季号={season_num}")
                else:
                    logger.debug("季删除特殊处理: 未从JSON对象中找到季号信息")
            except Exception as e:
                logger.error(f"季删除特殊处理时发生异常: {str(e)}")
        else:
            # 添加调试信息，帮助诊断为什么特殊处理没有触发
            if media_type and media_type.lower() == "season":
                logger.debug(
                    f"季删除特殊处理未触发: media_type=season, season_num={season_num}, episode_num={episode_num}, has_json_object={event_data and hasattr(event_data, 'json_object') and event_data.json_object is not None}")

        # 记录从通知中提取的参数用于调试
        logger.debug(f"从通知中提取的媒体参数: item_type={item_type}, media_type={media_type}, "
                     f"season_num={season_num}, episode_num={episode_num}")

        return media_type, season_num, episode_num, item_type

    def _extract_season_episode_from_json_object(self, season_num: str, episode_num: str, event_data, caller: str) -> Tuple[str, str]:
        """
        从事件数据的JSON对象中提取季数和集数
        """
        if not event_data or not hasattr(event_data, 'json_object') or not event_data.json_object:
            return season_num, episode_num

        try:
            item = event_data.json_object.get("Item", {})

            # 如果没有season_num，尝试从event_data的json_object中提取
            if not season_num:
                # 尝试提取SeasonNumber
                season_num_from_json = item.get("SeasonNumber")
                if season_num_from_json is not None:
                    season_num = season_num_from_json
                    logger.debug(f"从json_object中提取到SeasonNumber: {season_num}")

                # 如果还是没有，尝试其他方式
                if not season_num:
                    season_num_from_json = item.get("IndexNumber")
                    if season_num_from_json is not None:
                        season_num = season_num_from_json
                        logger.debug(
                            f"从json_object中提取到IndexNumber作为SeasonNumber: {season_num}")

            # 如果没有episode_num，尝试从event_data的json_object中提取
            if not episode_num:
                episode_num_from_json = item.get("EpisodeNumber")
                if episode_num_from_json is not None:
                    episode_num = episode_num_from_json
                    logger.debug(
                        f"从json_object中提取到EpisodeNumber: {episode_num}")

                # 如果还是没有，尝试其他方式
                if not episode_num:
                    episode_num_from_json = item.get("IndexNumber")
                    if episode_num_from_json is not None:
                        episode_num = episode_num_from_json
                        logger.debug(
                            f"从json_object中提取到IndexNumber作为EpisodeNumber: {episode_num}")

        except Exception as e:
            logger.error(f"从json_object提取季集信息时发生异常: {str(e)}")

        return season_num, episode_num

    def _determine_media_type(self, title: str, media_type: str, season_num: str, episode_num: str, item_type: str, caller: str) -> str:
        """
        根据参数判断媒体类型
        """
        # 中文媒体类型映射
        media_type_chinese = {
            "movie": "电影",
            "series": "剧集",
            "season": "季",
            "episode": "集",
            "unknown": "未知"
        }

        # 根据item_type判断基础类型：MOV=电影，TV=电视剧
        # 但需要进一步检查season_num和episode_num来确定具体类型
        if item_type == "MOV":
            logger.debug(
                f"通过item_type识别为{media_type_chinese['movie']}类型: {title}")
            result = "movie"
        elif item_type == "TV":
            # 对于TV类型，需要进一步根据season_num和episode_num判断具体类型
            if media_type == "Episode" or (season_num and episode_num):
                logger.debug(f"识别为集类型: {title}")
                result = "episode"
            elif media_type == "Season" or (season_num and not episode_num):
                logger.debug(f"识别为季类型: {title}")
                result = "season"
            else:
                logger.debug(
                    f"通过item_type识别为{media_type_chinese['series']}类型: {title}")
                result = "series"
        elif media_type == "Episode" or (season_num and episode_num):
            logger.debug(f"识别为集类型: {title}")
            result = "episode"
        elif media_type == "Season" or (season_num and not episode_num):
            logger.debug(f"识别为季类型: {title}")
            result = "season"
        elif media_type == "Series" or (not season_num and not episode_num):
            logger.debug(f"识别为剧集类型: {title}")
            result = "series"
        elif media_type == "Movie" or media_type == "MOV":
            logger.debug(f"识别为电影类型: {title}")
            result = "movie"
        else:
            logger.warn(f"无法识别媒体类型: {title}，默认为电影类型")
            result = "movie"  # 默认为电影类型，更安全的选择

        return result

    # ==================== 媒体缓存 ====================
    def _cache_media_type_result(self, result: str, item_name: str, cloud_path: str, media_type: str,
                                 season_num: str, episode_num: str, item_type: str, event_data, caller: str):
        """
        缓存媒体类型结果
        """
        if not event_data:
            return

        # 创建更稳定的唯一标识符，不依赖event_id
        unique_id = self._get_event_unique_id(event_data)

        # 如果没有唯一标识符，使用其他信息创建缓存键
        if not unique_id:
            # 使用媒体名称、路径和类型信息创建缓存键
            cache_key = (item_name, cloud_path, media_type,
                         season_num, episode_num, item_type, caller)
            logger.debug(f"事件数据中无唯一标识符，使用复合键缓存: {item_name} (调用者: {caller})")
        else:
            # 创建更稳定的缓存键，只依赖unique_id和title，确保不同调用者可以共享缓存
            cache_key = (unique_id, item_name)

        # 如果类中有缓存字典，则缓存结果
        if hasattr(self, '_media_type_cache'):
            self._media_type_cache[cache_key] = result
            logger.debug(f"缓存媒体类型结果: {item_name} -> {result} (调用者: {caller})")

    def _get_cached_media_type_result(self, item_name: str, cloud_path: str, media_type: str, season_num: str,
                                      episode_num: str, item_type: str, event_data, caller: str) -> Optional[str]:
        """
        从缓存中获取媒体类型结果
        """
        if not event_data:
            return None

        # 创建更稳定的唯一标识符，不依赖event_id
        unique_id = self._get_event_unique_id(event_data)

        # 如果没有唯一标识符，使用其他信息创建缓存键
        if not unique_id:
            # 使用媒体名称、路径和类型信息创建缓存键
            cache_key = (item_name, cloud_path, media_type,
                         season_num, episode_num, item_type, caller)
            logger.debug(f"事件数据中无唯一标识符，使用复合键查询缓存: {item_name} (调用者: {caller})")
        else:
            # 创建更稳定的缓存键，只依赖unique_id和title，确保不同调用者可以共享缓存
            # 同时避免传递进来的参数为None时影响缓存命中
            cache_key = (unique_id, item_name)

        # 如果类中有缓存字典，则检查是否已有缓存结果
        if hasattr(self, '_media_type_cache') and cache_key in self._media_type_cache:
            cached_result = self._media_type_cache[cache_key]
            logger.debug(
                f"使用缓存的媒体类型结果: {item_name} -> {cached_result} (调用者: {caller})")
            return cached_result

        return None

    # ==================== 通过MoviePilot自带程序获取媒体详细信息 ====================
    def _get_media_info(self, item_name: str, tmdb_id: str, media_type: str) -> Optional[MediaInfo]:
        if not item_name:
            return None

        try:
            # 创建MetaInfo对象
            meta = MetaInfo(item_name)
            # 确定媒体类型
            # 正确处理MediaType对象
            media_type_str = str(media_type).lower() if media_type else ""
            if "movie" in media_type_str or "show" in media_type_str:
                mtype = MediaType.MOVIE
            elif "tv" in media_type_str or "series" in media_type_str or "episode" in media_type_str or "season" in media_type_str:
                mtype = MediaType.TV
            else:
                mtype = None

            # 识别媒体
            if not self._transferchain:
                logger.warning("_transferchain 未初始化，无法识别媒体信息")
                return None
            mediainfo: MediaInfo = self._transferchain.recognize_media(
                meta=meta, tmdbid=tmdb_id, mtype=mtype)
            if mediainfo:
                logger.info(f"成功识别媒体: {mediainfo.title} ({mediainfo.year})")
                return mediainfo
        except Exception as e:
            logger.error(f"识别媒体信息失败: {str(e)}")
            logger.error(traceback.format_exc())

        return None

    # ==================== 获取封面图片URL ====================
    def _get_poster_image_url(self, event_data: WebhookEventInfo = None, tmdb_path: str = None, prefix: str = "w500") -> str:
        """
        获取封面图片URL
        通过系统链获取图片
        """
        # 1.首先尝试从event_data.image_url获取
        if event_data and hasattr(event_data, 'image_url') and event_data.image_url:
            logger.info(f"从事件数据中获取到海报地址: {event_data.image_url}")
            return event_data.image_url

        # 直接使用已有的缓存机制获取媒体信息
        if event_data:
            # 使用event_data中的实际字段值，让_get_media_type_from_emby_notification从event_data中获取所需信息
            media_type_str = self._get_media_type_from_emby_notification(
                item_name=event_data.item_name if hasattr(
                    event_data, 'item_name') else "",  # 使用实际的item_name字段
                cloud_path=event_data.item_path if hasattr(
                    event_data, 'item_path') else "",  # 使用实际的item_path字段
                media_type=None,
                season_num=None,
                episode_num=None,
                item_type=None,
                event_data=event_data,
                caller="图片获取"
            )

            # 2.通过系统链获取海报图片
            tmdb_id = self._get_tmdb_id(event_data, "", media_type_str)

            logger.debug(
                f"通过系统链获取海报图片参数: media_type={media_type_str}, tmdb_id={tmdb_id}")

            if tmdb_id:
                try:
                    # 根据媒体类型字符串确定MediaType枚举
                    if media_type_str in ["movie"]:
                        mtype = MediaType.MOVIE
                    elif media_type_str in ["tv", "series", "episode", "season"]:
                        mtype = MediaType.TV
                    else:
                        mtype = MediaType.TV  # 默认为TV类型

                    logger.debug(f"确定媒体类型: {mtype}")

                    # 使用系统链获取海报图片
                    if not self._transferchain:
                        logger.warning("_transferchain 未初始化，无法获取图片")
                        return ""
                    poster_image = self._transferchain.obtain_specific_image(
                        mediaid=int(tmdb_id),
                        mtype=mtype,
                        image_type=MediaImageType.Poster,
                        season=None,
                        episode=None
                    )

                    if poster_image:
                        logger.info(f"通过系统链获取到海报图片: {poster_image}")
                        return poster_image

                    # 如果海报图片获取失败，尝试获取背景图片
                    if not self._transferchain:
                        logger.warning("_transferchain 未初始化，无法获取图片")
                        return ""
                    backdrop_image = self._transferchain.obtain_specific_image(
                        mediaid=int(tmdb_id),
                        mtype=mtype,
                        image_type=MediaImageType.Backdrop,
                        season=None,
                        episode=None
                    )

                    if backdrop_image:
                        logger.info(f"通过系统链获取到背景图片: {backdrop_image}")
                        return backdrop_image

                    logger.warning(f"通过系统链未获取到图片")

                except Exception as e:
                    logger.error(f"通过系统链获取图片失败: {str(e)}")
                    logger.error(traceback.format_exc())

        return ""

    # ==================== 查询转移记录 ====================
    def __get_transfer_his(
        self,
        media_type: str,
        media_name: str,
        media_path: str,
        tmdb_id: int,
        season_num: str,
        episode_num: str,
    ):
        """
        查询转移记录
        """
        logger.debug(
            f"开始查询转移记录: media_name={media_name},media_type={media_type},  "f"tmdb_id={tmdb_id}, season_num={season_num}, episode_num={episode_num}")

        # 季数
        if season_num and str(season_num).isdigit():
            season_num_int = int(season_num)
            if season_num_int > 0:
                season_num = str(season_num_int).rjust(2, "0")
                logger.debug(f"格式化季数: {season_num}")
            else:
                season_num = "0"  # 特殊季
                logger.debug(f"特殊季: {season_num}")
        else:
            season_num = None
            if episode_num:
                logger.debug("季数为空但集数不为空")

        # 集数
        if episode_num and str(episode_num).isdigit():
            episode_num = str(episode_num).rjust(2, "0")
            logger.debug(f"格式化集数: {episode_num}")
        else:
            episode_num = None
            if season_num:
                logger.debug("集数为空但季数不为空")

        # 类型
        mtype = MediaType.MOVIE if media_type in [
            "Movie", "MOV", "movie"] else MediaType.TV
        logger.debug(f"媒体类型映射: {media_type} -> {mtype}")

        # 查询电影
        if mtype == MediaType.MOVIE:
            msg = f"电影 {media_name} {tmdb_id}"
            logger.debug(f"查询电影转移记录: {msg}")
            transfer_history: List[TransferHistory] = self._transferhis.get_by(
                media_id=tmdb_id, media_source=MediaSource.TMDB, mtype=mtype.value, dest=media_path
            )
            logger.debug(f"查询到 {len(transfer_history)} 条电影转移记录")
        # 查询电视剧
        elif mtype == MediaType.TV and not season_num and not episode_num:
            msg = f"剧集 {media_name} {tmdb_id}"
            logger.debug(f"查询剧集转移记录: {msg}")
            transfer_history: List[TransferHistory] = self._transferhis.get_by(
                media_id=tmdb_id, media_source=MediaSource.TMDB, mtype=mtype.value
            )
            logger.debug(f"查询到 {len(transfer_history)} 条剧集转移记录")
        # 查询季
        elif mtype == MediaType.TV and season_num and not episode_num:
            if not season_num or not str(season_num).isdigit():
                logger.error(f"{media_name} 季同步删除失败，未获取到具体季")
                return "", []
            msg = f"剧集 {media_name} S{season_num} {tmdb_id}"
            logger.debug(f"查询季转移记录: {msg}")
            transfer_history: List[TransferHistory] = self._transferhis.get_by(
                media_id=tmdb_id, media_source=MediaSource.TMDB, mtype=mtype.value, season=f"S{season_num}"
            )
            logger.debug(f"查询到 {len(transfer_history)} 条剧集季转移记录")
        # 查询集
        elif mtype == MediaType.TV and season_num and episode_num:
            if (
                not season_num
                or not str(season_num).isdigit()
                or not episode_num
                or not str(episode_num).isdigit()
            ):
                logger.error(f"{media_name} 集同步删除失败，未获取到具体集")
                return "", []
            msg = f"剧集 {media_name} S{season_num}E{episode_num} {tmdb_id}"
            logger.debug(f"查询剧集集转移记录: {msg}")
            transfer_history: List[TransferHistory] = self._transferhis.get_by(
                media_id=tmdb_id, media_source=MediaSource.TMDB,
                mtype=mtype.value,
                season=f"S{season_num}",
                episode=f"E{episode_num}",
                dest=media_path,
            )
            logger.debug(f"查询到 {len(transfer_history)} 条剧集集转移记录")
        else:
            logger.warning(
                f"未匹配到任何查询条件: mtype={mtype}, season_num={season_num}, episode_num={episode_num}")
            return "", []

        logger.info(f"查询转移记录完成: {msg}, 记录数={len(transfer_history)}")
        return msg, transfer_history

    # ==================== 音乐文件删除方法 ====================
    def _delete_music_file(self, music_file_path: str, media_name: str, event_data=None):
        """
        删除音乐文件（OpenList独立直连）
        只删除事件中指定的具体文件，然后检查目录是否为空，如果为空再删除目录
        Args:
            music_file_path: 音乐云盘文件路径
            media_name: 媒体名称
            event_data: 事件数据
        Returns:
            删除结果字典
        """
        from pathlib import Path

        result = {
            "success": False,
            "deleted_paths": [],
            "error_message": "",
            "media_type_category": "music"
        }

        try:
            # 检查是否为保护目录
            file_name = Path(music_file_path).name
            if file_name in self._protected_directories:
                result["error_message"] = f"拒绝删除受保护目录下的音乐文件: {music_file_path}"
                logger.error(result["error_message"])
                return result

            # 使用独立的 OpenList API 删除云盘文件
            logger.info(f"开始删除音乐文件: {music_file_path}")

            music_dir = str(Path(music_file_path).parent)
            music_name = Path(music_file_path).name

            # 确认目标存在
            dir_content = self._openlist_list_dir(music_dir)
            if dir_content is None:
                logger.warning(f"音乐文件目录不存在，无需删除: {music_dir}")
                result["success"] = True
                result["error_message"] = "目录不存在，无需删除"
                return result

            file_exists = any(
                isinstance(item, dict) and item.get("name") == music_name
                for item in dir_content
            )
            if not file_exists:
                logger.warning(f"音乐文件不存在，无需删除: {music_file_path}")
                result["success"] = True
                result["error_message"] = "文件不存在，无需删除"
                return result

            # 执行文件删除操作
            logger.info(f"使用OpenList独立API方法删除音乐文件: {music_file_path}")
            delete_result = self._openlist_remove(music_dir, [music_name])

            if delete_result:
                logger.info(f"音乐文件删除成功: {music_file_path}")

                # 刷新文件所在目录
                self._refresh_openlist_directory(music_dir)

                # 检查目录是否为空，如果为空则删除目录
                logger.debug(f"检查目录是否为空: {music_dir}")
                self.__check_and_remove_empty_directory(music_dir)

                result["success"] = True
                result["deleted_paths"].append(music_file_path)
            else:
                error_msg = f"OpenList删除操作失败: {music_file_path}"
                result["error_message"] = error_msg
                logger.error(error_msg)

        except Exception as e:
            error_msg = f"删除音乐文件失败: {music_file_path}, 错误: {str(e)}"
            result["error_message"] = error_msg
            logger.error(error_msg)
            logger.error(traceback.format_exc())

        return result

    def __check_and_remove_empty_directory(self, directory_path: str):
        """
        检查并清理空目录（OpenList独立直连）。
        直接调用官方 /api/fs/remove_empty_directory，由服务端递归清理整条空目录链，
        非空的目录会被服务端自动跳过，无需客户端先判断。
        Args:
            directory_path: 起始目录路径
        """
        try:
            if not directory_path or directory_path in ("/", "."):
                return
            self._openlist_remove_empty_directory(directory_path)
        except Exception as e:
            logger.warning(f"检查空目录时发生异常: {str(e)}")

    def _send_music_deletion_notification(self, media_name: str, media_path: str, music_cloud_path: str,
                                          deletion_result: dict, event_data=None):
        """
        发送音乐删除通知
        Args:
            media_name: 媒体名称
            media_path: 原始媒体路径
            music_cloud_path: 云盘删除路径
            deletion_result: 删除结果
            event_data: 事件数据
        """
        if not self._notify:
            return

        try:
            # 构建通知内容
            title = f"音乐删除完成"

            if deletion_result["success"]:
                message = (
                    f"音乐名称: {media_name}\n"
                    f"原始路径: {media_path}\n"
                    f"云盘路径: {music_cloud_path}\n"
                    f"删除状态: 成功\n"
                    f"删除路径数: {len(deletion_result['deleted_paths'])}"
                )
            else:
                message = (
                    f"音乐名称: {media_name}\n"
                    f"原始路径: {media_path}\n"
                    f"云盘路径: {music_cloud_path}\n"
                    f"删除状态: 失败\n"
                    f"错误信息: {deletion_result['error_message']}"
                )

            # 发送通知
            self.post_message(
                mtype=NotificationType.MediaServer,
                title=title,
                text=message,
                image="https://emby.media/notificationicon.png"
            )

            logger.info(f"音乐删除通知已发送: {media_name}")

        except Exception as e:
            logger.error(f"发送音乐删除通知失败: {str(e)}")

    # ==================== 源文件删除主流程 ====================
    def __sync_del(
        self,
        media_type: str,
        media_name: str,
        media_path: str,
        tmdb_id: int,
        season_num: str,
        episode_num: str,
        media_storage: str,
        media_suffix: str,
        event_image_url: str = "",
        event_data: WebhookEventInfo = None
    ):

        if not media_type:
            logger.error(
                f"{media_name} 同步删除失败，未获取到媒体类型，请检查媒体是否刮削"
            )
            return

        # 处理路径映射
        if self._local_library_path and media_storage == "local":
            status, sub_paths = self.__get_local_media_path(media_path)
            if status:
                media_path = media_path.replace(sub_paths[0], sub_paths[1]).replace(
                    "\\", "/"
                )
            else:
                logger.warn(f"媒体路径 {media_path} 未在配置的本地媒体库路径中找到匹配项")
                return

        # 对于音乐类型，直接从Mount Paths删除文件，不需要查询转移记录
        if media_type == "music":
            logger.debug(f"音乐类型直接删除: {media_name}")

            # 从事件数据中获取Mount Paths文件路径
            music_file_path = self.__get_music_mount_path(event_data)
            if not music_file_path:
                logger.warn(f"音乐事件中未找到有效的Mount Paths: {media_path}")
                return

            # 直接删除音乐文件
            local_del_result = self._delete_music_file(
                music_file_path, media_name, event_data)

            # 发送通知
            self._send_music_deletion_notification(
                media_name, media_path, music_file_path, local_del_result, event_data)
            return

        # 调用查询转移记录（非音乐类型）
        msg, transfer_history = self.__get_transfer_his(
            media_type=media_type,
            media_name=media_name,
            media_path=media_path,
            tmdb_id=tmdb_id,
            season_num=season_num,
            episode_num=episode_num,
        )

        logger.debug(f"正在同步删除{msg}")

        if not transfer_history:
            logger.warn(
                f"{media_type} {media_name} 未获取到可删除数据，请检查路径映射是否配置错误，请检查tmdbid获取是否正确"
            )
            # 发送未找到转移记录的通知
            self._send_no_transfer_history_notification(
                media_name, media_type, media_path, tmdb_id, season_num, episode_num, event_data
            )
            return

        logger.debug(f"获取到 {len(transfer_history)} 条转移记录，开始同步删除源文件和转移记录")

        # 删除本地存储的媒体文件
        local_del_result = self._delete_local_files(
            transfer_history, media_type, media_name, season_num, episode_num, event_data)

        # 从local_del_result中获取已经判断好的媒体类型
        deleted_targets = local_del_result.get("deleted_targets", set())
        updated_media_type = local_del_result.get(
            "media_type_category", media_type)

        if updated_media_type:
            logger.debug(f"使用已判断的媒体类型: {updated_media_type}")
        else:
            # 如果没有获取到已判断的媒体类型，则重新判断
            if deleted_targets:
                first_target = next(iter(deleted_targets))
                logger.debug(
                    f"调用_get_media_type_from_emby_notification函数重新判断媒体类型: title={media_name}, cloud_path={first_target}")
                updated_media_type = self._get_media_type_from_emby_notification(
                    item_name=media_name, cloud_path=first_target, media_type=media_type, season_num=season_num, episode_num=episode_num, event_data=event_data, caller="云盘删除"
                )
            else:
                updated_media_type = media_type

        # 提取网盘路径并验证是否符合CAS和OpenList服务地址
        mount_paths_url = self._extract_mount_paths_url(event_data)
        cas_result = self._process_cloud_deletion(
            mount_paths_url, local_del_result, media_name, tmdb_id, updated_media_type, season_num, episode_num, event_data)

        # 发送通知（历史记录将在通知方法中保存）
        self._send_notifications(media_name, tmdb_id, media_type, season_num,
                                 episode_num, local_del_result, cas_result, media_path, event_data)

    # ==================== 本地电视剧目录路替换 ====================
    def _get_series_directory_path(self, src_path_obj: Path) -> str:
        """
        智能计算电视剧目录路径，处理多种目录结构：
        1. 单季结构：/剧名 (年份)/剧名 - S01E01.strm
        2. 多季结构：/剧名 (年份)/Season 1/剧名 - S01E01.strm
        3. 多季结构：/剧名 (年份)/Season 2/剧名 - S01E01.strm

        确保返回的目录路径都是包含剧名的完整路径：/剧名 (年份)
        """
        # 获取文件路径的各个父目录
        path_parts = list(src_path_obj.parents)

        # 从最深的目录开始检查，找到包含剧名的目录
        for i, parent_path in enumerate(path_parts):
            parent_name = parent_path.name

            # 检查目录名是否包含季信息（Season X 或 SXX 格式）
            if re.search(r'(Season\s*\d+|S\d+)', parent_name, re.IGNORECASE):
                # 如果找到季目录，返回季目录的父目录（剧集目录）
                series_dir = path_parts[i+1] if i + \
                    1 < len(path_parts) else parent_path.parent
                logger.debug(f"检测到季目录结构，返回剧集目录: {series_dir}")
                return str(series_dir)

            # 检查目录名是否包含年份信息（通常剧集目录包含年份）
            if re.search(r'\(\d{4}\)', parent_name):
                # 如果找到包含年份的目录，这就是剧集目录
                logger.debug(f"检测到包含年份的剧集目录: {parent_path}")
                return str(parent_path)

        # 如果没有找到明显的季目录或年份目录，使用原来的逻辑（parent.parent）
        series_dir = src_path_obj.parent.parent
        logger.debug(f"使用默认剧集目录路径: {series_dir}")
        return str(series_dir)

    # ==================== 删除本地文件 ====================
    def _delete_local_files(self, transfer_history, media_type, media_name, season_num, episode_num, event_data=None):
        """
        删除本地文件 清理转移记录 删除源文件清理空目录
        """
        year = None
        error_cnt = 0
        image = "https://emby.media/notificationicon.png"
        deleted_targets = set()
        is_single_episode = bool(season_num and episode_num)
        is_whole_season = bool(season_num and not episode_num)
        is_whole_series = bool(not season_num and not episode_num)

        # 先判断媒体类型（只判断一次，避免重复判断）
        media_type_category = None
        dir_path = None
        has_determined_media_type = False

        # 遍历所有转移记录，逐个处理
        for transferhis in transfer_history:
            title = transferhis.title
            # 安全检查：确保当前转移记录的标题包含要删除的媒体名称，防止误删
            if title not in media_name:
                logger.warn(
                    f"当前转移记录 {transferhis.id} {title} {transferhis.tmdbid} 与删除媒体{media_name}不符，防误删，暂不自动删除"
                )
                continue
            # 保存媒体图片和年份信息，用于后续通知
            image = transferhis.image or image
            year = transferhis.year
            # 删除数据库中的转移记录
            try:
                self._transferhis.delete(transferhis.id)
                logger.info(
                    f"转移记录删除成功: ID={transferhis.id}, 标题={transferhis.title}")
            except Exception as e:
                logger.error(
                    f"转移记录删除失败: ID={transferhis.id}, 标题={transferhis.title}, 错误={str(e)}")

            # 删除源文件
            if self._del_source:
                logger.info(f"开始删除本地源文件: {transferhis.src}")
                if (
                    transferhis.src
                    and Path(transferhis.src).suffix in settings.RMT_MEDIAEXT
                    and transferhis.src_storage == "local"
                    and transferhis.mode != "move"
                ):
                    src_path_obj = Path(transferhis.src)  # 避免重复调用Path函数

                    # 先判断媒体类型，再决定是否删除目标文件和源文件
                    src_path = str(src_path_obj)

                    # 只在第一次需要时判断媒体类型
                    if not has_determined_media_type:
                        # 调用_get_media_type_from_emby_notification函数判断媒体类型
                        logger.debug(
                            f"调用_get_media_type_from_emby_notification函数判断媒体类型: title={media_name}, cloud_path={src_path}")
                        media_type_category = self._get_media_type_from_emby_notification(
                            item_name=media_name, cloud_path=src_path, media_type=media_type, season_num=season_num, episode_num=episode_num, event_data=event_data, caller="删除源文件"
                        )
                        is_single_episode = (media_type_category == "episode")
                        has_determined_media_type = True

                        # 根据媒体类型决定删除策略
                        if media_type_category in ["movie", "series"]:
                            # 对于电影和电视剧，计算目录路径
                            if media_type_category == "series":
                                # 对于电视剧，使用智能路径计算函数获取正确的剧集目录
                                dir_path = self._get_series_directory_path(
                                    src_path_obj)
                            else:
                                # 对于电影，使用正常的父目录
                                dir_path = str(src_path_obj.parent)
                            # 检查是否已经添加过该目录路径，避免重复添加
                            if dir_path not in deleted_targets:
                                deleted_targets.add(dir_path)
                                logger.debug(f"添加目录路径作为删除目标: {dir_path}")
                            else:
                                logger.debug(f"目录路径已存在，跳过重复添加: {dir_path}")

                    # 根据媒体类型决定删除策略
                    if media_type_category in ["movie", "series"]:
                        # 对于电影和电视剧，计算目录路径
                        if media_type_category == "series":
                            # 对于电视剧，使用智能路径计算函数获取正确的剧集目录
                            dir_path = self._get_series_directory_path(
                                src_path_obj)
                        else:
                            # 对于电影，使用正常的父目录
                            dir_path = str(src_path_obj.parent)
                        # 检查是否已经添加过该目录路径，避免重复添加
                        if dir_path not in deleted_targets:
                            deleted_targets.add(dir_path)
                            logger.debug(f"添加目录路径作为删除目标: {dir_path}")
                        else:
                            logger.debug(f"目录路径已存在，跳过重复添加: {dir_path}")

                        # 对于电影和电视剧，正常删除目标文件和源文件
                        if self.__delete_file_with_protection(transferhis.dest, "目标文件"):
                            self.__remove_parent_dir(Path(transferhis.dest))

                        if self.__delete_file_with_protection(transferhis.src, "源文件"):
                            # 添加源文件路径到删除目标
                            deleted_targets.add(transferhis.src)
                            logger.debug(f"来源-源文件路径: {transferhis.src}")

                        # 对于电影/电视剧类型，调用__remove_parent_dir清理空目录
                        self.__remove_parent_dir(src_path_obj)
                    else:
                        # 对于未知类型，正常删除目标文件和源文件
                        if self.__delete_file_with_protection(transferhis.dest, "目标文件"):
                            self.__remove_parent_dir(Path(transferhis.dest))

                        if self.__delete_file_with_protection(transferhis.src, "源文件"):
                            # 添加源文件路径到删除目标
                            deleted_targets.add(transferhis.src)
                            logger.debug(f"来源-源文件路径: {transferhis.src}")

                            # 修改逻辑：针对不同媒体类型添加不同的路径到删除目标
                            if media_type_category == "episode":
                                if not src_path.endswith('.strm'):
                                    # 对于单集文件，直接作为删除目标
                                    deleted_targets.add(src_path)
                                    logger.debug(f"添加单集路径作为删除目标: {src_path}")
                            elif media_type_category == "season":
                                # 对于季，添加季目录路径作为删除目标
                                season_dir_path = str(src_path_obj.parent)
                                # 检查目录是否存在再添加
                                if Path(season_dir_path).exists():
                                    deleted_targets.add(season_dir_path)
                                    logger.debug(
                                        f"添加季目录路径作为删除目标: {season_dir_path}")
                                else:
                                    logger.warn(
                                        f"季目录不存在，跳过添加到删除目标: {season_dir_path}")

                            # 对于非电影/电视剧类型，调用__remove_parent_dir清理空目录
                            self.__remove_parent_dir(src_path_obj)
                else:
                    logger.debug(f"本地文件不存在: {transferhis.src}")

        logger.info(f"本地文件删除最终目标列表:")
        for target in deleted_targets:
            logger.info(f"  - {target}")

        # 判断本地删除是否成功：只要转移记录被删除或者有文件被删除，就认为是成功的
        local_success = len(transfer_history) > 0 or len(deleted_targets) > 0

        # 将基础删除目标传递给后续处理
        return {
            "transfer_count": len(transfer_history),
            "error_cnt": error_cnt,
            "deleted_targets": deleted_targets,
            "year": year,
            "image": image,
            "media_type_category": media_type_category,  # 添加已经判断好的媒体类型
            "success": local_success  # 添加本地删除成功状态
        }

    # ==================== 清理本地空目录 ====================

    def __remove_parent_dir(self, file_path: Path):
        """
        清理空目录
        """
        try:
            i = 0
            for parent_path in file_path.parents:
                i += 1
                if i > 2:
                    break
                if str(parent_path.parent) != str(file_path.root):
                    # 检查目录是否存在
                    if not parent_path.exists():
                        logger.debug(f"目录不存在，跳过清理: {parent_path}")
                        continue
                    # 只有目录完全空时才删除
                    has_anything = False
                    for f in parent_path.iterdir():
                        logger.debug(
                            f"清理目录: {parent_path} 包含文件: {f.name} 后缀: {f.suffix.lower()}")
                        has_anything = True
                        break
                    if not has_anything:
                        shutil.rmtree(parent_path, ignore_errors=True)
                        logger.info(f"清理空目录: {parent_path}")
        except Exception as e:
            logger.warn(f"清理空目录时发生异常: {str(e)}")

    # ==================== 检测并清理网盘空目录 ====================
    def __remove_cloud_parent_dir(self, cloud_path: str, storage_type="alist"):
        """
        检测并清理网盘空目录（OpenList独立直连）。
        直接调用官方 /api/fs/remove_empty_directory，由服务端从父目录开始递归清理整条空目录链。
        Args:
            cloud_path: 被删除条目的完整云盘路径
            storage_type: 保留签名兼容（OpenList 固定为 alist）
        """
        try:
            from pathlib import Path

            parent_dir = str(Path(cloud_path).parent)
            if parent_dir in ("/", "."):
                return
            logger.debug(f"触发OpenList递归清理空目录，起点: {parent_dir}")
            self._openlist_remove_empty_directory(parent_dir)

        except Exception as e:
            logger.warn(f"检测网盘空目录时发生异常: {str(e)}")

    # ==================== 使用网盘原生API删除空目录 ====================
    def _delete_openlist_directory_by_api(self, directory_path: str):
        """
        使用OpenList原生API递归删除空目录（独立直连），兼容旧调用入口
        """
        try:
            logger.debug(f"使用原生API清理空目录: {directory_path}")
            return self._openlist_remove_empty_directory(str(Path(directory_path).parent))
        except Exception as e:
            logger.error(f"使用原生API删除目录异常: {str(e)}")
            return False

    # ==================== 提取网盘文件路径Mount Paths ====================

    def _extract_mount_paths_url(self, event_data):
        """
        从EMBY通知中提取网盘文件路径 URL
        """
        logger.info(f"开始网盘删除:")

        mount_paths_url = None
        if event_data and hasattr(event_data, 'json_object') and event_data.json_object:
            try:
                description = event_data.json_object.get("Description", "")
                # 只处理包含Mount Paths的部分，忽略其他描述信息
                lines = description.split('\n')
                mount_paths_found = False
                for i, line in enumerate(lines):
                    if line.strip() == "Mount Paths:":
                        mount_paths_found = True
                        # 下一行应该是URL
                        if i + 1 < len(lines):
                            mount_path_url = lines[i + 1].strip()
                            if mount_path_url.startswith(('http://', 'https://')):
                                mount_paths_url = mount_path_url
                                logger.info(f"提取到文件路径 URL: {mount_paths_url}")
                            else:
                                logger.warn(f"文件路径 URL格式不正确: {mount_path_url}")
                        break

                if not mount_paths_found:
                    logger.debug("EMBY事件中未找到文件路径部分")
            except Exception as e:
                logger.warn(f"从EMBY事件中提取文件路径 URL时出错: {str(e)}")
        else:
            logger.debug("EMBY事件数据中不包含必要的json_object")

        return mount_paths_url

    # ==================== 判断启动CAS还是OpenList ====================
    def _process_cloud_deletion(self, mount_paths_url, local_del_result, media_name, tmdb_id, media_type, season_num, episode_num, event_data=None):
        """
        处理云盘文件删除操作，根据URL匹配CAS或OpenList服务并执行相应删除
        """
        # 检查文件地址是否符合要求
        cloud_result = None

        # 注意：OpenList删除基于transferhis真实路径(deleted_targets)，不再依赖mount_paths_url；
        # 因此不再因缺少URL而整体跳过。CAS分支内部仍会校验URL。

        # 检查CAS设置界面是否启动
        if not self._cas_mappings or not local_del_result.get("deleted_targets"):
            logger.info("CAS未启用或无映射配置，未启动CAS删除任务")
            return cloud_result

        # 检查CAS主机地址配置
        if self._cas_host:
            # 检查网盘路径 URL是否与CAS服务地址匹配
            normalized_cas_host = self._cas_host.rstrip('/').lower()
            logger.debug(f"规范化后的CAS主机地址: {normalized_cas_host}")
            logger.debug(f"检查的文件地址 URL: {mount_paths_url}")

            # 检查是否为URL格式
            if not mount_paths_url or not mount_paths_url.startswith(('http://', 'https://')):
                logger.debug("CAS需要Mount Paths URL进行匹配，当前无有效URL，跳过CAS删除")
            elif mount_paths_url.startswith(('http://', 'https://')):
                try:
                    # 提取URL中的主机部分
                    from urllib.parse import urlparse
                    parsed_url = urlparse(mount_paths_url)
                    url_host = f"{parsed_url.scheme}://{parsed_url.hostname}"
                    if parsed_url.port:
                        url_host += f":{parsed_url.port}"

                    logger.debug(f"解析出的URL主机地址: {url_host.lower()}")
                    # 比较主机地址是否匹配
                    if url_host.lower() == normalized_cas_host:
                        logger.debug(f"检测到文件地址与CAS服务地址匹配: {url_host}")
                        # 直接执行CAS删除逻辑
                        deleted_targets = local_del_result.get(
                            "deleted_targets", set())
                        updated_media_type = media_type  # 使用正确的变量名

                        # 如果地址匹配，则执行CAS删除
                        logger.info("地址匹配成功，开始CAS删除操作")
                        # 删除云盘文件(CAS)
                        cloud_result = self._process_cas_deletion(
                            mount_paths_url, local_del_result, media_name, tmdb_id, updated_media_type, season_num, episode_num, event_data)  # 传递网盘路径和event_data和媒体类型以及季集信息

                        # 添加source字段标识删除源为CAS
                        if cloud_result and isinstance(cloud_result, dict):
                            cloud_result['source'] = 'CAS'

                        # 等待CAS删除完成（如果是Future对象）
                        if isinstance(cloud_result, concurrent.futures.Future):
                            logger.debug("等待CAS删除任务完成...")
                            try:
                                cloud_result = cloud_result.result(
                                    timeout=30)  # 等待最多30秒
                                logger.info(f"CAS删除任务已完成，结果: {cloud_result}")
                            except concurrent.futures.TimeoutError:
                                logger.warn("CAS删除任务超时")
                                cloud_result = {
                                    "success": False, "error": "删除任务超时"}
                            except Exception as e:
                                logger.error(f"CAS删除任务异常: {str(e)}")
                                cloud_result = {
                                    "success": False, "error": str(e)}

                        return cloud_result
                    else:
                        logger.debug(
                            f"URL主机地址不匹配CAS，期望: {normalized_cas_host}, 实际: {url_host.lower()}")
                except Exception as e:
                    logger.debug(f"解析URL时出错: {str(e)}")
            else:
                logger.debug(f"文件地址 URL格式不正确: {mount_paths_url}")
        else:
            logger.debug("CAS主机地址未配置")

        # 检查OpenList功能是否启用
        if self._openlist_enabled:  # 检查OpenList功能启用
            # 使用transferhis真实路径（local_del_result.deleted_targets）进行映射删除，
            # 不再依赖Emby的Mount Paths URL。
            deleted_targets = local_del_result.get("deleted_targets")
            if deleted_targets:
                logger.debug(f"检测到transferhis删除目标，开始OpenList删除: {media_name}")
                openlist_result = self._process_openlist_deletion(
                    deleted_targets, local_del_result, media_name, tmdb_id, media_type, season_num, episode_num, event_data)

                # 添加source字段标识删除源为OLT (OpenList)
                if openlist_result and isinstance(openlist_result, dict):
                    openlist_result['source'] = 'OLT'
                elif openlist_result is None:
                    openlist_result = {'source': 'OLT'}

                return openlist_result
            else:
                logger.debug("本地删除未返回deleted_targets，跳过OpenList删除")

        return cloud_result

    # ==================== openlist删除文件主程序 ====================
    def _process_openlist_deletion(self, deleted_targets, local_del_result, media_name, tmdb_id, media_type, season_num, episode_num, event_data=None):
        """
        处理OpenList文件删除操作
        流程：遍历transferhis真实路径 → 路径映射为alist路径 → 按媒体类型定位目标 → 执行删除
        使用transferhis记录的网盘真实路径（如 /STRM影视/网盘/天翼云盘完结/家庭4/电影/奇爱博士...HDH/...strm），
        通过配置映射（local_path#cloud_path）替换为alist路径（/天翼家庭4/家庭4/电影/奇爱博士...HDH/...strm），
        再按媒体类型删除对应目录/文件。
        """
        try:
            logger.info(f"开始处理OpenList删除: {media_name}")

            from pathlib import Path

            # 1. 基于传入的季集信息判断媒体类型（简化判断，避免重复调用）
            if season_num and episode_num:
                media_type_category = "episode"
            elif season_num and not episode_num:
                media_type_category = "season"
            elif media_type and media_type.lower() in ["movie", "mov"]:
                media_type_category = "movie"
            elif media_type and media_type.lower() in ["series", "tv", "show"]:
                media_type_category = "series"
            else:
                media_type_category = "movie"

            # 将英文媒体类型映射为中文
            media_type_map = {
                "movie": "电影",
                "series": "剧集",
                "season": "季",
                "episode": "单集",
                "unknown": "未知"
            }
            media_type_chinese = media_type_map.get(
                media_type_category, media_type_category)
            logger.info(f"识别媒体类型: {media_name} -> {media_type_chinese}")

            total_deleted = 0
            deleted_items = []
            full_cloud_paths = []
            any_error = None

            # 2. 遍历每个transferhis真实路径，映射后删除
            for real_path in deleted_targets:
                # 2.1 直接按配置映射把真实路径替换为alist路径
                mapped_path = self._match_openlist_mount_path(real_path)
                if not mapped_path:
                    # 没找到替换路径：不启动该目标的删除，直接跳过
                    logger.warning(f"OpenList路径映射失败（未找到替换路径），不启动删除: {real_path}")
                    continue

                # 2.2 按媒体类型定位删除目标（基于已替换的alist路径）
                _real_path = Path(mapped_path)
                if media_type_category == "movie":
                    # 电影：删整个电影名目录。transferhis可能同时返回
                    #   (a) ".../电影名/电影名.strm"  —— 末级是文件
                    #   (b) ".../电影名"             —— 末级是目录（与(a)重复）
                    # 末级是目录的那条(b)是冗余记录，若按parent.parent定位会错误地指向"电影"库目录。
                    # 因此电影类型只认末级是文件(.strm/.mkv等)的记录来定位电影名目录，
                    # 跳过末级是目录的冗余记录，避免误删上级目录。
                    if not _real_path.suffix:
                        logger.debug(f"OpenList电影类型跳过目录型冗余记录: {mapped_path}")
                        continue
                    # 末级是文件：父目录即电影名目录，再上一级是电影库目录
                    delete_dir = str(_real_path.parent.parent)
                    delete_target = _real_path.parent.name
                elif media_type_category == "series":
                    # 整剧：删整个剧目录本身。
                    delete_dir = str(_real_path.parent)
                    delete_target = _real_path.name
                elif media_type_category == "season":
                    # 单季：删整季目录本身。
                    delete_dir = str(_real_path.parent)
                    delete_target = _real_path.name
                else:
                    # 单集(episode)：仅删该集文件本身。
                    delete_dir = str(_real_path.parent)
                    delete_target = _real_path.name

                logger.debug(f"OpenList寻找: 真实路径={real_path} -> 映射路径={mapped_path}")
                logger.debug(f"OpenList寻找: 媒体类型={media_type_category}，delete_dir(寻找目录)={delete_dir}，delete_target(目标名)={delete_target}")

                delete_result = self._delete_openlist_item(delete_dir, delete_target)
                if delete_result and delete_result.get("success"):
                    total_deleted += delete_result.get("deleted_count", 0)
                    deleted_items.extend(delete_result.get("deleted_items", []))
                    # 记录完整云盘路径（以/开头的映射路径），用于通知展示与acc_name提取
                    if delete_result.get("cloud_path") and not full_cloud_paths:
                        full_cloud_paths.append(delete_result.get("cloud_path"))
                elif delete_result and delete_result.get("error"):
                    any_error = delete_result.get("error")

            if total_deleted == 0 and any_error:
                return {"success": False, "error": any_error}

            return {
                "success": True,
                "deleted_count": total_deleted,
                "deleted_items": deleted_items,
                "source": "OLT",
                "cloud_path": full_cloud_paths[0] if full_cloud_paths else (deleted_items[0] if deleted_items else ""),
                "refresh_success": True
            }

        except Exception as e:
            logger.error(f"OpenList删除处理异常: {str(e)}")
            return {"success": False, "error": str(e)}

    # ==================== openlist匹配路径 ===================
    def _match_openlist_mount_path(self, real_path):
        """
        将transferhis真实路径映射为OpenList(alist)中的实际路径。
        通过配置的路径映射规则（local_path#cloud_path）做前缀替换：
          输入: /STRM影视/网盘/天翼云盘完结/家庭4/电影/奇爱博士...HDH/...strm
          映射: /STRM影视/网盘/天翼云盘完结/家庭4#/天翼家庭4/家庭4
          输出: /天翼家庭4/家庭4/电影/奇爱博士...HDH/...strm
        若传入的是带http的URL，则先提取路径部分再尝试映射。
        """
        try:
            path = real_path
            # 若传入的是URL，先提取路径部分
            if path and path.startswith(('http://', 'https://')):
                from urllib.parse import urlparse
                path = urlparse(path).path
                if path.startswith('/d/'):
                    path = '/' + path[3:]

            if not self._openlist_path_mapping:
                logger.debug("未配置OpenList路径映射，无法转换路径")
                return None

            # 按映射规则做前缀替换（取最长匹配的local_path，避免短前缀误替）
            matched = None
            for mapping in self._openlist_path_mapping:
                local_prefix = mapping.get("local_path", "")
                cloud_prefix = mapping.get("cloud_path", "")
                if not local_prefix or not cloud_prefix:
                    continue
                if path.startswith(local_prefix):
                    # 选取最长匹配的local_prefix，保证精确
                    if matched is None or len(local_prefix) > len(matched[0]):
                        matched = (local_prefix, cloud_prefix)

            if matched:
                local_prefix, cloud_prefix = matched
                mapped_path = path.replace(local_prefix, cloud_prefix, 1)
                logger.debug(f"OpenList路径映射成功: {path} -> {mapped_path}")
                return mapped_path
            else:
                logger.debug(f"OpenList路径映射未匹配: {path}")
                return None
        except Exception as e:
            logger.error(f"匹配OpenList挂载路径失败: {str(e)}")
            return None

    # ==================== openlist刷新目录 ===================
    def _refresh_openlist_directory(self, path):
        """
        刷新OpenList目录，使用独立的 OpenList API 获取最新文件列表
        """
        try:
            logger.debug(f"刷新OpenList目录: {path}")
            content = self._openlist_list_dir(path)
            if content is None:
                logger.warning(f"目录不存在或刷新失败: {path}")
                return None

            content_count = len(content)
            content_types = set()
            for item in content:
                if item.get('is_dir'):
                    content_types.add('目录')
                else:
                    content_types.add('文件')

            content_type_desc = '、'.join(
                content_types) if content_types else '空目录'
            logger.info(
                f"目录刷新成功: {path}, 内容: {content_count}项[{content_type_desc}]")

            return {
                "content": content,
                "total": content_count
            }

        except Exception as e:
            logger.error(f"刷新OpenList目录异常: {str(e)}")
            return None

    # ==================== openlist提取删除和刷新目录 ===================
    def _extract_directories_by_media_type(self, file_path, media_name, media_type_category, season_num=None, episode_num=None):
        """
        根据媒体类型提取删除目录
        集成智能目录查找逻辑，基于名称匹配查找目录，不受目录层级影响

        返回: {
            "delete_dir": 删除目录路径,
            "delete_target": 删除目标名称
        }
        """
        import re
        from pathlib import Path

        try:
            # 智能目录查找逻辑 - 从文件路径向上查找包含媒体名称的目录
            current_path = Path(file_path)

            # 从文件路径的父目录开始向上查找（排除文件本身）
            current_path = current_path.parent

            # 记录是否已经遇到过保护目录
            encountered_protected = False

            # 初始化path_obj为None
            path_obj = None

            # 向上遍历直到根目录
            while current_path != current_path.parent:  # 直到根目录
                current_name = current_path.name

                # 检查是否为受保护目录
                if current_name in self._protected_directories:
                    if not encountered_protected:
                        logger.debug(f"遇到保护目录: {current_path}，将在此停止搜索")
                        encountered_protected = True
                    else:
                        logger.debug(f"已在保护目录范围内: {current_path}，停止搜索")
                        break

                    # 在保护目录级别尝试最后一次匹配
                    main_media_name = re.sub(
                        r'\s*S\d+E\d+.*', '', media_name).strip()
                    clean_dir_name = re.sub(
                        r'\s*\(\d{4}\)', '', current_name).strip()

                    if (main_media_name and clean_dir_name and
                        (main_media_name in clean_dir_name or
                         clean_dir_name in main_media_name or
                         main_media_name == clean_dir_name)):
                        logger.debug(f"在保护目录级别找到匹配目录: {current_path}")
                        path_obj = current_path
                        break

                    if (media_name in current_name or
                        current_name in media_name or
                            media_name == current_name):
                        logger.debug(f"在保护目录级别找到原始名称匹配目录: {current_path}")
                        path_obj = current_path
                        break

                    # 保护目录级别未找到匹配，停止搜索
                    break

                # 改进的名称匹配逻辑：提取主要名称部分进行匹配
                main_media_name = re.sub(
                    r'\s*S\d+E\d+.*', '', media_name).strip()
                clean_dir_name = re.sub(
                    r'\s*\(\d{4}\)', '', current_name).strip()

                # 检查主要名称是否匹配（双向包含）
                if (main_media_name and clean_dir_name and
                    (main_media_name in clean_dir_name or
                     clean_dir_name in main_media_name or
                     main_media_name == clean_dir_name)):
                    logger.debug(
                        f"基于名称匹配找到目录: {current_path} (媒体名: {main_media_name}, 目录名: {clean_dir_name})")
                    path_obj = current_path
                    break

                # 同时保留原始名称的匹配逻辑作为备用
                if (media_name in current_name or
                    current_name in media_name or
                        media_name == current_name):
                    logger.debug(
                        f"基于原始名称匹配找到目录: {current_path} (媒体名: {media_name})")
                    path_obj = current_path
                    break

                # 移动到上一级目录
                current_path = current_path.parent
            else:
                logger.warning(f"基于名称匹配未找到包含 '{media_name}' 的目录")

            # 检查path_obj是否已定义
            if path_obj is None:
                    logger.error(f"未找到匹配的目录路径，无法确定删除策略")
                    return None

            # 根据媒体类型确定删除策略
            if media_type_category == "movie":
                # 电影：提取电影的父目录
                return {
                    "delete_dir": str(path_obj.parent),
                    "delete_target": path_obj.name
                }

            elif media_type_category == "series":
                # 剧集：提取剧名的父目录
                return {
                    "delete_dir": str(path_obj.parent),
                    "delete_target": path_obj.name
                }

            elif media_type_category == "season":
                # 季：提取季的父目录
                season_num_int = int(season_num) if season_num and str(
                    season_num).isdigit() else 0
                if season_num_int > 0:
                    season_dir = str(path_obj / f"Season {season_num_int}")
                    delete_target = f"Season {season_num_int}"
                else:
                    season_dir = str(path_obj / "Specials")
                    delete_target = "Specials"
                return {
                    "delete_dir": season_dir,
                    "delete_target": delete_target
                }

            elif media_type_category == "episode":
                # 单集：提取扩展名的父目录
                episode_file = Path(file_path).name
                episode_dir = str(
                    path_obj / f"Season {season_num}" if season_num > 0 else "Specials")
                return {
                    "delete_dir": episode_dir,
                    "delete_target": episode_file
                }

            else:
                logger.error(f"未知的媒体类型: {media_type_category}")
                return None

        except Exception as e:
            logger.error(f"提取目录时发生异常: {str(e)}")
            return None

    # ==================== openlist执行删除函数 ===================
    def _delete_openlist_item(self, delete_dir, item_name):
        """
        统一的OpenList删除函数，使用独立的 OpenList API 进行删除操作
        """
        from pathlib import Path

        try:
            logger.debug(f"执行OpenList删除 - 删除目录: {delete_dir}, 目标: {item_name}")

            # 0. 保护目录检查
            if item_name in self._protected_directories:
                logger.error(f"拒绝删除受保护目录: {item_name}")
                return {"success": False, "error": f"受保护目录禁止删除: {item_name}"}

            # 1. 前置检查 - 用独立 API 列出目录确认目标存在
            _delete_dir_norm = str(Path(delete_dir))
            logger.info(f"[OpenList寻找] 用于查找目录的完整路径 = {_delete_dir_norm}")

            dir_content = self._openlist_list_dir(_delete_dir_norm)
            if dir_content is None:
                logger.warning(f"[OpenList寻找] 在路径 {_delete_dir_norm} 未找到目录，跳过删除")
                return {
                    "success": True,
                    "deleted_count": 0,
                    "deleted_items": [],
                    "source": "OLT",
                    "cloud_path": f"{_delete_dir_norm}/{item_name}",
                    "skip_reason": "目录不存在"
                }

            logger.debug(f"[OpenList寻找] 期望寻找的目标名称 = {item_name}")
            file_exists = any(
                isinstance(item, dict) and item.get("name") == item_name
                for item in dir_content
            )

            # 列表未命中时，用 /api/fs/get 精确确认完整路径是否存在（部分版本 list 对深层路径不敏感）
            _final_target_path = f"{_delete_dir_norm}/{item_name}"
            if not file_exists:
                target_info = self._openlist_get(_final_target_path)
                if target_info:
                    file_exists = True
                    logger.debug(f"[OpenList寻找] /api/fs/get 确认目标存在: {_final_target_path}")

            if not file_exists:
                # 仍不存在：尝试按文件名在父目录范围内搜索反查真实路径
                search_hits = self._openlist_search(item_name, parent=_delete_dir_norm, max_results=5)
                if search_hits:
                    hit = search_hits[0]
                    _delete_dir_norm = str(Path(hit.get("parent") or _delete_dir_norm))
                    item_name = hit.get("name") or item_name
                    _final_target_path = f"{_delete_dir_norm}/{item_name}"
                    file_exists = True
                    logger.info(f"[OpenList寻找] 搜索反查到真实路径: {_final_target_path}")
                else:
                    logger.warning(
                        f"[OpenList寻找] 在目录 {_delete_dir_norm} 下未匹配到目标 {item_name}，跳过删除")
                    return {
                        "success": True,
                        "deleted_count": 0,
                        "deleted_items": [],
                        "source": "OLT",
                        "cloud_path": _final_target_path,
                        "skip_reason": "文件/目录不存在"
                    }

            # 2. 执行删除 - 调用 alist /api/fs/remove
            _final_target_path = f"{_delete_dir_norm}/{item_name}"
            logger.debug(f"[OpenList寻找] 最终用于删除的完整路径 = {_final_target_path}")

            if self._openlist_remove(_delete_dir_norm, [item_name]):
                logger.info(f"OpenList删除成功: 删除了 {item_name}")

                cloud_path = f"{_delete_dir_norm}/{item_name}"

                # 3. 后置刷新
                refresh_success = self._openlist_list_dir(_delete_dir_norm) is not None

                # 4. 检测并清理网盘空目录
                self.__remove_cloud_parent_dir(cloud_path, "alist")

                result_data = {
                    "success": True,
                    "deleted_count": 1,
                    "deleted_items": [item_name],
                    "source": "OLT",
                    "cloud_path": cloud_path,
                    "refresh_success": refresh_success
                }

                if not refresh_success:
                    result_data["refresh_error"] = "目录刷新失败"

                return result_data
            else:
                logger.error(f"OpenList删除失败: 无法删除 {item_name}")
                return {"success": False, "error": "OpenList删除操作失败"}

        except Exception as e:
            logger.error(f"执行OpenList删除异常: {str(e)}")
            return {"success": False, "error": str(e)}

    # ==================== openlist构建通知 ===================
    def _prepare_openlist_notification(self, delete_result, media_name, media_type, season_num, episode_num):
        """
        准备OpenList删除通知数据
        """
        try:
            if delete_result.get('success'):
                deleted_count = delete_result.get('deleted_count', 0)
                if deleted_count > 0:
                    message = f"OpenList删除成功: 删除了 {deleted_count} 个文件/目录"

                    # 根据媒体类型构建标题
                    if media_type == MediaType.MOVIE:
                        title = f"电影删除: {media_name}"
                    elif media_type == MediaType.TV:
                        if season_num and episode_num:
                            title = f"剧集删除: {media_name} S{season_num:02d}E{episode_num:02d}"
                        elif season_num:
                            title = f"季删除: {media_name} 第{season_num}季"
                        else:
                            title = f"剧集删除: {media_name}"
                    else:
                        title = f"媒体删除: {media_name}"

                    return {
                        "title": title,
                        "message": message,
                        "success": True
                    }
                else:
                    return None  # 没有实际删除，不发送通知
            else:
                error_msg = delete_result.get('error', '未知错误')
                return {
                    "title": "OpenList删除失败",
                    "message": f"删除失败: {error_msg}",
                    "success": False
                }

        except Exception as e:
            logger.error(f"准备OpenList通知异常: {str(e)}")
            return None

    # ==================== CAS删除文件主程序 ====================
    def _process_cas_deletion(self, mount_paths_url, local_del_result, media_name, tmdb_id, media_type, season_num, episode_num, event_data=None):
        """
        删除云盘文件(CAS)── 查找匹配路径── 调用CAS API── 处理删除结果
        """
        cas_result = None
        account_id = None
        account_name = None
        matched_dir = None
        matched_file = None

        logger.info("CAS开始替换扩展名")

        deleted_targets = local_del_result.get("deleted_targets", set())

        # 1.替换.strm扩展名为实际扩展名
        cas_deleted_targets = self._cas_extension_replace(
            deleted_targets, mount_paths_url)

        logger.info("CAS开始路径匹配")

        # 2.进行路径映射匹配
        account_id, account_name, matched_dir, matched_file = self._match_cas_path(
            cas_deleted_targets,
            media_type == "episode",  # is_single_episode
            media_type == "season",   # is_whole_season
            media_type in ["series", "movie"]  # is_whole_series or movie
        )

        # 针对电影、剧集和季，确保只处理目录路径
        if media_type in ["series", "movie"] or (media_type == "season" and season_num):
            # 对于这些类型，我们只关心文件夹路径
            if matched_file and not matched_dir:
                # 只有当没有匹配到目录，但匹配到文件时，才提取其父目录作为匹配目录
                matched_dir = str(Path(matched_file).parent)
                matched_file = None
                logger.debug(f"针对{media_type}类型，将文件路径转换为目录路径: {matched_dir}")
            elif matched_dir:
                # 如果已经匹配到目录，直接使用目录路径
                logger.debug(f"针对{media_type}类型，使用已匹配的目录路径: {matched_dir}")

        # 4.判断媒体类型
        logger.info(f"CAS开始识别媒体类型: {media_name}")
        # 直接调用CAS延迟处理函数，不检查路径
        target_path = matched_file or matched_dir
        result = self._cas_delayed_process(
            media_name,
            target_path,
            account_id,
            None,  # year
            account_name,
            event_data,  # 传递完整的event_data用于在_cas_delayed_process中提取媒体信息
            media_type,  # 传递媒体类型
            season_num,  # 传递季号
            episode_num   # 传递集号
        )
        logger.info("CAS删除已完成")
        return result  # 返回结果用于后续处理

# ==================== CAS获取目录和id ====================
    def _get_cas_directory_id(self, account_id: str, target_path: str):
        """
        获取CAS云盘中指定路径的目录ID和名称
        """
        try:
            parts = [p for p in target_path.strip('/').split('/') if p]
            if not parts:
                logger.error("[CAS查询ID]无效路径")
                return None, None
            target_name = parts[-1]
            parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else ""
            logger.info(f"[CAS查询ID]获取CAS父目录内容: {parent_path or '根目录'}")
            url = f"{self._cas_host.rstrip('/')}/api/accounts/{account_id}/files"
            headers = {"x-api-key": self._cas_api_key}
            params = {"path": parent_path, "forceRefresh": "true"}
            res = self._safe_request(
                "GET", url, headers=headers, params=params)
            if not res:
                logger.error("[CAS查询ID]无法获取父目录内容")
                return None, None
            if res.status_code != 200:
                logger.error(
                    f"[CAS查询ID]获取父目录内容失败 ({res.status_code}): {res.text}")
                return None, None
            data = res.json()

            # 创建目录和文件的名称到信息的映射，提高查找效率
            folders = data.get("data", {}).get("folderList", [])
            files = data.get("data", {}).get("fileList", [])

            # 创建名称到信息的字典映射
            folder_map = {folder.get("name", ""): folder for folder in folders}
            file_map = {file.get("name", ""): file for file in files}

            # 调试日志已移除
            # 直接在字典中查找目录
            if target_name in folder_map:
                folder = folder_map[target_name]
                logger.info(f"找到目录ID: {target_name} (ID: {folder.get('id')})")
                return folder.get("id"), folder.get("name")

            # 调试日志已移除
            # 直接在字典中查找文件
            if target_name in file_map:
                file = file_map[target_name]
                logger.info(f"找到文件ID: {target_name} (ID: {file.get('id')})")
                return file.get("id"), file.get("name")

            # 如果都没找到，记录详细信息
            all_names = list(folder_map.keys()) + list(file_map.keys())
            logger.error(f"目标项目不存在: {target_name}，当前目录/文件列表: {all_names}")
            return None, target_name  # 返回目标名称，即使找不到也要传递路径信息
        except Exception as e:
            logger.error(f"刷新CAS目录异常: {str(e)}")
            logger.error(traceback.format_exc())

    # ==================== CAS替换.strm扩展名 ====================
    def _cas_extension_replace(self, deleted_targets, mount_paths_url):
        """
        将.strm文件路径替换为实际的媒体文件扩展名

        Args:
            deleted_targets: 要处理的删除目标路径集合
            mount_paths_url: 包含实际文件扩展名的URL

        Returns:
            set: 处理后的路径集合
        """
        cas_deleted_targets = set()
        for deleted_target in deleted_targets:
            logger.debug(f"待转换路径: {deleted_target}")
            # 对于.strm文件，尝试替换为实际扩展名
            if deleted_target.endswith('.strm') and mount_paths_url:
                # 从mount_paths中提取文件扩展名
                from urllib.parse import urlparse
                parsed_url = urlparse(mount_paths_url)
                url_path = parsed_url.path
                if url_path and '.' in url_path:
                    # 获取mount_paths中文件的扩展名
                    url_file_extension = url_path[url_path.rfind('.'):]
                    # 将源路径的.strm扩展名替换为mount_paths中的实际扩展名
                    # -5是因为.strm长度为5
                    actual_file_path = deleted_target[:-5] + url_file_extension
                    cas_deleted_targets.add(actual_file_path)
                    logger.info(f"CAS替换扩展名: {actual_file_path}")
                else:
                    # 如果没有mount_paths_url，直接添加原始路径
                    cas_deleted_targets.add(deleted_target)
            else:
                # 对于非.strm文件，直接添加
                cas_deleted_targets.add(deleted_target)

        return cas_deleted_targets

    # ==================== CAS路径匹配 ====================
    def _match_cas_path(self, cas_deleted_targets, is_single_episode, is_whole_season, is_whole_series):
        """
        匹配CAS路径映射，返回匹配结果

        Args:
            cas_deleted_targets: 要匹配的CAS删除目标路径集合
            is_single_episode: 是否为单集
            is_whole_season: 是否为整季
            is_whole_series: 是否为整剧

        Returns:
            tuple: (account_id, account_name, matched_dir, matched_file)
        """
        account_id = None
        account_name = None
        matched_dir = None
        matched_file = None

        for deleted_target in cas_deleted_targets:
            for mapping in self._cas_mappings:
                mapping_path = mapping["path"]
                logger.debug(
                    f"正在尝试匹配: CAS删除目标路径={deleted_target} 与 CAS映射配置={mapping_path} (账户ID: {mapping['account_id']})")
                if deleted_target.startswith(mapping_path):
                    rel_path = deleted_target[len(mapping_path):]
                    rel_path = "/" + rel_path.lstrip("/")
                    account_id = mapping["account_id"]
                    account_name = mapping.get("account_name", account_id)
                    logger.info(
                        f"映射命中: {mapping_path}，转换后云盘路径: {rel_path}，账户ID: {account_id} 名称: {account_name}")
                    # 修改逻辑：根据路径本身判断是文件还是目录
                    # 文件扩展名
                    if rel_path.endswith(('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.ts', '.strm')):
                        matched_file = rel_path
                    else:  # 目录
                        matched_dir = rel_path
                    break
            if account_id:
                break

        return account_id, account_name, matched_dir, matched_file

    # ==================== CAS判断使用哪种删除方法 ====================
    def _cas_delayed_process(self, title: str, cloud_path: str, account_id: str,
                             year: Optional[int] = None,
                             account_name: str = None, event_data=None,
                             media_type: str = None, season_num: str = None, episode_num: str = None):
        """
        CAS是启动剧集，单集还是季删除方法
        """
        try:
            logger.info(
                f"开始处理CAS目录删除: {title} (路径: {cloud_path}, 账户ID: {account_id})")

            # 如果已经传递了有效的媒体类型，直接使用它，避免重复判断
            if media_type and media_type.lower() in ["movie", "series", "season", "episode"]:
                media_type_category = media_type.lower()
                logger.debug(f"使用传递的媒体类型: {media_type_category}")
            else:
                # 调用识别媒体缓存
                media_type_category = self._get_media_type_from_emby_notification(
                    item_name=title, cloud_path=cloud_path, media_type=None, season_num=None,
                    episode_num=None, event_data=event_data, caller="判断CAS删除方法"
                )

            # 对于所有媒体类型删除，直接使用cloud_path路径
            # cloud_path: 要删除的目标目录路径（如：/影视剧/电视剧/国产剧/巨塔之后 (2025)）
            # Path(cloud_path).parent: 父目录路径，用于刷新操作（如：/影视剧/电视剧/国产剧）
            actual_cloud_path = cloud_path

            # 根据媒体类型调用相应的处理函数
            if media_type_category == "season":
                # 删除季目录
                logger.info(f"识别为季目录删除: {title}")
                return self._delete_cas_season_folder(title, actual_cloud_path, account_id, account_name)
            elif media_type_category == "episode":
                # 删除单集文件
                logger.info(f"识别为单集文件删除: {title}")
                return self._delete_cas_episode_file(title, cloud_path, account_id, account_name)
            elif media_type_category == "series":
                # 删除剧集目录
                logger.info(f"识别为剧集目录删除: {title}")
                return self._delete_cas_media_folder(title, actual_cloud_path, account_id, account_name)
            elif media_type_category == "movie":
                # 删除电影目录
                logger.info(f"识别为电影目录删除: {title}")
                return self._delete_cas_media_folder(title, actual_cloud_path, account_id, account_name)
            else:
                # 这种情况不应该再出现，因为_get_media_type_from_emby_notification现在总是返回有效类型
                logger.error(f"无法识别媒体类型，且默认判断方法未能识别: {title}")
                return {"success": False, "cloud_path": cloud_path, "error": "无法识别媒体类型"}
        except Exception as e:
            logger.error(f"CAS延迟处理异常: {str(e)}")
            logger.error(traceback.format_exc())
            return {"success": False, "cloud_path": cloud_path, "error": str(e)}

    # ==================== CAS删除单集文件 ====================
    def _delete_cas_episode_file(self, title: str, cloud_path: str, account_id: str, account_name: str = None):
        """
        删除单集文件
        """
        logger.info(f"开始删除单集文件: {title} (路径: {cloud_path})")

        # 从路径中提取目录路径和文件名
        if cloud_path:
            dir_path, file_name = str(
                Path(cloud_path).parent), str(Path(cloud_path).name)

            # 在删除前刷新父目录，确保获取最新状态
            logger.debug(f"刷新父目录: {dir_path}")
            self._refresh_cas_directory(account_id, dir_path)
        else:
            dir_path, file_name = None, None

        # 检查账户ID和路径是否存在
        logger.debug(f"确认天翼云盘地址={cloud_path}, 天翼云盘账户ID={account_id}")
        if not cloud_path or not account_id:
            logger.error("删除文件失败: 缺少路径或账户ID")
            return {"success": False, "cloud_path": cloud_path, "account_id": account_id, "account_name": account_name, "error": "缺少路径或账户ID"}
        try:

            # 先获取文件的ID和名称
            file_id, file_real_name = self._get_cas_directory_id(
                account_id, cloud_path)
            if not file_id or not file_real_name:
                error_msg = f"网盘文件不存在: {cloud_path}"
                logger.error(error_msg)
                # 确保在文件不存在时，同时传递cloud_path和target_path
                return {
                    "success": False,
                    "cloud_path": cloud_path,
                    "target_path": cloud_path,  # 添加target_path字段
                    "account_id": account_id,
                    "account_name": account_name,
                    "error": error_msg
                }

            # 构造删除请求，使用文件ID进行删除
            del_url = f"{self._cas_host.rstrip('/')}/api/account/files"
            del_headers = {"x-api-key": self._cas_api_key,
                           "Content-Type": "application/json"}
            payload = {
                "accountId": account_id,
                "files": [
                    {
                        "id": str(file_id),
                        "name": file_real_name,
                        "isFolder": 0
                    }
                ]

            }
            logger.debug(
                f"请求CAS删除文件: url={del_url}, payload={json.dumps(payload, ensure_ascii=False)}")
            del_res = self._safe_request(
                "DELETE", del_url, headers=del_headers, json=payload)
            cloud_file_deleted = cloud_path
            if del_res and del_res.status_code == 200:
                try:
                    del_data = del_res.json()
                    if del_data.get("success", False):
                        logger.info(f"CAS文件删除成功: {file_name}")

                        # 删除成功后刷新父目录
                        self._refresh_cas_directory(account_id, dir_path)

                        logger.debug(
                            f"_delete_episode_file return: success=True, cloud_path={cloud_file_deleted}")
                        return {"success": True, "cloud_path": cloud_file_deleted, "account_id": account_id, "account_name": account_name}
                    logger.error(f"CAS文件删除失败: {del_data.get('error', '未知错误')}")
                except Exception:
                    logger.error(f"CAS删除文件响应不是JSON: {del_res.text}")
            else:
                logger.error(
                    f"CAS文件删除失败 (HTTP {del_res.status_code if del_res else '无响应'})")
            logger.debug(
                f"_delete_episode_file return: success=False, cloud_path={cloud_file_deleted}")
            return {"success": False, "cloud_path": cloud_file_deleted, "account_id": account_id, "account_name": account_name, "error": "云盘文件删除失败"}
        except Exception as e:
            logger.error(f"删除文件异常: {str(e)}")
            logger.error(traceback.format_exc())
            logger.debug(
                f"_delete_episode_file return: success=False, cloud_path='' (异常)")
            return {"success": False, "cloud_path": cloud_path, "account_id": account_id, "account_name": account_name, "error": str(e)}

    def __check_protection_and_log(self, path: str, operation: str, item_type: str) -> bool:
        """
        检查路径是否受保护，并记录日志

        Args:
            path: 要检查的路径
            operation: 操作类型（删除、修改等）
            item_type: 项目类型（文件、目录等）

        Returns:
            bool: 如果路径受保护则返回True，否则返回False
        """
        if not path or not self._protected_directories:
            return False

        # 检查路径是否在任何保护目录中
        for protected_dir in self._protected_directories:
            if protected_dir and path.startswith(protected_dir):
                logger.error(
                    f"拒绝{operation}受保护{item_type}: {path} (保护目录: {protected_dir})")
                return True

        return False

    # ==================== CAS删除目录 ====================
    def _delete_cas_media_folder(self, title: str, cloud_path: str, account_id: str, account_name: str = None):
        """
        删除媒体文件夹（电影/剧集），不再检查目录是否为空
        """
        logger.info(f"开始删除媒体目录: {title} (路径: {cloud_path})")

        # 添加调试日志，检查CAS删除任务功能开关状态
        logger.debug(f"CAS删除任务功能开关状态: "
                     f"_cas_delete_normal_task_enabled={self._cas_delete_normal_task_enabled}, "
                     f"_cas_delete_crystal_task_enabled={self._cas_delete_crystal_task_enabled}")

        # 修复：检查是否启用了删除任务功能（使用正确的配置项）
        # 检查是否启用了任何一种删除任务功能
        if self._cas_delete_normal_task_enabled or self._cas_delete_crystal_task_enabled:
            logger.debug("CAS删除任务功能已启用，尝试删除CAS任务")
            # 确定要删除的任务类型
            if self._cas_delete_normal_task_enabled and self._cas_delete_crystal_task_enabled:
                # 两个都启用，查询所有任务
                task_type = "all"
            elif self._cas_delete_normal_task_enabled:
                task_type = "normal"
            else:
                task_type = "systemProxy"  # 玄晶任务对应的类型是systemProxy
            # 尝试先删除CAS任务，传递account_id进行筛选
            task_result = self._delete_cas_task(title, account_id, task_type)
            if task_result.get("success"):
                logger.info(f"[CAS任务删除] CAS任务删除成功: {title}")
                # 任务删除成功，表示文件已经被删除，直接返回成功结果
                return {
                    "success": True,
                    "cloud_path": cloud_path,
                    "account_id": account_id,
                    "account_name": account_name,
                    "task_deleted": True,
                    "task_result": task_result
                }
            else:
                # 任务删除失败，记录日志并继续执行常规目录删除
                logger.warning(
                    f"[CAS任务删除] CAS任务删除失败: {title}, 错误: {task_result.get('error')}")

                # 注意：这里不返回失败结果，而是继续执行常规目录删除逻辑
        else:
            logger.debug("[CAS任务删除] 功能未启用，执行常规目录删除")

        # 检查插件是否仍启用
        if not self._cas_enabled:
            logger.info(f"CAS插件已禁用，取消处理: {title}")
            return {"success": False, "cloud_path": cloud_path, "error": "CAS未启用"}

        # 使用统一的保护目录检查函数
        if cloud_path and self.__check_protection_and_log(str(Path(cloud_path).parent), "删除", "文件"):
            return {"success": False, "cloud_path": cloud_path, "error": f"受保护目录禁止删除: {Path(cloud_path).parent.name}"}

        # 在删除前刷新父目录，确保获取最新状态
        # cloud_path: 要删除的目标目录路径（如：/影视剧/电视剧/国产剧/巨塔之后 (2025)）
        # Path(cloud_path).parent: 父目录路径，用于刷新操作（如：/影视剧/电视剧/国产剧）
        if cloud_path:
            parent_path = str(Path(cloud_path).parent)
            logger.debug(f"刷新媒体目录的父目录: {parent_path}")
            self._refresh_cas_directory(account_id, parent_path)

    # ==================== CAS删除季文件夹 ====================
    def _delete_cas_season_folder(self, title: str, cloud_path: str, account_id: str, account_name: str = None):
        """
        删除季文件夹
        """
        logger.info(f"开始删除季目录: {title} (路径: {cloud_path})")

        # 检查插件是否启用
        if not self._cas_enabled:
            logger.info(f"CAS插件已禁用，取消处理: {title}")
            return {"success": False, "cloud_path": cloud_path, "error": "CAS未启用"}

        # 在删除前刷新父目录，确保获取最新状态
        if cloud_path:
            parent_path = str(Path(cloud_path).parent)
            logger.debug(f"刷新季目录的父目录: {parent_path}")
            self._refresh_cas_directory(account_id, parent_path)

        try:
            # 获取目录ID和名称
            dir_id, dir_name = self._get_cas_directory_id(
                account_id, cloud_path)
            if not dir_id or not dir_name:
                error_msg = f"网盘目录不存在: {cloud_path}"
                logger.error(error_msg)
                # 确保在找不到目录时，同时传递cloud_path和target_path
                return {
                    "success": False,
                    "cloud_path": cloud_path,
                    "target_path": cloud_path,  # 添加target_path字段
                    "account_id": account_id,
                    "account_name": account_name,
                    "error": error_msg
                }

            # 通过ID+name删除目录
            url = f"{self._cas_host.rstrip('/')}/api/account/files"
            headers = {"x-api-key": self._cas_api_key,
                       "Content-Type": "application/json"}
            payload = {
                "accountId": account_id,
                "files": [
                    {
                        "id": str(dir_id),
                        "name": dir_name,
                        "isFolder": 1
                    }
                ]
            }
            logger.debug(
                f"请求CAS删除季目录: url={url}, payload={json.dumps(payload, ensure_ascii=False)}")
            res = self._safe_request(
                "DELETE", url, headers=headers, json=payload)
            result = False
            if res and res.status_code == 200:
                try:
                    data = res.json()
                    if data.get("success", False):
                        logger.info(f"CAS季目录删除成功: {cloud_path}")
                        result = True

                        # 删除成功后刷新父目录
                        self._refresh_cas_directory(account_id, parent_path)
                    else:
                        logger.error(
                            f"CAS季目录删除失败: {data.get('error', '未知错误')}")
                except Exception:
                    logger.error(f"CAS删除季目录响应不是JSON: {res.text}")
                    return {"success": False, "cloud_path": cloud_path, "account_id": account_id, "account_name": account_name, "error": "响应解析失败"}
            else:
                logger.error(
                    f"CAS季目录删除失败 (HTTP {res.status_code if res else '无响应'})")
                return {"success": False, "cloud_path": cloud_path, "account_id": account_id, "account_name": account_name, "error": "删除请求失败"}

            if result:
                # 检查剧集目录是否为空
                if parent_path and parent_path != "/" and parent_path != cloud_path:
                    logger.info(f"季目录删除成功，检查剧集目录是否为空: {parent_path}")
                    # 检查剧集目录是否为空，如果为空则删除
                    url = f"{self._cas_host.rstrip('/')}/api/accounts/{account_id}/files"
                    headers = {"x-api-key": self._cas_api_key}
                    params = {"path": parent_path, "forceRefresh": "true"}
                    res = self._safe_request(
                        "GET", url, headers=headers, params=params)
                    if res and res.status_code == 200:
                        data = res.json()
                        files = data.get("data", {}).get("fileList", [])

                        # 检查目录下是否有媒体文件或txt文件
                        media_exts = [ext.lower()
                                      for ext in settings.RMT_MEDIAEXT] + [".txt"]
                        has_media = any(os.path.splitext(f.get("name", ""))[
                                        1].lower() in media_exts for f in files)
                        if not has_media:
                            # 剧集目录已空，删除剧集目录
                            logger.info(f"剧集目录为空，删除剧集目录: {parent_path}")
                            # 删除剧集目录
                            from pathlib import Path
                            delete_result = self._delete_cas_media_folder(
                                f"Series: {Path(parent_path).name}", parent_path, account_id, account_name)

                            # 如果剧集目录删除成功，刷新剧集的父目录
                            if delete_result.get("success"):
                                grandparent_path = str(
                                    Path(parent_path).parent)
                                self._refresh_cas_directory(
                                    account_id, grandparent_path)
                    else:
                        logger.error(f"无法获取剧集目录内容: {parent_path}")

            return {"success": result, "cloud_path": cloud_path, "account_id": account_id, "account_name": account_name}
        except Exception as e:
            logger.error(f"删除目录异常: {str(e)}")
            logger.error(traceback.format_exc())
            return {"success": False, "cloud_path": cloud_path, "account_id": account_id, "account_name": account_name, "error": str(e)}

    # ==================== CAS刷新目录 ====================
    def _refresh_cas_directory(self, account_id: str, path: str):
        """
        刷新CAS目录
        """
        try:
            if not self._cas_enabled or not account_id or not path:
                logger.debug("CAS未启用或缺少参数，跳过目录刷新")
                return

            refresh_url = f"{self._cas_host.rstrip('/')}/api/accounts/{account_id}/files"
            headers = {"x-api-key": self._cas_api_key}
            params = {
                "path": path,
                "forceRefresh": "true"
            }

            logger.debug(f"开始刷新CAS目录: {path}")
            res = self._safe_request(
                "GET", refresh_url, headers=headers, params=params)

            if res and res.status_code == 200:
                logger.info(f"CAS目录刷新成功: {path}")
            else:
                logger.warning(
                    f"CAS目录刷新失败: {path} (HTTP {res.status_code if res else '无响应'})")
        except Exception as e:
            logger.error(f"刷新CAS目录异常: {str(e)}")
            logger.error(traceback.format_exc())

    # ==================== 删除CAS任务 ====================
    def _delete_cas_task(self, title: str, account_id: str, account_name: str = None, task_type: str = None):
        """
        删除CAS任务

        该函数用于根据媒体标题查询并删除CAS任务，同时删除网盘中的文件

        Args:


            title (str): 媒体标题
            account_id (str): CAS账户ID
            account_name (str, optional): CAS账户名称
            task_type (str, optional): 任务类型，可选值: "normal", "systemProxy", "all"

        Returns:
            dict: 包含操作结果的字典
                - success (bool): 操作是否成功
                - task_id (str): 删除的任务ID（如果找到）
                - account_id (str): 账户ID
                - account_name (str): 账户名称
                - error (str, optional): 错误信息（如果操作失败）
        """
        logger.info(f"开始查询并删除CAS任务: {title} (天翼网盘账户ID: {account_id}")

        # 检查是否启用了删除任务功能
        # 根据task_type参数判断需要哪种任务删除功能是否启用
        should_delete = False
        if task_type == "all":
            # 需要删除所有类型的任务，检查是否至少启用了一种
            should_delete = self._cas_delete_normal_task_enabled or self._cas_delete_crystal_task_enabled
            logger.debug(
                f"任务类型为'all'，检查是否至少启用一种任务删除功能: 普通任务={self._cas_delete_normal_task_enabled}, 玄鲸任务={self._cas_delete_crystal_task_enabled}")
        elif task_type == "normal":
            # 只需要删除普通任务
            should_delete = self._cas_delete_normal_task_enabled
            logger.debug(
                f"任务类型为'normal'，检查普通任务删除功能是否启用: {self._cas_delete_normal_task_enabled}")
        elif task_type == "systemProxy":
            # 只需要删除玄鲸任务（systemProxy类型）
            should_delete = self._cas_delete_crystal_task_enabled
            logger.debug(
                f"任务类型为'systemProxy'，检查玄鲸任务删除功能是否启用: {self._cas_delete_crystal_task_enabled}")
        elif task_type is None:
            # 对于None值（默认情况），检查是否至少启用了一种任务删除功能
            should_delete = self._cas_delete_normal_task_enabled or self._cas_delete_crystal_task_enabled
            logger.debug(
                f"任务类型为None（默认），检查是否至少启用一种任务删除功能: 普通任务={self._cas_delete_normal_task_enabled}, 玄鲸任务={self._cas_delete_crystal_task_enabled}")
        else:
            # 对于其他未知的任务类型，不启用删除功能
            should_delete = False
            logger.debug(f"未知的任务类型'{task_type}'，不启用CAS任务删除功能")

        if not should_delete:
            logger.debug(
                f"[CAS任务删除] 功能未启用，执行常规目录删除。当前配置: 普通任务删除功能={self._cas_delete_normal_task_enabled}, 玄鲸任务删除功能={self._cas_delete_crystal_task_enabled}, 请求任务类型={task_type}")
            return {"success": False, "error": "CAS删除任务未启用"}

        # 根据配置自动选择正确的任务类型
        if task_type == "all" or task_type is None:
            # 如果请求类型是'all'或None，根据实际配置选择正确的类型
            if self._cas_delete_normal_task_enabled and not self._cas_delete_crystal_task_enabled:
                actual_task_type = "normal"
            elif not self._cas_delete_normal_task_enabled and self._cas_delete_crystal_task_enabled:
                actual_task_type = "systemProxy"
            else:
                actual_task_type = "all"  # 两者都启用或都不启用时使用'all'
        else:
            actual_task_type = task_type

        task_type_map = {
            'all': '全部任务',
            'normal': '普通任务',
            'systemProxy': '玄鲸任务',
            None: '未知任务'
        }
        logger.info(
            f"实际使用的任务类型: {task_type_map.get(actual_task_type, '未知任务')}（{actual_task_type}）")

        try:
            # 查询任务ID
            query_url = f"{self._cas_host.rstrip('/')}/api/tasks"
            headers = {"x-api-key": self._cas_api_key}
            params = {
                "page": 1,
                "pageSize": 20,
                "status": "all",
                "type": actual_task_type,      # 使用自动选择的正确类型
                "search": title,        # 使用媒体标题作为搜索关键字
                "group": "all",
                "accountId": account_id  # 指定账户ID
            }

            logger.debug(f"请求CAS任务查询: url={query_url}, params={params}")
            res = self._safe_request(
                "GET", query_url, headers=headers, params=params)

            if not res or res.status_code != 200:
                logger.error(
                    f"CAS任务查询失败 (HTTP {res.status_code if res else '无响应'})")
                return {"success": False, "error": "任务查询失败"}

            try:
                data = res.json()
            except Exception:
                logger.error(f"CAS任务查询响应不是JSON: {res.text}")
                return {"success": False, "error": "任务查询响应解析失败"}

            # 查找匹配的任务
            task_list = data.get("data", {}).get("tasks", [])  # 修正数据结构路径
            target_task = None

            # 遍历任务列表，使用多种可能的字段名进行匹配
            for task in task_list:
                # 检查任务名称是否匹配（支持多种字段名）
                task_names = [
                    task.get("name", ""),
                    task.get("resourceName", ""),
                    task.get("title", "")
                ]

                # 如果任何一个字段匹配title，则认为是目标任务
                if title in task_names or any(title in name for name in task_names if name):
                    target_task = task
                    break

                # 如果直接匹配失败，尝试去除年份部分再匹配
                clean_title = re.sub(r'\s*\(\d{4}\)$', '', title).strip()
                if clean_title != title:
                    if clean_title in task_names or any(clean_title in name for name in task_names if name):
                        target_task = task
                        logger.debug(f"通过清理后的标题匹配到任务: {clean_title}")
                        break

            # 如果没有找到匹配的任务
            if not target_task:
                logger.info(f"未找到匹配的CAS任务: {title}")
                # 记录所有任务名称用于调试
                all_task_names = []
                for task in task_list:
                    name = task.get("name") or task.get(
                        "resourceName") or task.get("title") or "Unknown"
                    all_task_names.append(name)
                logger.debug(f"所有可用任务: {all_task_names}")
                return {"success": False, "error": "未找到匹配的任务"}

            task_id = target_task.get("id")
            task_name = target_task.get("name") or target_task.get(
                "resourceName") or target_task.get("title") or "Unknown"
            logger.info(f"找到匹配的CAS任务: {task_name} (任务ID: {task_id})")

            # 删除任务，同时删除网盘文件
            delete_url = f"{self._cas_host.rstrip('/')}/api/tasks/{task_id}"
            delete_data = {"deleteCloud": True}  # 删除网盘一并删除
            logger.debug(f"请求CAS删除任务: url={delete_url}, data={delete_data}")

            delete_res = self._safe_request(
                "DELETE", delete_url, headers=headers, json=delete_data)
            if delete_res and delete_res.status_code == 200:
                logger.info(f"[CAS任务删除] 任务删除成功: {task_id}")
                return {
                    "success": True,
                    "task_id": task_id,
                    "target_path": title,  # 添加target_path字段，使用标题作为路径参考
                    "account_id": account_id,
                    "account_name": account_name
                }
            else:
                logger.error(
                    f"[CAS任务删除] 任务删除失败 (HTTP {delete_res.status_code if delete_res else '无响应'})")
                return {
                    "success": False,
                    "task_id": task_id,
                    "target_path": title,  # 添加target_path字段，使用标题作为路径参考
                    "account_id": account_id,
                    "account_name": account_name,
                    "error": "删除请求失败"
                }

        except Exception as e:
            logger.error(f"删除CAS任务异常: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "target_path": title,  # 添加target_path字段，使用标题作为路径参考
                "account_id": account_id,
                "account_name": account_name,
                "error": str(e)
            }

    # ==================== 通知主流程 ====================
    def _send_notifications(self, media_name, tmdb_id, media_type, season_num, episode_num,
                            local_del_result, cas_result, media_path, event_data=None):
        """
        发送通知── 获取媒体信息── 格式化消息── 发送通知── 保存历史
        """
        # 初始化变量
        media_info = None
        image_url = ""
        poster_url = ""
        backdrop_url = ""
        main_title = media_name
        main_year = local_del_result.get("year")

        logger.debug(f"开始获取通知图片")

        # 1.使用_get_poster_image_url函数获取图片URL
        image_url = self._get_poster_image_url(event_data=event_data)
        poster_url = ""
        backdrop_url = ""

        # 设置界面历史用图
        history_image = image_url

        # 通知用图片
        notify_image = image_url

        # 如果没有获取到图片URL，则使用默认图片
        if not image_url:
            image_url = "https://emby.media/notificationicon.png"
            logger.debug("没有获取到图片URL，使用默认图片")

        # 2.调用_get_media_type_from_emby_notification的缓存机制获取媒体类型
        cached_media_type = self._get_media_type_from_emby_notification(
            item_name=media_name, cloud_path=media_path, media_type=media_type, season_num=season_num, episode_num=episode_num,
            event_data=event_data, caller="通知图片获取"
        ) if event_data else media_type

        # 使用缓存的媒体类型作为主要类型
        main_title = media_name
        main_year = local_del_result.get("year")

        # 3.发送通知（直接发送，不使用Future回调）
        self._send_merged_notification(
            main_title, main_year, cached_media_type, notify_image, local_del_result, cas_result,
            season_num if (season_num and episode_num) else None,
            episode_num if (season_num and episode_num) else None,
            media_path=media_path
        )

        # 4.保存历史（仅在本地删除成功或CAS清理成功时保存）
        try:
            # 检查本地删除是否成功或CAS清理是否成功
            local_success = local_del_result.get(
                "success", False) if local_del_result else False
            cas_success = cas_result.get(
                "success", False) if cas_result else False

            # 仅在本地删除成功或CAS清理成功时保存历史记录
            if local_success or cas_success:
                historys = self.get_data("history") or []
                historys.append({
                    "type": cached_media_type,
                    "title": media_name,
                    "year": main_year,
                    "path": media_path,
                    "season": season_num if season_num and str(season_num).isdigit() else None,
                    "episode": episode_num if episode_num and str(episode_num).isdigit() else None,
                    "image": history_image if history_image else "https://emby.media/notificationicon.png",  # 确保始终有图片URL
                    "del_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "unique": f"{media_name}:{tmdb_id}:{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "cas_success": cas_success,  # 云盘删除是否成功（兼容旧字段）
                    "cloud_source": cas_result.get("source") if isinstance(cas_result, dict) else None  # 云盘删除来源：CAS 或 OLT
                })

                # 限制历史记录数量，只保留最新的16条
                if len(historys) > 16:
                    historys = historys[-16:]
                    logger.info(f"历史记录数量超过16条，已清理旧记录，保留最新16条")

                self.save_data("history", historys)
                logger.info(f"历史已保存，当前历史数: {len(historys)}")
            else:
                logger.info(f"本地删除和CAS清理均失败，不保存历史记录")
        except Exception as e:
            logger.error(f"保存历史失败: {e}")

    # ==================== 通知年份处理 ====================
    def _format_notify_title(self, media_type, title, year, season_num=None, episode_num=None, local_del_result=None, cas_result=None):
        try:
            meta = MetaInfo(title)
            pure_title = meta.title or title
            pure_year = meta.year or year
        except Exception:
            pure_title = title
            pure_year = year

        # 避免重复添加年份，如果标题中已经包含年份则不添加
        if pure_year and f"({pure_year})" not in title:
            title_with_year = f"{pure_title} ({pure_year})"
        else:
            title_with_year = pure_title

        # 根据删除结果动态设置标题
        local_success = local_del_result.get(
            "success", False) if local_del_result else False
        cas_success = cas_result.get("success", False) if cas_result else False

        # 判断删除状态
        if local_success and cas_success:
            # 本地和云盘都删除成功
            status_text = "本地和云盘已删"
        elif local_success and not cas_success:
            # 仅本地删除成功
            status_text = "本地已删"
        elif not local_success and cas_success:
            # 仅云盘删除成功
            status_text = "云盘已删"
        else:
            # 都删除失败
            status_text = "删除失败"

        if season_num and episode_num:
            return f"《{title_with_year}》 第{season_num}季 第{episode_num}集 {status_text}"
        else:
            return f"《{title_with_year}》 {status_text}"

    # ==================== 通知文本样式 ====================
    def _format_notify_content(self, transfer_count, account_name, title, year, season_num=None, episode_num=None, cas_result=None, media_path=None, media_type=None, local_del_result=None):
        try:
            type_map = {
                "TV": "电视剧",
                "Series": "电视剧",
                "tv": "电视剧",
                "Movie": "电影",
                "MOV": "电影",
                "movie": "电影",
                "episode": "单集",
                "season": "季",
                "series": "剧集"
            }
            type_str = type_map.get(str(media_type), str(media_type) or "")

            # 第一行通知图标
            type_icon = "🎬"
            record_icon = "📦"
            source_icon = "🔧"

            # 添加删除任务来源信息（CAS或OLT）
            source_info = "未启动"
            if cas_result is not None:
                source = cas_result.get('source')
                if source == 'CAS':
                    source_info = f"CAS"
                elif source == 'OLT':
                    source_info = f"OLT"

            # 通知界面样式
            type_record_line = f"{type_icon}类型:{type_str} ｜ {record_icon}记录:{transfer_count}个｜{source_icon}删除:{source_info}\n\u3000\n"

            text = type_record_line
            success_emoji = "✅"
            fail_emoji = "⛔️"
            path_emoji = "⚠️"
            if cas_result is not None:
                source = cas_result.get('source')
                cloud_path = cas_result.get('cloud_path', '')

                # 区分CAS和OpenList的acc_name提取方式
                if source == 'OLT' and cloud_path and cloud_path.startswith('/'):
                    # OpenList删除：从路径中提取第一个路径部分作为acc_name
                    # 例如：从 "/天翼云盘5/影视剧/电视剧/日韩剧/瑞草洞 (2025)/Season 1" 提取 "天翼云盘5"
                    path_parts = cloud_path.strip('/').split('/')
                    if path_parts:
                        acc_name = path_parts[0]  # 第一个路径部分
                    else:
                        acc_name = cas_result.get('account_name', account_name)
                else:
                    # CAS删除：使用原有的account_name
                    acc_name = cas_result.get('account_name', account_name)

                is_file = any(cloud_path and cloud_path.lower().endswith(ext)
                              for ext in ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv'])

                # 确保总是显示网盘路径
                show_path = cloud_path or cas_result.get('target_path', '未知路径')
                path_label = "网盘路径"

                if cas_result.get("success", True):
                    # 删除成功
                    # 根据媒体类型判断显示文本
                    if media_type and media_type.lower() in ['movie', 'mov']:
                        # 电影类型显示目录删除成功
                        text += f"{success_emoji}删除网盘：{acc_name}（目录删除成功）\n"
                    elif is_file or (season_num and episode_num):
                        # 文件或明确的单集显示单集删除成功
                        text += f"{success_emoji}删除网盘：{acc_name}（单集删除成功）\n"
                    else:
                        # 其他情况显示目录删除成功
                        text += f"{success_emoji}删除网盘：{acc_name}（目录删除成功）\n"
                    # 删除成功时也显示网盘路径
                    text += f"{path_emoji}{path_label}：{show_path}\n"
                else:
                    # 删除失败/未命中
                    reason = cas_result.get('error', '未知原因')

                    # 特别处理"网盘目录不存在"的情况，确保路径信息清晰显示
                    if "网盘目录不存在" in reason or "找不到" in reason:
                        text += f"{fail_emoji}删除网盘：{acc_name}（失败-找不到目录）\n"
                        text += f"{path_emoji}{path_label}：{show_path}\n"
                    else:
                        text += f"{fail_emoji}删除网盘：{acc_name}（失败-{reason}）\n"
                        text += f"{path_emoji}{path_label}：{show_path}\n"
            elif account_name:
                text += f"{success_emoji}删除网盘：{account_name}\n"

            # 令牌失效主动提示
            if getattr(self, "_openlist_token_invalid", False):
                text += f"⚠️OpenList令牌可能已失效，云盘删除可能失败，请到设置页更新管理令牌\n"

            # 显示本地删除路径（便于核对实际删了哪些本地文件）
            if local_del_result:
                local_targets = local_del_result.get("deleted_targets") or set()
                local_paths = sorted(str(p) for p in local_targets)
                if local_paths:
                    text += f"📁本地删除：\n"
                    for lp in local_paths[:10]:
                        text += f"  - {lp}\n"
                    if len(local_paths) > 10:
                        text += f"  - …等共 {len(local_paths)} 项\n"

            text += f"🕒删除时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            logger.debug(f'_format_notify_content生成内容: {text}')
            return text
        except Exception as e:
            logger.error(f'_format_notify_content异常: {e}', exc_info=True)
            return '通知内容生成异常'

    # ==================== 未找到转移记录通知 ====================
    def _send_no_transfer_history_notification(self, media_name, media_type, media_path, tmdb_id, season_num, episode_num, event_data=None):
        """
        发送未找到转移记录的通知
        """
        try:
            # 获取媒体类型的中文名称
            type_map = {
                "TV": "电视剧",
                "Series": "电视剧",
                "tv": "电视剧",
                "Movie": "电影",
                "MOV": "电影",
                "movie": "电影",
                "episode": "单集",
                "season": "季",
                "series": "剧集"
            }
            type_str = type_map.get(str(media_type), str(media_type) or "未知")

            # 使用_get_poster_image_url函数获取图片URL
            image_url = self._get_poster_image_url(event_data=event_data)
            if not image_url:
                image_url = "https://emby.media/notificationicon.png"
                logger.debug("未找到转移记录通知：没有获取到图片URL，使用默认图片")

            # 格式化通知标题
            notify_title = f"《{media_name}》 未找到转移记录"

            # 构建通知内容
            content_lines = []
            # 第一行：类型和记录数
            content_lines.append(f"🎬类型:{type_str} ｜ 📦记录:0个｜ 🔧删除:未启动\n\u3000")

            # 第二行：失败信息和路径
            content_lines.append("⛔️删除本地：未找到转移记录\n")
            content_lines.append("⚠️本地路径：媒体库中未找到匹配记录\n")

            # 第三行：删除时间
            content_lines.append(
                f"🕒删除时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            notify_content = "\n".join(content_lines)

            logger.info(
                f"发送未找到转移记录通知: title={notify_title}, content={notify_content}")

            # 发送通知
            self.post_message(
                mtype=NotificationType.Plugin,
                title=notify_title,
                text=notify_content,
                image=image_url
            )

            logger.info(f"已发送未找到转移记录通知: {media_name}")

            # 保存历史记录
            try:
                historys = self.get_data("history") or []
                historys.append({
                    "type": media_type,
                    "title": media_name,
                    "year": None,  # 未找到转移记录时无法获取年份
                    "path": media_path,
                    "season": season_num if season_num and str(season_num).isdigit() else None,
                    "episode": episode_num if episode_num and str(episode_num).isdigit() else None,
                    "image": image_url,
                    "del_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "unique": f"{media_name}:{tmdb_id}:{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "no_transfer_record": True  # 标记为未找到转移记录
                })

                # 限制历史记录数量，只保留最新的16条
                if len(historys) > 16:
                    historys = historys[-16:]
                    logger.info(f"历史记录数量超过16条，已清理旧记录，保留最新16条")

                self.save_data("history", historys)
                logger.info(f"未找到转移记录历史已保存，当前历史数: {len(historys)}")
            except Exception as e:
                logger.error(f"保存未找到转移记录历史失败: {e}")

        except Exception as e:
            logger.error(
                f'_send_no_transfer_history_notification异常: {e}', exc_info=True)

    # ==================== 通知发送逻辑 ====================
    def _send_notify(self, title: str, year: Optional[int], media_type: Optional[str], image_url: str,
                     transfer_count: int, account_name: str = None, season_num: str = None, episode_num: str = None, cas_result: dict = None, media_path: str = None, local_del_result: dict = None):
        try:
            logger.debug(f"准备发送通知: title={title}, year={year}, media_type={media_type}, image_url={image_url}, transfer_count={transfer_count}, account_name={account_name}, season_num={season_num}, episode_num={episode_num}, cas_result={cas_result}, media_path={media_path}, local_del_result={local_del_result}")
            use_image = image_url
            notify_title = self._format_notify_title(
                media_type, title, year, season_num, episode_num, local_del_result, cas_result)
            notify_content = self._format_notify_content(
                transfer_count, account_name, title, year, season_num, episode_num, cas_result, media_path, media_type, local_del_result)
            logger.info(
                f"最终通知内容: title={notify_title}, content={notify_content}")
            self.post_message(
                mtype=NotificationType.Plugin,
                title=notify_title,
                text=notify_content,
                image=use_image
            )
            logger.info(f"已发送统一格式通知: {title}")
        except Exception as e:
            logger.error(f'_send_notify异常: {e}', exc_info=True)

    # ==================== 本地删除通知 ====================
    def _send_local_notification(self, title: str, year: Optional[int],
                                 media_type: Optional[str], image_url: str,
                                 local_del_result: dict, season_num: str = None, episode_num: str = None, account_name: str = None, cas_result: dict = None, media_path: str = None):
        self._send_notify(title, year, media_type, image_url, local_del_result.get(
            'transfer_count', 0), account_name, season_num, episode_num, cas_result, media_path, local_del_result)

    # ==================== CAS清理通知 ====================
    def _send_cas_clean_notification(self, title: str, year: Optional[int],
                                     media_type: Optional[str], cloud_path: str,
                                     account_id: str, image_url: str = "", season_num: str = None, episode_num: str = None, account_name: str = None, cas_result: dict = None, media_path: str = None):
        self._send_notify(title, year, media_type, image_url, 0, account_name or account_id,
                          season_num, episode_num, cas_result, media_path, None)

    # ==================== 合并所有通知 ====================
    def _send_merged_notification(self, title: str, year: Optional[int],
                                  media_type: Optional[str], image_url: str,
                                  local_del_result: dict, cas_result: dict, season_num: str = None, episode_num: str = None, account_name: str = None, media_path: str = None):
        # account_name 优先从 cas_result 获取
        acc_name = account_name
        if cas_result is not None:
            acc_name = cas_result.get(
                'account_name', cas_result.get('account_id', account_name))
        self._send_notify(title, year, media_type, image_url, local_del_result.get(
            'transfer_count', 0), acc_name, season_num, episode_num, cas_result, media_path, local_del_result)

    # ==================== 退出插件 ====================
    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None

            # 关闭CAS线程池
            if self._cas_thread_pool:
                self._cas_thread_pool.shutdown(wait=False)
                self._cas_thread_pool = None

        except Exception as e:
            logger.error(f"停止服务时出错: {str(e)}")

    # ==================== 清理重复事件历史记录 ====================
    def _cleanup_expired_handled_events(self):
        """
        清理过期的已处理事件记录（超过5分钟的记录）
        """
        current_time = time.time()
        expired_keys = []

        # 找出过期的事件键
        for key, timestamp in self._handled_delete_events_timestamps.items():
            if current_time - timestamp > 300:  # 5分钟 = 300秒
                expired_keys.append(key)

        # 删除过期的记录
        for key in expired_keys:
            self._handled_delete_events.discard(key)
            self._handled_delete_events_timestamps.pop(key, None)

        if expired_keys:
            logger.debug(f"清理了 {len(expired_keys)} 个过期的已处理事件记录")

    # ==================== 清理缓存 ====================
    def _clear_media_type_cache(self):
        """
        清理媒体类型缓存
        """
        if hasattr(self, '_media_type_cache'):
            cache_size = len(self._media_type_cache)
            self._media_type_cache.clear()
            logger.debug(f"已清理媒体类型缓存，清理前缓存项数: {cache_size}")

        if hasattr(self, '_media_type_cache_timestamps'):
            self._media_type_cache_timestamps.clear()

    # ==================== 设置界面删除历史记录按钮 ====================
    def delete_history(self, key: str, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        # 历史记录
        historys = self.get_data("history")
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")
        # 删除指定记录
        historys = [h for h in historys if h.get("unique") != key]
        self.save_data("history", historys)
        return schemas.Response(success=True, message="删除成功")

    # ==================== 测试OpenList连接 ====================
    def _test_openlist_connection(self) -> tuple[bool, str]:
        """
        核心测试逻辑（仅令牌方式）：建立令牌连接并列举根目录验证权限。
        返回 (是否成功, 说明信息)。
        """
        if not self._openlist_host:
            return False, "OpenList地址未配置，请先填写地址"
        if not self._openlist_token_raw:
            return False, "未配置管理令牌，请先填写令牌（仅支持令牌方式）"

        # 使用令牌建立连接
        if not self._openlist_login():
            return False, "令牌连接失败：请检查地址与令牌是否正确"

        # 连接成功后尝试列根目录，验证权限
        content = self._openlist_list_dir("/")
        if content is None:
            return False, "令牌连接成功但列举根目录失败，可能令牌权限不足"

        return True, f"连接成功，已获取根目录，共 {len(content)} 个项目"

    def _auto_test_openlist(self):
        """插件启动时自动测试OpenList连接，结果输出到日志（不弹窗）"""
        ok, msg = self._test_openlist_connection()
        if ok:
            logger.info(f"[OpenList自动测试] {msg}")
        else:
            self._openlist_token_invalid = True
            logger.warning(f"[OpenList自动测试] {msg}（如为令牌问题，请到设置页更新管理令牌）")

    def test_openlist(self, apikey: str):
        """手动测试OpenList连接是否正常（仅令牌方式）"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        try:
            ok, msg = self._test_openlist_connection()
            return schemas.Response(success=ok, message=msg)
        except Exception as e:
            logger.error(f"测试OpenList连接异常: {str(e)}")
            return schemas.Response(success=False, message=f"连接异常: {str(e)}")

    # ==================== 执行删除历史记录 ====================
    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除历史记录",
            },
            {
                "path": "/test_openlist",
                "endpoint": self.test_openlist,
                "methods": ["GET"],
                "summary": "测试OpenList连接",
            },
        ]

    # ==================== 删除历史数据卡片 ====================
    def get_page(self) -> List[dict]:
        historys = self.get_data("history") or []
        logger.debug(f"get_page 读取到的历史数据: {historys}")
        if not historys:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {
                        "class": "text-center",
                    },
                }
            ]
        # 类型映射
        type_mapping = {
            "movie": "电影",
            "tv": "电视剧",
            "show": "电视剧",
            "series": "电视剧",
            "season": "季",
            "episode": "单集"
        }

        # 数据按时间降序排序
        historys = sorted(historys, key=lambda x: x.get(
            "del_time"), reverse=True)
        contents = self._build_history_cards(historys, type_mapping, show_delete_btn=True)
        return [
            {
                "component": "div",
                "props": {
                    "class": "grid gap-3 grid-info-card",
                },
                "content": contents,
            }
        ]

    # ==================== 历史记录卡片构造（get_page 与 get_dashboard 共用） ====================
    def _build_history_cards(self, historys: List[Dict[str, Any]],
                             type_mapping: Dict[str, str],
                             show_delete_btn: bool = True) -> List[dict]:
        """
        根据历史记录列表拼装卡片
        :param historys: 已按时间降序排序的历史记录
        :param type_mapping: 类型中文映射
        :param show_delete_btn: 是否显示删除按钮（设置页为True，仪表盘只读为False）
        """
        contents = []
        for history in historys:
            htype = history.get("type")
            title = history.get("title")
            unique = history.get("unique")
            year = history.get("year")
            season = history.get("season")
            episode = history.get("episode")
            image = history.get("image")
            del_time = history.get("del_time")

            # 检查是否为"未找到转移记录"的情况
            is_no_transfer_record = history.get("no_transfer_record", False)

            if is_no_transfer_record:
                # 为"未找到转移记录"的情况创建特殊显示
                sub_contents = [
                    {"component": "VCardText", "props": {
                        "class": "pa-0 px-2"}, "text": f"标题：{title}"},
                    {"component": "VCardText", "props": {"class": "pa-0 px-2"},
                        "text": f"类型：{type_mapping.get(htype, htype)}"},
                ]

                # 添加年份信息（如果有）
                if year:
                    sub_contents.append(
                        {"component": "VCardText", "props": {
                            "class": "pa-0 px-2"}, "text": f"年份：{year}"}
                    )

                # 添加季集信息（如果有）
                if season:
                    sub_contents.append(
                        {"component": "VCardText", "props": {"class": "pa-0 px-2"},
                         "text": f"季集：第{season}季 第{episode}集"}
                    )

                # 只在"未找到转移记录"这部分使用红色文本
                sub_contents.append(
                    {"component": "VCardText", "props": {"class": "pa-0 px-2 red--text"},
                     "text": "网盘：未找到转移记录"}
                )
            elif season:
                sub_contents = [
                    {"component": "VCardText", "props": {
                        "class": "pa-0 px-2"}, "text": f"标题：{title}"},
                    {"component": "VCardText", "props": {"class": "pa-0 px-2"},
                        "text": f"类型：{type_mapping.get(htype, htype)}　第{season}季 第{episode}集"},
                    {"component": "VCardText", "props": {
                        "class": "pa-0 px-2"}, "text": f"年份：{year}"},
                ]
            else:
                sub_contents = [
                    {"component": "VCardText", "props": {
                        "class": "pa-0 px-2"}, "text": f"标题：{title}"},
                    {"component": "VCardText", "props": {"class": "pa-0 px-2"},
                        "text": f"类型：{type_mapping.get(htype, htype)}"},
                    {"component": "VCardText", "props": {
                        "class": "pa-0 px-2"}, "text": f"年份：{year}"},
                ]

            # 添加CAS删除状态信息（仅对非"未找到转移记录"的情况）
            if not is_no_transfer_record:
                cas_success = history.get("cas_success")
                if cas_success is not None:
                    # 根据真实云盘来源显示正确文案：CAS 或 OpenList(OLT)
                    cloud_source = history.get("cloud_source")
                    if cloud_source == "OLT":
                        status_text = "OLT已删除" if cas_success else "未删除"
                    else:
                        status_text = "CAS已删除" if cas_success else "未删除"
                    sub_contents.append(
                        {"component": "VCardText", "props": {
                            "class": "pa-0 px-2"}, "text": f"网盘：{status_text}"}
                    )

            # 添加时间信息
            sub_contents.append(
                {"component": "VCardText", "props": {
                    "class": "pa-0 px-2"}, "text": f"时间：{del_time}"}
            )

            # 根据是否为"未找到转移记录"设置不同的卡片样式
            card_props = {
                "class": "relative",  # 新增，确保绝对定位生效
                "variant": "outlined",  # 添加variant属性，使用outlined样式替代默认的圆角卡片
            }

            # 如果是"未找到转移记录"，添加红色边框和背景
            if is_no_transfer_record:
                card_props["class"] += " border-red-500 red-lighten-5"
                card_props["color"] = "red-lighten-5"

            # 图片区域：仪表盘只读时不显示删除按钮
            image_area_content = []
            if show_delete_btn:
                image_area_content.append(
                    {
                        "component": "VDialogCloseBtn",
                        "props": {
                            "innerClass": "absolute top-0 left-0 p-0 z-10",  # 调整按钮位置到图片左上角
                            "size": "x-small",
                        },
                        "events": {
                            "click": {
                                "api": "plugin/embysyncdeletioncloud/delete_history",
                                "method": "get",
                                "params": {
                                    "key": unique,
                                    "apikey": settings.API_TOKEN,
                                },
                            }
                        },
                    }
                )
            image_area_content.append(
                {
                    "component": "VImg",
                    "props": {
                        "src": image if image else "https://emby.media/notificationicon.png",
                        "width": "100%",
                        "aspect-ratio": "2/3",
                        "class": "object-cover shadow ring-gray-500",
                        "cover": True,
                        "style": "border-radius: 0px;",
                        "fallback-src": "https://emby.media/notificationicon.png",  # 添加备用图片
                    },
                }
            )

            contents.append(
                {
                    "component": "VCard",
                    "props": card_props,
                    "content": [
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex justify-space-start flex-nowrap flex-row",
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "props": {
                                        "class": "relative",  # 添加相对定位，为删除按钮提供定位上下文
                                        "style": "width: 80px; flex: 0 0 80px;",
                                    },
                                    "content": image_area_content,
                                },
                                {"component": "div",
                                 "props": {
                                     "class": "flex-grow"  # 添加flex-grow类，使文本内容区域充分利用可用空间
                                 },
                                    "content": sub_contents},
                            ],
                        },
                    ],
                }
            )

        return contents

    # ==================== 仪表盘 Widget ====================
    def get_dashboard_meta(self) -> Optional[List[Dict[str, str]]]:
        """
        获取插件仪表盘元信息
        """
        return [{
            "key": "sync_deletion_stats",
            "name": "Emby同步删除统计"
        }]

    def get_dashboard(self, key: str, **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[dict]]]:
        """
        获取同步删除统计仪表盘
        """
        try:
            historys = self.get_data("history") or []
            total_count = len(historys)

            col_config = {
                "cols": 12,
                "md": 12
            }

            global_config = {
                "refresh": 30,
                "border": True,
                "title": "Emby同步删除历史"
            }

            type_mapping = {
                "movie": "电影",
                "tv": "电视剧",
                "show": "电视剧",
                "series": "电视剧",
                "season": "季",
                "episode": "单集"
            }

            # 数据按时间降序排序
            historys = sorted(historys, key=lambda x: x.get("del_time"), reverse=True)

            if not historys:
                dashboard_content = [
                    {
                        "component": "VCard",
                        "props": {"class": "pa-4"},
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "text-center text-medium-emphasis"},
                                "text": "暂无同步删除记录"
                            }
                        ]
                    }
                ]
            else:
                cards = self._build_history_cards(historys, type_mapping, show_delete_btn=False)
                dashboard_content = [
                    {
                        "component": "VCard",
                        "props": {"class": "pa-2"},
                        "content": [
                            {
                                "component": "div",
                                "props": {
                                    "class": "grid gap-3 grid-info-card",
                                },
                                "content": cards,
                            }
                        ]
                    }
                ]

            return col_config, global_config, dashboard_content
        except Exception as e:
            logger.error(f"获取仪表盘数据失败: {e}")
            return None

    # ==================== 不能删除 无法进入设置界面 ====================
    def get_state(self):
        return self._enabled

    # ==================== 设置界面 ====================
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        # 本地媒体配置设置
        local_media_tab = [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {
                            "cols": 12,
                        },
                        "content": [
                            {
                                "component": "VTextarea",
                                "props": {
                                    "model": "local_library_path",
                                    "rows": "4",
                                    "label": "本地媒体库路径映射",
                                    "placeholder": "EMBY路径#MoviePilot路径（一行一个）",
                                },
                            }
                        ],
                    }
                ],
            },
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "density": "compact",
                    "class": "mt-2",
                },
                "content": [
                    {
                        "component": "div",
                        "text": "关于路径映射：",
                    },
                    {
                        "component": "div",
                        "text": "emby目录：/Downloads/STRM/strm影视/电影/A.mp4",
                    },
                    {
                        "component": "div",
                        "text": "moviepilot目录：/STRM/strm影视/电影/A.mp4",
                    },
                    {
                        "component": "div",
                        "text": "路径映射填：/Downloads/STRM/strm影视#/STRM/strm影视",
                    },
                    {
                        "component": "div",
                        "text": "不正确配置会导致查询不到转移记录！",
                    },
                ],
            },
        ]

        # CAS界面设置
        cas_config_tab = [
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
                                    "model": "cas_enabled",
                                    "label": "启用CAS清理",
                                }
                            }
                        ]
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "cas_delete_normal_task_enabled",
                                    "label": "删除普通任务",
                                }
                            }
                        ]
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "cas_delete_crystal_task_enabled",
                                    "label": "删除玄晶任务",
                                }
                            }
                        ]
                    },
                ]
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
                                    "rows": "4",
                                    "label": "天翼网盘路径映射",
                                    "placeholder": "本地源文件路径#CAS挂载网盘ID#网盘名（一行一个）",
                                },
                            }
                        ],
                    }
                ],
            },
            {
                "component": "VDivider",
                "props": {"class": "my-2"},
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "cas_host",
                                    "label": "CAS 服务地址",
                                    "placeholder": "http://192.168.1.100:3005"
                                }
                            }
                        ]
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "cas_api_key",
                                    "label": "API Key",
                                    "placeholder": "在CAS设置中获取"
                                }
                            }
                        ]
                    }
                ]
            },
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "density": "compact",
                    "class": "mt-2",
                    "text": "CAS服务用于清理天翼云盘中的对应目录。删除本地文件后,将自动清理云盘中对应的目录。",
                },
            },
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "density": "compact",
                    "class": "mt-2",
                },
                "content": [
                    {
                        "component": "div",
                        "text": "天翼网盘路径映射格式：本地源文件路径#CAS挂载网盘ID#网盘名",
                    },
                    {
                        "component": "div",
                        "text": "例如：/Downloads/STRM/strm网盘/天翼云盘5#10#天翼云盘5",
                    },
                    {
                        "component": "div",
                        "text": "表示该目录下的文件夹删除会同步到账户ID为64、名称为天翼云盘5的网盘路径",
                    },
                    {
                        "component": "div",
                        "text": "不正确配置会导致无法清理云盘文件！",
                    },
                ],
            },
            {
                "component": "VDivider",
                "props": {"class": "my-2"},
            },
        ]

        # OpenList界面设置
        openlist_config_tab = [
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
                                    "model": "openlist_enabled",
                                    "label": "启用OpenList清理",
                                }
                            }
                        ]
                    }
                ]
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "openlist_host",
                                    "label": "OpenList地址",
                                    "placeholder": "http://192.168.1.10:5244",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "openlist_token",
                                    "label": "管理令牌",
                                    "placeholder": "后台生成的令牌",
                                    "type": "password",
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
                                    "model": "openlist_path_mapping",
                                    "rows": "4",
                                    "label": "OpenList路径映射",
                                    "placeholder": "本地路径#OpenList路径（每行一条，支持多个网盘，例如：/STRM影视/网盘/天翼云盘完结/家庭4#/天翼家庭4/家庭4）",
                                },
                            },
                            {
                                "component": "div",
                                "props": {
                                    "class": "text-caption text-medium-emphasis pa-1",
                                    "text": "格式：本地路径#OpenList路径，每行一条，可配置多个网盘。例如：/STRM影视/网盘/天翼云盘完结/家庭4#/天翼家庭4/家庭4。注意：本地路径必须一直写到与网盘里完全相同的路径为止，否则多网盘挂载时路径替换会出错。",
                                },
                            }
                        ],
                    }
                ],
            },
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "density": "compact",
                    "class": "mt-2",
                    "text": "OpenList服务用于清理对应目录中的文件。删除本地文件后,将自动清理OpenList中对应的目录。路径映射格式为：本地路径#OpenList路径（一行一个），本地路径必须一直写到与网盘里完全相同的路径为止，否则多网盘挂载时路径替换会出错。",
                },
            },
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "density": "compact",
                    "class": "mt-2",
                },
                "content": [
                    {
                        "component": "div",
                        "text": "路径映射：本地路径#OpenList路径(网盘里相同路径)",
                    },
                    {
                        "component": "div",
                        "text": "例如：/STRM影视/网盘/天翼云盘完结/家庭4#/天翼家庭4/家庭4",
                    },
                    {
                        "component": "div",
                        "text": "本地路径必须一直写到与网盘里完全相同的路径为止，配置不正确将无法正确清理OpenList中的文件！",
                    },
                ],
            },

        ]

        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-3"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center"},
                        "content": [
                            {
                                "component": "VIcon",
                                "props": {
                                    "icon": "mdi-cog",
                                    "color": "primary",
                                    "class": "mr-2",
                                },
                            },
                            {"component": "span", "text": "基础设置"},
                        ],
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 2},
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
                                        "props": {"cols": 12, "md": 2},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "notify",
                                                    "label": "发送通知",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 2},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "del_source",
                                                    "label": "删源文件",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 2},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "del_history",
                                                    "label": "删除历史",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VSelect",
                                                "props": {
                                                    "multiple": True,
                                                    "chips": True,
                                                    "clearable": True,
                                                    "model": "mediaservers",
                                                    "label": "媒体服务器",
                                                    "items": [
                                                        {
                                                            "title": config.name,
                                                            "value": config.name,
                                                        }
                                                        for config in self._mediaserver_helper.get_configs().values()
                                                        if config.type == "emby"
                                                    ],
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
                                                    "model": "protected_directories",
                                                    "rows": "2",
                                                    "label": "保护目录",
                                                    "placeholder": "多个保护目录用#符号分隔，如：日韩剧#国产剧#电影#电视剧",
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
                                                    "type": "warning",
                                                    "variant": "tonal",
                                                    "text": "保护目录功能：防止误删除重要分类目录。本地删除、CAS删除和OpenList删除操作都会遵循此规则。",
                                                },
                                            },
                                        ],
                                    }
                                ],
                            },
                        ],
                    },
                ],
            },
            {
                "component": "VCard",
                "props": {"variant": "outlined"},
                "content": [
                    {
                        "component": "VTabs",
                        "props": {"model": "tab", "grow": True, "color": "primary"},
                        "content": [
                            {
                                "component": "VTab",
                                "props": {"value": "tab-local"},
                                "content": [
                                    {"component": "span", "text": "本地媒体配置"},
                                ],
                            },
                            {
                                "component": "VTab",
                                "props": {"value": "tab-cas"},
                                "content": [
                                    {"component": "span", "text": "CAS配置"},
                                ],
                            },
                            {
                                "component": "VTab",
                                "props": {"value": "tab-openlist"},
                                "content": [
                                    {"component": "span", "text": "OpenList配置"},
                                ],
                            },
                        ],
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VWindow",
                        "props": {"model": "tab"},
                        "content": [
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab-local"},
                                "content": [
                                    {
                                        "component": "VCardText",
                                        "content": local_media_tab,
                                    }
                                ],
                            },
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab-cas"},
                                "content": [
                                    {
                                        "component": "VCardText",
                                        "content": cas_config_tab,
                                    }
                                ],
                            },
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab-openlist"},
                                "content": [
                                    {
                                        "component": "VCardText",
                                        "content": openlist_config_tab,
                                    }
                                ],
                            },
                        ],
                    },
                ],
            },
        ], {
            "enabled": False,  # 插件是否启用
            "notify": True,  # 是否发送通知
            "del_source": False,  # 是否删除源文件
            "del_history": False,  # 是否删除历史记录
            "local_library_path": "/STRM/strm影视#/Downloads/STRM/strm影视",  # 本地媒体库路径
            "cas_path_mapping": "/Downloads/STRM/strm网盘/天翼云盘5#10#天翼云盘5",  # CAS路径映射配置
            "mediaservers": ["emby"],  # 媒体服务器列表，默认开启emby
            "cas_enabled": False,  # CAS功能是否启用
            "cas_host": "http://192.168.1.100:3005",  # CAS服务地址
            "cas_api_key": "",  # CAS API密钥
            "cas_debug_log": True,  # CAS调试日志是否开启
            "openlist_enabled": False,  # OpenList功能是否启用
            "openlist_host": "",  # OpenList地址
            "openlist_token": "",  # OpenList管理令牌（仅支持令牌方式）
            "openlist_path_mapping": "",  # OpenList路径映射配置
            "tab": "tab-local",  # 默认显示的标签页，tab-local - 本地媒体配置，tab-cas - CAS配置，tab-openlist - OpenList配置
        }
