from collections import deque
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
import json
import requests
import time
import re
import datetime
import traceback
import concurrent.futures
import hashlib

from app.core.event import eventmanager, Event
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfo
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType, MediaType
from app.schemas import WebhookEventInfo
from app.utils.http import RequestUtils
from app.core.config import settings
from app.helper.mediaserver import MediaServerHelper

# 任务状态常量映射
TASK_STATUS_MAP = {
    "completed": "已完结",
    "processing": "追剧中",
    "failed": "失败",
    "pending": "等待中",
    "shareLinkError": "链接异常",
    "unknown": "未知"
}

MEDIA_TYPE_MAP = {
    "movie": "电影",
    "tv": "电视剧",
    "series": "剧集",
    "episode": "剧集",
    "season": "季",
    "show": "剧集"
}


class CASTaskCleaner(_PluginBase):
    # 插件名称
    plugin_name = "CAS任务清理"
    # 插件描述
    plugin_desc = "自动清理《天翼云盘自动转存》中已完成任务，并通知追剧进度。仅支持EMBY媒体服务器。"
    # 插件图标
    plugin_icon = "cloud189.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "CASTaskCleaner"
    # 加载顺序
    plugin_order = 10
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled: bool = False
    _notify: bool = False
    _server: str = "emby"  # 固定为emby
    _host: Optional[str] = None
    _api_key: Optional[str] = None
    _delay_seconds: int = 0
    _debug_log: bool = True  # 默认开启调试日志
    _delete_normal_tasks: bool = True
    _delete_proxy_tasks: bool = True
    _session: requests.Session = None
    _event_cache = {}
    _thread_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None

    # 媒体库过滤相关属性
    _enable_library_filter: bool = False
    _excluded_library_ids: List[str] = []
    _library_cache: Dict[str, str] = {}  # 缓存：folder_id -> library_id

    # CAS服务配置属性
    _cas_host: Optional[str] = None
    _cas_api_key: Optional[str] = None

    def init_plugin(self, config: Optional[dict] = None):
        # 初始化线程池（最大5个线程）
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=5)

        # 初始化请求会话
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "CASTaskCleaner"})

        if config:
            self._enabled = config.get("enabled", False)
            self._server = "emby"  # 固定为emby
            self._notify = config.get("notify", False)
            # 保持调试日志始终开启，不从配置读取
            self._delete_normal_tasks = config.get("delete_normal_tasks", True)
            self._delete_proxy_tasks = config.get("delete_proxy_tasks", True)
            self._enable_library_filter = config.get(
                "enable_library_filter", False)

            # 处理CAS服务配置
            self._cas_host = config.get("host", "")
            self._cas_api_key = config.get("api_key", "")

            # 处理排除的媒体库ID（清理格式）
            excluded_ids_str = config.get("excluded_library_ids", "")
            if excluded_ids_str:
                # 正确分割逗号分隔的ID列表
                excluded_ids = [id.strip()
                                for id in excluded_ids_str.split(",") if id.strip()]
                # 过滤掉空值和无效字符，只保留数字ID
                valid_ids = [
                    id for id in excluded_ids if id and id.replace(",", "").isdigit()]
                self._excluded_library_ids = list(set(valid_ids))  # 去重
                logger.debug(f"解析后的排除媒体库ID: {self._excluded_library_ids}")
            else:
                self._excluded_library_ids = []

            # 改进：从系统配置中获取Emby服务信息
            self._get_emby_service_info()

            try:
                self._delay_seconds = int(config.get("delay_seconds", 0))
            except (ValueError, TypeError):
                logger.warning("delay_seconds配置无效，使用默认值0")
                self._delay_seconds = 0

            # 验证配置
            if not self._validate_config():
                self._enabled = False
                return

            # 测试连接
            if self._enabled:
                self._test_cas_connection()

            # 记录启用的任务类型
            enabled_tasks = []
            if self._delete_normal_tasks:
                enabled_tasks.append("普通任务")
            if self._delete_proxy_tasks:
                enabled_tasks.append("玄鲸任务")

            if enabled_tasks:
                logger.info(f"已启用任务类型: {", ".join(enabled_tasks)}")
            else:
                logger.info("未启用任何任务类型，仅查询不删除")

            # 记录媒体库过滤状态
            if self._enable_library_filter and self._excluded_library_ids:
                logger.info(
                    f"已启用媒体库过滤，排除的媒体库ID: {', '.join(self._excluded_library_ids)}")
            elif self._enable_library_filter:
                logger.info("已启用媒体库过滤，但未配置排除的媒体库ID，将处理所有事件")
            else:
                logger.info("未启用媒体库过滤，将处理所有媒体库事件")

    def _get_emby_service_info(self):
        """从系统配置中获取Emby服务信息"""
        try:
            mediaserver_helper = MediaServerHelper()
            services = mediaserver_helper.get_services()

            if not services:
                logger.warning("系统未配置Emby媒体服务器")
                return

            for service_name, service_info in services.items():
                if service_info.type.lower() == 'emby' and not service_info.instance.is_inactive():
                    emby_instance = service_info.instance
                    if hasattr(emby_instance, '_host') and hasattr(emby_instance, '_apikey'):
                        self._host = emby_instance._host
                        self._api_key = emby_instance._apikey
                        logger.info(f"已从系统配置获取Emby服务信息: {self._host}")
                        return

            logger.warning("未找到可用的Emby媒体服务器")
        except Exception as e:
            logger.error(f"获取Emby服务信息失败: {str(e)}")

    def _validate_config(self) -> bool:
        """验证配置是否有效"""
        # 验证Emby服务配置
        if not self._host or not self._api_key:
            # 尝试从系统配置获取
            self._get_emby_service_info()

            if not self._host or not self._api_key:
                logger.error("插件启用但缺少Emby服务配置，请确保系统已配置Emby媒体服务器")
                return False

        # 确保Emby host格式正确
        if self._host:
            if not self._host.startswith("http"):
                self._host = "http://" + self._host
            if not self._host.endswith("/"):
                self._host += "/"

        # 验证CAS服务配置
        if not self._cas_host or not self._cas_api_key:
            logger.error("插件启用但缺少CAS服务配置，请填写CAS服务地址和API Key")
            return False

        # 确保CAS host格式正确
        if self._cas_host:
            if not self._cas_host.startswith("http"):
                self._cas_host = "http://" + self._cas_host
            if not self._cas_host.endswith("/"):
                self._cas_host += "/"

        return True

    def _safe_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """带重试机制的安全请求"""
        # 确保会话存在
        if not self._session:
            self._session = requests.Session()
            self._session.headers.update({"User-Agent": "CASTaskCleaner/1.8"})

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 如果kwargs中已经包含timeout，则不重复添加
                request_kwargs = {"timeout": 10}
                request_kwargs.update(kwargs)

                response = self._session.request(
                    method,
                    url,
                    **request_kwargs
                )
                # 只有在真正成功返回响应时才记录成功
                if self._debug_log:
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
        """测试CAS服务连接"""
        try:
            # 获取CAS服务配置
            cas_host = getattr(self, '_cas_host', '') or ''
            cas_api_key = getattr(self, '_cas_api_key', '') or ''

            if not cas_host or not cas_api_key:
                logger.warning("未配置CAS服务地址或API Key，跳过连接测试")
                return

            logger.debug("测试CAS服务连接...")
            headers = {"x-api-key": cas_api_key}

            # 确保CAS服务地址格式正确
            if not cas_host.startswith("http"):
                cas_host = "http://" + cas_host
            if not cas_host.endswith("/"):
                cas_host += "/"

            res = self._safe_request(
                "GET",
                f"{cas_host}api/tasks",
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

    def get_command(self) -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "delete_normal_tasks",
                                            "label": "普通任务",
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "delete_proxy_tasks",
                                            "label": "玄鲸任务",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enable_library_filter",
                                            "label": "媒体库过滤",
                                            "hint": "启用后只处理指定媒体库的事件"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 5},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "excluded_library_ids",
                                            "label": "排除的媒体库ID",
                                            "placeholder": "逗号分隔，如：123456,789012",
                                            "hint": "不处理这些媒体库的媒体添加事件"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "delay_seconds",
                                            "label": "延迟查询时间(秒)",
                                            "placeholder": "默认0秒（立即执行）"
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
                                            "model": "host",
                                            "label": "CAS 服务地址",
                                            "placeholder": "http://IP:端口/"
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
                                            "model": "api_key",
                                            "label": "API Key",
                                            "placeholder": "在CAS设置中获取"
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "🔒 仅支持EMBY媒体服务器"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": self._enabled,
            "notify": self._notify,
            "debug_log": self._debug_log,
            "delete_normal_tasks": self._delete_normal_tasks,
            "delete_proxy_tasks": self._delete_proxy_tasks,
            "enable_library_filter": self._enable_library_filter,
            "excluded_library_ids": self._excluded_library_ids,
            "host": self._host or "",
            "api_key": self._api_key or "",
            "delay_seconds": self._delay_seconds,
        }

    def get_state(self) -> bool:
        return self._enabled

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        # 关闭线程池
        if self._thread_pool:
            self._thread_pool.shutdown(wait=False)
            self._thread_pool = None

        # 关闭会话
        if self._session:
            self._session.close()
            self._session = None

        logger.info("插件服务已停止")

    def _convert_media_type(self, media_type: Optional[str]) -> str:
        """将媒体类型转换为中文"""
        if not media_type:
            return "未知类型"
        return MEDIA_TYPE_MAP.get(media_type.lower(), media_type)

    @eventmanager.register(EventType.WebhookMessage)
    def handle_media_added(self, event: Event):
        """处理媒体添加事件"""
        if not self._enabled:
            return

        try:
            event_info: WebhookEventInfo = event.event_data

            # 记录调试信息
            if self._debug_log:
                try:
                    logger.debug(
                        f"收到事件: {event_info.model_dump_json(indent=2)}")
                except:
                    logger.debug(f"收到事件: {vars(event_info)}")

            # 1. 快速过滤不相关事件
            if not self._should_process_event(event_info):
                return

            # 2. 媒体库过滤（新增）
            if not self._should_process_by_library_filter(event_info):
                return

            # 3. 事件去重处理
            if not self._should_process_unique_event(event_info):
                return

            # 4. 提取并优化标题
            title = self._extract_and_optimize_title(event_info)
            if not title:
                logger.warn(f"添加的媒体标题为空，跳过")
                return

            # 5. 记录并启动处理
            media_type = getattr(event_info, "media_type", None)
            media_type_chinese = self._convert_media_type(media_type)
            logger.info(f"检测到新媒体添加: {title} (类型: {media_type_chinese})")

            # 启动异步处理任务，传递事件信息以便在通知中使用tmdb_info
            self._start_async_process(title, event_info)

        except Exception as e:
            logger.error(f"处理事件时发生异常: {str(e)}")
            logger.error(traceback.format_exc())

    def _should_process_event(self, event_info: WebhookEventInfo) -> bool:
        """判断是否应该处理该事件"""
        # 检查事件来源（固定为emby）
        event_channel = getattr(event_info, "channel", "")
        if event_channel != "emby":
            if self._debug_log:
                logger.debug(f"事件来源 {event_channel} 不是emby，跳过")
            return False

        # 检查事件类型（固定为library.new）
        event_type = getattr(event_info, "event", "")
        if event_type != "library.new":
            return False

        # 检查媒体类型 - 只处理MOV（电影）或TV（电视剧）类型
        item_type = getattr(event_info, "item_type", None)

        # 详细记录调试信息，包括所有可能的字段名
        if self._debug_log:
            item_type_alt = getattr(event_info, "ItemType", None)
            item_type_raw = getattr(event_info, "itemType", None)
            logger.debug(
                f"媒体类型检查 - item_type: {item_type}, ItemType: {item_type_alt}, itemType: {item_type_raw}")
            # 打印事件对象的所有属性
            try:
                event_attrs = vars(event_info)
                logger.debug(f"事件对象所有属性: {list(event_attrs.keys())}")
                # 查找可能包含媒体类型的属性
                for key, value in event_attrs.items():
                    if 'type' in key.lower():
                        logger.debug(f"  {key}: {value}")
            except Exception as e:
                logger.debug(f"无法获取事件对象属性: {str(e)}")

        # 检查所有可能的字段名变体
        if item_type is None:
            item_type = getattr(event_info, "ItemType", None)
            if self._debug_log and item_type is not None:
                logger.debug(f"使用ItemType字段获取到值: {item_type}")
        if item_type is None:
            item_type = getattr(event_info, "itemType", None)
            if self._debug_log and item_type is not None:
                logger.debug(f"使用itemType字段获取到值: {item_type}")

        if item_type is None or item_type.upper() not in ["MOV", "TV"]:
            if self._debug_log:
                logger.debug(f"媒体类型 {item_type} 不是MOV或TV，跳过处理")
            return False

        return True

    def _should_process_unique_event(self, event_info: WebhookEventInfo) -> bool:
        """判断是否为需要处理的唯一事件（去重）"""
        # 生成唯一键
        event_type = getattr(event_info, "event", "")
        event_channel = getattr(event_info, "channel", "")
        title = getattr(event_info, "item_name", "")
        tmdb_id = getattr(event_info, "tmdb_id", "")
        item_path = getattr(event_info, "item_path", "")
        season_id = getattr(event_info, "season_id", "")
        episode_id = getattr(event_info, "episode_id", "")

        key_data = (
            f"{event_type}_{event_channel}_{title}_"
            f"{tmdb_id}_{item_path}_{season_id}_{episode_id}"
        )
        unique_key = hashlib.md5(key_data.encode("utf-8")).hexdigest()

        # 清理过期缓存
        current_time = time.time()
        expire_time = current_time - 10  # 10秒去重窗口
        expired_keys = [key for key,
                        ts in self._event_cache.items() if ts < expire_time]
        for key in expired_keys:
            if self._debug_log:
                logger.debug(f"清理过期事件缓存: {key}")
            del self._event_cache[key]

        # 检查是否在10秒内处理过
        if unique_key in self._event_cache:
            cache_time = self._event_cache[unique_key]
            if self._debug_log:
                logger.debug(
                    f"10秒内已处理过该事件，跳过: {key_data} "
                    f"(缓存时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cache_time))})"
                )
            return False

        # 记录新事件
        self._event_cache[unique_key] = current_time
        if self._debug_log:
            logger.debug(f"记录新事件: {unique_key} (数据: {key_data})")

        return True

    def _extract_and_optimize_title(self, event_info: WebhookEventInfo) -> str:
        """提取并优化媒体标题"""
        title = getattr(event_info, "item_name", "")
        media_type = getattr(event_info, "media_type", None)

        # 如果是电视剧类型，尝试获取剧集名称
        if media_type and media_type.lower() in ["tv", "series", "episode", "season", "show"]:
            json_object = getattr(event_info, "json_object", {})
            try:
                if isinstance(json_object, str):
                    json_data = json.loads(json_object)
                else:
                    json_data = json_object

                if json_data and isinstance(json_data, dict):
                    item_data = json_data.get("Item") or {}
                    series_name = item_data.get("SeriesName")
                    if series_name:
                        title = series_name
                        if self._debug_log:
                            logger.debug(f"检测到电视剧类型，使用剧集名称作为搜索关键字: {title}")
                    else:
                        if self._debug_log:
                            logger.debug(f"未找到SeriesName，将使用原标题: {title}")
            except Exception as e:
                logger.warn(
                    f"解析json_object获取SeriesName失败: {str(e)}，将使用原标题: {title}")
        else:
            if self._debug_log:
                logger.debug(f"媒体类型 {media_type}，使用原标题作为搜索关键字: {title}")

        return title

    def _should_process_by_library_filter(self, event_info: WebhookEventInfo) -> bool:
        """根据媒体库过滤判断是否应该处理该事件"""
        if not self._enable_library_filter:
            return True

        if not self._excluded_library_ids:
            if self._debug_log:
                logger.debug("媒体库过滤已启用但未配置排除名单，处理所有事件")
            return True

        try:
            # 获取事件的Item ID
            item_id = self._get_event_item_id(event_info)
            if not item_id:
                if self._debug_log:
                    logger.debug("无法获取事件的Item ID，继续处理")
                return True

            # 使用新的查询方法：直接使用文件ID去搜索排除的媒体库ID
            if self._is_item_excluded_by_direct_query(item_id):
                logger.info(f"事件来自排除的媒体库，跳过处理 (Item ID: {item_id})")
                return False
            else:
                logger.info(f"事件不在排除的媒体库中，继续处理 (Item ID: {item_id})")
                return True

        except Exception as e:
            logger.error(f"媒体库过滤处理异常: {str(e)}")
            logger.error(traceback.format_exc())
            # 发生异常时默认继续处理，避免影响正常功能
            return True

    def _query_emby_library_structure(self, physical_folder_id: str) -> Optional[str]:
        """查询Emby媒体库结构来找到对应的View ID"""
        try:
            if not self._host or not self._api_key:
                return None

            server_url = self._host.rstrip('/')
            headers = {"X-Emby-Token": self._api_key,
                       "Accept": "application/json"}

            # 获取用户ID
            user_res = requests.get(
                f"{server_url}/emby/Users", headers=headers, timeout=5)
            if user_res.status_code != 200:
                return None

            users = user_res.json()
            if not users:
                return None

            user_id = users[0]['Id']

            # 查询所有媒体库视图
            views_url = f"{server_url}/emby/Users/{user_id}/Items"
            params = {
                "IncludeItemTypes": "CollectionFolder",
                "Recursive": True,
                "ParentId": "1"
            }

            res = requests.get(views_url, headers=headers,
                               params=params, timeout=5)
            if res.status_code == 200:
                items = res.json().get("Items", [])
                for item in items:
                    view_id = item.get("Id")
                    view_name = item.get("Name", "")

                    # 检查该View是否包含我们的物理文件夹
                    children_url = f"{server_url}/emby/Users/{user_id}/Items"
                    children_params = {
                        "ParentId": view_id,
                        "Recursive": True
                    }
                    children_res = requests.get(
                        children_url, headers=headers, params=children_params, timeout=5)
                    if children_res.status_code == 200:
                        children = children_res.json().get("Items", [])
                        for child in children:
                            if child.get("Id") == physical_folder_id:
                                if self._debug_log:
                                    logger.debug(
                                        f"找到对应View: ID={view_id}, Name={view_name}")
                                return view_id

            return None

        except Exception as e:
            if self._debug_log:
                logger.debug(f"查询Emby媒体库结构异常: {str(e)}")
            return None

    def _get_event_item_id(self, event_info: WebhookEventInfo) -> Optional[str]:
        """获取事件对应的Item ID"""
        try:
            # 从事件中获取Item ID
            json_object = getattr(event_info, "json_object", {})
            if isinstance(json_object, str):
                json_data = json.loads(json_object)
            else:
                json_data = json_object

            if not json_data or not isinstance(json_data, dict):
                if self._debug_log:
                    logger.debug("事件中缺少有效的json_object数据")
                return None

            item_data = json_data.get("Item") or {}
            item_id = item_data.get("Id")

            if not item_id:
                if self._debug_log:
                    logger.debug("事件中缺少Item ID信息")
                return None

            return str(item_id)

        except Exception as e:
            logger.error(f"获取事件Item ID异常: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _is_item_excluded_by_direct_query(self, item_id: str) -> bool:
        """
        检查指定项目是否属于排除名单中的媒体库
        参考 test_emby_library.py 的实现
        """
        if not self._host or not self._api_key:
            logger.error("无法查询媒体库: 缺少Emby服务配置")
            return False

        try:
            server_url = self._host.rstrip('/')
            headers = {"X-Emby-Token": self._api_key,
                       "Accept": "application/json"}

            # 获取用户ID
            user_res = requests.get(
                f"{server_url}/emby/Users", headers=headers, timeout=5)
            if user_res.status_code != 200:
                if self._debug_log:
                    logger.debug(f"获取用户列表失败，状态码: {user_res.status_code}")
                return False

            users = user_res.json()
            if not users:
                if self._debug_log:
                    logger.debug("未找到用户信息")
                return False

            user_id = users[0]['Id']  # 使用第一个管理员用户

            # 直接在排除名单里循环
            for lib_id in self._excluded_library_ids:
                # 定向查询：在这个特定的 ParentId 下搜索目标 ItemId
                # Recursive=true 确保能穿透所有文件夹层级
                url = f"{server_url}/emby/Items"
                params = {
                    "ParentId": lib_id,
                    "Ids": item_id,
                    "Recursive": "true",
                    "Fields": "Id"
                }

                try:
                    res = requests.get(url, headers=headers,
                                       params=params, timeout=3)
                    if res.status_code == 200:
                        data = res.json()
                        # 如果 TotalRecordCount > 0，说明这个项目属于这个要排除的库
                        if data.get('TotalRecordCount', 0) > 0:
                            if self._debug_log:
                                logger.debug(
                                    f"[跳过] 项目 {item_id} 属于排除库 (ID: {lib_id})，不处理。")
                            return True
                except Exception as e:
                    logger.warning(f"查询排除库时出错: {e}")

            return False

        except Exception as e:
            logger.error(f"检查项目是否排除异常: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _get_emby_item(self, item_id: str) -> Optional[dict]:
        """通过Emby API获取项目信息（用于其他方法）"""
        try:
            # 清理URL格式
            server_url = self._host.rstrip('/')
            headers = {"X-Emby-Token": self._api_key,
                       "Accept": "application/json"}

            # 获取用户ID
            user_res = requests.get(
                f"{server_url}/emby/Users", headers=headers, timeout=5)
            if user_res.status_code != 200:
                return None

            users = user_res.json()
            if not users:
                return None

            user_id = users[0]['Id']

            # 使用用户级接口获取项目信息
            url = f"{server_url}/emby/Users/{user_id}/Items/{item_id}"
            response = requests.get(url, headers=headers, timeout=5)

            if response.status_code == 200:
                return response.json()
            else:
                if self._debug_log:
                    logger.warning(f"获取Emby项目信息失败，状态码: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"获取Emby项目信息异常: {str(e)}")
            return None

    def _start_async_process(self, title: str, event_info=None):
        """启动异步处理任务"""
        if not self._enabled or not self._thread_pool:
            return

        # 简化延迟日志
        delay_text = f"延迟 {self._delay_seconds} 秒" if self._delay_seconds > 0 else "立即"
        logger.info(f"{delay_text}处理: {title}")

        try:
            # 传递事件信息，以便在通知中使用tmdb_info
            self._thread_pool.submit(self._delayed_process, title, self._delay_seconds, event_info
                                     ).add_done_callback(self._handle_result)
        except Exception as e:
            logger.error(f"启动异步任务失败: {str(e)}")

    def _handle_result(self, future: concurrent.futures.Future):
        """处理任务结果"""
        try:
            future.result()
        except Exception as e:
            logger.error(f"任务执行异常: {str(e)}")
            logger.error(traceback.format_exc())

    # ====================主处理函数 ====================
    def _delayed_process(self, title: str, delay_seconds: int, event_info=None):
        try:
            logger.info(f"开始处理: {title}")

            # 检查插件是否仍启用
            if not self._enabled:
                logger.info(f"插件已禁用，取消处理: {title}")
                return

            if delay_seconds > 0:
                logger.info(f"延迟处理: {title}，等待 {delay_seconds} 秒...")
                # 分段等待，每2秒检查一次插件状态
                for _ in range(delay_seconds // 2):
                    time.sleep(2)
                    if not self._enabled:
                        logger.info(f"插件已禁用，取消延迟处理: {title}")
                        return

                remaining = delay_seconds % 2
                if remaining > 0:
                    time.sleep(remaining)

                logger.info(f"延迟等待结束，开始处理: {title}")

            # 获取任务信息
            task_info = self._get_task_info_by_title(title)
            if task_info is None:
                logger.error(f"查询 {title} 的 CAS 任务失败")
                return
            elif not task_info["ids"]:
                logger.info(f"未找到与 {title} 相关的任务")
                # 发送未找到任务通知，传递事件信息以便使用相同的图片获取方案
                self._send_no_tasks_found_notification(title, event_info)
                return
            else:
                # 获取第一个任务的resourceName作为显示的任务名称
                task_name = title
                if task_info["ids"] and task_info["details"]:
                    first_task_id = task_info["ids"][0]
                    first_task_detail = task_info["details"].get(
                        first_task_id, {})
                    task_name = first_task_detail.get("resourceName", title)

                logger.info(
                    f"查询成功，找到 {len(task_info['ids'])} 个相关任务: {task_name}")

            # 处理任务（删除完成状态的任务）
            result = self._process_and_delete_tasks(task_info, title)
            deleted_count = result["deleted_count"]
            skipped_count = result["skipped_count"]
            deleted_proxy_tasks = result["deleted_proxy_tasks"]
            deleted_normal_tasks = result["deleted_normal_tasks"]
            account_info = result.get("account_info", {})
            media_info = result.get("media_info", {})

            # 发送清理任务通知
            if deleted_count > 0:
                self._send_clean_notification(
                    title, deleted_count, skipped_count,
                    deleted_proxy_tasks, deleted_normal_tasks,
                    account_info, media_info
                )
            elif self._debug_log:
                logger.debug("未成功删除任何任务，不发送清理通知")

        except Exception as e:
            logger.error(f"处理任务时发生异常: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            logger.info(f"处理完成: {title}")

    # ==================== CAS 任务查询====================
    def _get_task_info_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """根据标题查询任务信息"""
        # 前置检查
        if not self._cas_host or not self._cas_api_key:
            logger.error("未配置 CAS 服务地址或 API Key")
            return None

        if not self._enabled:
            logger.info("插件已禁用，取消任务查询")
            return None

        # 获取查询类型
        query_type = self._get_query_type()
        if query_type == "none":
            logger.info("删除任务开关全部关闭，跳过任务查询")
            return None

        try:
            # 优化搜索关键字
            clean_title = re.sub(r"\(.*?\)|（.*?）|\s", "", title)
            logger.info(f"使用优化后的搜索关键字: {clean_title}")

            # 准备数据结构
            task_ids = []
            task_status = {}
            task_details = {}

            # 构建请求参数
            params = {
                "status": "all",
                "search": clean_title,
                "type": query_type,
                "group": "all",
                "accountId": "all",
                "page": 1,
                "pageSize": 20
            }

            headers = {"x-api-key": self._cas_api_key}

            # 发送请求
            if self._debug_log:
                logger.debug(f"请求任务页 1")

            res = self._safe_request(
                "GET",
                f"{self._cas_host}api/tasks",
                headers=headers,
                params=params
            )

            # 检查响应
            if not res:
                logger.error("请求CAS任务失败，响应为空")
                return None
                
            if res.status_code != 200:
                logger.error(f"请求CAS任务失败，状态码: {res.status_code}")
                if res.text:
                    logger.error(f"错误响应: {res.text[:500]}")
                return None
            
            if self._debug_log:
                logger.debug(f"CAS API响应状态码: {res.status_code}")
                logger.debug(f"CAS API响应头: {dict(res.headers)}")
                if res.text:
                    logger.debug(f"CAS API响应内容预览: {res.text[:200]}")

            # 解析数据
            try:
                data = res.json()
                if self._debug_log:
                    logger.debug(f"CAS API响应数据: {json.dumps(data, ensure_ascii=False)[:500]}")
            except Exception as e:
                logger.error(f"解析JSON失败: {str(e)}")
                if res.text:
                    logger.error(f"响应内容: {res.text[:500]}")
                return None

            # 处理任务列表
            task_list = self._extract_task_list(data)
            if self._debug_log:
                logger.debug(f"提取的任务列表: {task_list}")
            
            if not task_list:
                if self._debug_log:
                    logger.debug("第 1 页无任务")
            else:
                for task in task_list:
                    self._process_task_data(
                        task, task_ids, task_status, task_details)

            return {
                "ids": task_ids,
                "status": task_status,
                "details": task_details
            }

        except Exception as e:
            logger.error(f"获取任务异常: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _extract_task_list(self, data: dict) -> list:
        """从响应数据中提取任务数据"""
        if data.get("success") and "data" in data:
            return data["data"].get("tasks", [])
        else:
            return data.get("tasks", [])

    def _process_task_data(self, task: dict, task_ids: list, task_status: dict, task_details: dict):
        """提取任务数据"""
        task_id = task.get("id")
        if not task_id:
            logger.warning("任务条目缺少ID，跳过")
            return

        task_id_str = str(task_id)
        status = task.get("status", "unknown").lower()
        task_name = task.get("resourceName", "未知名称")

        task_ids.append(task_id_str)  # 任务ID列表
        task_status[task_id_str] = status  # 任务状态映射：task_id -> status
        task_details[task_id_str] = {
            "resourceName": task_name,  # 资源名称/媒体标题
            "currentEpisodes": task.get("currentEpisodes", 0),  # 当前已更新集数（电视剧）
            "totalEpisodes": task.get("totalEpisodes", 0),  # 总集数（电视剧）
            # 媒体类型：tv/movie/unknown
            "videoType": task.get("videoType", "unknown"),
            # 是否玄鲸任务：True=玄鲸任务，False=普通任务
            "enableSystemProxy": task.get("enableSystemProxy", False),
            "account": task.get("account", {}),  # 账号信息：包含username等字段
            "year": task.get("year", ""),  # 发布年份
            "tmdb": task.get("tmdb", {})  # TMDB元数据：包含海报、评分、发布日期等
        }

        if self._debug_log:
            status_cn = TASK_STATUS_MAP.get(status, status)
            current_ep = task.get("currentEpisodes", 0)
            total_ep = task.get("totalEpisodes", 0)
            video_type = task.get("videoType", "unknown")
            logger.debug(
                f"找到任务: ID={task_id}, 名称={task_name}, "
                f"类型={video_type}, 状态={status_cn}, "
                f"进度={current_ep}/{total_ep}"
            )

    def _get_query_type(self) -> str:
        """获取任务类型"""
        if self._delete_normal_tasks and self._delete_proxy_tasks:
            return "all"
        elif self._delete_normal_tasks:
            return "normal"
        elif self._delete_proxy_tasks:
            return "systemProxy"
        else:
            return "none"

    # ==================== 删除任务主函数 ====================
    def _process_and_delete_tasks(self, task_info: dict, title: str) -> dict:
        """处理并删除任务，同时发送进度通知"""
        deleted_count = 0
        skipped_count = 0
        processed_tasks = {}  # 记录已处理的任务，避免重复通知
        deleted_proxy_tasks = 0  # 删除的玄鲸任务数量
        deleted_normal_tasks = 0  # 删除的普通任务数量
        first_account_info = {}  # 第一个删除任务的账号信息
        first_media_info = {}  # 第一个删除任务的媒体信息

        for task_id in task_info["ids"]:
            # 检查插件是否仍启用
            if not self._enabled:
                logger.info(f"插件已禁用，取消任务处理")
                break

            task_status = task_info["status"].get(task_id, "unknown")
            task_detail = task_info["details"].get(task_id, {})

            if task_status == "completed":
                # 检查任务类型是否允许删除
                enable_system_proxy = task_detail.get(
                    "enableSystemProxy", False)

                # 普通任务：enableSystemProxy=False
                # 玄鲸任务：enableSystemProxy=True

                if (not enable_system_proxy and not self._delete_normal_tasks) or \
                        (enable_system_proxy and not self._delete_proxy_tasks):
                    task_type_name = "玄鲸任务" if enable_system_proxy else "普通任务"
                    logger.info(
                        f"跳过删除任务 (ID: {task_id}, 类型: {task_type_name}，该类型已禁用删除)")
                    skipped_count += 1
                    continue

                if self._debug_log:
                    logger.debug(f"准备删除任务 (ID: {task_id})")
                if self._delete_cloud189_task(task_id):
                    deleted_count += 1
                    # 统计任务类型
                    if enable_system_proxy:
                        deleted_proxy_tasks += 1
                    else:
                        deleted_normal_tasks += 1

                    # 记录第一个删除任务的账号信息和媒体信息
                    if deleted_count == 1:
                        first_account_info = task_detail.get("account", {})
                        first_media_info = task_detail
                        if self._debug_log:
                            logger.debug(
                                f"记录第一个删除任务的账号信息: {first_account_info}")
                            logger.debug(f"记录第一个删除任务的媒体信息: {first_media_info}")
                else:
                    logger.error(f"删除任务失败 (ID: {task_id})")
            else:
                status_cn = TASK_STATUS_MAP.get(task_status, task_status)
                logger.info(f"跳过非完成状态任务 (ID: {task_id}, 状态: {status_cn})")
                skipped_count += 1

                # 只处理电视剧类型的进行中任务
                if task_status == "processing" and task_detail.get("videoType", "unknown") == "tv":
                    task_name = task_detail.get("resourceName", "未知名称")
                    enable_system_proxy = task_detail.get(
                        "enableSystemProxy", False)
                    account_info = task_detail.get("account", {})

                    # 检查是否已处理过相同任务（避免重复通知）
                    if task_name not in processed_tasks:
                        processed_tasks[task_name] = {
                            "current": task_detail.get("currentEpisodes", 0),
                            "total": task_detail.get("totalEpisodes", 0),
                            "enableSystemProxy": enable_system_proxy,
                            "account": account_info
                        }

                        # 检查是否有实际更新（当前集数 > 0）
                        current_ep = task_detail.get("currentEpisodes", 0)
                        if current_ep > 0:
                            # 直接使用任务详情作为媒体信息
                            media_info = task_detail
                            # 发送单个剧集进度通知
                            self._send_single_processing_notification(
                                task_name=task_name,
                                current_ep=current_ep,
                                total_ep=task_detail.get("totalEpisodes", 0),
                                media_info=media_info,
                                enable_system_proxy=enable_system_proxy,
                                account_info=account_info
                            )
                        else:
                            logger.info(f"跳过0集进度通知: {task_name}")
                elif self._debug_log and task_status == "processing":
                    logger.debug(f"跳过电影类型的进行中任务 (ID: {task_id})")

        return {
            "deleted_count": deleted_count,
            "skipped_count": skipped_count,
            "deleted_proxy_tasks": deleted_proxy_tasks,
            "deleted_normal_tasks": deleted_normal_tasks,
            "account_info": first_account_info,
            "media_info": first_media_info
        }

    # ==================== 执行删除任务 ====================
    def _delete_cloud189_task(self, task_id: str) -> bool:
        """删除天翼云盘任务（按照cURL命令格式实现）"""
        if not task_id:
            logger.error("删除任务失败: 缺少任务ID")
            return False

        try:
            # 构建URL和头部，完全按照cURL命令格式
            url = f"{self._cas_host}api/tasks/{task_id}"
            headers = {"x-api-key": self._cas_api_key,
                       "Content-Type": "application/json"}

            # 构建请求体，添加deleteCloud参数
            data = {"deleteCloud": False}

            if self._debug_log:
                # 隐藏API密钥的部分字符以保护安全
                masked_key = f"{self._cas_api_key[:5]}...{self._cas_api_key[-5:]}" if self._cas_api_key else "无"
                logger.debug(f"删除任务请求: DELETE {url}")
                logger.debug(f"请求头: x-api-key: {masked_key}")
                logger.debug(f"请求体: {data}")

            # 使用requests发送DELETE请求，包含请求体
            response = requests.delete(
                url, headers=headers, json=data, timeout=10)

            # 记录响应状态
            if self._debug_log:
                logger.debug(f"删除响应状态码: {response.status_code}")
                if response.text:
                    logger.debug(f"删除响应内容: {response.text[:200]}")

            # 检查响应状态和内容
            if response.status_code == 200:
                try:
                    # 解析响应JSON
                    result = response.json()
                    if result.get("success") == True:
                        logger.info(f"删除任务成功 (ID: {task_id})")
                        return True
                    else:
                        logger.error(
                            f"删除任务失败 (ID: {task_id}): API返回success=false")
                        if result.get("message"):
                            logger.error(f"错误信息: {result['message']}")
                        return False
                except Exception as json_error:
                    logger.error(
                        f"解析响应JSON失败 (ID: {task_id}): {str(json_error)}")
                    logger.error(f"原始响应: {response.text[:200]}")
                    return False
            else:
                logger.error(
                    f"删除任务失败 (ID: {task_id})，状态码: {response.status_code}")
                if response.text:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get("message", "未知错误")
                        logger.error(f"错误详情: {error_msg}")
                    except:
                        logger.error(f"错误响应: {response.text[:200]}")
                return False

        except requests.exceptions.Timeout:
            logger.error(f"删除任务超时 (ID: {task_id}): 请求超过10秒未响应")
            return False
        except requests.exceptions.ConnectionError:
            logger.error(f"删除任务连接错误 (ID: {task_id}): 无法连接到CAS服务")
            return False
        except Exception as e:
            logger.error(f"删除任务异常 (ID: {task_id}): {str(e)}")
            logger.error(traceback.format_exc())
            return False

    # ==================== 追剧进度通知 ====================
    def _send_single_processing_notification(self, task_name: str, current_ep: int,
                                             total_ep: int, media_info: dict,
                                             enable_system_proxy: bool = False,
                                             account_info: dict = None):
        """发送单个剧集的进度通知"""
        if not self._notify or not media_info:
            return

        # 计算剩余集数
        remaining = total_ep - current_ep

        # 直接使用原始数据
        total_episodes = total_ep

        # 根据任务类型添加标识
        task_type = "玄鲸任务" if enable_system_proxy else "普通任务"

        # 构建通知标题
        notification_title = f"⏳《{task_name}》| CAS更新中"

        # 根据媒体类型添加标识
        video_type = media_info.get("videoType", "unknown")
        media_type_icon = "📺" if video_type == "tv" else "🎬"
        media_type_text = "电视剧" if video_type == "tv" else "电影"

        # 获取账号信息
        account_username = account_info["username"]

        # 构建通知文本 - 加亮剩余集数
        task_type_icon = "⚡" if enable_system_proxy else "🗂️"
        text = (
            f"{media_type_icon}类型：{media_type_text} ｜ {task_type_icon}任务：{task_type}\n"
            f"\n\n"
            f"🔄进度：共{total_episodes}集 已更{current_ep}集 剩余：🔥{remaining}集🔥\n"
            f"👤账号：{account_username}\n"
            f"🕛时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        # 获取图片URL，优先从backdropPath获取，如果没有再从posterPath获取
        image_url = ""
        tmdb_info = media_info.get("tmdb", {})
        if tmdb_info:
            backdrop_path = tmdb_info.get("backdropPath", "")
            poster_path = tmdb_info.get("posterPath", "")
            if backdrop_path:
                image_url = backdrop_path
            elif poster_path:
                image_url = poster_path

        self.post_message(
            mtype=NotificationType.Plugin,
            title=notification_title,
            text=text,
            image=image_url
        )
        logger.info(f"已发送追剧进度通知: {task_name}")

    # ==================== 清理任务通知 ====================
    def _send_clean_notification(self, title: str, deleted_count: int,
                                 skipped_count: int,
                                 deleted_proxy_tasks: int = 0, deleted_normal_tasks: int = 0,
                                 account_info: dict = None, media_info: dict = None):
        """发送清理任务通知"""
        if not self._notify:
            return

        # 构建标题
        notification_title = f" ✅《{title}》| CAS任务已清理"

        # 根据任务类型构建标题（同一个媒体不可能同时有普通任务和玄鲸任务）
        if deleted_proxy_tasks > 0:
            task_type = "玄鲸任务"
        else:
            task_type = "普通任务"

        # 获取媒体类型信息
        media_type_icon = "📺"
        media_type_text = "电视剧"
        if media_info:
            video_type = media_info.get("videoType", "tv")
            media_type_icon = "📺" if video_type == "tv" else "🎬"
            media_type_text = "电视剧" if video_type == "tv" else "电影"

        # 获取账号信息
        account_username = account_info["username"]

        # 构建通知文本
        task_type_icon = "⚡" if deleted_proxy_tasks > 0 else "🗂️"
        text = (
            f"{media_type_icon}类型：{media_type_text} ｜ {task_type_icon}任务：{task_type}\n"
            f"\n\n"
            f"👤账号：{account_username}\n"
            f"🕛时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        # 获取图片URL，优先从backdropPath获取，如果没有再从posterPath获取
        image_url = ""
        tmdb_info = media_info.get("tmdb", {})
        if tmdb_info:
            backdrop_path = tmdb_info.get("backdropPath", "")
            poster_path = tmdb_info.get("posterPath", "")
            if backdrop_path:
                image_url = backdrop_path
            elif poster_path:
                image_url = poster_path

        self.post_message(
            mtype=NotificationType.Plugin,
            title=notification_title,
            text=text,
            image=image_url
        )
        logger.info(f"已发送清理任务通知: {title}")

    def _send_no_tasks_found_notification(self, title: str, event_info=None):
        """发送未找到任务的通知"""
        if not self._notify:
            return

        notification_title = f" ❌《{title}》| CAS查询失败"
        text = (
            f"🔍在CAS中未找到相关的任务\n"
            f"🕛时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        image_url = ""
        if event_info:
            try:
                tmdb_id = getattr(event_info, "tmdb_id", "")
                if tmdb_id:
                    pass
            except Exception as e:
                if self._debug_log:
                    logger.debug(f"从事件信息获取图片失败: {str(e)}")

        self.post_message(
            mtype=NotificationType.Plugin,
            title=notification_title,
            text=text,
            image=image_url
        )
        logger.info(f"已发送未找到任务通知: {title}")


# 确保插件类被正确导出
__plugin__ = CASTaskCleaner
