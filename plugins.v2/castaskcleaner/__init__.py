from collections import deque
from functools import lru_cache
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
import json
import requests
import threading
import time
import re
import datetime
import traceback

from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.schemas import WebhookEventInfo

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
    plugin_desc = "自动清理《天翼云盘自动转存》中已完成任务，并通知追剧进度。"
    # 插件图标
    plugin_icon = "cloud189.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137"
    # 插件配置项ID前缀
    plugin_config_prefix = "CASTaskCleaner"
    # 加载顺序
    plugin_order = 10
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled: bool = False
    _notify: bool = False
    _server: str = "emby"
    _host: Optional[str] = None
    _api_key: Optional[str] = None
    _delay_seconds: int = 0
    _debug_log: bool = False
    _session: requests.Session = None
    _event_cache = deque(maxlen=200)

    def init_plugin(self, config: Optional[dict] = None):
        # 初始化请求会话
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "CASTaskCleaner/1.0"})
        
        if config:
            self._enabled = config.get("enabled", False)
            self._server = config.get("server", "emby")
            self._notify = config.get("notify", False)
            self._debug_log = config.get("debug_log", False)
            self._host = config.get("host", "")
            self._api_key = config.get("api_key", "")
            
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
            
            if self._debug_log:
                logger.info("调试日志已启用")

    def _validate_config(self) -> bool:
        """验证配置是否有效"""
        if self._host:
            if not self._host.startswith("http"):
                self._host = "http://" + self._host
            if not self._host.endswith("/"):
                self._host += "/"
        
        if self._enabled and (not self._host or not self._api_key):
            logger.error("插件启用但缺少必要配置（host 或 api_key）")
            return False
            
        return True

    def _safe_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """带重试机制的安全请求"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self._session.request(
                    method, 
                    url, 
                    timeout=10, 
                    **kwargs
                )
                if self._debug_log:
                    logger.debug(f"请求成功: {method} {url} (尝试 {attempt+1}/{max_retries})")
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"请求失败: {method} {url} (尝试 {attempt+1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2)
        
        logger.error(f"所有请求尝试均失败: {method} {url}")
        return None

    def _test_cas_connection(self):
        """测试CAS服务连接"""
        try:
            logger.info("测试CAS服务连接...")
            headers = {"x-api-key": self._api_key}
            res = self._safe_request(
                "GET",
                f"{self._host}api/tasks",
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
        server_options = [
            {"title": "Emby", "value": "emby"},
            {"title": "Jellyfin", "value": "jellyfin"},
            {"title": "Plex", "value": "plex"},
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
                                            "model": "notify",
                                            "label": "发送通知",
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
                                            "model": "debug_log",
                                            "label": "调试日志",
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "server",
                                            "label": "媒体服务器",
                                            "items": server_options
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
                                            "text": "本插件会在影片追更完结后，自动清理《天翼云盘自动转存》项目中已完成任务，和通知追剧进度。"
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
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": "请确保：\n1. 天翼云盘自动转存服务正在运行\n2. API Key，地址和端口正确"
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
                                            "text": "任务查询优化说明：\n1. 使用影片名称进行搜索\n2.只查删除普通的已完成任务，玄鲸任务不做处理\n3.追剧进度通知只针对电视剧类型"
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
            "server": self._server,
            "notify": self._notify,
            "debug_log": self._debug_log,
            "host": self._host or "",
            "api_key": self._api_key or "",
            "delay_seconds": self._delay_seconds,
        }

    def get_state(self) -> bool:
        return self._enabled

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        if self._session:
            self._session.close()

    def _convert_media_type(self, media_type: Optional[str]) -> str:
        """将媒体类型转换为中文"""
        if not media_type:
            return "未知类型"
        return MEDIA_TYPE_MAP.get(media_type.lower(), media_type)

    @eventmanager.register(EventType.WebhookMessage)
    def handle_media_added(self, event: Event):
        if not self._enabled:
            return
            
        try:
            event_info: WebhookEventInfo = event.event_data
            
            if self._debug_log:
                try:
                    logger.debug(f"收到事件: {event_info.model_dump_json(indent=2)}")
                except:
                    logger.debug(f"收到事件: {vars(event_info)}")
            
            unique_key = f"{getattr(event_info, 'item_id', '')}_{getattr(event_info, 'event', '')}_{getattr(event_info, 'channel', '')}"
            if unique_key in self._event_cache:
                if self._debug_log:
                    logger.debug(f"已处理过该事件，跳过: {unique_key}")
                return
            self._event_cache.append(unique_key)

            event_channel = getattr(event_info, "channel", "")
            if event_channel != self._server:
                if self._debug_log:
                    logger.debug(f"事件来源 {event_channel} 不匹配配置的 {self._server}，跳过")
                return
            
            expected_events = {
                "emby": ["library.new"],
                "jellyfin": ["library.new"],
                "plex": ["library.new"]
            }.get(self._server, ["library.new"])
            
            event_type = getattr(event_info, "event", None)
            if event_type not in expected_events:
                if self._debug_log:
                    logger.debug(f"事件类型 {event_type} 不在允许列表 {expected_events} 中，跳过")
                return

            if event_type == "deep.delete":
                if self._debug_log:
                    logger.debug(f"忽略删除事件: {event_type}")
                return

            media_type = getattr(event_info, "media_type", None)
            title = getattr(event_info, "item_name", None)
            year = getattr(event_info, "year", None)

            # 根据媒体类型调整搜索关键字
            json_object = getattr(event_info, "json_object", {})
            if media_type and media_type.lower() in ["tv", "series", "episode", "season", "show"]:
                # 电视剧类型：尝试从json_object中获取SeriesName
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
                except Exception as e:
                    logger.warn(f"解析json_object获取SeriesName失败: {str(e)}，将使用原标题: {title}")
            else:
                if self._debug_log:
                    logger.debug(f"媒体类型 {media_type}，使用原标题作为搜索关键字: {title}")

            if not title:
                logger.warn(f"添加的媒体标题为空，跳过")
                return

            # 将媒体类型转换为中文显示
            media_type_chinese = self._convert_media_type(media_type)
            logger.info(f"检测到新媒体添加: {title} (类型: {media_type_chinese})")
            
            self._start_processing_thread(title, year, media_type)
            
        except Exception as e:
            logger.error(f"处理事件时发生异常: {str(e)}")
            logger.error(traceback.format_exc())

    def _start_processing_thread(self, title: str, year: Optional[int] = None, 
                                 media_type: Optional[str] = None):
        """启动处理线程"""
        if self._delay_seconds > 0:
            logger.info(f"将在 {self._delay_seconds} 秒后开始处理: {title}")
        else:
            logger.info(f"将立即开始处理: {title}")

        thread = threading.Thread(
            target=self._delayed_process,
            args=(title, self._delay_seconds, year, media_type)
        )
        thread.daemon = True
        thread.start()
        logger.info(f"已启动处理线程: {title}")

    def _delayed_process(self, title: str, delay_seconds: int, 
                         year: Optional[int] = None, media_type: Optional[str] = None):
        try:
            logger.info(f"开始处理: {title}")
            
            if delay_seconds > 0:
                logger.info(f"延迟处理: {title}，等待 {delay_seconds} 秒...")
                time.sleep(delay_seconds)
                logger.info(f"延迟等待结束，开始处理: {title}")

            # 获取任务信息
            task_info = self._get_task_info_by_title(title)
            if task_info is None:
                logger.error(f"查询 '{title}' 的 CAS 任务失败")
                return
            elif not task_info["ids"]:
                logger.info(f"未找到与 '{title}' 相关的任务")
                return
            else:
                logger.info(
                    f"查询成功，找到 {len(task_info['ids'])} 个相关任务，"
                    f"状态分布: {self._format_task_status_counts(task_info)}"
                )

            # 处理任务（删除完成状态的任务）
            result = self._process_and_delete_tasks(task_info)
            deleted_count = result["deleted_count"]
            skipped_count = result["skipped_count"]
            processing_tasks = result["processing_tasks"]
            
            # 发送通知
            self._maybe_send_notifications(
                title, year, media_type, 
                deleted_count, skipped_count, processing_tasks
            )
                
        except Exception as e:
            logger.error(f"处理任务时发生异常: {str(e)}")
            logger.error(traceback.format_exc())

    def _get_task_info_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """根据标题查询任务信息"""
        if not self._host or not self._api_key:
            logger.error("未配置 CAS 服务地址或 API Key")
            return None
        
        try:
            # 优化搜索关键字：去除括号和空格
            clean_title = re.sub(r"\(.*?\)|（.*?）|\s", "", title)
            logger.info(f"使用优化后的搜索关键字: '{clean_title}'")
            
            task_ids = []
            task_status = {}
            task_details = {}
            status_counts = {k: 0 for k in TASK_STATUS_MAP.keys()}
            status_counts["unknown"] = 0
            
            page = 1
            total_pages = 1
            
            while page <= total_pages:
                headers = {"x-api-key": self._api_key}
                params = {
                    "status": "all",
                    "search": clean_title,
                    "type": "normal",  # 只查询普通任务
                    "group": "all",    # 所有组
                    "accountId": "all",  # 所有账户
                    "page": page,
                    "pageSize": 100
                }
                
                if self._debug_log:
                    logger.debug(f"请求任务页 {page}/{total_pages}")
                
                res = self._safe_request(
                    "GET",
                    f"{self._host}api/tasks",
                    headers=headers,
                    params=params
                )
                
                if not res:
                    return None
                    
                if res.status_code != 200:
                    logger.error(f"请求 CAS 任务失败，状态码: {res.status_code}")
                    if res.text:
                        logger.error(f"错误响应: {res.text[:500]}")
                    return None
                    
                try:
                    data = res.json()
                except Exception as e:
                    logger.error(f"解析JSON失败: {str(e)}")
                    if res.text:
                        logger.error(f"响应内容: {res.text[:500]}")
                    return None
                
                # 兼容两种可能的响应格式
                if data.get("success") and "data" in data:
                    task_list = data["data"].get("tasks", [])
                    pagination = data["data"].get("pagination", {})
                else:
                    task_list = data.get("tasks", [])
                    pagination = data.get("pagination", {})
                
                if pagination:
                    total_pages = pagination.get("totalPages", 1)
                    if self._debug_log:
                        logger.debug(f"分页信息: 当前页 {page}, 总页数 {total_pages}")
                else:
                    total_pages = 1
                
                if not task_list:
                    if self._debug_log:
                        logger.debug(f"第 {page} 页无任务")
                    break
                
                for task in task_list:
                    task_id = task.get("id")
                    status = task.get("status", "unknown").lower()
                    task_name = task.get("resourceName", "未知名称")
                    
                    if task_id:
                        task_id_str = str(task_id)
                        task_ids.append(task_id_str)
                        task_status[task_id_str] = status
                        task_details[task_id_str] = {
                            "resourceName": task_name,
                            "currentEpisodes": task.get("currentEpisodes", 0),
                            "totalEpisodes": task.get("totalEpisodes", 0),
                            "videoType": task.get("videoType", "unknown")
                        }
                        
                        # 更新状态计数
                        if status in status_counts:
                            status_counts[status] += 1
                        else:
                            status_counts["unknown"] += 1
                        
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
                    else:
                        logger.warning("任务条目缺少ID，跳过")
                
                page += 1
                time.sleep(0.5)
            
            return {
                "ids": task_ids,
                "status": task_status,
                "details": task_details,
                "counts": status_counts
            }
            
        except Exception as e:
            logger.error(f"获取任务异常: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _process_and_delete_tasks(self, task_info: dict) -> dict:
        """处理并删除任务"""
        deleted_count = 0
        skipped_count = 0
        processing_tasks = []
        
        for task_id in task_info["ids"]:
            task_status = task_info["status"].get(task_id, "unknown")
            task_detail = task_info["details"].get(task_id, {})
            
            if task_status == "completed":
                if self._debug_log:
                    logger.debug(f"准备删除任务 (ID: {task_id})")
                if self._delete_cloud189_task(task_id):
                    deleted_count += 1
                    logger.info(f"删除任务成功 (ID: {task_id})")
                else:
                    logger.error(f"删除任务失败 (ID: {task_id})")
            else:
                status_cn = TASK_STATUS_MAP.get(task_status, task_status)
                logger.info(f"跳过非完成状态任务 (ID: {task_id}, 状态: {status_cn})")
                skipped_count += 1
                
                # 只记录电视剧类型的进行中任务
                if task_status == "processing" and task_detail.get("videoType") == "tv":
                    processing_tasks.append({
                        "id": task_id,
                        "name": task_detail.get("resourceName", "未知名称"),
                        "current": task_detail.get("currentEpisodes", 0),
                        "total": task_detail.get("totalEpisodes", 0)
                    })
                elif self._debug_log and task_status == "processing":
                    logger.debug(f"跳过电影类型的进行中任务 (ID: {task_id})")
        
        return {
            "deleted_count": deleted_count,
            "skipped_count": skipped_count,
            "processing_tasks": processing_tasks
        }

    def _maybe_send_notifications(self, title: str, year: Optional[int], 
                                  media_type: Optional[str], deleted_count: int, 
                                  skipped_count: int, processing_tasks: list):
        """根据需要发送通知"""
        # 发送进行中任务通知（仅电视剧类型）
        if processing_tasks:
            self._send_processing_notification(processing_tasks)
        
        # 发送清理任务通知
        if deleted_count > 0:
            self._send_clean_notification(title, year, media_type, deleted_count, skipped_count)
        elif self._debug_log:
            logger.debug("未成功删除任何任务，不发送清理通知")

    def _format_task_status_counts(self, task_info: dict) -> str:
        """格式化任务状态统计信息"""
        counts = []
        for status, count in task_info["counts"].items():
            if count > 0:
                status_cn = TASK_STATUS_MAP.get(status, status)
                counts.append(f"{status_cn}: {count}")
        
        return ", ".join(counts)

    def _send_processing_notification(self, tasks: List[Dict]):
        """发送进行中任务通知（仅电视剧类型）"""
        if not self._notify:
            return
            
        if not tasks:
            return
            
        # 构建通知标题
        title = "⏳ CAS追剧进度"
        
        # 构建通知内容
        lines = []
        for task in tasks:
            # 计算剩余集数
            remaining = task["total"] - task["current"]
            
            # 添加表情符号表示进度
            if remaining == 0:
                emoji = "🎉"
            elif remaining <= 5:
                emoji = "🔥"
            elif remaining <= 10:
                emoji = "🚀"
            else:
                emoji = "📺"
                
            lines.append(
                f"{emoji} 《{task['name']}》: "
                f"更 {task['current']} 集 / 共 {task['total']} 集 "
                f"(余 {remaining} 集)"
            )
        
        # 添加分隔行
        text = "\n".join(lines)
        
        # 添加当前时间
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        text += f"\n🕒 {current_time}"
        
        self.post_message(
            mtype=NotificationType.Plugin,
            title=title,
            text=text
        )

    def _send_clean_notification(self, title: str, year: Optional[int], 
                                media_type: Optional[str], deleted_count: int, 
                                skipped_count: int):
        """发送清理任务通知"""
        if not self._notify:
            return
            
        # 将媒体类型转换为中文
        media_type_chinese = self._convert_media_type(media_type)
        
        # 构建标题
        if year:
            title_with_year = f"{title} ({year})"
        else:
            title_with_year = title
            
        # 获取当前时间
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 构建通知文本
        text = (
            f"\n"
            f"🎬 媒体：《{title_with_year}》({media_type_chinese})\n"
            f"🧹 清理: 共 {deleted_count} 个任务\n"
            f"🕒 {current_time}"
        )
        
        self.post_message(
            mtype=NotificationType.Plugin,
            title="🔔 CAS任务清理",
            text=text
        )

    def _delete_cloud189_task(self, task_id: str) -> bool:
        if not task_id:
            logger.error("删除任务失败: 缺少任务ID")
            return False
            
        try:
            url = f"{self._host}api/tasks/{task_id}"
            headers = {"x-api-key": self._api_key}
            
            if self._debug_log:
                logger.debug(f"删除任务: ID={task_id}")
            
            res = self._safe_request("DELETE", url, headers=headers)
            
            if not res:
                return False
                
            if res.status_code == 200:
                if self._debug_log:
                    logger.debug(f"删除任务成功 (ID: {task_id})")
                return True
            else:
                logger.error(f"删除任务失败 (ID: {task_id})，状态码: {res.status_code}")
                try:
                    error_detail = res.json().get("message", "无错误详情")
                    logger.error(f"错误详情: {error_detail}")
                except:
                    if res.text:
                        logger.error(f"响应内容: {res.text[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"删除任务异常 (ID: {task_id}): {str(e)}")
            logger.error(traceback.format_exc())
            return False
