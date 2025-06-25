import os
import re
import requests
import threading
import time
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from difflib import SequenceMatcher

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from app.plugins import _PluginBase
from app.log import logger
from app.schemas import NotificationType

class Cloud189helper(_PluginBase):
    # 插件基本信息
    plugin_name = "cloud189助手"
    plugin_desc = "STRM文件删除后自动清理对应的cloud189任务"
    plugin_icon = "cloud_delete.png"
    plugin_version = "1.0"
    plugin_author = "gldl137"
    author_url = "https://github.com/gldl137"
    plugin_config_prefix = "cloud189_helper_"
    plugin_order = 16
    auth_level = 1

    # 配置项默认值
    _enabled = False
    _notify = False
    _api_url = ""
    _api_key = ""
    _monitor_dirs = ""
    _observer = None
    _file_handler = None
    _initialized = False
    _last_auth_error = 0
    _min_similarity = 0.7
    _delay_seconds = 0  # 新增默认值
    _delete_cloud_content = True  # 新增默认值
    _deleted_folders = []
    _batch_timer = None
    _batch_lock = threading.Lock()
    _batch_delay = 5  # 秒，批量合并时间窗口

    API_TASKS_BATCH = "/api/tasks/batch"
    API_TASKS = "/api/tasks"

    def init_plugin(self, config: dict = None):
        # 确保只初始化一次
        if self._initialized:
            logger.debug("插件已经初始化，跳过重复初始化")
            return

        logger.info("初始化cloud189助手插件")
        self._initialized = True
        self._last_auth_error = 0

        # 加载配置
        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", False)
            self._api_url = config.get("api_url", "").strip().rstrip('/')
            self._api_key = config.get("api_key", "")
            self._monitor_dirs = config.get("monitor_dirs", "")
            self._min_similarity = float(config.get("min_similarity", 0.7))
            self._delay_seconds = int(config.get("delay_seconds", 0))
            val = config.get("delete_cloud_content", True)
            if isinstance(val, str):
                self._delete_cloud_content = val.lower() == "true"
            else:
                self._delete_cloud_content = bool(val)

        # 停止现有服务
        self.stop_service()

        # 如果启用则启动监控
        if self._enabled:
            # 验证必要配置
            if not self._api_url or not self._api_key:
                logger.error("云盘API地址或API密钥未配置")
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="cloud189助手配置错误",
                        text="请检查API地址和API密钥配置"
                    )
                return

            # 初始化监控目录
            monitor_paths = []
            if self._monitor_dirs:
                monitor_paths = [d.strip() for d in self._monitor_dirs.split("\n") if d.strip()]

            if not monitor_paths:
                logger.warning("未配置监控目录")
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="cloud189助手配置警告",
                        text="未配置监控目录，插件无法工作"
                    )
                return

            logger.info(f"配置的监控目录: {monitor_paths}")

            try:
                self._observer = Observer()
                self._file_handler = StrmFileMonitorHandler(self)
                monitored_count = 0

                for path in monitor_paths:
                    if not path:
                        continue

                    path_obj = Path(path)
                    if path_obj.exists() and path_obj.is_dir():
                        self._observer.schedule(
                            self._file_handler,
                            path,
                            recursive=True
                        )
                        monitored_count += 1
                        logger.info(f"✅ 成功监控目录: {path}")
                    else:
                        logger.warning(f"⛔ 监控目录不存在或不是目录: {path}")

                if monitored_count > 0:
                    self._observer.start()
                    logger.info(f"cloud189助手监控服务已启动，成功监控 {monitored_count}/{len(monitor_paths)} 个目录")
                else:
                    logger.error("没有有效的监控目录，监控服务未启动")

            except Exception as e:
                logger.error(f"启动监控失败: {str(e)}")
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="cloud189助手启动失败",
                        text=str(e)
                    )

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        """停止监控服务，并清理批量任务相关状态"""
        if self._observer:
            try:
                self._observer.stop()
                self._observer.join(timeout=5)
                if self._observer.is_alive():
                    logger.warning("监控线程未正常停止，强制终止")
                else:
                    logger.info("cloud189助手监控服务已停止")
            except Exception as e:
                logger.error(f"停止监控服务失败: {str(e)}")
            finally:
                self._observer = None
                self._file_handler = None

        # 清理批量删除相关状态，防止热重载残留
        with self._batch_lock:
            self._deleted_folders = []
            if self._batch_timer is not None:
                self._batch_timer.cancel()
                self._batch_timer = None

        # 重置初始化标志
        self._initialized = False

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置表单"""
        return [
            {
                "component": "VForm",
                "content": [
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
                                            "title": "cloud189助手",
                                            "text": "当STRM文件被删除后，自动查询并删除对应的云盘任务。需要配合 cloud189-auto-save 插件使用。",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件", "hint": "首次安装请先保存一次配置"}},
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}},
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "delete_cloud_content", "label": "删除天翼网盘内容", "hint": "关闭则只删除任务，保留网盘文件"}},
                                ],
                            },
                        ],
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
                                            "model": "api_url",
                                            "label": "API地址",
                                            "placeholder": "http://192.168.2.6:3005",
                                            "hint": "cloud189-auto-save 的API地址"
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
                                            "model": "api_key",
                                            "label": "API密钥",
                                            "placeholder": "9EGBxNPdw9kJFkwjmbgW1kyprwRD3q3u",
                                            "hint": "cloud189-auto-save 的API密钥"
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
                                            "model": "monitor_dirs",
                                            "label": "监控目录",
                                            "rows": 3,
                                            "placeholder": "每行一个目录路径（STRM文件所在目录）",
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "min_similarity",
                                            "label": "匹配相似度阈值",
                                            "type": "number",
                                            "min": 0.5,
                                            "max": 1.0,
                                            "step": 0.05,
                                            "placeholder": "0.7",
                                            "hint": "值范围0.5~1.0，值越低匹配越宽松，值越高匹配越严格"
                                        }
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "推荐值：0.7-0.8。提取文件名与资源的匹配值，最大1，1表示与资源名完全匹配。",
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "delay_seconds",
                                            "label": "延迟查询秒数",
                                            "type": "number",
                                            "min": 0,
                                            "max": 300,
                                            "step": 1,
                                            "placeholder": "0",
                                            "hint": "STRM文件删除后延迟多少秒再查询任务ID，0为不延迟"
                                        }
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "建议在任务较多时设置适当延迟，以避免瞬时删除导致的查询失败。",
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": "首次安装请点击右下角设置打开'启用插件'并保存。如果安装提示404错误，请重新打开设置页面保存一次。",
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "确保 cloud189-auto-save 插件正在运行，且API地址和密钥配置正确。插件将监控指定目录的STRM文件删除事件，并自动清理对应的云盘任务和天翼网盘文件。",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "api_url": "http://192.168.2.6:3005",
            "api_key": "",
            "monitor_dirs": "",
            "min_similarity": 0.7,
            "delay_seconds": 0,  # 默认不延迟
            "delete_cloud_content": True  # 必须有这一行，否则删除任务时无法控制是否同时删除云盘内容
        }

    def get_api(self) -> List[Dict[str, Any]]:
        """获取插件API"""
        return []


    def get_page(self) -> List[dict]:
        """点击插件进入设置"""
        pass



    def handle_folder_deleted(self, folder_path: Path):
        """收集被删除的文件夹，批量处理"""
        if not self._enabled:
            return

        # 排除 Season 目录
        if re.match(r"season\s*\d+", folder_path.name, re.IGNORECASE):
            logger.info(f"忽略Season目录删除事件: {folder_path}")
            return

        logger.info(f"检测到资源文件夹被删除: {folder_path}")

        with self._batch_lock:
            self._deleted_folders.append(folder_path)
            if self._batch_timer is not None:
                self._batch_timer.cancel()
            self._batch_timer = threading.Timer(self._batch_delay, self._process_batch_deleted_folders)
            self._batch_timer.start()


    def _process_batch_deleted_folders(self):
        """批量处理收集到的被删除文件夹"""
        with self._batch_lock:
            folders = self._deleted_folders
            self._deleted_folders = []
            self._batch_timer = None

        if not folders:
            return

        # 延迟（只延迟一次，批量处理）
        if getattr(self, "_delay_seconds", 0) > 0:
            logger.info(f"批量删除，等待 {self._delay_seconds} 秒后再查询任务ID")
            time.sleep(self._delay_seconds)

        all_task_ids = []
        resource_names = []
        for folder_path in folders:
            resource_name = folder_path.name
            resource_name = re.sub(r' \(\d{4}\)$', '', resource_name)
            logger.info(f"批量处理资源目录: {resource_name}")
            task_ids = self.find_cloud_tasks(resource_name)
            if task_ids:
                all_task_ids.extend(task_ids)
                resource_names.append(f"《{resource_name}》")
            else:
                logger.info(f"未找到资源对应的云盘任务: {resource_name}")
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="云盘任务清理",
                        text=f"ℹ️ 未找到匹配任务\n🎬 资源: 《{resource_name}》"
                    )

        # 去重
        all_task_ids = list(set(all_task_ids))
        if not all_task_ids:
            return

        # 删除所有匹配的云盘任务
        success_count, fail_count, success_ids, fail_ids = self.delete_cloud_tasks(all_task_ids)

        # 发送合并通知
        if self._notify:
            if self._delete_cloud_content:
                cloud_msg = "🗑️ 已删除网盘文件"
            else:
                cloud_msg = "📁 未删除网盘内容"
            display_resources = "，".join(resource_names)
            if success_count > 0 and fail_count == 0:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="云盘任务批量清理成功",
                    text=f"✅ 已清理 {success_count} 个任务\n🎬 资源: {display_resources}\n{cloud_msg}"
                )
            elif success_count > 0 and fail_count > 0:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="云盘任务批量部分清理",
                    text=f"⚠️ 成功 {success_count} 个, 失败 {fail_count} 个\n🎬 资源: {display_resources}\n{cloud_msg}"
                )
            else:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="云盘任务批量清理失败",
                    text=f"❌ 清理任务失败\n🎬 资源: {display_resources}\n{cloud_msg}"
                )


    def delete_cloud_tasks(self, task_ids: List[str]) -> Tuple[int, int, List[str], List[str]]:
        """
        批量删除云盘任务，支持同时删除云盘文件。
        返回 (成功数量, 失败数量, 成功任务ID列表, 失败任务ID列表)。
        日志中详细记录每个任务的删除结果。
        """
        logger.info(f"删除天翼网盘内容开关状态: {getattr(self, '_delete_cloud_content', True)}")
        task_ids = [str(tid) for tid in task_ids]
        success_ids = []
        fail_ids = []
        try:
            headers = {
                "x-api-key": self._api_key,
                "Content-Type": "application/json"
            }
            resp = requests.delete(
                f"{self._api_url}{self.API_TASKS_BATCH}",
                headers=headers,
                json={
                    "taskIds": task_ids,
                    "deleteCloud": getattr(self, "_delete_cloud_content", True)
                },
                timeout=30
            )
            if resp.status_code == 200:
                data = resp.json()
                # 兼容不同后端返回结构
                if data.get("success"):
                    detail = data.get("data", {})
                    # 1. 如果有 per-task 结果
                    if isinstance(detail, dict) and ("successIds" in detail or "failIds" in detail):
                        success_ids = [str(i) for i in detail.get("successIds", [])]
                        fail_ids = [str(i) for i in detail.get("failIds", [])]
                    # 2. 如果只返回整体 success
                    else:
                        success_ids = task_ids
                        fail_ids = []
                    # 日志详细记录每个任务的删除结果
                    for tid in success_ids:
                        logger.info(f"任务ID {tid} 删除成功")
                    for tid in fail_ids:
                        logger.warning(f"任务ID {tid} 删除失败")
                    logger.info(f"批量删除结果：成功 {len(success_ids)} 个，失败 {len(fail_ids)} 个")
                    return len(success_ids), len(fail_ids), success_ids, fail_ids
                else:
                    # 失败时尝试解析失败ID
                    detail = data.get("data", {})
                    if isinstance(detail, dict) and "failIds" in detail:
                        fail_ids = [str(i) for i in detail.get("failIds", [])]
                    else:
                        fail_ids = task_ids
                    for tid in fail_ids:
                        logger.error(f"任务ID {tid} 删除失败")
                    logger.error(f"批量删除失败: {data.get('error', '')}，失败任务: {fail_ids}")
                    return 0, len(fail_ids), [], fail_ids
            else:
                logger.error(f"批量删除请求失败: HTTP {resp.status_code}")
                logger.error(f"响应内容: {resp.text[:500]}")
                for tid in task_ids:
                    logger.error(f"任务ID {tid} 删除失败（请求失败）")
                return 0, len(task_ids), [], task_ids
        except Exception as e:
            logger.error(f"批量删除请求异常: {str(e)}")
            for tid in task_ids:
                logger.error(f"任务ID {tid} 删除失败（异常）")
            return 0, len(task_ids), [], task_ids


    def find_cloud_tasks(self, resource_name: str) -> Optional[List[str]]:
        """查询资源名称对应的云盘任务ID列表（模糊匹配），自动遍历所有页"""
        try:
            headers = {"x-api-key": self._api_key}
            logger.info(f"查询所有云盘任务")
            page = 1
            page_size = 200  # 优化：增大分页，减少请求次数
            matched_tasks = []
            normalized_resource = self.normalize_name(resource_name)
            logger.info(f"规范化资源名称: '{resource_name}' → '{normalized_resource}'")
            while True:
                params = {
                    "status": "all",
                    "search": "",
                    "type": "all",
                    "page": page,
                    "pageSize": page_size
                }
                response = requests.get(
                    f"{self._api_url}{self.API_TASKS}",
                    headers=headers,
                    params=params,
                    timeout=15
                )

                if response.status_code == 401:
                    self._handle_auth_error("查询")
                    return None

                if response.status_code != 200:
                    logger.error(f"查询任务失败: HTTP {response.status_code}")
                    logger.error(f"响应内容: {response.text[:500]}")  # 优化：记录详细响应
                    return []

                try:
                    data = response.json()
                    if not data.get("success", False):
                        logger.error(f"API返回错误: {data.get('message', '未知错误')}")
                        return []

                    tasks = data.get("data", {}).get("tasks", [])
                    if not tasks:
                        break

                    for task in tasks:
                        task_name = task.get("resourceName", "")
                        if not task_name:
                            continue
                        normalized_task = self.normalize_name(task_name)
                        similarity = self.calculate_similarity(normalized_resource, normalized_task)
                        sim_log = f"任务 '{task_name}': 相似度 {similarity:.2f} ({normalized_resource} vs {normalized_task})"
                        if similarity >= self._min_similarity:
                            logger.info(f"✅ {sim_log} - 匹配")
                            matched_tasks.append(task)
                        else:
                            logger.debug(f"❌ {sim_log} - 未匹配")

                    total_pages = data.get("data", {}).get("totalPages", 1)
                    if page >= total_pages:
                        break
                    page += 1

                except ValueError as e:
                    logger.error(f"解析JSON响应失败: {str(e)}")
                    logger.error(f"响应内容: {response.text[:500]}")  # 优化：记录详细响应
                    return []

            if not matched_tasks:
                logger.info(f"未找到与 '{resource_name}' 相似的任务 (阈值: {self._min_similarity})")
                return []

            return [str(task.get("id")) for task in matched_tasks]

        except requests.exceptions.RequestException as e:
            logger.error(f"请求API失败: {str(e)}")
            return []


    def normalize_name(self, name: str) -> str:
        """规范化资源名称，用于相似度比较（优先提取中文）"""
        # 1. 移除文件扩展名
        name = re.sub(r'\.[a-zA-Z0-9]+$', '', name)
        
        # 2. 优先提取中文部分
        chinese_part = re.findall(r'[\u4e00-\u9fff]+', name)
        if chinese_part:
            # 连接所有中文部分
            result = ''.join(chinese_part)
            logger.debug(f"提取中文部分: '{name}' → '{result}'")
            return result
        
        # 3. 如果没有中文，则尝试移除技术规格
        patterns = [
            r'BluRay', r'REMUX', r'H264', r'H265', r'HEVC', 
            r'DTSHD', r'DTS', r'MA', r'AC3', r'Atmos',
            r'[0-9]{3,4}p', r'[0-9]{4}\b', r'\b\d+\.\d+', 
            r'\.\d+\.', r'\d{1,2}ch', r'\d{1,2}bit'
        ]
        
        for pattern in patterns:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        # 4. 移除特殊字符和多余空格
        name = re.sub(r'[^\w\u4e00-\u9fff]+', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        # 5. 转换为小写
        return name.lower()


    def calculate_similarity(self, str1: str, str2: str) -> float:
        """使用SequenceMatcher计算两个字符串的相似度"""
        if not str1 or not str2:
            return 0.0
        
        # 使用SequenceMatcher计算相似度
        return SequenceMatcher(None, str1, str2).ratio()

class StrmFileMonitorHandler(FileSystemEventHandler):
    """监控文件夹删除事件的处理器"""
    def __init__(self, plugin):
        super().__init__()
        self.plugin = plugin

    def on_deleted(self, event):
        # 只处理目录删除
        if event.is_directory:
            folder_path = Path(event.src_path)
            self.plugin.handle_folder_deleted(folder_path)
