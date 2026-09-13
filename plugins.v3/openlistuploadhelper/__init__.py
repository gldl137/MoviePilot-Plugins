import threading
import time
from concurrent.futures import as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# MoviePilot框架导入
from app.chain.storage import StorageChain
from app.core.cache import Cache
from app.core.event import Event, eventmanager
from app.db.plugindata_oper import PluginDataOper
from app.helper.thread import ThreadHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.utils.system import SystemUtils
# 第三方库导入
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

# 定时任务相关导入
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False
    logger.warning("APScheduler 不可用，定时任务功能将受限")

# 常量定义
TEMP_FILE_MARKERS = {'~', 'tmp', 'temp', 'part',
                     'writing', 'incomplete', '::TMPNAME:'}
TEMP_EXTENSIONS = {
    '.tmp', '.temp', '.part', '.incomplete', '.writing', '.swp', '.swx', '.bak'
}
NETWORK_ERROR_KEYWORDS = {'connection',
                          'timeout', 'network', 'socket', 'http', 'ssl'}

# 缓存键前缀
CACHE_PREFIX_PROCESSING = "openlist_upload:processing:"
CACHE_PREFIX_PROCESSED = "openlist_upload:processed:"
CACHE_PREFIX_DELETED = "openlist_upload:deleted:"
CACHE_PREFIX_PLUGIN_DIRS = "openlist_upload:plugin_dirs:"

# 缓存区域 - 改为系统区域
CACHE_REGION = "plugin_openlist_upload"  # 使用插件特定的区域名

# 缓存过期时间（秒）
CACHE_TTL_PROCESSING = 3600  # 处理中文件：1小时
CACHE_TTL_PROCESSED = 86400  # 已处理文件：24小时
CACHE_TTL_DELETED = 1800     # 已删除文件：30分钟
CACHE_TTL_PLUGIN_DIRS = 1800  # 插件目录：30分钟


class FileMonitorHandler(FileSystemEventHandler):
    """文件监控处理器"""

    def __init__(self, monpath: str, plugin: Any, group_config: dict = None, **kwargs):
        super().__init__(**kwargs)
        self._watch_path = monpath
        self.plugin = plugin
        self.group_config = group_config

    def _should_process_event(self, file_path: Path) -> bool:
        """检查是否应该处理文件系统事件"""
        # 检查是否是临时文件/目录
        if any(marker in file_path.name for marker in TEMP_FILE_MARKERS):
            return False

        # 检查是否是插件自己删除的文件
        if self.plugin._cache_backend:
            file_path_str = str(file_path.resolve())
            deleted_key = f"{CACHE_PREFIX_DELETED}{file_path_str}"
            # 从插件实例获取缓存区域
            cache_region = getattr(self.plugin, '_cache_region', CACHE_REGION)
            if self.plugin._cache_backend.get(deleted_key, region=cache_region):
                self.plugin._cache_backend.delete(
                    deleted_key, region=cache_region)
                return False

        return True

    def _schedule_directory_processing(self, target_dir: Path):
        """调度目录处理"""
        self.plugin.schedule_directory_process(
            target_dir,
            self._watch_path,
            self.group_config,
            is_plugin_operation=False
        )

    def on_created(self, event):
        """处理文件创建事件"""
        file_path = Path(event.src_path)

        if not self._should_process_event(file_path):
            return

        target_dir = file_path if event.is_directory else file_path.parent
        self._schedule_directory_processing(target_dir)

    def on_modified(self, event):
        """处理文件修改事件"""
        if not event.is_directory:
            return

        file_path = Path(event.src_path)

        if not self._should_process_event(file_path):
            return

        self._schedule_directory_processing(file_path)

    def on_moved(self, event):
        """处理文件移动事件"""
        dest_path = event.dest_path if hasattr(
            event, 'dest_path') else event.src_path
        dest_file = Path(dest_path)

        if not self._should_process_event(dest_file):
            return

        target_dir = dest_file if event.is_directory else dest_file.parent
        self._schedule_directory_processing(target_dir)


class OpenListUploadHelper(_PluginBase):
    """
    OpenList上传助手插件主类
    """
    plugin_name = "OpenList上传助手"
    plugin_desc = "文件上传工具 - 支持本地文件自动上传到OpenList网盘，支持命令交互"
    plugin_icon = "https://raw.githubusercontent.com/opentvmedia/OpenTV/main/static/logo.png"
    plugin_version = "1.0.0"
    plugin_author = "gldl137"
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "OpenListUploadHelper"
    plugin_order = 10
    auth_level = 1

    # 插件配置参数
    _enabled: bool = False
    _monitor_groups: List[Dict] = []
    _raw_path_mappings: str = ""  # 保存原始文本配置，防止运行态数据结构污染配置
    _mode: str = "fast"
    _onlyonce: bool = False
    _clean_cache_once: bool = False  # 新增：清理缓存开关
    _delay_seconds: int = 0
    _enable_schedule: bool = False
    _schedule_cron: str = "0 2 * * *"
    _file_ext_filter: List[str] = []
    _enable_notify: bool = False
    _protected_dirs: List[str] = []  # 新增：需要保护的主目录列表
    _remote_file_action: str = "跳过"  # 新增：网盘文件存在时的处理方式（跳过/覆盖）

    # 运行时状态
    _storage_chain: Optional[StorageChain] = None
    _observer: List[Any] = []
    _scheduler: Optional[BackgroundScheduler] = None
    _cache_backend: Optional[Cache] = None
    _thread_pool: ThreadHelper = None
    _cleanup_task_started: bool = False
    data_oper: PluginDataOper = None  # 数据库操作实例

    def __init__(self):
        """初始化插件实例"""
        super().__init__()
        # 初始化数据库操作实例
        self.data_oper = PluginDataOper()

    def _parse_path_mappings(self, config: dict) -> list:
        """解析路径映射配置"""
        mappings = []
        raw = config.get("path_mappings", "")

        if isinstance(raw, list):
            return self._parse_array_format_mappings(raw)

        return self._parse_text_format_mappings(raw)

    def _parse_array_format_mappings(self, raw_list: list) -> list:
        """解析数组格式的路径映射"""
        mappings = []
        for mapping in raw_list:
            if mapping.get("monitor_path") and mapping.get("target_path"):
                mappings.append({
                    "monitor_path": mapping["monitor_path"],
                    "target_path": mapping["target_path"],
                    "transfer_mode": mapping.get("transfer_mode", "移动"),
                    "storage_type": "alist"
                })
        return mappings

    def _parse_text_format_mappings(self, raw_text: str) -> list:
        """解析文本格式的路径映射"""
        mappings = []
        for line in raw_text.splitlines():
            line = line.strip()
            if not line:
                continue

            mapping = self._parse_single_mapping_line(line)
            if mapping:
                mappings.append(mapping)

        return mappings

    def _load_legacy_config(self, config: dict):
        """加载旧格式配置"""
        monitor_path = config.get("monitor_path", "")
        target_path = config.get("target_path", "")
        if monitor_path and target_path:
            self._monitor_groups = [{
                "monitor_path": monitor_path,
                "target_path": target_path,
                "transfer_mode": config.get("transfer_mode", "移动"),
                "storage_type": "alist"
            }]

    def _serialize_path_mappings(self) -> str:
        """将路径映射配置序列化为文本格式"""
        lines = []
        for group in self._monitor_groups:
            line = f"{group.get('monitor_path', '')}:{group.get('target_path', '')}#{group.get('transfer_mode', '移动')}"
            lines.append(line)
        return "\n".join(lines)

    def _parse_single_mapping_line(self, line: str) -> dict:
        """解析单行路径映射配置"""
        line_parts = line.split("#")
        if len(line_parts) < 1:
            return None

        path_parts = line_parts[0].split(":")
        if len(path_parts) < 2:
            return None

        return {
            "monitor_path": path_parts[0].strip(),
            "target_path": path_parts[1].strip(),
            "transfer_mode": line_parts[1].strip() if len(line_parts) > 1 else "移动",
            "storage_type": "alist"
        }

    def _init_runtime_state(self):
        """初始化运行时状态"""
        if self._cache_backend is None:
            self._cache_backend = Cache()
            cache_type = "Redis" if self._cache_backend.is_redis() else "内存缓存"
            logger.info(f"缓存后端已初始化，使用系统缓存 ({cache_type})")

            # 初始化缓存区域
            self._cache_region = CACHE_REGION
            # 测试缓存写入
            try:
                self._cache_backend.set(
                    "test_key", "test_value", ttl=10, region=self._cache_region)
                logger.debug("缓存区域测试成功")
            except Exception as e:
                logger.warning(f"缓存区域测试失败，将使用默认区域: {e}")
                self._cache_region = "openlist_upload"  # 回退到原有区域

        if self._thread_pool is None:
            self._thread_pool = ThreadHelper()

        if not hasattr(self, '_stats'):
            self._stats = {
                'total_files_processed': 0,
                'total_files_failed': 0,
                'last_operation_time': None,
                'average_processing_time': 0.0,
                'peak_processing_rate': 0.0,
                'uptime_start': time.time()
            }

        # 初始化处理状态集合 - 改为使用字典跟踪不同监控目录
        if not hasattr(self, '_processing_files'):
            self._processing_files = {}
        if not hasattr(self, '_processed_files'):
            self._processed_files = {}
        if not hasattr(self, '_recently_deleted_files'):
            self._recently_deleted_files = {}
        if not hasattr(self, '_dir_processing'):
            self._dir_processing = {}
            self._dir_lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        """初始化插件 - 加载配置并启动服务"""
        try:
            self.stop_service()
            logger.info("正在初始化插件...")

            self._init_runtime_state()

            # 如果config为None，尝试从数据库加载配置
            if config is None:
                try:
                    config = self.get_config()
                    logger.info("从数据库加载配置")
                except Exception as e:
                    logger.warning(f"从数据库加载配置失败: {e}")
                    config = {}

            if config:
                self._load_config(config)

                if self._enabled:
                    if not self._cleanup_task_started:
                        self._start_cleanup_task()
                        self._cleanup_task_started = True

                    if self._enable_schedule:
                        logger.info("⏰ 启用定时处理模式，实时监控已禁用")
                        self._start_schedule_service()
                    else:
                        self._start_file_monitor()

                    # 检查是否需要立即运行一次全量同步
                    if self._onlyonce:
                        logger.info("🚀 开始全量同步...")
                        self.api_sync()
                        # 同步完成后更新配置
                        self._onlyonce = False
                        self.__update_config()
                        logger.info("✅ 全量同步完成，配置已更新")

                    # 检查是否需要清理缓存
                    if self._clean_cache_once:
                        def async_clean_cache():
                            try:
                                success = self.clean_cache()
                                if success:
                                    logger.debug("✅ 上传记录清理完成（内部状态）")
                                else:
                                    logger.error("❌ 上传记录清理失败")

                                # 清理完成后更新配置（现在使用原始文本配置，不会导致前端显示问题）
                                self._clean_cache_once = False
                                self.__update_config()
                            except Exception as e:
                                logger.error(f"❌ 清理缓存异常: {e}")

                        # 使用系统线程池替换原生threading
                        self._thread_pool.submit(async_clean_cache)
                        logger.info("📢 清理上传记录已启动")
            else:
                self._enabled = False
                logger.info("未提供配置，插件已禁用")

        except Exception as e:
            logger.error(f"❌ 插件初始化失败: {e}")
            import traceback
            logger.debug(f"初始化失败的详细堆栈信息: {traceback.format_exc()}")
            self._enabled = False

    def _load_config(self, config: dict):
        """加载配置参数"""
        self._enabled = config.get("enabled", False)
        self._mode = config.get("mode", "fast")
        self._onlyonce = config.get("onlyonce", False)

        # 加载网盘文件存在时的处理方式
        self._remote_file_action = config.get("remote_file_action", "跳过")
        if self._remote_file_action not in ["跳过", "覆盖"]:
            self._remote_file_action = "跳过"

        # 文件扩展名过滤器：白名单模式，只处理指定格式，为空则处理所有文件
        file_ext_filter = config.get("file_ext_filter", [])
        if isinstance(file_ext_filter, str):
            # 如果是字符串，按逗号分割，并确保扩展名以点号开头
            self._file_ext_filter = []
            for ext in file_ext_filter.split(","):
                ext = ext.strip()
                if ext:
                    # 如果扩展名不以点号开头，自动添加点号
                    if not ext.startswith("."):
                        ext = "." + ext
                    self._file_ext_filter.append(ext)
        else:
            # 如果是列表，直接使用，确保扩展名格式正确
            self._file_ext_filter = []
            for ext in file_ext_filter:
                ext = str(ext).strip()
                if ext:
                    if not ext.startswith("."):
                        ext = "." + ext
                    self._file_ext_filter.append(ext)

        self._delay_seconds = config.get("delay_seconds", 0)
        self._enable_schedule = config.get("enable_schedule", False)
        self._schedule_cron = config.get("schedule_cron", "0 2 * * *")
        self._enable_notify = config.get("enable_notify", False)

        # 加载保护目录配置
        protected_dirs_str = config.get("protected_dirs", "")
        if protected_dirs_str:
            self._protected_dirs = [
                dir.strip() for dir in protected_dirs_str.split(",") if dir.strip()]
        else:
            self._protected_dirs = []

        # 加载网盘文件检查配置
        self._check_remote_files = config.get("check_remote_files", False)

        # 保存原始文本配置
        self._raw_path_mappings = config.get("path_mappings", "")
        self._monitor_groups = self._parse_path_mappings(config)

        if not self._monitor_groups:
            self._load_legacy_config(config)
            # 如果从旧配置加载，也要更新原始文本配置
            if self._monitor_groups:
                self._raw_path_mappings = self._serialize_path_mappings()

    def _start_file_monitor(self):
        """启动文件监控服务"""
        if not self._monitor_groups:
            logger.error("❌ 未配置监控目录，文件监控功能无法启动")
            return

        if not self._init_storage_chain():
            logger.warning("⚠️ 存储链初始化失败，仅启动文件监控（无法上传文件）")

        for group in self._monitor_groups:
            try:
                monitor_path = Path(group["monitor_path"])
                if not monitor_path.exists():
                    try:
                        SystemUtils.mkdir(monitor_path)
                        logger.info(f"✅ 创建监控目录: {monitor_path}")
                    except Exception as e:
                        logger.error(f"❌ 创建监控目录失败: {e}")
                        continue

                if self._mode == "compatibility":
                    observer = PollingObserver(timeout=10)
                    mode_text = "兼容模式"
                else:
                    observer = Observer()
                    mode_text = "性能模式"

                self._observer.append(observer)
                observer.schedule(FileMonitorHandler(group["monitor_path"], self, group_config=group),
                                  path=group["monitor_path"], recursive=True)
                observer.daemon = True
                observer.start()

                logger.info(f"✅ 文件监控服务已启动 ({mode_text})")
                logger.info(
                    f"组配置 - 目标路径: {group['target_path']}, 监控目录: {group['monitor_path']}, 转移方式: {group['transfer_mode']}")

            except Exception as e:
                logger.error(f"❌ 启动文件监控失败: {e}")

    def _mark_plugin_operation(self, path_str: str):
        """标记路径为插件操作，避免触发文件系统事件循环"""
        if self._cache_backend:
            plugin_dir_key = f"{CACHE_PREFIX_PLUGIN_DIRS}{path_str}"
            self._cache_backend.set(plugin_dir_key, True, ttl=CACHE_TTL_PLUGIN_DIRS, region=getattr(
                self, '_cache_region', CACHE_REGION))

    def _init_storage_chain(self) -> bool:
        """初始化存储链实例"""
        try:
            self._storage_chain = StorageChain()
            test_result = self._test_storage_connection()
            if test_result:
                logger.info(f"✅ 存储链初始化成功")
                return True
            else:
                logger.error("❌ 存储连接测试失败")
                self._storage_chain = None
                return False

        except Exception as e:
            logger.error(f"❌ 存储链初始化失败: {e}")
            self._storage_chain = None
            return False

    def _test_storage_connection(self) -> bool:
        """测试存储连接是否可用"""
        try:
            if self._monitor_groups:
                storage_type = self._monitor_groups[0]["storage_type"]
                root_item = self._storage_chain.get_file_item(
                    storage=storage_type,
                    path=Path("/")
                )

                if root_item:
                    logger.debug(f"存储连接测试成功: OpenList")
                    return True
                else:
                    logger.warning(f"存储连接测试返回空结果: OpenList")
                    return False
            else:
                return False

        except Exception as e:
            logger.error(f"存储连接测试异常: {e}")
            return False

    def __update_config(self):
        """更新插件配置到数据库"""
        config_data = {
            "enabled": self._enabled,
            "enable_notify": self._enable_notify,
            "path_mappings": self._raw_path_mappings,  # 使用原始文本配置，避免运行态数据结构污染
            "mode": self._mode,
            "onlyonce": self._onlyonce,
            "protected_dirs": ",".join(self._protected_dirs),  # 新增：保护目录配置
            "file_ext_filter": self._file_ext_filter,
            "delay_seconds": self._delay_seconds,
            "enable_schedule": self._enable_schedule,
            "schedule_cron": self._schedule_cron,
            "remote_file_action": self._remote_file_action  # 新增：网盘文件处理方式配置
        }

        try:
            self.update_config(config_data)
        except Exception as e:
            logger.error(f"❌ 保存配置失败: {e}")

    def handle_file(self, file_path: str, mon_path: str, group_config: dict = None, retry_count: int = 0, batch_processing: bool = False):
        """处理检测到新文件/目录"""
        file_path = Path(file_path)

        if not group_config and self._monitor_groups:
            for group in self._monitor_groups:
                if group["monitor_path"] == mon_path:
                    group_config = group
                    break

        if not group_config and self._monitor_groups:
            group_config = self._monitor_groups[0]

        if file_path.is_dir():
            if "::TMPNAME:" in file_path.name:
                return
            self.schedule_directory_process(
                file_path, mon_path, group_config, is_plugin_operation=False)
            return

        # 检查文件是否应该被处理
        if not self._should_process_file(file_path, mon_path, retry_count):
            if "::TMPNAME:" in file_path.name:
                return

            if retry_count < 3:
                def delayed_retry():
                    time.sleep(5)
                    self.handle_file(str(file_path), mon_path,
                                     group_config, retry_count + 1)
                threading.Thread(target=delayed_retry, daemon=True).start()
            return

        if not batch_processing:
            logger.info(f"📄 检测到新文件: {file_path.name}")

        if not self._enabled:
            logger.info(f"📝 插件未启用，跳过文件处理: {file_path.name}")
            return

        if not self._ensure_directory_structure(file_path, group_config, mon_path):
            logger.error(f"❌ 创建目录结构失败，无法上传文件: {file_path.name}")
            return

        success = self._upload_with_retry(file_path, group_config, mon_path)

        if success:
            if not batch_processing:
                logger.info(f"✅ 文件上传成功: {file_path.name}")
                self._mark_plugin_operation(str(file_path.parent.resolve()))
                transfer_mode = group_config["transfer_mode"] if group_config else "移动"
                if transfer_mode == "移动":
                    self._safe_delete_file(file_path, mon_path)
                    # 异步清理空目录

                    def async_cleanup():
                        time.sleep(0.5)
                        self._cleanup_empty_directories()
                    self._thread_pool.submit(async_cleanup)
                else:
                    logger.info(f"📝 文件保留在本地（复制模式）: {file_path.name}")

                if self._enable_notify:
                    self._send_notification([file_path], status="success")

            return True
        else:
            logger.error(f"❌ 文件上传失败: {file_path.name}")

            if self._enable_notify:
                self._send_notification([file_path], status="failed")

            return False

    def _should_process_file(self, file_path: Path, mon_path: str, retry_count: int = 0) -> bool:
        """判断文件是否应该被处理"""
        try:
            if not file_path.exists():
                logger.warning(f"文件不存在，跳过处理: {file_path}")
                return False

            if self._is_temp_file(file_path):
                return False

            if not self._passes_extension_filter(file_path):
                return False

            if retry_count == 0 and not self._is_file_stable(file_path):
                logger.debug(f"文件不稳定，稍后重试: {file_path.name}")
                return False

            logger.debug(f"文件处理通过: {file_path.name}")
            return True

        except Exception as e:
            logger.error(f"文件过滤检查异常: {e}")
            return False

    def _is_temp_file(self, file_path: Path) -> bool:
        """检查是否是临时文件"""
        file_name = file_path.name.lower()

        if any(marker in file_name for marker in TEMP_FILE_MARKERS):
            logger.debug(f"跳过临时文件: {file_path.name}")
            return True

        file_ext = file_path.suffix.lower()
        if file_ext in TEMP_EXTENSIONS:
            logger.debug(f"跳过临时文件扩展名: {file_path.name} ({file_ext})")
            return True

        return False

    def _passes_extension_filter(self, file_path: Path) -> bool:
        """
        检查文件扩展名是否通过过滤
        白名单模式：只处理过滤器中的扩展名，为空则处理所有文件
        """
        # 如果过滤器为空，处理所有文件
        if not self._file_ext_filter:
            return True

        file_ext = file_path.suffix.lower()
        if not file_ext:
            return len(self._file_ext_filter) == 0 or '' in [ext.lower() for ext in self._file_ext_filter]

        # 白名单模式：文件扩展名必须在过滤器列表中才处理
        # 统一处理点号：确保文件扩展名和过滤器中的扩展名都以点号开头
        normalized_file_ext = file_ext if file_ext.startswith(
            ".") else "." + file_ext
        normalized_filter = [ext if ext.startswith(
            ".") else "." + ext for ext in self._file_ext_filter]

        if normalized_file_ext not in [ext.lower() for ext in normalized_filter]:
            logger.debug(
                f"文件扩展名被过滤（只处理 {self._file_ext_filter} 格式）: {file_path.name} ({file_ext})")
            return False

        return True

    def _is_file_stable(self, file_path: Path, check_interval: float = 1.0) -> bool:
        """检查文件是否稳定（不再被写入）"""
        try:
            stat1 = file_path.stat()
            size1 = stat1.st_size

            # 如果文件大小为0，可能还在写入中
            if size1 == 0:
                return False

            # 等待一段时间后再次检查
            time.sleep(check_interval)

            stat2 = file_path.stat()
            size2 = stat2.st_size

            # 文件大小相同且不为0，认为文件稳定
            return size1 == size2 and size1 > 0

        except Exception as e:
            logger.debug(f"文件稳定性检查异常: {e}")
            return False

    def _upload_with_retry(self, file_path: Path, group_config: dict = None, monitor_path: str = None) -> bool:
        """使用智能重试机制上传文件"""
        max_attempts = 4
        last_exception = None

        for attempt in range(max_attempts):
            try:
                if attempt > 0:
                    retry_delay = self._calculate_retry_delay(
                        attempt, last_exception)
                    logger.info(
                        f"🔄 重试上传 ({attempt}/{max_attempts-1}): {file_path.name}")
                    time.sleep(retry_delay)

                success = self._upload_to_storage(
                    file_path, group_config, monitor_path)
                if success:
                    return True

                logger.warning(f"上传失败，准备重试: {file_path.name}")

            except Exception as e:
                last_exception = e
                logger.error(f"上传过程异常 (尝试 {attempt + 1}): {e}")

        logger.error(f"❌ 所有重试次数已用完，上传失败: {file_path.name}")
        return False

    def _calculate_retry_delay(self, attempt: int, last_exception: Exception = None) -> int:
        """计算智能重试延迟时间"""
        base_delay = 5

        if last_exception and self._is_network_error(last_exception):
            base_delay = 10
        elif last_exception and self._is_storage_error(last_exception):
            base_delay = 7

        delay = base_delay + (attempt - 1) * base_delay
        return min(delay, 60)

    def _is_network_error(self, error: Exception) -> bool:
        """判断是否为网络相关错误"""
        error_str = str(error).lower()
        return any(keyword in error_str for keyword in NETWORK_ERROR_KEYWORDS)

    def _is_storage_error(self, error: Exception) -> bool:
        """判断是否为存储相关错误"""
        error_str = str(error).lower()
        storage_keywords = {'storage', 'disk',
                            'space', 'quota', 'full', 'permission'}
        return any(keyword in error_str for keyword in storage_keywords)

    def _upload_to_storage(self, file_path: Path, group_config: dict = None, monitor_path: str = None) -> bool:
        """使用存储链上传文件到指定存储"""
        try:
            if not self._storage_chain:
                logger.error("❌ 存储链未初始化 - 无法上传文件")
                return False

            if not group_config and self._monitor_groups:
                group_config = self._monitor_groups[0]
            elif not group_config:
                logger.error("❌ 未配置监控组 - 无法上传文件")
                return False

            storage_type = group_config["storage_type"]
            target_path = group_config["target_path"]

            if not file_path.exists():
                logger.error(f"❌ 文件不存在: {file_path}")
                return False

            if monitor_path and file_path.is_relative_to(monitor_path):
                relative_path = file_path.relative_to(monitor_path)
                if relative_path.parent != Path('.'):
                    remote_dir_path = str(
                        Path(target_path) / relative_path.parent)
                else:
                    remote_dir_path = target_path
            else:
                remote_dir_path = target_path

            file_size = file_path.stat().st_size
            logger.debug(f"📤 开始上传文件: {file_path.name} ({file_size} bytes)")
            logger.debug(f"目标存储: OpenList -> {remote_dir_path}")

            target_dir = self._ensure_directory_exists(
                storage_type, remote_dir_path)
            if not target_dir:
                logger.error(
                    f"❌ 无法获取或创建目标目录: {storage_type}:{remote_dir_path}")
                return False

            result = self._storage_chain.upload_file(
                fileitem=target_dir,
                path=file_path,
                new_name=file_path.name
            )

            if result:
                logger.info(
                    f"✅ 上传成功 - 文件已发送到 OpenList:{remote_dir_path}/{file_path.name}")
                # 保存成功记录
                self._save_upload_record(
                    filename=file_path.name,
                    status="成功",
                    file_path=str(file_path),
                    target_path=remote_dir_path
                )
                return True
            else:
                logger.error(f"❌ 上传失败 - 存储链返回空结果: {file_path.name}")
                # 保存失败记录
                self._save_upload_record(
                    filename=file_path.name,
                    status="失败",
                    file_path=str(file_path),
                    target_path=remote_dir_path
                )
                return False

        except Exception as e:
            logger.error(f"❌ 上传过程异常 - 文件: {file_path.name}, 错误: {e}")
            import traceback
            logger.debug(f"上传异常的详细堆栈信息: {traceback.format_exc()}")
            return False

    def _create_target_directory(self, storage_type: str, target_path: str) -> bool:
        """创建目标目录"""
        try:
            path_parts = Path(target_path).parts
            current_path = Path("/")
            parent_dir = None

            for part in path_parts:
                if not part or part == "/":
                    continue

                current_path = current_path / part

                dir_item = self._storage_chain.get_file_item(
                    storage=storage_type,
                    path=current_path
                )

                if not dir_item:
                    if parent_dir is None:
                        parent_dir = self._storage_chain.get_file_item(
                            storage=storage_type,
                            path=current_path.parent
                        )

                    if parent_dir is None and str(current_path.parent) == "/":
                        root_dir = self._storage_chain.get_file_item(
                            storage=storage_type,
                            path=Path("/")
                        )

                        if root_dir:
                            created_dir = self._storage_chain.create_folder(
                                fileitem=root_dir,
                                name=part
                            )
                        else:
                            created_dir = self._storage_chain.create_folder(
                                fileitem=None,
                                name=part
                            )
                    else:
                        created_dir = self._storage_chain.create_folder(
                            fileitem=parent_dir,
                            name=part
                        )

                    if created_dir:
                        logger.debug(f"网盘目录创建成功: OpenList:{current_path}")
                        parent_dir = created_dir
                    else:
                        logger.error(f"❌ 创建目录失败: OpenList:{current_path}")
                        return False
                else:
                    parent_dir = dir_item

            return True

        except Exception as e:
            logger.error(f"❌ 创建目标目录异常: {e}")
            return False

    def _ensure_directory_exists(self, storage_type: str, remote_dir_path: str):
        """确保目录存在，如果不存在则创建"""
        try:
            dir_item = self._storage_chain.get_file_item(
                storage=storage_type,
                path=Path(remote_dir_path)
            )

            if not dir_item:
                logger.debug(f"网盘目录不存在: OpenList:{remote_dir_path}")
                if self._create_target_directory(storage_type, remote_dir_path):
                    # 创建成功后重新获取目录项
                    dir_item = self._storage_chain.get_file_item(
                        storage=storage_type,
                        path=Path(remote_dir_path)
                    )
                    return dir_item
                else:
                    return None

            return dir_item

        except Exception as e:
            logger.error(f"❌ 确保目录存在异常: {e}")
            return None

    def _ensure_directory_structure(self, file_path: Path, group_config: dict, monitor_path: str) -> bool:
        """确保文件对应的目录结构存在"""
        try:
            storage_type = group_config["storage_type"]
            target_path = group_config["target_path"]

            relative_path = file_path.relative_to(Path(monitor_path))

            if relative_path.parent != Path('.'):
                remote_dir_path = str(Path(target_path) / relative_path.parent)

                dir_item = self._ensure_directory_exists(
                    storage_type, remote_dir_path)
                if not dir_item:
                    logger.error(f"❌ 创建目录结构失败: {remote_dir_path}")
                    return False
                else:
                    logger.debug(f"📁 目录结构已存在: {remote_dir_path}")

            return True

        except Exception as e:
            logger.error(f"❌ 确保目录结构异常: {e}")
            return False

    def _batch_process_files_in_directory(self, dir_path: Path, group_config: dict, monitor_path: str):
        """批量处理同一目录下的所有文件"""
        valid_files = []

        try:
            if not dir_path.exists():
                logger.debug(f"目录不存在，跳过处理: {dir_path}")
                return

            valid_files, skipped_files = self._filter_valid_files(
                dir_path, monitor_path, group_config)

            # 发送跳过文件的通知
            if skipped_files and self._enable_notify:
                self._send_notification(skipped_files, status="skipped")

            if not valid_files and not skipped_files:
                logger.info(f"ℹ️ 目录 {dir_path.name} 中没有符合条件的未处理文件")
                return

            if valid_files:
                logger.info(
                    f"📊 待处理目录 {dir_path.name} 中的 {len(valid_files)} 个文件")

                if not self._ensure_directory_structure(valid_files[0], group_config, monitor_path):
                    logger.error(f"❌ 创建目录结构失败，无法批量处理文件")
                    return

                success_files, failed_files = self._process_files_concurrently(
                    valid_files, group_config, monitor_path
                )

                self._handle_batch_results(
                    dir_path, group_config, success_files, failed_files)

            if skipped_files:
                logger.info(
                    f"⚠️ 目录 {dir_path.name} 中跳过 {len(skipped_files)} 个文件（网盘已存在）")

        except Exception as e:
            logger.error(f"❌ 批量处理目录异常: {dir_path.name}, 错误: {e}")
        finally:
            if valid_files:
                self._cleanup_batch_processing_state(valid_files)

    def _filter_valid_files(self, dir_path: Path, monitor_path: str, group_config: dict = None) -> tuple:
        """过滤需要处理的文件，返回(有效文件列表, 跳过文件列表)"""
        # 使用SystemUtils获取目录下所有文件
        all_files = SystemUtils.list_sub_files(
            dir_path, extensions=self._file_ext_filter)
        valid_files = []
        skipped_files = []

        for file_path in all_files:
            if not self._should_process_file(file_path, monitor_path, 0):
                continue

            # 检查网盘上是否存在同名文件
            if group_config and self._remote_file_action in ["跳过", "覆盖"]:
                remote_file_item = self._get_remote_file_item(
                    file_path, group_config, monitor_path)
                if remote_file_item:
                    if self._remote_file_action == "跳过":
                        logger.debug(f"网盘已存在同名文件，跳过上传: {file_path.name}")
                        skipped_files.append(file_path)
                        continue
                    # 如果是"覆盖"模式，删除远程文件后继续处理
                    else:
                        logger.info(f"网盘已存在同名文件，准备覆盖: {file_path.name}")
                        self._delete_remote_file(remote_file_item)

            valid_files.append(file_path)

        return valid_files, skipped_files

    def _process_files_concurrently(self, files: List[Path], group_config: dict, monitor_path: str) -> tuple:
        """并发处理文件"""
        success_files = []
        failed_files = []

        futures = {}
        for file_path in files:
            future = self._thread_pool.submit(
                self._upload_with_retry,
                file_path,
                group_config,
                monitor_path
            )
            futures[future] = file_path

        for future in as_completed(futures):
            file_path = futures[future]
            try:
                success = future.result()

                if success:
                    success_files.append(file_path)
                else:
                    failed_files.append(file_path)
            except Exception as e:
                logger.error(f"文件处理异常: {file_path.name}, {e}")
                failed_files.append(file_path)

        return success_files, failed_files

    def _handle_batch_results(self, dir_path: Path, group_config: dict,
                              success_files: List[Path], failed_files: List[Path]):
        """处理批量处理结果"""
        if success_files:
            self._mark_plugin_operation(str(dir_path.resolve()))

            if self._enable_notify:
                self._send_notification(success_files, status="success")

            transfer_mode = group_config.get("transfer_mode", "移动")
            if transfer_mode == "移动":
                self._delete_local_files(
                    success_files, group_config["monitor_path"])
                # 异步清理空目录

                def async_cleanup():
                    time.sleep(0.5)
                    self._cleanup_empty_directories()
                self._thread_pool.submit(async_cleanup)
            else:
                logger.info(f"📝 文件保留在本地（复制模式）: {dir_path.name}")

        if failed_files:
            failed_names = ", ".join([f.name for f in failed_files[:3]])
            if len(failed_files) > 3:
                failed_names += f" 等 {len(failed_files)} 个文件"
            logger.error(f"❌ 目录 {dir_path.name} 批量上传失败: {failed_names}")

            if self._enable_notify:
                self._send_notification(failed_files, status="failed")

        if success_files or failed_files:
            logger.info(
                f"🎉 目录 {dir_path.name} 批量处理完成: 成功 {len(success_files)} 个，失败 {len(failed_files)} 个")

    def _delete_local_files(self, files: List[Path], monitor_path: str):
        """删除本地文件"""
        deleted_count = 0
        for file_path in files:
            if file_path.exists():
                file_path_str = str(file_path.resolve())

                # 添加到已删除文件记录
                if monitor_path not in self._recently_deleted_files:
                    self._recently_deleted_files[monitor_path] = set()
                self._recently_deleted_files[monitor_path].add(file_path_str)

                try:
                    file_path.unlink()
                    deleted_count += 1
                    time.sleep(0.05)
                except Exception as e:
                    logger.error(f"❌ 删除文件失败: {file_path.name}, {e}")

        if deleted_count > 0:
            logger.info(f"🗑️ 清理完成 - 已删除 {deleted_count} 个本地文件")
            time.sleep(0.5)

    def _cleanup_batch_processing_state(self, files: List[Path]):
        """清理批量处理状态（已废弃，不再使用缓存）"""
        # 不再使用缓存，直接返回
        pass

    def _get_remote_file_item(self, file_path: Path, group_config: dict, monitor_path: str):
        """
        获取网盘上的同名文件项
        :param file_path: 本地文件路径
        :param group_config: 监控组配置
        :param monitor_path: 监控路径
        :return: 远程文件项对象，不存在则返回None
        """
        try:
            if not self._storage_chain:
                logger.warning("存储链未初始化，无法检查网盘文件")
                return None

            storage_type = group_config["storage_type"]
            target_path = group_config["target_path"]

            # 计算相对路径和远程目标路径
            if monitor_path and file_path.is_relative_to(monitor_path):
                relative_path = file_path.relative_to(monitor_path)
                if relative_path.parent != Path('.'):
                    remote_dir_path = str(
                        Path(target_path) / relative_path.parent)
                else:
                    remote_dir_path = target_path
            else:
                remote_dir_path = target_path

            # 获取远程目录项
            remote_dir_item = self._storage_chain.get_file_item(
                storage=storage_type,
                path=Path(remote_dir_path)
            )

            if not remote_dir_item:
                logger.debug(f"远程目录不存在: {remote_dir_path}")
                return None

            # 列出远程目录中的所有文件
            remote_files = self._storage_chain.list_files(
                fileitem=remote_dir_item,
                recursion=False  # 只检查当前目录，不递归
            )

            if not remote_files:
                logger.debug(f"远程目录为空: {remote_dir_path}")
                return None

            # 查找同名文件
            file_name = file_path.name
            for remote_file in remote_files:
                if remote_file.type == "file" and remote_file.name == file_name:
                    logger.debug(f"网盘已存在同名文件: {file_name} ({remote_dir_path})")
                    return remote_file

            return None

        except Exception as e:
            logger.error(f"获取网盘文件项异常: {file_path.name}, {e}")
            return None

    def _delete_remote_file(self, file_item) -> bool:
        """
        删除网盘上的文件
        :param file_item: 文件项对象
        :return: 删除是否成功
        """
        try:
            if not file_item:
                logger.warning("文件项为空，无法删除")
                return False

            logger.info(f"正在删除网盘文件: {file_item.name}")

            success = self._storage_chain.delete_file(file_item)

            if success:
                logger.info(f"✅ 网盘文件删除成功: {file_item.name}")
                return True
            else:
                logger.error(f"❌ 网盘文件删除失败: {file_item.name}")
                return False

        except Exception as e:
            logger.error(f"删除网盘文件异常: {file_item.name}, {e}")
            return False

    def _safe_delete_file(self, file_path: Path, monitor_path: str):
        """安全删除本地文件"""
        try:
            if file_path.exists():
                file_path_str = str(file_path.resolve())
                deleted_key = f"{CACHE_PREFIX_DELETED}{file_path_str}"
                if self._cache_backend:
                    self._cache_backend.set(deleted_key, True, ttl=CACHE_TTL_DELETED, region=getattr(
                        self, '_cache_region', CACHE_REGION))

                self._mark_plugin_operation(str(file_path.parent.resolve()))

                file_path.unlink()
                logger.debug(f"🗑️ 清理成功 - 已删除本地文件: {file_path.name}")
            else:
                logger.debug(f"文件不存在，无需删除: {file_path}")

        except Exception as e:
            logger.error(f"❌ 删除本地文件失败: {file_path.name}, 错误: {e}")

    def _cleanup_empty_directories(self):
        """立即清理所有监控目录中的空目录（移动模式下）"""
        try:
            if self._mode == "compatibility":
                logger.debug("Polling模式下跳过空目录清理，避免事件循环")
                return

            deleted_count = 0
            for group in self._monitor_groups:
                if group["transfer_mode"] != "移动":
                    continue

                monitor_path = Path(group["monitor_path"])
                if not monitor_path.exists():
                    continue

                self._mark_plugin_operation(str(monitor_path.resolve()))

                # 使用SystemUtils清理空目录
                logger.info(f"🔍 检查空目录: {monitor_path.name}")

                # 使用SystemUtils的clear方法清理空目录
                SystemUtils.clear(monitor_path, days=0)

                # 使用递归方式实时清理空目录，避免缓存问题
                def cleanup_empty_dirs_recursive(current_dir):
                    if not current_dir.exists() or not current_dir.is_dir():
                        return 0

                    local_deleted = 0

                    # 检查是否在保护目录列表中
                    current_dir_str = str(current_dir.resolve())
                    is_protected = False

                    # 检查当前目录是否在保护目录列表中
                    for protected_dir in self._protected_dirs:
                        if protected_dir and current_dir_str.endswith(protected_dir):
                            is_protected = True
                            logger.debug(f"🔒 保护目录，跳过删除: {current_dir.name}")
                            break

                    # 如果目录被保护，只清理子目录，不删除本身
                    if is_protected:
                        for subdir in SystemUtils.list_sub_directory(current_dir):
                            if "::TMPNAME:" in subdir.name:
                                continue
                            local_deleted += cleanup_empty_dirs_recursive(
                                subdir)
                        return local_deleted

                    # 先递归清理子目录
                    for subdir in SystemUtils.list_sub_directory(current_dir):
                        if "::TMPNAME:" in subdir.name:
                            continue
                        local_deleted += cleanup_empty_dirs_recursive(subdir)

                    # 检查当前目录是否为空
                    try:
                        # 使用SystemUtils检查目录是否为空
                        if not SystemUtils.list_sub_file(current_dir):
                            # 再次确认目录为空
                            time.sleep(0.1)
                            if not SystemUtils.list_sub_file(current_dir):
                                # 添加到已删除目录记录
                                if monitor_path not in self._recently_deleted_files:
                                    self._recently_deleted_files[monitor_path] = set(
                                    )
                                self._recently_deleted_files[monitor_path].add(
                                    str(current_dir.resolve()))

                                self._mark_plugin_operation(
                                    str(current_dir.resolve()))

                                current_dir.rmdir()
                                local_deleted += 1
                                logger.debug(
                                    f"🗑️ 清理成功 - 已删除空目录: {current_dir.relative_to(monitor_path)}")
                                time.sleep(0.5)
                    except Exception as e:
                        logger.debug(f"检查目录时遇到异常，跳过: {current_dir}, {e}")

                    return local_deleted

                # 开始递归清理
                deleted_count += cleanup_empty_dirs_recursive(monitor_path)

            if deleted_count > 0:
                logger.info(f"✅ 清理完成，共删除 {deleted_count} 个空目录")
            else:
                logger.info("ℹ️ 没有发现需要清理的空目录")

        except Exception as e:
            logger.error(f"❌ 清理空目录异常: {e}")

    def _get_current_time(self) -> str:
        """获取当前时间字符串"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def sync_all(self):
        """立即同步所有文件 - 全量同步功能"""
        if not self._monitor_groups:
            logger.error("❌ 未配置监控目录，无法进行全量同步")
            return

        total_success = 0
        total_files = 0
        start_time = time.time()

        for group in self._monitor_groups:
            monitor_path = Path(group["monitor_path"])
            if not monitor_path.exists():
                logger.error(f"❌ 监控目录不存在，跳过: {monitor_path}")
                continue

            logger.info(f"🚀 开始全量同步监控目录: {monitor_path}")
            logger.info(f"目标存储: OpenList -> {group['target_path']}")

            # 使用SystemUtils获取所有文件，包括扩展名过滤
            # 如果扩展名过滤器为空，则返回所有文件
            extensions = self._file_ext_filter if self._file_ext_filter else None
            all_files = SystemUtils.list_files(
                monitor_path, extensions=extensions)
            # 直接处理所有文件，不检查缓存
            file_count = len(all_files)
            total_files += file_count

            if file_count == 0:
                logger.info("ℹ️ 监控目录中没有文件需要处理")
                # 即使目录为空也发送通知
                if self._enable_notify:
                    self._send_empty_directory_notification(monitor_path)
                continue

            logger.info(f"📊 开始处理 {file_count} 个文件")

            success_count = self._process_files_in_batch(
                all_files, group, group["monitor_path"])
            total_success += success_count

        total_time = time.time() - start_time
        logger.info(
            f"✅ 所有监控目录全量同步完成 - 成功 {total_success}/{total_files} 个文件, 耗时 {total_time:.1f}秒")

        if total_success > 0:
            logger.info("🧹 开始清理空目录...")
            # 异步清理空目录

            def async_cleanup():
                self._cleanup_empty_directories()
            self._thread_pool.submit(async_cleanup)

    def _calculate_dynamic_batch_size(self, total_files: int) -> int:
        """根据系统负载和文件数量动态计算批量大小"""
        base_batch_size = 10

        # 根据文件数量调整批量大小
        if total_files <= 10:
            return min(5, total_files)  # 小文件数量使用小批量
        elif total_files <= 50:
            return 10  # 中等文件数量使用默认批量
        elif total_files <= 200:
            return 20  # 大文件数量使用较大批量
        else:
            return 30  # 超大文件数量使用最大批量

        # TODO: 未来可以添加系统负载检测
        # 根据CPU使用率、内存使用率等进一步优化

        return base_batch_size

    def _process_files_in_batch(self, files: List[Path], group: dict, monitor_path: str) -> int:
        """批量处理文件"""
        success_count = 0
        batch_size = self._calculate_dynamic_batch_size(len(files))

        logger.info(f"📊 动态批量大小: {batch_size} (总文件数: {len(files)})")

        for i in range(0, len(files), batch_size):
            batch_files = files[i:i + batch_size]

            futures = {}
            for file_path in batch_files:
                future = self._thread_pool.submit(
                    self._upload_with_retry,
                    file_path,
                    group,
                    monitor_path
                )
                futures[future] = file_path

            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                        if group.get("transfer_mode", "移动") == "移动":
                            self._safe_delete_file(file_path, monitor_path)
                except Exception as e:
                    logger.error(f"❌ 文件处理失败: {file_path.name}, {e}")

            progress = min(i + batch_size, len(files))
            logger.debug(
                f"📤 处理进度: {progress}/{len(files)} ({(progress/len(files))*100:.1f}%)")

        return success_count

    def schedule_directory_process(self, dir_path: Path, mon_path: str, group_config: dict, is_plugin_operation: bool = False):
        """目录级调度器 - 统一处理目录和文件事件"""
        if not hasattr(self, "_processing_files"):
            self._processing_files = {}
        if not hasattr(self, "_dir_processing"):
            self._dir_processing = {}
            self._dir_lock = threading.Lock()

        if is_plugin_operation:
            logger.debug(f"跳过插件自身操作触发的目录处理: {dir_path}")
            return

        with self._dir_lock:
            if mon_path not in self._dir_processing:
                self._dir_processing[mon_path] = set()
            if dir_path in self._dir_processing[mon_path]:
                return
            self._dir_processing[mon_path].add(dir_path)

        def delayed():
            time.sleep(2)
            try:
                if not dir_path.exists():
                    logger.debug(f"目录不存在，跳过处理: {dir_path}")
                    return

                try:
                    # 使用SystemUtils检查目录是否为空
                    if not SystemUtils.list_sub_file(dir_path):
                        logger.debug(f"目录为空，不触发处理: {dir_path}")
                        # 即使目录为空也发送通知
                        if self._enable_notify:
                            self._send_empty_directory_notification(dir_path)
                        return
                except Exception as e:
                    logger.debug(f"检查目录内容失败，继续处理: {dir_path}, {e}")

                logger.debug(f"开始处理目录: {dir_path}")
                self._batch_process_files_in_directory(
                    dir_path, group_config, mon_path)

                time.sleep(0.5)

            except Exception as e:
                logger.error(f"处理目录异常: {dir_path}, 错误: {e}")
            finally:
                with self._dir_lock:
                    if mon_path in self._dir_processing:
                        self._dir_processing[mon_path].discard(dir_path)

        threading.Thread(target=delayed, daemon=True).start()

    def _start_cleanup_task(self):
        """启动清理已处理文件缓存的定时任务"""
        if not hasattr(self, '_processed_files'):
            self._processed_files = {}
        if not hasattr(self, '_recently_deleted_files'):
            self._recently_deleted_files = {}
        if not hasattr(self, '_dir_processing'):
            self._dir_processing = {}
            self._dir_lock = threading.Lock()

        def cleanup_task():
            while True:
                try:
                    time.sleep(1800)
                    # 清理过期的内存缓存
                    self._cleanup_memory_cache()
                except Exception as e:
                    logger.error(f"清理任务异常: {e}")

        cleanup_thread = threading.Thread(
            target=cleanup_task, daemon=True, name="CleanupTask")
        cleanup_thread.start()
        logger.debug("已启动文件处理缓存清理任务")

    def _cleanup_memory_cache(self):
        """清理内存缓存（已废弃，不再使用缓存）"""
        # 不再使用缓存，直接返回
        pass

    def clean_cache(self):
        """清理所有缓存记录（已废弃，不再使用缓存）"""
        # 不再使用缓存，直接返回成功
        logger.info("✅ 已禁用缓存功能，所有文件每次都会作为新文件处理")
        return True

    def get_state(self) -> bool:
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """获取插件配置表单"""
        unified_settings = [
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
                                    "hint": "启用后插件开始工作"
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
                                    "model": "enable_notify",
                                    "label": "发送通知",
                                    "hint": "功能处理完成后发送系统通知"
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
                                    "model": "onlyonce",
                                    "label": "立即运行一次",
                                    "hint": "保存配置后立即全量同步一次"
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
                                "component": "VTextField",
                                "props": {
                                    "model": "delay_seconds",
                                    "label": "延迟处理秒数",
                                    "type": "number",
                                    "placeholder": "0",
                                    "hint": "文件创建后延迟多少秒再处理"
                                }
                            }
                        ]
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                'component': 'VSelect',
                                'props': {
                                    'model': 'remote_file_action',
                                    'label': '网盘文件存在时',
                                    'hint': '当网盘已存在同名文件时的处理方式',
                                    'items': [
                                        {'title': '跳过', 'value': '跳过'},
                                        {'title': '覆盖', 'value': '覆盖'}
                                    ]
                                }
                            }
                        ]
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "file_ext_filter",
                                    "label": "文件扩展名过滤",
                                    "placeholder": ".mp4,.mkv,.avi",
                                    "hint": "只处理指定扩展名，为空则处理所有文件。例：输入mp3只上传MP3文件，留空上传所有文件"
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
                                    "model": "enable_schedule",
                                    "label": "启用定时处理",
                                    "hint": "启用后目录实时监控失效，按定时任务处理"
                                }
                            }
                        ]
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VCronField",
                                "props": {
                                    "model": "schedule_cron",
                                    "label": "定时表达式"
                                }
                            }
                        ]
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                'component': 'VSelect',
                                'props': {
                                    'model': 'mode',
                                    'label': '监控模式',
                                    'hint': '选择文件监控方式',
                                    'items': [
                                        {'title': '性能模式', 'value': 'fast'},
                                        {'title': '兼容模式', 'value': 'compatibility'}
                                    ]
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
                                "component": "VTextarea",
                                "props": {
                                    "model": "path_mappings",
                                    "label": "路径映射配置",
                                    "rows": 6,
                                    "placeholder": (
                                        "# 本地目录:OpenList目录#方式"
                                    ),
                                    "hint": (
                                        "支持多组映射，每行一个配置"
                                    )
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
                                    "text": "示例：本地路径1:OpenList路径1#移动 或 本地路径2:OpenList路径2#复制"
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
                                "component": "VTextarea",
                                "props": {
                                    "model": "protected_dirs",
                                    "label": "保护目录列表",
                                    "rows": 3,
                                    "placeholder": "/音乐/整理,/电影/下载",
                                    "hint": "逗号分隔的目录路径列表，这些目录不会被删除，即使为空"
                                }
                            }
                        ]
                    }
                ]
            }
        ]

        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined"},
                "content": [
                    {
                        "component": "VCardText",
                        "content": unified_settings,
                    },
                ],
            }
        ], {
            "enabled": self._enabled,
            "enable_notify": self._enable_notify,
            "path_mappings": self._monitor_groups if self._monitor_groups else [
                {
                    "monitor_path": "",
                    "target_path": "",
                    "transfer_mode": "移动",
                    "storage_type": "alist"
                }
            ],
            "mode": self._mode,
            "onlyonce": self._onlyonce,
            "file_ext_filter": self._file_ext_filter,
            "delay_seconds": self._delay_seconds,
            "enable_schedule": self._enable_schedule,
            "schedule_cron": self._schedule_cron,
            # 新增
            "protected_dirs": ",".join(self._protected_dirs) if self._protected_dirs else "",
            "remote_file_action": self._remote_file_action  # 新增
        }

    def get_page(self) -> List[dict]:
        """
        插件数据页面 - 上传记录列表和API调用
        """
        from app.core.config import settings

        # 获取上传记录
        records = self._get_upload_records()

        # 统计总数和失败数
        total_count = len(records)
        failed_count = sum(1 for rec in records if rec.get("status") == "失败")

        # 格式化记录数据，添加状态颜色样式
        formatted_records = []
        for index, record in enumerate(records, start=1):
            # 根据文件扩展名判断文件类型
            filename = record.get("filename", "")
            file_type = self._get_file_type(filename)
            status = record.get("status", "")
            file_size = record.get("file_size", "")

            # 获取上传路径（优先使用已保存的upload_path，如果没有则格式化target_path）
            upload_path = record.get("upload_path", "")
            if not upload_path:
                upload_path = self._format_upload_path(
                    record.get("target_path", ""))

            # 根据状态设置文字颜色
            if status == "失败":
                status_class = "text-error"
            elif status == "成功":
                status_class = "text-success"
            else:
                status_class = "text-white"

            formatted_records.append({
                "index": index,
                "filename": filename,
                "file_type": file_type,
                "status": status,
                "status_class": status_class,
                "upload_path": upload_path,
                "file_size": file_size,
                "timestamp": record.get("timestamp", ""),
                "actions": "查看详情"
            })

        plugin_prefix = self.__class__.__name__

        return [
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined"
                },
                "content": [
                    {
                        "component": "VCardTitle",
                        "content": "操作按钮"
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {
                                            "cols": 12,
                                            "md": 3
                                        },
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "variant": "tonal",
                                                    "color": "success",
                                                    "block": True
                                                },
                                                "text": f"总共{total_count}个 失败{failed_count}个"
                                            }
                                        ]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {
                                            "cols": 12,
                                            "md": 3
                                        },
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "variant": "tonal",
                                                    "color": "info",
                                                    "block": True
                                                },
                                                "text": "立即同步",
                                                "events": {
                                                    "click": {
                                                        "type": "request",
                                                        "api": f"plugin/{plugin_prefix}/optsync",
                                                        "method": "POST",
                                                        "params": {"apikey": settings.API_TOKEN},
                                                        "confirm": {
                                                            "title": "确认操作",
                                                            "text": "确定要手动触发全量同步吗？"
                                                        },
                                                        "success": "手动触发全量同步成功",
                                                        "fail": "手动触发全量同步失败"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {
                                            "cols": 12,
                                            "md": 3
                                        },
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "variant": "tonal",
                                                    "color": "warning",
                                                    "block": True
                                                },
                                                "text": "删除失败",
                                                "events": {
                                                    "click": {
                                                        "type": "request",
                                                        "api": f"plugin/{plugin_prefix}/delete_failed",
                                                        "method": "POST",
                                                        "params": {"apikey": settings.API_TOKEN},
                                                        "confirm": {
                                                            "title": "确认操作",
                                                            "text": "确定要删除所有失败的上传记录及其对应的源文件吗？此操作不可逆！"
                                                        },
                                                        "success": "删除失败记录成功",
                                                        "fail": "删除失败记录失败"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {
                                            "cols": 12,
                                            "md": 3
                                        },
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "variant": "tonal",
                                                    "color": "error",
                                                    "block": True
                                                },
                                                "text": "清空记录",
                                                "events": {
                                                    "click": {
                                                        "type": "request",
                                                        "api": f"plugin/{plugin_prefix}/clear_records",
                                                        "method": "POST",
                                                        "params": {"apikey": settings.API_TOKEN},
                                                        "confirm": {
                                                            "title": "确认操作",
                                                            "text": "确定要清空所有上传记录吗？此操作不可逆！"
                                                        },
                                                        "success": "清空上传记录成功",
                                                        "fail": "清空上传记录失败"
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined"
                },
                "content": [
                    {
                        "component": "VCardTitle",
                        "content": "上传记录列表"
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VTable",
                                "props": {
                                    "density": "compact",
                                    "hover": True,
                                    "fixed-header": True,
                                    "height": "600px"
                                },
                                "content": [
                                    {
                                        "component": "thead",
                                        "content": [
                                            {
                                                "component": "tr",
                                                "content": [
                                                    {
                                                        "component": "th",
                                                        "text": "序号",
                                                        "props": {
                                                            "width": "60px"
                                                        }
                                                    },
                                                    {
                                                        "component": "th",
                                                        "text": "文件名"
                                                    },
                                                    {
                                                        "component": "th",
                                                        "text": "上传路径",
                                                        "props": {
                                                            "width": "240px"
                                                        }
                                                    },
                                                    {
                                                        "component": "th",
                                                        "text": "大小",
                                                        "props": {
                                                            "width": "100px"
                                                        }
                                                    },
                                                    {
                                                        "component": "th",
                                                        "text": "类型",
                                                        "props": {
                                                            "width": "80px"
                                                        }
                                                    },
                                                    {
                                                        "component": "th",
                                                        "text": "状态",
                                                        "props": {
                                                            "width": "80px"
                                                        }
                                                    },
                                                    {
                                                        "component": "th",
                                                        "text": "时间",
                                                        "props": {
                                                            "width": "200px"
                                                        }
                                                    }
                                                ]
                                            }
                                        ]
                                    },
                                    {
                                        "component": "tbody",
                                        "content": [
                                            {
                                                "component": "tr",
                                                "content": [
                                                    {
                                                        "component": "td",
                                                        "text": item.get("index", "")
                                                    },
                                                    {
                                                        "component": "td",
                                                        "text": item.get("filename", "")
                                                    },
                                                    {
                                                        "component": "td",
                                                        "text": item.get("upload_path", "")
                                                    },
                                                    {
                                                        "component": "td",
                                                        "text": item.get("file_size", "")
                                                    },
                                                    {
                                                        "component": "td",
                                                        "text": item.get("file_type", "")
                                                    },
                                                    {
                                                        "component": "td",
                                                        "content": [
                                                            {
                                                                "component": "VChip",
                                                                "props": {
                                                                    "color": "success" if item.get("status") == "成功" else "error",
                                                                    "size": "small"
                                                                },
                                                                "text": item.get("status", "")
                                                            }
                                                        ]
                                                    },
                                                    {
                                                        "component": "td",
                                                        "text": item.get("timestamp", "")
                                                    }
                                                ]
                                            } for item in formatted_records
                                        ] if formatted_records else [
                                            {
                                                "component": "tr",
                                                "content": [
                                                    {
                                                        "component": "td",
                                                        "props": {
                                                            "colspan": 7
                                                        },
                                                        "text": "暂无上传记录"
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

    def get_command(self) -> List[Dict[str, Any]]:
        """
        注册插件远程命令
        """
        return [{
            "cmd": "/opt转移",
            "event": EventType.PluginAction,
            "desc": "OpenList上传文件",
            "category": "插件",
            "data": {
                "action": "api_sync"
            }
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        """获取插件API接口"""
        from app.core.config import settings
        return [
            {
                "path": "/optsync",
                "endpoint": self.api_sync,
                "methods": ["POST"],
                "summary": "手动触发全量同步",
                "description": "手动触发全量文件同步操作",
                "auth": "bear"
            },
            {
                "path": "/clear_records",
                "endpoint": self.clear_records,
                "methods": ["POST"],
                "summary": "清空上传记录",
                "description": "清空所有上传记录",
                "auth": "bear"
            },
            {
                "path": "/get_records",
                "endpoint": self.get_records,
                "methods": ["GET"],
                "summary": "获取上传记录",
                "description": "获取上传记录列表",
                "auth": "bear"
            },
            {
                "path": "/delete_failed",
                "endpoint": self.delete_failed_records,
                "methods": ["POST"],
                "summary": "删除失败记录和源文件",
                "description": "删除所有失败的上传记录及其对应的源文件",
                "auth": "bear"
            }
        ]

    def api_sync(self) -> Dict[str, Any]:
        """手动触发文件同步API"""
        try:
            logger.info("API调用: 手动触发文件同步")

            def async_sync():
                try:
                    self.sync_all()
                    logger.info("API同步操作完成")
                except Exception as e:
                    logger.error(f"API同步操作失败: {e}")

            # 使用系统线程池替换原生threading
            self._thread_pool.submit(async_sync)

            return {
                "success": True,
                "message": "文件同步操作已开始，将在后台执行",
                "data": {
                    "operation": "sync",
                    "status": "started",
                    "timestamp": time.time()
                }
            }
        except Exception as e:
            logger.error(f"API同步操作异常: {e}")
            return {
                "success": False,
                "message": f"同步操作失败: {str(e)}",
                "error": str(e)
            }

    def clear_records(self) -> Dict[str, Any]:
        """清空上传记录API"""
        try:
            logger.info("API调用: 清空上传记录")

            # 清空数据库中的记录
            self._clear_upload_records()

            return {
                "success": True,
                "message": "上传记录已清空",
                "data": {
                    "operation": "clear_records",
                    "status": "completed",
                    "timestamp": time.time()
                }
            }
        except Exception as e:
            logger.error(f"清空上传记录异常: {e}")
            return {
                "success": False,
                "message": f"清空上传记录失败: {str(e)}",
                "error": str(e)
            }

    def get_records(self) -> Dict[str, Any]:
        """获取上传记录API"""
        try:
            logger.info("API调用: 获取上传记录")

            # 从数据库获取真实记录
            records = self._get_upload_records()

            # 格式化记录用于显示
            formatted_records = []
            for record in records:
                formatted_records.append({
                    "filename": record.get("filename", ""),
                    "status": record.get("status", ""),
                    "timestamp": record.get("timestamp", ""),
                    "actions": "查看详情"
                })

            # MoviePilot 表格组件期望直接返回数据数组
            return {
                "code": 0,
                "data": formatted_records,
                "total": len(formatted_records)
            }
        except Exception as e:
            logger.error(f"获取上传记录异常: {e}")
            return {
                "code": -1,
                "message": str(e),
                "data": [],
                "total": 0
            }

    def delete_failed_records(self) -> Dict[str, Any]:
        """删除失败记录和源文件API"""
        try:
            logger.info("API调用: 删除失败记录和源文件")
            
            # 获取所有上传记录
            all_records = self._get_upload_records()
            
            # 筛选出失败的记录
            failed_records = [rec for rec in all_records if rec.get("status") == "失败"]
            failed_count = len(failed_records)
            
            if failed_count == 0:
                return {
                    "success": True,
                    "message": "没有找到失败的记录，无需删除",
                    "data": {
                        "operation": "delete_failed",
                        "status": "completed",
                        "deleted_files": 0,
                        "deleted_records": 0,
                        "timestamp": time.time()
                    }
                }
            
            # 删除失败的源文件
            deleted_files = 0
            for record in failed_records:
                file_path = record.get("file_path", "")
                if file_path and Path(file_path).exists():
                    try:
                        Path(file_path).unlink()
                        logger.info(f"🗑️ 删除失败源文件: {file_path}")
                        deleted_files += 1
                    except Exception as e:
                        logger.warning(f"删除源文件失败: {file_path}, 错误: {e}")
            
            # 从数据库中删除失败的记录
            remaining_records = [rec for rec in all_records if rec.get("status") != "失败"]
            
            # 保存更新后的记录到数据库
            self.data_oper.save(self.plugin_name, 'upload_records', remaining_records)
            
            deleted_records = len(all_records) - len(remaining_records)
            
            logger.info(f"🗑️ 删除失败记录完成: 删除了 {deleted_files} 个源文件和 {deleted_records} 条记录")
            
            return {
                "success": True,
                "message": f"成功删除 {deleted_files} 个失败源文件和 {deleted_records} 条失败记录",
                "data": {
                    "operation": "delete_failed",
                    "status": "completed",
                    "deleted_files": deleted_files,
                    "deleted_records": deleted_records,
                    "timestamp": time.time()
                }
            }
            
        except Exception as e:
            logger.error(f"删除失败记录异常: {e}")
            return {
                "success": False,
                "message": f"删除失败记录失败: {str(e)}",
                "error": str(e)
            }

    @eventmanager.register(EventType.PluginAction)
    def command_action(self, event: Event):
        """
        远程命令响应
        """
        event_data = event.event_data
        if not event_data:
            return

        action = event_data.get("action")
        if not action:
            return

        # 处理不同的命令动作
        if action == "interactive_demo":
            # 获取用户信息
            channel = event_data.get("channel")
            source = event_data.get("source")
            user = event_data.get("user")

            # 发送带有交互按钮的消息
            self._send_main_menu(channel, source, user)
        elif action == "api_sync":
            # 处理/opt转移命令
            self.api_sync()

    @eventmanager.register(EventType.MessageAction)
    def message_action(self, event: Event):
        """
        处理消息按钮回调
        """
        event_data = event.event_data
        if not event_data:
            return

        # 检查是否为本插件的回调
        plugin_id = event_data.get("plugin_id")
        if plugin_id != self.__class__.__name__:
            return

        # 获取回调数据
        text = event_data.get("text", "")
        channel = event_data.get("channel")
        source = event_data.get("source")
        userid = event_data.get("userid")
        # 获取原始消息ID和聊天ID（用于直接更新原消息）
        original_message_id = event_data.get("original_message_id")
        original_chat_id = event_data.get("original_chat_id")

        # 根据回调内容处理不同的交互
        if text == "menu1":
            self._handle_menu1(channel, source, userid,
                               original_message_id, original_chat_id)
        elif text == "menu2":
            self._handle_menu2(channel, source, userid,
                               original_message_id, original_chat_id)
        elif text == "back":
            self._send_main_menu(channel, source, userid,
                                 original_message_id, original_chat_id)
        elif text == "status":
            self._handle_status(channel, source, userid,
                                original_message_id, original_chat_id)
        elif text.startswith("action_"):
            action_id = text.replace("action_", "")
            self._handle_action(action_id, channel, source,
                                userid, original_message_id, original_chat_id)

    def stop_service(self):
        """停止插件服务"""
        try:
            if self._observer:
                for observer in self._observer:
                    try:
                        observer.stop()
                        observer.join(timeout=5)
                    except Exception as e:
                        logger.error(f"停止监控服务失败: {e}")
                self._observer.clear()
                logger.info("文件监控服务已停止")

            if self._scheduler and self._scheduler.running:
                try:
                    self._scheduler.shutdown(wait=True, timeout=30)
                    logger.info("定时任务服务已停止")
                except Exception as e:
                    logger.error(f"停止定时任务服务失败: {e}")
                finally:
                    self._scheduler = None

            logger.info("✅ 插件服务已完全停止")

        except Exception as e:
            logger.error(f"❌ 停止插件服务异常: {e}")

    def _start_schedule_service(self):
        """启动定时处理服务"""
        try:
            if not self._monitor_groups:
                logger.error("❌ 未配置监控目录，定时处理功能无法启动")
                return

            if not self._init_storage_chain():
                logger.error("❌ 存储链初始化失败，定时处理功能无法启动")
                return

            if not APSCHEDULER_AVAILABLE:
                logger.error("❌ APScheduler不可用，定时处理功能无法启动")
                return

            if self._scheduler and self._scheduler.running:
                self._scheduler.shutdown()

            self._scheduler = BackgroundScheduler()

            try:
                cron_trigger = CronTrigger.from_crontab(self._schedule_cron)
                self._scheduler.add_job(
                    name="OpenList上传助手定时任务",
                    func=self.sync_all,
                    trigger=cron_trigger,
                    misfire_grace_time=300
                )
                logger.info(f"⏰ 定时处理服务已启动，表达式: {self._schedule_cron}")
            except Exception as e:
                logger.error(f"❌ 添加定时任务失败: {e}")
                try:
                    cron_trigger = CronTrigger.from_crontab("0 2 * * *")
                    self._scheduler.add_job(
                        name="OpenList上传助手定时任务（默认）",
                        func=self.sync_all,
                        trigger=cron_trigger,
                        misfire_grace_time=300
                    )
                    logger.info("⏰ 使用默认表达式启动定时任务: 0 2 * * *")
                except Exception as e2:
                    logger.error(f"❌ 使用默认表达式启动定时任务失败: {e2}")
                    return

            if self._scheduler.get_jobs():
                self._scheduler.start()
                logger.info("✅ 定时任务调度器已启动")

                def initial_run():
                    time.sleep(5)
                    logger.info("🔧 执行定时任务初始化检查...")
                    try:
                        self.sync_all()
                        logger.info("✅ 定时任务初始化检查完成")
                    except Exception as e:
                        logger.warning(f"⚠️ 定时任务初始化检查失败: {e}")

                threading.Thread(target=initial_run, daemon=True).start()

        except Exception as e:
            logger.error(f"❌ 定时处理服务启动失败: {e}")
            if self._scheduler:
                self._scheduler = None

    def _send_notification(self, files: List[Path], status: str = "success"):
        """发送系统通知"""
        try:
            # 调试日志：确认通知被触发
            logger.debug(
                f"📢 通知发送被触发: 文件数={len(files)}, 状态={status}, 通知开关={self._enable_notify}")

            if not files:
                logger.warning("📢 通知发送被跳过: 文件列表为空")
                return

            # 根据状态设置标题和前缀
            if status == "success":
                title = "OpenList上传成功"
                prefix = "✅ 成功文件："
            elif status == "skipped":
                title = "OpenList跳过上传"
                prefix = "⚠️ 跳过文件："
            else:  # failed
                title = "OpenList上传失败"
                prefix = "❌ 失败文件："

            # 构建文件列表
            file_list = ""
            for i, file_path in enumerate(files, 1):
                file_list += f"\n{i}. {file_path.name}"

            # 添加时间
            time_str = self._get_current_time()

            # 构建完整消息
            message = f"{prefix}\n{file_list}\n🕐 时间：{time_str}"

            # 调试日志：显示将要发送的消息内容
            logger.debug(f"📢 准备发送通知: 标题={title}, 文件数={len(files)}")

            # 发送通知
            result = self.post_message(
                mtype=NotificationType.Plugin,
                title=title,
                text=message,
                image="https://gitee.com/gldl137/wechat-work-bot/raw/master/images/OpenList.jpg"
            )

            # 调试日志：显示发送结果
            logger.info(f"📢 通知已发送: {title}, 文件数={len(files)}, 结果={result}")

        except Exception as e:
            logger.error(f"❌ 发送通知失败: {e}")
            import traceback
            logger.debug(f"通知发送失败的详细堆栈信息: {traceback.format_exc()}")

    def _send_empty_directory_notification(self, dir_path: Path):
        """发送空目录通知"""
        try:
            if not self._enable_notify:
                return

            # 使用相同的通知模板
            title = "OpenList上传目录为空"
            prefix = "📁 空目录："

            # 构建文件列表（这里使用目录信息）
            file_list = f"\n1. {dir_path}"

            # 添加时间
            time_str = self._get_current_time()

            # 构建完整消息
            message = f"{prefix}\n{file_list}\n🕐 时间：{time_str}"

            # 调试日志：显示将要发送的消息内容
            logger.debug(f"📢 准备发送空目录通知: 标题={title}, 目录={dir_path.name}")

            # 发送通知
            result = self.post_message(
                mtype=NotificationType.Plugin,
                title=title,
                text=message,
                image="https://gitee.com/gldl137/wechat-work-bot/raw/master/images/OpenList.jpg"
            )

            # 调试日志：显示发送结果
            logger.info(
                f"📢 空目录通知已发送: {title}, 目录={dir_path.name}, 结果={result}")

        except Exception as e:
            logger.error(f"❌ 发送空目录通知失败: {e}")

    # =================== 交互功能相关方法 ===================
    def _send_main_menu(self, channel, source, userid, original_message_id=None, original_chat_id=None):
        """
        发送主菜单
        """
        buttons = [
            [
                {"text": "🎬 媒体管理",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|menu1"},
                {"text": "⚙️ 系统设置",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|menu2"}
            ],
            [
                {"text": "📊 查看状态",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|status"}
            ]
        ]

        self.post_message(
            channel=channel,
            title="🤖 OpenList上传助手",
            text="请选择要执行的操作：",
            userid=userid,
            buttons=buttons,
            original_message_id=original_message_id,
            original_chat_id=original_chat_id
        )

    def _handle_menu1(self, channel, source, userid, original_message_id, original_chat_id):
        """
        处理媒体管理菜单
        """
        buttons = [
            [
                {"text": "🔍 搜索媒体",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|action_search"},
                {"text": "📥 下载管理",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|action_download"}
            ],
            [
                {"text": "🔙 返回主菜单",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|back"}
            ]
        ]

        self.post_message(
            channel=channel,
            title="🎬 媒体管理",
            text="选择媒体管理功能：",
            userid=userid,
            buttons=buttons,
            original_message_id=original_message_id,
            original_chat_id=original_chat_id
        )

    def _handle_menu2(self, channel, source, userid, original_message_id, original_chat_id):
        """
        处理系统设置菜单
        """
        buttons = [
            [
                {"text": "🔧 配置同步",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|action_config"},
                {"text": "🚀 启动服务",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|action_start"}
            ],
            [
                {"text": "🔙 返回主菜单",
                    "callback_data": f"[PLUGIN]{self.__class__.__name__}|back"}
            ]
        ]

        self.post_message(
            channel=channel,
            title="⚙️ 系统设置",
            text="选择系统设置功能：",
            userid=userid,
            buttons=buttons,
            original_message_id=original_message_id,
            original_chat_id=original_chat_id
        )

    def _handle_status(self, channel, source, userid, original_message_id, original_chat_id):
        """
        处理查看状态操作
        """
        # 获取插件状态信息
        status_info = {
            "插件状态": "✅ 已启用" if self._enabled else "❌ 已禁用",
            "监控目录数": len(self._monitor_groups) if self._monitor_groups else 0,
            "定时任务": "⏰ 已启动" if (self._scheduler and self._scheduler.running) else "⏸️ 未启动",
            "服务模式": "实时监控" if not self._enable_schedule else "定时处理"
        }

        # 构建状态消息
        status_text = "\n".join(
            [f"• {key}: {value}" for key, value in status_info.items()])

        buttons = [
            [{"text": "🔙 返回主菜单", "callback_data": f"[PLUGIN]{self.__class__.__name__}|back"}]
        ]

        self.post_message(
            channel=channel,
            title="📊 插件状态",
            text=status_text,
            userid=userid,
            buttons=buttons,
            original_message_id=original_message_id,
            original_chat_id=original_chat_id
        )

    def _handle_action(self, action_id, channel, source, userid, original_message_id, original_chat_id):
        """
        处理具体动作
        """
        result = ""
        if action_id == "search":
            # 执行搜索逻辑
            result = "搜索功能已执行"
        elif action_id == "download":
            # 执行下载逻辑
            result = "下载管理已开启"
        elif action_id == "config":
            # 执行配置同步逻辑
            result = "配置同步功能已执行"
        elif action_id == "start":
            # 执行启动服务逻辑
            result = "服务已启动"
        else:
            result = "未知操作"

        # 发送执行结果并提供返回按钮
        buttons = [
            [{"text": "🔙 返回主菜单", "callback_data": f"[PLUGIN]{self.__class__.__name__}|back"}]
        ]

        self.post_message(
            channel=channel,
            title="✅ 操作完成",
            text=result,
            userid=userid,
            buttons=buttons,
            original_message_id=original_message_id,
            original_chat_id=original_chat_id
        )

    def _save_upload_record(self, filename: str, status: str, file_path: str = "", target_path: str = ""):
        """保存上传记录到数据库"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 获取文件大小
            file_size = ""
            if file_path and Path(file_path).exists():
                try:
                    size_bytes = Path(file_path).stat().st_size
                    file_size = self._format_file_size(size_bytes)
                except Exception as e:
                    logger.warning(f"获取文件大小失败: {e}")

            # 格式化上传路径（只显示前4个目录）
            upload_path = self._format_upload_path(target_path)

            record = {
                "filename": filename,
                "status": status,
                "timestamp": timestamp,
                "file_path": file_path,
                "target_path": target_path,
                "upload_path": upload_path,
                "file_size": file_size
            }

            logger.debug(f"📝 开始保存上传记录到数据库: {filename} - {status}")

            # 获取现有记录
            existing_records = self.data_oper.get_data(
                self.plugin_name, 'upload_records') or []
            logger.debug(f"📝 当前数据库记录数: {len(existing_records)}")

            # 查找同名记录
            found_index = None
            for i, rec in enumerate(existing_records):
                # 检查文件名、大小、上传路径是否都相同
                if (rec.get("filename") == filename and
                    rec.get("file_size") == file_size and
                        rec.get("target_path") == target_path):
                    found_index = i
                    break

            if found_index is not None:
                # 同名、同大小、同路径记录存在，直接覆盖
                logger.debug(f"📝 发现相同记录(文件名、大小、路径相同)，进行覆盖: {filename}")
                existing_records[found_index] = record
            else:
                # 相同记录不存在，插入到列表最前面(最新)
                existing_records.insert(0, record)
                logger.debug(f"📝 新增记录，插入到列表最前面: {filename}")

            # 限制最多保存1000条记录
            if len(existing_records) > 1000:
                existing_records = existing_records[:1000]  # 保留最新的1000条
                logger.debug(f"📝 记录数超过1000条，已截断为最新1000条")

            # 保存到数据库
            self.data_oper.save(
                self.plugin_name, 'upload_records', existing_records)

            logger.info(
                f"📝 上传记录已成功保存到数据库: {filename} - {status}，总记录数: {len(existing_records)}")

        except Exception as e:
            logger.error(f"❌ 保存上传记录到数据库失败: {e}")
            import traceback
            logger.debug(f"保存失败的详细堆栈信息: {traceback.format_exc()}")

    def _get_upload_records(self) -> List[dict]:
        """从数据库获取上传记录"""
        try:
            logger.debug("📝 开始从数据库获取上传记录")
            records = self.data_oper.get_data(
                self.plugin_name, 'upload_records') or []
            logger.info(f"📝 成功从数据库获取上传记录，共 {len(records)} 条")
            return records
        except Exception as e:
            logger.error(f"❌ 从数据库获取上传记录失败: {e}")
            import traceback
            logger.debug(f"获取失败的详细堆栈信息: {traceback.format_exc()}")
            return []

    def _clear_upload_records(self):
        """清空上传记录"""
        try:
            logger.debug("🗑️ 开始清空数据库中的上传记录")
            self.data_oper.save(self.plugin_name, 'upload_records', [])
            logger.info("🗑️ 上传记录已成功从数据库清空")
        except Exception as e:
            logger.error(f"❌ 清空数据库中的上传记录失败: {e}")
            import traceback
            logger.debug(f"清空失败的详细堆栈信息: {traceback.format_exc()}")
            raise e

    def _format_upload_path(self, target_path: str) -> str:
        """
        格式化上传路径，只显示前4个目录
        :param target_path: 目标路径
        :return: 格式化后的路径字符串
        """
        if not target_path:
            return ""

        # 分割路径
        parts = [p for p in Path(target_path).parts if p]

        # 只取前4个目录
        if len(parts) > 4:
            display_parts = parts[:4]
            return "/".join(display_parts) + "/..."
        else:
            return target_path

    def _format_file_size(self, size_bytes: int) -> str:
        """
        格式化文件大小
        :param size_bytes: 文件大小（字节）
        :return: 格式化后的文件大小字符串
        """
        try:
            size_bytes = float(size_bytes) if size_bytes not in (None, "") else 0.0
        except (TypeError, ValueError):
            size_bytes = 0.0
        size_bytes = float(size_bytes)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"

    def _get_file_type(self, filename: str) -> str:
        """
        根据文件扩展名判断文件类型
        :param filename: 文件名
        :return: 文件类型（视频、音频、图片、文档、其他）
        """
        # 文件扩展名映射
        video_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv',
                            '.webm', '.m4v', '.mpg', '.mpeg', '.3gp', '.ts',
                            '.m2ts', '.rmvb', '.rm'}
        audio_extensions = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma',
                            '.m4a', '.opus', '.alac', '.ape', '.ac3', '.dts'}
        image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff',
                            '.webp', '.svg', '.ico', '.heic', '.raw', '.psd'}
        document_extensions = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt',
                               '.pptx', '.txt', '.rtf', '.odt', '.ods', '.odp',
                               '.csv', '.md', '.json', '.xml', '.html', '.epub'}

        # 获取文件扩展名（小写）
        ext = Path(filename).suffix.lower()

        if ext in video_extensions:
            return '视频'
        elif ext in audio_extensions:
            return '音频'
        elif ext in image_extensions:
            return '图片'
        elif ext in document_extensions:
            return '文档'
        else:
            return '其他'

    def get_dashboard_meta(self) -> Optional[List[Dict[str, str]]]:
        """
        获取插件仪表盘元信息
        """
        logger.info("📊 获取仪表盘元信息")
        return [{
            "key": "upload_stats",
            "name": "上传助手统计"
        }]

    def get_dashboard(self, key: str, **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[dict]]]:
        """
        获取上传助手统计仪表盘
        """
        logger.info(f"📊 获取仪表盘: key={key}")
        try:
            # 获取上传记录
            records = self._get_upload_records()

            # 统计成功和失败的数量
            success_count = len(
                [r for r in records if r.get('status') == '成功'])
            failed_count = len([r for r in records if r.get('status') == '失败'])
            total_count = len(records)

            # 计算成功率
            success_rate = round(
                (success_count / total_count * 100), 2) if total_count > 0 else 0

            # 只取最新的20条记录
            recent_records = records[:20]

            # 格式化记录数据
            formatted_records = []
            for record in recent_records:
                filename = record.get("filename", "")
                status = record.get("status", "")
                timestamp = record.get("timestamp", "")

                # 获取上传路径
                upload_path = record.get("upload_path", "")
                if not upload_path:
                    upload_path = self._format_upload_path(
                        record.get("target_path", ""))

                # 根据状态设置颜色
                if status == "失败":
                    status_color = "error"
                elif status == "成功":
                    status_color = "success"
                else:
                    status_color = "grey"

                formatted_records.append({
                    "filename": filename,
                    "upload_path": upload_path,
                    "file_type": self._get_file_type(filename),
                    "file_size": self._format_file_size(record.get("file_size", 0)),
                    "status": status,
                    "status_color": status_color,
                    "timestamp": timestamp
                })

            # 构建表格行
            table_rows = []
            for rec in formatted_records:
                table_rows.append({
                    "component": "tr",
                    "content": [
                        {
                            "component": "td",
                            "content": [
                                {
                                    "component": "div",
                                    "props": {
                                        "class": "text-truncate",
                                        "style": "max-width: 400px;"
                                    },
                                    "text": rec["filename"]
                                }
                            ]
                        },
                        {
                            "component": "td",
                            "content": [
                                {
                                    "component": "div",
                                    "props": {
                                        "class": "text-truncate",
                                        "style": "max-width: 120px;"
                                    },
                                    "text": rec["upload_path"]
                                }
                            ]
                        },
                        {
                            "component": "td",
                            "text": rec["file_size"]
                        },
                        {
                            "component": "td",
                            "text": rec["file_type"]
                        },
                        {
                            "component": "td",
                            "content": [
                                {
                                    "component": "VChip",
                                    "props": {
                                        "size": "small",
                                        "color": rec["status_color"],
                                        "label": True
                                    },
                                    "text": rec["status"]
                                }
                            ]
                        },
                        {
                            "component": "td",
                            "content": [
                                {
                                    "component": "span",
                                    "props": {
                                        "style": "display: block; text-align: right; width: 150px;"
                                    },
                                    "text": rec["timestamp"]
                                }
                            ]
                        }
                    ]
                })

            # 仪表板col配置
            col_config = {
                "cols": 12,
                "sm": 6
            }

            # 全局配置
            global_config = {
                "refresh": 30,  # 30秒自动刷新
                "border": True,
                "title": "上传助手统计"
            }

            # 页面配置
            dashboard_content = [
                {
                    "component": "VCard",
                    "content": [
                        {
                            "component": "VCardText",
                            "props": {
                                "class": "d-flex align-center"
                            },
                            "content": [
                                {
                                    "component": "VIcon",
                                    "props": {
                                        "icon": "mdi-cloud-upload",
                                        "size": "48",
                                        "color": "primary",
                                        "class": "mr-4"
                                    }
                                },
                                {
                                    "component": "VRow",
                                    "content": [
                                        {
                                            "component": "VCol",
                                            "content": [
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "text-h6"
                                                    },
                                                    "text": f"{total_count}"
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "text-caption"
                                                    },
                                                    "text": "总上传"
                                                }
                                            ]
                                        },
                                        {
                                            "component": "VCol",
                                            "content": [
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "text-h6 text-error"
                                                    },
                                                    "text": f"{failed_count}"
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "text-caption"
                                                    },
                                                    "text": "失败"
                                                }
                                            ]
                                        },
                                        {
                                            "component": "VCol",
                                            "content": [
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "text-h6 text-info"
                                                    },
                                                    "text": f"{success_rate}%"
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "text-caption"
                                                    },
                                                    "text": "成功率"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    "component": "VCard",
                    "props": {
                        "class": "mt-4"
                    },
                    "content": [
                        {
                            "component": "VCardTitle",
                            "text": f"最近上传记录 ({len(formatted_records)}条)"
                        },
                        {
                            "component": "VCardText",
                            "content": [
                                {
                                    "component": "VTable",
                                    "props": {
                                        "density": "compact",
                                        "hover": True
                                    },
                                    "content": [
                                        {
                                            "component": "thead",
                                            "content": [
                                                {
                                                    "component": "tr",
                                                    "content": [
                                                        {
                                                            "component": "th",
                                                            "text": "文件名"
                                                        },
                                                        {
                                                            "component": "th",
                                                            "text": "上传路径"
                                                        },
                                                        {
                                                            "component": "th",
                                                            "text": "大小"
                                                        },
                                                        {
                                                            "component": "th",
                                                            "text": "类型"
                                                        },
                                                        {
                                                            "component": "th",
                                                            "text": "状态"
                                                        },
                                                        {
                                                            "component": "th",
                                                            "props": {
                                                                "style": "width: 150px;"
                                                            },
                                                            "text": "时间"
                                                        }
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            "component": "tbody",
                                            "content": table_rows if table_rows else [
                                                {
                                                    "component": "tr",
                                                    "content": [
                                                        {
                                                            "component": "td",
                                                            "props": {
                                                                "colspan": 6,
                                                                "class": "text-center text-grey"
                                                            },
                                                            "text": "暂无上传记录"
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]

            return col_config, global_config, dashboard_content

        except Exception as e:
            logger.error(f"获取仪表盘失败: {e}")
            import traceback
            logger.debug(f"获取仪表盘失败的详细堆栈信息: {traceback.format_exc()}")
            return None
