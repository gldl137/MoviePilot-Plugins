import datetime
import shutil
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app import schemas
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType
from app.utils.system import SystemUtils
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import APIRouter, Request
from mutagen import File
from mutagen.easyid3 import EasyID3
from mutagen.flac import FLAC
from mutagen.id3 import APIC, USLT
from pydantic import BaseModel
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

lock = threading.Lock()


class BaseApiReq(BaseModel):
    """基础API请求模型"""
    apikey: str


class DeleteRecordReq(BaseApiReq):
    """删除记录请求模型"""
    source_path: str


class RetryFileReq(BaseApiReq):
    """重新整理文件请求模型"""
    file_path: str


class MusicFileMonitorHandler(FileSystemEventHandler):
    """音乐文件监控响应类 - 增强事件过滤"""

    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(MusicFileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync
        # 添加事件时间戳记录，防止短时间内重复处理
        self._last_processed = {}

    def _should_process_event(self, file_path: str, event_type: str) -> bool:
        """判断是否应该处理该事件，防止重复触发"""
        current_time = datetime.datetime.now()

        # 对于修改事件，简化处理：只要文件被修改就允许处理
        # 不再检查是否之前是缺标签文件，恢复原来的简单逻辑
        if event_type == "modified":
            # 更新最后处理时间并返回True，允许处理修改事件
            self._last_processed[file_path] = current_time
            return True

        # 检查是否在最近60秒内处理过相同文件
        if file_path in self._last_processed:
            last_time = self._last_processed[file_path]
            time_diff = (current_time - last_time).total_seconds()
            if time_diff < 60:  # 60秒内不重复处理同一文件
                # 直接跳过，不输出任何日志
                return False

        # 更新最后处理时间
        self._last_processed[file_path] = current_time

        # 清理过期的记录（避免内存泄漏）
        expired_keys = []
        for path, timestamp in self._last_processed.items():
            if (current_time - timestamp).total_seconds() > 600:  # 10分钟前的记录清理掉
                expired_keys.append(path)
        for key in expired_keys:
            del self._last_processed[key]

        return True

    def on_created(self, event):
        if not event.is_directory:
            # 只处理音频文件
            file_ext = Path(event.src_path).suffix.lower()
            if file_ext in self.sync.AUDIO_EXTENSIONS:
                if self._should_process_event(event.src_path, "created"):
                    self.sync.handle_file(
                        event.src_path, self._watch_path, event_type="created")

    def on_moved(self, event):
        if not event.is_directory:
            # 处理移动的文件
            dest_path = event.dest_path if hasattr(
                event, 'dest_path') else event.src_path

            # 只处理移动到监控目录的文件（避免处理移出事件）
            dest_ext = Path(dest_path).suffix.lower()
            if dest_ext in self.sync.AUDIO_EXTENSIONS:
                if self._should_process_event(dest_path, "moved_to"):
                    self.sync.handle_file(
                        dest_path, self._watch_path, event_type="moved_to")

    def on_modified(self, event):
        if not event.is_directory:
            # 只处理音频文件
            file_ext = Path(event.src_path).suffix.lower()
            if file_ext in self.sync.AUDIO_EXTENSIONS:
                # 对于修改事件，更严格地检查是否真的需要处理
                # 通常创建事件后会有修改事件，可以适当过滤
                if self._should_process_event(event.src_path, "modified"):
                    self.sync.handle_file(
                        event.src_path, self._watch_path, event_type="modified")

    def on_deleted(self, event):
        if not event.is_directory:
            # 直接忽略删除事件，不进入处理流程
            return


class MusicFileOrganizer(_PluginBase):
    # 插件名称
    plugin_name = "音乐整理"
    # 插件描述
    plugin_desc = "监控指定目录音频文件，按规则自动整理音乐文件"
    # 插件图标
    plugin_icon = "music.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "MusicFileOrganizer"
    # 加载顺序
    plugin_order = 10
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _observer = []
    _enabled = False
    _notify = False
    _onlyonce = False  # 立即运行一次
    _require_tags = True  # 必须包含标签信息
    _clear_history = False  # 清除历史记录
    _monitor_dirs = ""
    _folder_rule = "artist_album"  # 文件夹命名规则
    _filename_rule = "artist_title"  # 文件命名规则
    _transfer_type = "move"  # 文件操作方式
    _conflict_strategy = "skip"  # 文件冲突处理策略
    _mode = "fast"  # 监控模式: fast/compatibility
    _cron = ""  # 定时任务cron表达式
    _enabled_scheduler = False  # 定时任务是否启用
    # 聚合通知相关配置（基于TV聚合模式）
    _batch_notify = False  # 是否启用聚合通知
    _batch_delay = 0  # 聚合时间（秒），默认0秒（不聚合）
    # 存储源目录与目的目录关系，以及转移方式
    _dirconf: Dict[str, Dict[str, Any]] = {}
    # 退出事件
    _event = threading.Event()
    # 调度器
    _scheduler = None

    # 阶段2：根治重复调度 - 极简状态表
    _processing_files: set = set()  # 正在处理的文件集合

    # 聚合通知相关数据结构（基于TV聚合模式）
    # 按歌手分组的批量处理结果 {歌手名: [结果列表]}
    _artist_batch_results: Dict[str, List[Dict]] = {}
    _artist_timers: Dict[str, Optional[threading.Timer]] = {}  # 按歌手分组的定时器
    _batch_lock = threading.Lock()  # 批量处理锁

    # 歌曲查重索引 - 用于加速重复歌曲检测
    _song_index: Dict[str, Dict[str, Any]] = {}  # {歌曲索引键: 历史记录}
    _index_lock = threading.Lock()  # 索引操作锁

    # 标签不完整文件事件管理
    _incomplete_tags_files: list = []  # 标签不完整的文件列表（使用列表代替集合）
    _sent_incomplete_files: set = set()  # 已发送事件的文件集合（避免重复发送）
    _last_file_event_time: datetime.datetime = None  # 最后一个文件事件时间
    _incomplete_tags_timer: threading.Timer = None  # 30秒事件发送计时器

    # 支持的音频格式
    AUDIO_EXTENSIONS = {'.mp3', '.flac', '.wav', '.m4a',
                        '.ogg', '.wma', '.aac', '.alac', '.ape', '.opus'}
    # 不再支持歌词文件格式，直接处理音频文件
    # LYRICS_EXTENSIONS = {'.lrc'}

    # 音频格式标签映射表 - 用于标准化不同格式的标签名称
    AUDIO_TAG_MAPPING = {
        "MP3": {
            "title": "TIT2",
            "artist": "TPE1",
            "album": "TALB",
            "date": "TDRC",
            "tracknumber": "TRCK",
            "discnumber": "TPOS",
            "genre": "TCON",
            "albumartist": "TPE2",
            "composer": "TCOM",
            "lyricist": "TEXT",
            "description": "COMM",
            "lyrics": "USLT",
            "cover": "APIC"
        },
        "FLAC": {
            "title": "TITLE",
            "artist": "ARTIST",
            "album": "ALBUM",
            "date": "DATE",
            "tracknumber": "TRACKNUMBER",
            "discnumber": "DISCNUMBER",
            "genre": "GENRE",
            "albumartist": "ALBUMARTIST",
            "composer": "COMPOSER",
            "lyricist": "LYRICIST",
            "description": "DESCRIPTION",
            "lyrics": "LYRICS",
            "cover": "METADATA_BLOCK_PICTURE"
        },
        "OGG": {
            "title": "TITLE",
            "artist": "ARTIST",
            "album": "ALBUM",
            "date": "DATE",
            "tracknumber": "TRACKNUMBER",
            "discnumber": "DISCNUMBER",
            "genre": "GENRE",
            "albumartist": "ALBUMARTIST",
            "composer": "COMPOSER",
            "lyricist": "LYRICIST",
            "description": "DESCRIPTION",
            "lyrics": "LYRICS",
            "cover": "METADATA_BLOCK_PICTURE"
        },
        "M4A": {
            "title": "©nam",
            "artist": "©ART",
            "album": "©alb",
            "date": "©day",
            "tracknumber": "trkn",
            "discnumber": "disk",
            "genre": "©gen",
            "albumartist": "aART",
            "composer": "©wrt",
            "lyricist": "©lyr",
            "description": "©cmt",
            "lyrics": "©lyr",
            "cover": "covr"
        }
    }

    def init_plugin(self, config: dict = None):
        # 清空配置
        self._dirconf = {}

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._require_tags = config.get("require_tags", True)
            self._monitor_dirs = config.get("monitor_dirs") or ""
            self._clear_history = config.get("clear_history", False)
            # 聚合通知相关配置
            self._batch_notify = config.get("batch_notify", False)
            self._batch_delay = int(config.get(
                "batch_delay", 0))  # 使用默认值0秒（不聚合），转换为整数
            # 发送事件配置
            self._send_events = config.get("send_events", False)  # 默认关闭发送事件

        # API注册由MoviePilot系统自动处理，无需手动注册

        self._folder_rule = config.get("folder_rule", "artist_album")
        self._filename_rule = config.get("filename_rule", "artist_title")
        self._transfer_type = config.get("transfer_type", "move")
        self._conflict_strategy = config.get("conflict_strategy", "skip")
        self._mode = config.get("mode", "fast")
        self._cron = config.get("cron", "")
        self._enabled_scheduler = config.get("enabled_scheduler", False)

        # 停止现有任务
        self.stop_service()

        # 检查必要配置项
        if self._enabled or self._onlyonce:
            missing_configs = []

            # 检查是否配置了监控目录
            if not self._monitor_dirs or not any(dir.strip() for dir in self._monitor_dirs.split("\n")):
                missing_configs.append("监控目录")

            # 检查其他配置项
            if not self._folder_rule:
                missing_configs.append("文件夹命名规则")
            if not self._transfer_type:
                missing_configs.append("转移方式")
            if not self._conflict_strategy:
                missing_configs.append("覆盖方式")

            if missing_configs:
                logger.warning(
                    f"插件未配置以下必要项: {', '.join(missing_configs)}，请检查配置后重试")
                return

        if self._enabled or self._onlyonce:
            # 定时服务管理器
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            # 读取目录配置
            monitor_dirs = self._monitor_dirs.split("\n")
            if not monitor_dirs:
                return

            valid_dirs = [d for d in monitor_dirs if d.strip()]

            logger.debug(f"开始解析监控目录配置，共 {len(valid_dirs)} 个有效目录")

            for dir_config in valid_dirs:
                if not dir_config:
                    continue

                # 解析目录配置，支持以下格式：
                # 1. 监控目录:转移目的目录
                # 2. 监控目录:转移目的目录#转移方式
                # 3. 监控目录 (使用默认目的目录)

                # 首先按冒号分割，分离监控目录和剩余部分
                parts = dir_config.split(":", 1)
                mon_path = parts[0].strip()

                # 配置信息
                dir_info = {
                    "target": None,
                    "transfer_type": self._transfer_type  # 默认使用全局配置
                }

                if len(parts) > 1:
                    # 有目的目录或目的目录+转移方式
                    rest = parts[1].strip()

                    # 检查是否有转移方式(用#分隔)
                    if "#" in rest:
                        target_part, transfer_type = rest.split("#", 1)
                        dir_info["target"] = Path(target_part.strip())
                        dir_info["transfer_type"] = transfer_type.strip()
                    else:
                        # 只有目的目录，使用全局转移方式
                        dir_info["target"] = Path(rest)
                else:
                    # 只有监控目录，使用默认目的目录
                    logger.debug(f"监控目录 {mon_path} 未设置目标目录，将使用默认目录")

                # 存储配置
                self._dirconf[mon_path] = dir_info
                target_text = "默认目录" if dir_info["target"] is None else str(
                    dir_info["target"])
                logger.debug(
                    f"成功配置监控目录: {mon_path} -> 目标目录: {target_text}, 转移方式: {dir_info['transfer_type']}")

            # 清理历史记录
            if self._clear_history:
                try:
                    # 获取历史记录
                    history = self.get_data('organize_history') or []

                    if not history:
                        logger.info("历史记录已为空，无需清除")
                    else:
                        # 清除所有历史记录
                        self.save_data('organize_history', [])
                        logger.info(f"已成功清除 {len(history)} 条历史记录")

                        # 发送通知（如果启用了通知）
                        if self._notify:
                            self._send_unified_notify(
                                title="历史记录已清除",
                                text=f"已成功清除 {len(history)} 条音乐整理历史记录"
                            )

                    # 关闭清除历史记录开关
                    self._clear_history = False
                    # 保存配置
                    self.__update_config()
                    logger.info("清除历史记录操作完成，开关已关闭")

                except Exception as e:
                    logger.error(f"清除历史记录失败: {e}")

                    # 发送错误通知（如果启用了通知）
                    if self._notify:
                        self._send_unified_notify(
                            title="清除历史记录失败",
                            text=f"清除历史记录时发生错误: {str(e)}"
                        )

                    # 关闭清除历史记录开关
                    self._clear_history = False
                    # 保存配置
                    self.__update_config()

            # 运行一次定时服务
            if self._onlyonce:
                # 使用线程池异步执行，避免界面卡住
                import threading

                def async_sync_all():
                    try:
                        # 确保通知状态在异步线程中正确传递
                        notify_status = self._notify
                        batch_notify_status = self._batch_notify
                        logger.debug(
                            f"异步执行全量整理，通知状态: 启用={notify_status}, 聚合={batch_notify_status}")

                        # 执行同步（sync_all内部会处理聚合通知）
                        self.sync_all()

                        # 不再发送额外的完成通知，因为聚合通知已经在sync_all中处理
                        # 聚合通知会按照统一模板显示成功和失败的文件列表
                    except Exception as e:
                        logger.error(f"异步执行全量整理失败: {e}")
                        # 发送错误通知
                        if self._notify:
                            self._send_unified_notify(
                                title="音乐文件整理失败",
                                text=f"全量整理任务失败: {str(e)}"
                            )

                thread = threading.Thread(target=async_sync_all)
                thread.daemon = True
                thread.start()

                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

            # 构建歌曲查重索引
            try:
                self._build_song_index()
                logger.info("歌曲查重索引初始化完成")
            except Exception as e:
                logger.error(f"初始化歌曲索引失败: {e}")

            if self._enabled:

                for mon_path, dir_info in self._dirconf.items():
                    target_path = dir_info.get("target")
                    transfer_type = dir_info.get("transfer_type")

                    # 启用目录监控
                    try:
                        # 检查媒体库目录是不是下载目录的子目录
                        try:
                            if target_path and target_path.is_relative_to(Path(mon_path)):
                                logger.warning(
                                    f"目标目录 {target_path} 是监控目录 {mon_path} 的子目录，无法监控")
                                if hasattr(self, 'systemmessage'):
                                    self.systemmessage.put(
                                        f"{target_path} 是下载目录 {mon_path} 的子目录，无法监控")
                                continue
                        except Exception as e:
                            logger.debug(f"检查目录关系时出错: {str(e)}")
                            pass

                        # 根据模式创建不同的观察者
                        if self._mode == "compatibility":
                            # 兼容模式，目录同步性能降低且NAS不能休眠，但可以兼容挂载的远程共享目录如SMB
                            observer = PollingObserver(timeout=10)
                        else:
                            # 内部处理系统操作类型选择最优解
                            observer = Observer(timeout=10)

                        self._observer.append(observer)
                        observer.schedule(MusicFileMonitorHandler(
                            mon_path, self), path=mon_path, recursive=True)
                        observer.daemon = True
                        observer.start()

                        mode_text = "性能模式" if self._mode == "fast" else "兼容模式"
                        logger.info(f"音乐文件监控服务已启动 ({mode_text}): {mon_path}")
                    except Exception as e:
                        logger.error(f"{mon_path} 启动音乐文件监控失败：{str(e)}")
                        if hasattr(self, 'systemmessage'):
                            self.systemmessage.put(
                                f"{mon_path} 启动音乐文件监控失败：{str(e)}")

            # 添加定时任务
            if self._enabled_scheduler and self._cron:
                try:
                    self._scheduler.add_job(name="音乐文件定时整理",
                                            func=self.sync_all,
                                            trigger=CronTrigger.from_crontab(
                                                self._cron)
                                            )
                except Exception as e:
                    logger.error(f"添加定时任务失败: {e}")

            # 启动定时服务
            if self._scheduler.get_jobs():
                self._scheduler.start()
            elif not self._enabled:
                # 如果插件被禁用且没有定时任务，则不启动定时服务
                pass

    def __update_config(self):
        """
        更新配置
        """
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "require_tags": self._require_tags,
            "clear_history": self._clear_history,
            "monitor_dirs": self._monitor_dirs,
            "folder_rule": self._folder_rule,
            "filename_rule": self._filename_rule,
            "transfer_type": self._transfer_type,
            "conflict_strategy": self._conflict_strategy,
            "mode": self._mode,
            "cron": self._cron,
            "enabled_scheduler": self._enabled_scheduler,
            "batch_notify": self._batch_notify,
            "batch_delay": self._batch_delay,
            "send_events": self._send_events,
        })

    def _get_artist_name(self, result: Dict) -> str:
        """
        从处理结果中提取歌手名
        :param result: 文件处理结果
        :return: 歌手名
        """
        # 尝试从标签信息中获取歌手名
        record = result.get('record', {})

        # 首先尝试从record的根级别获取artist字段
        artist = record.get('artist')
        if artist:
            return str(artist)

        # 如果根级别没有，再尝试从tags字段获取（兼容旧版本）
        tags = record.get('tags', {})
        artist = tags.get('artist')
        if artist:
            return str(artist)

        # 尝试从文件名中提取歌手名（假设格式为 "歌手 - 歌曲名"）
        filename = record.get('filename', '')
        if ' - ' in filename:
            return filename.split(' - ')[0].strip()

        # 如果无法识别，归为"未知歌手"
        return "未知歌手"

    def _add_batch_result(self, result: Dict):
        """
        添加批量处理结果（基于TV聚合模式）
        同一歌手的歌曲在时间窗口内聚合发送
        :param result: 单个文件处理结果
        """
        if not self._batch_notify or not self._notify:
            return

        # 如果聚合时间为0，直接发送单个通知而不聚合
        if self._batch_delay <= 0:
            self._send_single_notify(result)
            return

        # 对所有文件都进行聚合通知，包括失败的文件（保持原有通知样式）
        # 检查文件名，从record字段中获取
        filename = result.get('filename') or result.get(
            'record', {}).get('filename')
        if not filename:
            logger.debug("聚合通知跳过：缺少文件名")
            return

        artist_name = self._get_artist_name(result)
        logger.debug(f"从文件中提取的歌手名: {artist_name}")

        # 如果歌手名为空或未知歌手，使用默认分组
        if not artist_name or artist_name == "未知歌手":
            artist_name = "未知歌手"

        # 使用非阻塞锁避免死锁
        if not self._batch_lock.acquire(timeout=5):
            logger.warning("获取聚合结果添加锁超时，跳过聚合处理")
            return

        try:
            # 参数校验
            if not artist_name:
                logger.warning("无效的歌手名")
                return

            # 初始化该歌手的处理结果列表
            if artist_name not in self._artist_batch_results:
                self._artist_batch_results[artist_name] = []

            # 添加结果到对应歌手
            self._artist_batch_results[artist_name].append(result)

            # 如果已经有定时器，取消它并重新设置（类似TV聚合模式）
            if artist_name in self._artist_timers:
                try:
                    self._artist_timers[artist_name].cancel()
                except Exception as e:
                    logger.debug(f"取消定时器时出错: {str(e)}")

            # 设置新的聚合时间窗口（使用TV聚合的时间窗口概念）
            logger.debug(
                f"为歌手 {artist_name} 设置新的聚合时间窗口，将在 {self._batch_delay} 秒后触发")
            try:
                timer = threading.Timer(
                    self._batch_delay, self._send_artist_batch_notify, [artist_name])
                self._artist_timers[artist_name] = timer
                timer.daemon = True
                timer.start()
            except Exception as e:
                logger.error(f"设置定时器时出错: {str(e)}")
                # 如果定时器设置失败，直接发送消息
                self._send_artist_batch_notify(artist_name)

            logger.debug(
                f"已添加歌曲到歌手 {artist_name} 的聚合队列，当前队列长度: {len(self._artist_batch_results[artist_name])}，聚合时间窗口将在 {self._batch_delay} 秒后触发")

        except Exception as e:
            logger.error(f"聚合处理过程中出现异常: {str(e)}", exc_info=True)
        finally:
            self._batch_lock.release()

    def _send_artist_batch_notify(self, artist_name: str):
        """
        发送按歌手分组的聚合通知（基于TV聚合模式）
        :param artist_name: 歌手名
        """
        # 使用非阻塞锁避免死锁
        if not self._batch_lock.acquire(timeout=5):
            logger.warning(f"获取歌手 {artist_name} 的聚合通知锁超时，跳过发送")
            return

        try:
            logger.debug(f"定时器触发，准备发送歌手 {artist_name} 的聚合消息")

            # 检查通知配置
            logger.debug(
                f"通知配置检查: notify={self._notify}, batch_notify={self._batch_notify}")
            if not self._notify or not self._batch_notify:
                logger.debug(f"通知未启用或聚合通知未启用，跳过发送")
                # 清理队列和定时器
                self._artist_batch_results.pop(artist_name, None)
                self._artist_timers.pop(artist_name, None)
                return

            # 获取该歌手的待处理消息
            if artist_name not in self._artist_batch_results or not self._artist_batch_results[artist_name]:
                # 清除定时器引用
                self._artist_timers.pop(artist_name, None)
                return

            results = self._artist_batch_results.pop(artist_name)
            # 清除定时器引用
            self._artist_timers.pop(artist_name, None)

            # 构造聚合消息
            if not results:
                return

            # 使用统一通知方法
            self._send_unified_notify(artist_name=artist_name, results=results)

        except Exception as e:
            logger.error(f"发送聚合消息时出现异常: {str(e)}", exc_info=True)
        finally:
            self._batch_lock.release()

    def _send_all_pending_notifies(self):
        """
        发送所有待处理的聚合通知（在插件停止或任务完成时调用）
        """
        # 使用非阻塞锁避免死锁
        if not self._batch_lock.acquire(timeout=5):
            logger.warning("获取聚合通知锁超时，跳过发送剩余通知")
            return

        try:
            # 获取所有有未发送结果的歌手列表
            artists_with_results = list(self._artist_batch_results.keys())
            logger.debug(f"发现 {len(artists_with_results)} 个歌手有待处理的聚合通知")

            for artist_name in artists_with_results:
                if artist_name in self._artist_batch_results and self._artist_batch_results[artist_name]:
                    logger.debug(
                        f"准备发送歌手 {artist_name} 的聚合通知，队列长度: {len(self._artist_batch_results[artist_name])}")

                    # 取消该歌手的定时器，避免与立即发送冲突
                    if artist_name in self._artist_timers:
                        try:
                            self._artist_timers[artist_name].cancel()
                            logger.debug(f"已取消歌手 {artist_name} 的定时器")
                        except Exception as e:
                            logger.debug(f"取消定时器时出错: {str(e)}")

                    # 直接发送聚合通知，避免锁冲突
                    try:
                        if artist_name in self._artist_batch_results and self._artist_batch_results[artist_name]:
                            results = self._artist_batch_results.pop(
                                artist_name)
                            # 清除定时器引用
                            self._artist_timers.pop(artist_name, None)

                            # 使用统一通知方法直接发送
                            if results:
                                self._send_unified_notify(
                                    artist_name=artist_name, results=results)
                                logger.debug(f"歌手 {artist_name} 的聚合通知发送成功")
                    except Exception as e:
                        logger.error(f"发送歌手 {artist_name} 的聚合通知时出错: {str(e)}")
                        # 继续处理其他歌手，不中断整个流程
        finally:
            self._batch_lock.release()

    def _send_single_notify(self, result: Dict):
        """
        发送单个文件通知
        :param result: 文件处理结果
        """
        if not self._notify:
            return

        # 获取歌手名用于统一格式
        artist_name = self._get_artist_name(result)

        # 使用统一通知方法
        self._send_unified_notify(artist_name=artist_name, results=[result])

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        """
        远程同步
        """
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "music_sync":
                return

            # 直接执行同步，不发送开始和完成的通知
            self.sync_all()

    def sync_all(self):
        """立即运行一次，全量同步目录中所有音频文件"""
        logger.info("开始全量整理音乐文件监控目录...")
        total_files = 0

        # 记录需要清理的目录
        dirs_to_clean = set()

        # 获取所有监控目录的路径（用于避免删除监控目录）
        monitor_paths = [Path(mon_path) for mon_path in self._dirconf.keys()]

        for mon_path in self._dirconf.keys():
            # 获取音频文件进行处理
            audio_files = SystemUtils.list_files(
                Path(mon_path), list(self.AUDIO_EXTENSIONS))
            file_count = len(audio_files)
            total_files += file_count

            if file_count == 0:
                continue

            for file_path in audio_files:
                # 全量整理模式立即执行，不进行延迟处理
                result = self._organize_single_file(file_path, mon_path)

                # 检查result是否有效
                if result is None or not isinstance(result, dict) or 'success' not in result or 'record' not in result or result['record'] is None:
                    logger.error(f"文件处理返回无效结果: {file_path.name}")
                    # 创建失败记录
                    failed_record = {
                        'filename': file_path.name,
                        'artist': '',
                        'album': '',
                        'title': '',
                        'year': '',
                        'source_path': str(file_path),
                        'target_path': '',
                        'status': 'failed',
                        'transfer_type': 'unknown',
                        'file_size': self._get_file_size(str(file_path)),
                        'reason': '文件处理返回无效结果',
                        'has_lyrics': False,
                        'has_cover': False
                    }
                    self._add_organize_history(failed_record)

                    # 发送失败通知
                    if self._notify:
                        failed_result = {
                            'success': False,
                            'record': failed_record
                        }
                        if self._batch_notify:
                            self._add_batch_result(failed_result)
                        else:
                            self._send_single_notify(failed_result)

                    continue

                # 检查是否已有该文件的历史记录
                source_path = str(file_path)
                history = self.get_data('organize_history') or []
                existing_record = None

                for record in history:
                    if record.get('source_path') == source_path:
                        existing_record = record
                        break

                if existing_record:
                    # 使用完整记录替换现有记录
                    self._replace_organize_history(
                        source_path=source_path,
                        new_record=result.get('record', {})
                    )
                    logger.debug(f"已替换文件历史记录: {file_path.name}")
                else:
                    # 创建新记录
                    self._add_organize_history(result['record'])
                    logger.info(f"已创建文件历史记录: {file_path.name}")

                # 处理通知逻辑 - 对成功和需要刮削的文件都发送通知
                if self._notify:
                    if self._batch_notify:
                        # 聚合通知模式
                        logger.debug(
                            f"准备将文件添加到聚合队列: {file_path.name}, 成功: {result.get('success')}")
                        self._add_batch_result(result)
                    else:
                        # 单个通知模式
                        self._send_single_notify(result)

                if result['success']:
                    logger.info(
                        f"音频文件处理完成: {file_path.name} -> {result['target_path']}")
                else:
                    logger.warning(f"音频文件整理失败: {file_path.name}")

                # 记录文件所在的父目录，用于后续清理（排除监控目录）
                if file_path.parent not in monitor_paths:
                    dirs_to_clean.add(file_path.parent)

        # 全量整理完成后，清理所有涉及的空文件夹
        for dir_path in dirs_to_clean:
            self._clean_empty_dirs(dir_path)

        # 如果启用了聚合通知，确保发送所有剩余结果
        if self._batch_notify and self._notify:
            logger.debug("开始发送所有待处理的聚合通知")
            self._send_all_pending_notifies()
            logger.debug("聚合通知发送完成")

        logger.info(f"全量整理完成，共处理 {total_files} 个文件")

        # 如果没有处理任何文件，发送通知告知用户
        if total_files == 0 and self._notify:
            self._send_unified_notify(
                title="音乐文件整理完成",
                text="没有要整理的文件，监控目录中未发现可处理的音频文件"
            )

    def handle_file(self, event_path: str, mon_path: str, event_type: str = "unknown"):
        """
        生产级文件处理流程：延迟 + 文件大小稳定检测 + 可移动性校验 + 增强重复调度防护
        """
        try:
            file_path = Path(event_path)
            if file_path.is_dir():
                return

            file_ext = file_path.suffix.lower()

            # 只处理音频文件
            if file_ext in self.AUDIO_EXTENSIONS:
                file_key = str(file_path)  # 使用完整路径作为唯一标识

                # Step 1: 增强重复调度防护 - 添加事件类型和时间戳检查
                current_time = datetime.datetime.now()

                with lock:
                    # 检查是否已在处理中
                    if file_key in self._processing_files:
                        # 减少重复事件的日志输出，避免日志泛滥
                        return

                    # 检查最近处理记录（防止短时间内重复触发）
                    # 这里可以添加时间窗口检查，比如5分钟内不重复处理同一文件

                    # 添加到处理集合
                    self._processing_files.add(file_key)

                # 延迟启动处理（避免立即处理）
                import threading

                def process_file():
                    """生产级文件处理流程"""
                    try:
                        # Step 2: 延迟启动检测（避免size=0）- 默认5秒延迟
                        import time
                        time.sleep(5)

                        # 延迟后立即检查文件是否存在（可能已被其他线程处理）
                        if not file_path.exists():
                            return

                        # Step 3: 文件大小稳定检测
                        stable_count = 0
                        last_size = -1
                        max_attempts = 30  # 最大尝试次数（60秒超时）

                        for attempt in range(max_attempts):
                            try:
                                if not file_path.exists():
                                    return

                                current_size = file_path.stat().st_size

                                if current_size > 0 and current_size == last_size:
                                    stable_count += 1
                                else:
                                    stable_count = 0 if current_size != last_size else stable_count
                                    last_size = current_size

                                # 达到稳定条件
                                if stable_count >= 3:
                                    break

                                # 等待2秒后继续检查
                                time.sleep(2)

                            except (OSError, PermissionError):
                                time.sleep(2)

                        # 检查是否超时
                        if stable_count < 3:
                            logger.debug(f"文件大小稳定检测超时: {file_path.name}")
                            return

                        # 再次检查文件是否存在（可能在稳定检测期间被处理）
                        if not file_path.exists():
                            return

                        # Step 4: 可移动性校验（防止文件锁）
                        try:
                            # 尝试重命名到临时文件
                            temp_path = file_path.with_suffix(
                                file_path.suffix + '.tmp')
                            file_path.rename(temp_path)
                            # 重命名回来
                            temp_path.rename(file_path)
                        except (OSError, PermissionError) as e:
                            logger.debug(f"文件仍在被占用，无法移动: {file_path.name}")
                            return

                        # Step 5: 正式处理文件
                        result = self._organize_single_file(
                            file_path, mon_path)

                        # 检查result是否有效
                        if result is None or not isinstance(result, dict):
                            logger.error(f"文件处理返回无效结果: {file_path.name}")
                            # 创建失败记录
                            self._add_organize_history({
                                'filename': file_path.name,
                                'artist': '',
                                'album': '',
                                'title': '',
                                'year': '',
                                'source_path': str(file_path),
                                'target_path': '',
                                'status': 'failed',
                                'transfer_type': 'unknown',
                                'file_size': self._get_file_size(str(file_path)),
                                'reason': '文件处理返回无效结果',
                                'has_lyrics': False,
                                'has_cover': False
                            })
                            return

                        # 确保result中包含必要的键
                        if 'success' not in result or 'record' not in result or result['record'] is None:
                            logger.error(f"文件处理结果不完整: {file_path.name}")
                            # 创建失败记录
                            self._add_organize_history({
                                'filename': file_path.name,
                                'artist': '',
                                'album': '',
                                'title': '',
                                'year': '',
                                'source_path': str(file_path),
                                'target_path': '',
                                'status': 'failed',
                                'transfer_type': 'unknown',
                                'file_size': self._get_file_size(str(file_path)),
                                'reason': '文件处理结果不完整',
                                'has_lyrics': False,
                                'has_cover': False
                            })
                            return

                        # 检查是否已有该文件的历史记录
                        source_path = str(file_path)
                        history = self.get_data('organize_history') or []
                        existing_record = None

                        for record in history:
                            if record.get('source_path') == source_path:
                                existing_record = record
                                break

                        if existing_record:
                            # 使用完整记录替换现有记录
                            self._replace_organize_history(
                                source_path=source_path,
                                new_record=result.get('record', {})
                            )
                            logger.debug(f"已替换文件历史记录: {file_path.name}")
                        else:
                            # 创建新记录
                            self._add_organize_history(result['record'])
                            logger.info(f"已创建文件历史记录: {file_path.name}")

                        # 处理通知逻辑 - 对成功和需要刮削的文件都发送通知
                        if self._notify:
                            if self._batch_notify:
                                # 聚合通知模式
                                self._add_batch_result(result)
                            else:
                                # 单个通知模式
                                self._send_single_notify(result)

                        if result['success']:
                            logger.info(
                                f"音频文件处理完成: {file_path.name} -> {result['target_path']}")
                        else:
                            logger.warning(f"音频文件整理失败: {file_path.name}")

                    except Exception as e:
                        logger.error(f"文件处理流程出错: {file_path.name}, 错误: {e}")
                    finally:
                        # 无论成功失败，都从处理集合中移除
                        with lock:
                            if file_key in self._processing_files:
                                self._processing_files.remove(file_key)

                # 启动处理线程
                thread = threading.Thread(target=process_file)
                thread.daemon = True
                thread.start()

        except Exception as e:
            logger.error(f"处理文件事件时出错: {event_path}, 错误: {e}")
            # 异常情况下也要清理状态
            try:
                file_path = Path(event_path)
                file_key = str(file_path)
                with lock:
                    if file_key in self._processing_files:
                        self._processing_files.remove(file_key)
            except:
                pass

    def __handle_audio_file(self, audio_path: Path, mon_path: str):
        """
        直接处理音频文件：读取元数据并按规则整理
        :param audio_path: 音频文件路径
        :param mon_path: 监控目录
        :return: 返回字典格式的处理结果，包含'success', 'target_path'和'record'字段
        """
        try:
            logger.info(f"开始处理音频文件: {audio_path.name}")

            # 再次检查文件是否存在且大小大于0
            if not audio_path.exists():
                logger.debug(f"音频文件不存在，跳过处理: {audio_path}")
                return {
                    'success': False,
                    'target_path': '',
                    'record': {
                        'filename': audio_path.name,
                        'artist': '',
                        'album': '',
                        'title': '',
                        'year': '',
                        'source_path': str(audio_path),
                        'target_path': '',
                        'status': 'failed',
                        'transfer_type': 'unknown',
                        'file_size': 0,
                        'reason': '文件不存在',
                        'has_lyrics': False,
                        'has_cover': False
                    }
                }

            try:
                file_size = audio_path.stat().st_size
                if file_size == 0:
                    logger.debug(f"音频文件为空，跳过处理: {audio_path}")
                    return {
                        'success': False,
                        'target_path': '',
                        'record': {
                            'filename': audio_path.name,
                            'artist': '',
                            'album': '',
                            'title': '',
                            'year': '',
                            'source_path': str(audio_path),
                            'target_path': '',
                            'status': 'failed',
                            'transfer_type': 'unknown',
                            'file_size': 0,
                            'reason': '文件为空',
                            'has_lyrics': False,
                            'has_cover': False
                        }
                    }
                logger.debug(f"文件大小: {file_size / 1024 / 1024:.2f} MB")
            except (OSError, PermissionError) as e:
                logger.debug(f"无法访问文件状态，跳过处理: {audio_path}, 错误: {e}")
                return {
                    'success': False,
                    'target_path': '',
                    'record': {
                        'filename': audio_path.name,
                        'artist': '',
                        'album': '',
                        'title': '',
                        'year': '',
                        'source_path': str(audio_path),
                        'target_path': '',
                        'status': 'failed',
                        'transfer_type': 'unknown',
                        'file_size': 0,
                        'reason': f'无法访问文件状态: {str(e)}',
                        'has_lyrics': False,
                        'has_cover': False
                    }
                }

            # 1. 记录音频文件原始元数据
            self._log_audio_metadata(audio_path)

            # 2. 处理音频文件（按原规则整理）
            result = self._organize_single_file(audio_path, mon_path)

            if not result['success']:
                logger.warning(f"音频文件整理失败: {audio_path.name}")
                return result

            # 处理通知逻辑 - 对成功和需要刮削的文件都发送通知
            if self._notify:
                if self._batch_notify:
                    # 聚合通知模式
                    self._add_batch_result(result)
                else:
                    # 单个通知模式
                    self._send_single_notify(result)

            logger.info(
                f"音频文件处理完成: {audio_path.name} -> {result['target_path']}")
            return result

        except Exception as e:
            logger.error(
                f"处理音频文件时发生严重错误: {audio_path}, 错误: {e}, 堆栈: {traceback.format_exc()}")
            return {
                'success': False,
                'target_path': '',
                'record': {
                    'filename': audio_path.name,
                    'artist': '',
                    'album': '',
                    'title': '',
                    'year': '',
                    'source_path': str(audio_path),
                    'target_path': '',
                    'status': 'failed',
                    'transfer_type': 'unknown',
                    'file_size': 0,
                    'reason': f'处理异常: {str(e)}',
                    'has_lyrics': False,
                    'has_cover': False
                }
            }

    def _detect_audio_format(self, audio_path: Path) -> str:
        """
        根据文件扩展名检测音频格式
        :param audio_path: 音频文件路径
        :return: 音频格式字符串（MP3、FLAC等）
        """
        ext = audio_path.suffix.lower()

        if ext == '.mp3':
            return "MP3"
        elif ext == '.flac':
            return "FLAC"
        elif ext in {'.ogg', '.opus'}:
            return "OGG"
        elif ext in {'.m4a', '.mp4'}:
            return "M4A"
        elif ext in {'.wav'}:
            return "WAV"
        elif ext in {'.wma'}:
            return "WMA"
        elif ext in {'.aac'}:
            return "AAC"
        elif ext in {'.ape'}:
            return "APE"
        else:
            return "UNKNOWN"

    def _get_format_tag_names(self, audio_format: str, tag_key: str) -> List[str]:
        """
        根据音频格式和标签键获取可能的标签名称列表
        :param audio_format: 音频格式（MP3、FLAC等）
        :param tag_key: 标准标签键（title、artist等）
        :return: 可能的标签名称列表
        """
        tag_names = []

        # 获取格式特定的标签名
        format_mapping = self.AUDIO_TAG_MAPPING.get(audio_format.upper(), {})
        if tag_key in format_mapping:
            tag_names.append(format_mapping[tag_key])

        # 添加通用标签名和常见变体
        common_variants = {
            "title": ["title", "TITLE", "TIT2", "TIT1", "©nam"],
            "artist": ["artist", "ARTIST", "TPE1", "©ART"],
            "album": ["album", "ALBUM", "TALB", "TOAL", "©alb"],
            "date": ["date", "DATE", "TDRC", "TYER", "TDAT", "TIME", "YEAR", "©day"],
            "tracknumber": ["tracknumber", "TRACKNUMBER", "TRCK", "track", "trkn"],
            "discnumber": ["discnumber", "DISCNUMBER", "TPOS", "disc", "disk"],
            "genre": ["genre", "GENRE", "TCON", "©gen"],
            "albumartist": ["albumartist", "ALBUMARTIST", "TPE2", "aART"],
            "composer": ["composer", "COMPOSER", "TCOM", "©wrt"],
            "lyricist": ["lyricist", "LYRICIST", "writer", "TEXT", "TOLY", "©lyr"],
            "description": ["description", "DESCRIPTION", "comment", "COMM", "DESC", "©cmt"],
            "lyrics": ["lyrics", "LYRICS", "USLT", "unsynchronised_lyrics", "synchronised_lyrics", "SYLT", "UNSYNCEDLYRICS"],
            "cover": ["APIC", "covr", "cover", "metadata_block_picture", "picture", "PIC", "coverart", "albumart"]
        }

        # 添加常见变体，但避免重复
        if tag_key in common_variants:
            for variant in common_variants[tag_key]:
                if variant not in tag_names:
                    tag_names.append(variant)

        return tag_names

    def _get_audio_tag(self, audio, tag_name: str, default: str, audio_path: Path = None) -> str:
        """
        修复版：安全地获取音频标签，使用标准化的标签名映射
        :param audio: 音频文件对象
        :param tag_name: 标准标签名称（如 'title', 'artist'）
        :param default: 默认值
        :param audio_path: 音频文件路径（用于确定格式）
        :return: 标签值
        """
        try:
            # 如果没有提供音频路径，尝试获取文件扩展名确定格式
            audio_format = "UNKNOWN"
            if audio_path:
                audio_format = self._detect_audio_format(audio_path)

            # 获取该格式的可能标签名列表
            tag_names = self._get_format_tag_names(audio_format, tag_name)

            # 尝试从第一个有效标签获取值
            for name in tag_names:
                value = audio.get(name)
                if not value:
                    continue

                # --- 修复核心：优先处理 ID3 Frame (MP3) ---
                # mutagen 的 ID3 Frame (如 TIT2) 不是 list，但可以被 str() 转换，或包含 text 属性
                if hasattr(value, 'text'):
                    # 取 text 列表的第一个元素
                    result = str(value.text[0]).strip(
                    ) if value.text else default
                    if result and result != default:
                        return result
                    continue

                # --- 处理列表类型 (FLAC, OGG, APE 等) ---
                if isinstance(value, list) and len(value) > 0:
                    tag_value = value[0]

                    # 如果是字节类型，尝试多种编码
                    if isinstance(tag_value, bytes):
                        # 优先使用GB18030
                        encodings = ['gb18030', 'gbk',
                                     'gb2312', 'utf-8', 'utf-16', 'big5']
                        for encoding in encodings:
                            try:
                                decoded = tag_value.decode(encoding)
                                if any('一' <= char <= '鿿' for char in decoded):
                                    logger.debug(
                                        f"成功使用 {encoding} 解码: {decoded}")
                                return decoded.strip()
                            except UnicodeDecodeError:
                                continue
                        # 自动检测失败后的兜底
                        return tag_value.decode('utf-8', errors='ignore').strip()

                    # 如果列表内容已经是字符串
                    result = str(tag_value).strip()
                    if result:
                        return result

                # --- 兜底：直接转字符串 ---
                # 应对其他未知的 mutagen 对象类型
                result = str(value).strip()
                if result:
                    return result

            return default

        except Exception as e:
            logger.debug(f"读取标签 {tag_name} 失败: {e}")
        return default

    def _check_cover_tags(self, audio) -> bool:
        """
        检查音频文件是否包含封面信息
        :param audio: 音频文件对象
        :return: 包含封面返回True，否则返回False
        """
        try:
            # 对于MP3文件 (ID3格式)
            if hasattr(audio.tags, 'getall'):
                # MP3使用APIC标签
                apic_frames = audio.tags.getall('APIC')
                if apic_frames:
                    logger.debug(
                        f"发现MP3封面标签: APIC = 图片数据[{len(apic_frames)}帧]")
                    return True

                # 同时检查其他可能的封面标签
                for tag in ['covr', 'cover', 'picture', 'PIC']:
                    cover_value = audio.tags.get(tag)
                    if cover_value and str(cover_value).strip():
                        logger.debug(f"发现封面标签: {tag}")
                        return True

            # 对于FLAC等格式 (Vorbis评论)
            elif hasattr(audio, 'pictures'):
                if audio.pictures:
                    logger.debug(
                        f"发现FLAC封面标签: pictures = 图片数据[{len(audio.pictures)}张]")
                    return True

            # 对于OGG文件 (Vorbis格式)
            # OGG文件通常使用METADATA_BLOCK_PICTURE标签，但需要检查特定格式
            if hasattr(audio.tags, 'items'):
                for key, value in audio.tags.items():
                    key_lower = key.lower()
                    # 检查所有可能的封面标签，包括OGG特定的标签
                    if key_lower in ['metadata_block_picture', 'cover', 'coverart', 'albumart', 'pic', 'apic', 'covr',
                                     'covert art', 'covertart', 'covert_art', 'covert-art', 'covert art data', 'covert art file']:
                        if value and str(value).strip():
                            logger.debug(f"发现通用封面标签: {key}")
                            return True

                    # 专门检查OGG格式的封面标签（Vorbis评论格式）
                    if key_lower in ['metadata_block_picture']:
                        # 对于OGG文件，METADATA_BLOCK_PICTURE可能包含Base64编码的图片数据
                        if value and isinstance(value, str) and len(value.strip()) > 0:
                            logger.debug(
                                f"发现OGG封面标签: {key} = 数据长度{len(value)}")
                            return True
        except Exception as e:
            logger.debug(f"检查封面信息时出错: {e}")

        return False

    def _check_lyrics_tags(self, audio) -> bool:
        """
        检查音频文件是否包含歌词信息
        :param audio: 音频文件对象
        :return: 包含歌词返回True，否则返回False
        """
        try:
            # 对于MP3文件 (ID3格式)
            if hasattr(audio.tags, 'getall'):
                # MP3使用USLT标签（非同步歌词）
                uslt_frames = audio.tags.getall('USLT')
                if uslt_frames:
                    logger.debug(
                        f"发现MP3歌词标签: USLT = 歌词数据[{len(uslt_frames)}帧]")
                    return True

                # 同时检查SYLT标签（同步歌词）
                sylt_frames = audio.tags.getall('SYLT')
                if sylt_frames:
                    logger.debug(f"发现MP3同步歌词标签: SYLT")
                    return True

                # 检查其他可能的歌词标签
                for tag in ['TEXT', 'TOLY', 'lyrics', 'LYRICS']:
                    lyrics_value = audio.tags.get(tag)
                    if lyrics_value and str(lyrics_value).strip():
                        logger.debug(f"发现歌词标签: {tag}")
                        return True

            # 对于FLAC等格式 (Vorbis评论)
            if hasattr(audio.tags, 'items'):
                for key, value in audio.tags.items():
                    key_lower = key.lower()
                    # 检查所有可能的歌词标签
                    if key_lower in ['lyrics', 'unsynchronised_lyrics', 'synchronised_lyrics', 'uslt', 'sylt']:
                        if value and str(value).strip():
                            logger.debug(f"发现通用歌词标签: {key}")
                            return True
        except Exception as e:
            logger.debug(f"检查歌词信息时出错: {e}")

        return False

    def _check_required_tags(self, audio, title: str, artist: str, album: str, year: str, audio_path: Path) -> bool:
        """
        检查必须的标签信息是否完整
        :param audio: 音频文件对象
        :param title: 歌曲标题
        :param artist: 艺术家信息
        :param album: 专辑信息
        :param year: 年份信息
        :param audio_path: 音频文件路径
        :return: 标签完整返回True，否则返回False
        """
        missing_tags = []

        # 检查标题信息（不能是空值）
        if title in ['']:
            missing_tags.append("标题")

        # 检查艺术家信息
        if artist in ['未知艺术家', '']:
            missing_tags.append("艺术家")

        # 检查专辑信息
        if album in ['未知专辑', '']:
            missing_tags.append("专辑")

        # 检查年份信息
        if year in ['', '无']:
            missing_tags.append("年份")

        # 检查封面信息（必须包含封面）
        has_cover = self._check_cover_tags(audio)
        if not has_cover:
            missing_tags.append("封面")

        # 检查歌词信息（必须包含歌词）
        has_lyrics = self._check_lyrics_tags(audio)
        if not has_lyrics:
            missing_tags.append("歌词")

        if missing_tags:
            missing_text = "、".join(missing_tags)
            logger.warning(f"音频文件缺少必要的标签信息：{missing_text}，跳过整理")
            return False

        logger.info("✓ 音频文件标签信息完整：包含标题、艺术家、专辑、封面、年份、歌词数据")
        return True

    def _organize_single_file(self, file_path: Path, mon_path: str):
        """
        整理单个文件的核心操作
        :param file_path: 文件路径
        :param mon_path: 监控目录
        :return: 成功则返回目标路径，失败返回False
        """
        try:
            # 1. 读取元数据 (mutagen)
            audio = None
            max_retries = 3
            retry_delay = 0.5  # 0.5秒

            for attempt in range(max_retries):
                try:
                    audio = File(file_path)
                    if audio is None:
                        logger.error(f"无法读取音频文件: {file_path}")
                        return {
                            'success': False,
                            'target_path': '',
                            'record': {
                                'filename': file_path.name,
                                'artist': '',
                                'album': '',
                                'title': '',
                                'year': '',
                                'source_path': str(file_path),
                                'target_path': '',
                                'status': 'failed',
                                'transfer_type': 'unknown',
                                'file_size': 0,
                                'reason': '无法读取音频文件',
                                'has_lyrics': False,
                                'has_cover': False
                            }
                        }

                    # 尝试读取一个简单的标签来验证文件是否可读
                    test_tag = self._get_audio_tag(
                        audio, 'title', '', file_path)
                    if test_tag:
                        break  # 文件可读，跳出重试循环

                except Exception as e:
                    logger.debug(f"第{attempt+1}次读取元数据失败: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.error(f"读取元数据失败: {file_path}, {e}")
                        return {
                            'success': False,
                            'target_path': '',
                            'record': {
                                'filename': file_path.name,
                                'artist': '',
                                'album': '',
                                'title': '',
                                'year': '',
                                'source_path': str(file_path),
                                'target_path': '',
                                'status': 'failed',
                                'transfer_type': 'unknown',
                                'file_size': 0,
                                'reason': f'读取元数据失败: {str(e)}',
                                'has_lyrics': False,
                                'has_cover': False
                            }
                        }

            if audio is None:
                logger.error(f"无法读取音频文件: {file_path}")
                return {
                    'success': False,
                    'target_path': '',
                    'record': {
                        'filename': file_path.name,
                        'artist': '',
                        'album': '',
                        'title': '',
                        'year': '',
                        'source_path': str(file_path),
                        'target_path': '',
                        'status': 'failed',
                        'transfer_type': 'unknown',
                        'file_size': 0,
                        'reason': '无法读取音频文件',
                        'has_lyrics': False,
                        'has_cover': False
                    }
                }

            # 使用新的标准化标签获取方法
            artist = self._get_audio_tag(
                audio, 'artist', '未知艺术家', file_path)
            album = self._get_audio_tag(audio, 'album', '未知专辑', file_path)
            title = self._get_audio_tag(
                audio, 'title', file_path.stem, file_path)

            # 获取年份信息
            year = self._get_audio_tag(audio, 'date', '', file_path)

            # 获取歌词信息
            has_lyrics = self._check_lyrics_tags(audio)

            # 检查封面信息
            has_cover = self._check_cover_tags(audio)

            # 添加详细的调试信息
            logger.debug(f"歌词检测结果: {has_lyrics}")
            logger.debug(f"封面检测结果: {has_cover}")

            # 如果检测到歌词或封面，记录详细信息
            if has_lyrics:
                logger.info("✓ 检测到歌词信息")
            else:
                logger.info("✗ 未检测到歌词信息")

            if has_cover:
                logger.info("✓ 检测到封面信息")
            else:
                logger.info("✗ 未检测到封面信息")

            logger.info(
                f"读取音频元数据 - 艺术家: {artist}, 专辑: {album}, 歌曲名: {title}, 年份: {year if year else '无'}, 歌词: {'有' if has_lyrics else '无'}, 封面: {'有' if has_cover else '无'}")

            # 检查必须的标签信息是否完整（强制检查，无论是否启用标签验证）
            # 先检查标签是否完整，标签检查优先级高于查重
            tags_complete = self._check_required_tags(
                audio, title, artist, album, year, file_path)
            if not tags_complete:
                logger.debug(f"音频文件标签信息不完整，跳过整理: {file_path.name}")

                # 记录标签不完整的文件，准备发送事件
                self._handle_incomplete_tags_file(
                    file_path, artist, album, title, year)

                # 获取目录配置的转移方式
                dir_info = self._dirconf.get(mon_path, {})
                transfer_type = dir_info.get(
                    "transfer_type", self._transfer_type)

                # 记录标签信息不完整的失败记录
                record_data = {
                    'filename': file_path.name,
                    'artist': artist,
                    'album': album,
                    'title': title,
                    'year': year,
                    'source_path': str(file_path),
                    'target_path': '',
                    'status': 'failed',
                    'transfer_type': transfer_type,
                    'file_size': self._get_file_size(str(file_path)),
                    'reason': '标签缺失',
                    'has_lyrics': has_lyrics,
                    'has_cover': has_cover
                }
                # 不在这里添加历史记录，让上层调用者处理
                return {
                    'success': False,
                    'target_path': '',
                    'record': record_data
                }

            # 检查歌曲是否已经整理过（以歌曲名和歌手相同为准）
            is_organized, existing_record = self._is_song_already_organized(
                artist, title)
            if is_organized:
                logger.info(f"歌曲已整理过，跳过处理 - 歌手: {artist}, 歌曲名: {title}")

                # 获取目录配置的转移方式
                dir_info = self._dirconf.get(mon_path, {})
                transfer_type = dir_info.get(
                    "transfer_type", self._transfer_type)

                # 创建跳过记录
                skip_record = {
                    'filename': file_path.name,
                    'artist': artist,
                    'album': album,
                    'title': title,
                    'year': year,
                    'source_path': str(file_path),
                    'target_path': existing_record.get('target_path', '') if existing_record else '',
                    'status': 'skipped',
                    'transfer_type': transfer_type,
                    'file_size': self._get_file_size(str(file_path)),
                    'reason': '整理过',
                    'has_lyrics': has_lyrics,
                    'has_cover': has_cover
                }

                return {
                    'success': False,
                    'target_path': '',
                    'record': skip_record
                }

        except Exception as e:
            logger.error(f"读取元数据失败: {file_path}, {e}")
            return {
                'success': False,
                'target_path': '',
                'record': {
                    'filename': file_path.name,
                    'artist': '',
                    'album': '',
                    'title': '',
                    'year': '',
                    'source_path': str(file_path),
                    'target_path': '',
                    'status': 'failed',
                    'transfer_type': 'unknown',
                    'file_size': 0,
                    'reason': f'读取元数据失败: {str(e)}',
                    'has_lyrics': False,
                    'has_cover': False
                }
            }

        try:
            # 2. 清洗字符串 (移除非法字符)
            safe_artist = self._sanitize_filename(artist)
            safe_album = self._sanitize_filename(album)
            safe_title = self._sanitize_filename(title)

            # 3. 构建目标路径 (根据配置的规则)
            dir_info = self._dirconf.get(mon_path, {})
            target_root = dir_info.get("target")
            if not target_root:
                target_root = Path(mon_path) / "_Organized"

            # 根据文件夹规则构建文件夹结构
            safe_year = self._sanitize_filename(
                year) if year and year != "" else "未知年份"

            if self._folder_rule == "year_artist_album_title":
                # 年份/艺术家/专辑 文件夹结构
                target_dir = target_root / safe_year / safe_artist / safe_album
            elif self._folder_rule == "year_artist":
                # 年份/艺术家 文件夹结构
                target_dir = target_root / safe_year / safe_artist
            elif self._folder_rule == "artist_album":
                # 艺术家/专辑 文件夹结构
                target_dir = target_root / safe_artist / safe_album
            elif self._folder_rule == "artist":
                # 艺术家 文件夹结构
                target_dir = target_root / safe_artist
            elif self._folder_rule == "root":
                # 根目录 文件夹结构
                target_dir = target_root
            else:
                # 默认：艺术家/专辑 文件夹结构
                target_dir = target_root / safe_artist / safe_album

            target_dir.mkdir(parents=True, exist_ok=True)

            # 3.5 检查标签信息（不提取封面和歌词）

            # 4. 构建文件名 (根据文件命名规则)
            if self._filename_rule == "keep_original":
                # 保持不变，使用原始文件名
                base_filename = file_path.stem
            elif self._filename_rule == "title_artist":
                # 歌曲名-艺术家 格式
                if safe_artist != "未知艺术家":
                    base_filename = f"{safe_title} - {safe_artist}"
                else:
                    base_filename = safe_title
            elif self._filename_rule == "artist_album_title":
                # 艺术家-专辑-歌曲名 格式
                name_parts = []
                if safe_artist != "未知艺术家":
                    name_parts.append(safe_artist)
                if safe_album != "未知专辑":
                    name_parts.append(safe_album)
                name_parts.append(safe_title)
                base_filename = " - ".join(name_parts)
            else:
                # 默认：艺术家-歌曲名 格式
                if safe_artist != "未知艺术家":
                    base_filename = f"{safe_artist} - {safe_title}"
                else:
                    base_filename = safe_title

            new_filename = base_filename + file_path.suffix
            target_path = target_dir / new_filename

            # 5. 处理文件冲突 (跳过/覆盖)
            if target_path.exists() and self._conflict_strategy == "skip":
                logger.info(f"文件已存在，跳过处理: {target_path}")
                return {
                    'success': False,
                    'target_path': '',
                    'record': {
                        'filename': file_path.name,
                        'artist': artist,
                        'album': album,
                        'title': title,
                        'year': year,
                        'source_path': str(file_path),
                        'target_path': '',
                        'status': 'failed',
                        'transfer_type': transfer_type,
                        'file_size': self._get_file_size(str(file_path)),
                        'reason': '文件已存在，跳过处理',
                        'has_lyrics': has_lyrics,
                        'has_cover': has_cover
                    }
                }

            # 如果目标文件存在且策略为覆盖，先删除
            if target_path.exists() and self._conflict_strategy == "overwrite":
                try:
                    target_path.unlink()
                    logger.debug(f"已删除原有文件以进行覆盖: {target_path}")
                except Exception as e:
                    logger.error(f"删除原有文件失败，无法覆盖: {e}")
                    return {
                        'success': False,
                        'target_path': '',
                        'record': {
                            'filename': file_path.name,
                            'artist': artist,
                            'album': album,
                            'title': title,
                            'year': year,
                            'source_path': str(file_path),
                            'target_path': '',
                            'status': 'failed',
                            'transfer_type': transfer_type,
                            'file_size': self._get_file_size(str(file_path)),
                            'reason': f'删除原有文件失败: {e}',
                            'has_lyrics': has_lyrics,
                            'has_cover': has_cover
                        }
                    }

            # 6. 删除标签中的注释信息
            try:
                self._remove_comment_tags(audio)
            except Exception as e:
                logger.warning(f"删除标签注释信息失败，继续处理: {e}")

            # 7. 执行文件操作 (移动/复制/链接)
            transfer_type = dir_info.get("transfer_type", self._transfer_type)

            # 确保 transfer_type 不为 None
            if not transfer_type:
                transfer_type = "move"  # 默认使用移动操作

            # 在执行文件操作前获取文件大小
            file_size = self._get_file_size(str(file_path))

            operation_result = self._perform_file_operation(
                file_path, target_path, transfer_type)
            logger.debug(f"文件操作结果: {operation_result}, 转移方式: {transfer_type}")

            if operation_result:
                # 日志在 _perform_file_operation 方法中统一显示，这里不再重复显示
                # 返回一个字典，包含所有需要的历史记录信息
                return {
                    'success': True,
                    'target_path': str(target_path),
                    'record': {
                        'filename': file_path.name,
                        'artist': artist,
                        'album': album,
                        'title': title,
                        'year': year,
                        'source_path': str(file_path),
                        'target_path': str(target_path),
                        'status': 'success',
                        'transfer_type': transfer_type,
                        'file_size': file_size,
                        'has_lyrics': has_lyrics,
                        'has_cover': has_cover
                    }
                }

            # 返回失败信息
            return {
                'success': False,
                'target_path': '',
                'record': {
                    'filename': file_path.name,
                    'artist': artist,
                    'album': album,
                    'title': title,
                    'year': year,
                    'source_path': str(file_path),
                    'target_path': str(target_path),
                    'status': 'failed',
                    'transfer_type': transfer_type,
                    'file_size': file_size,
                    'reason': '文件操作失败',
                    'has_lyrics': has_lyrics,
                    'has_cover': has_cover
                }
            }

        except Exception as e:
            logger.error(f"整理文件时发生错误：{str(e)} - {traceback.format_exc()}")
            return {
                'success': False,
                'target_path': '',
                'record': {
                    'filename': file_path.name,
                    'artist': '',
                    'album': '',
                    'title': '',
                    'year': '',
                    'source_path': str(file_path),
                    'target_path': '',
                    'status': 'failed',
                    'transfer_type': 'unknown',
                    'file_size': 0,
                    'reason': f'整理异常: {str(e)}',
                    'has_lyrics': False,
                    'has_cover': False
                }
            }

    def _perform_file_operation(self, src_path: Path, dest_path: Path, transfer_type: str) -> bool:
        """
        执行文件操作（移动/复制/链接）
        """
        # 如果 transfer_type 为 None，直接返回 False，不进行操作
        if not transfer_type:
            logger.debug("transfer_type 为空，返回失败")
            return False

        action_map = {
            "move": ("移动", lambda s, d: shutil.move(str(s), str(d))),
            "copy": ("复制", lambda s, d: shutil.copy2(str(s), str(d))),
            "link": ("硬链接", lambda s, d: d.hardlink_to(s))
        }

        action, operation = action_map.get(
            transfer_type, ("移动", lambda s, d: shutil.move(str(s), str(d))))

        try:
            # 记录源文件所在目录，用于后续清理空文件夹
            src_parent_dir = src_path.parent

            # 确保目标目录存在
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # 执行文件操作前记录详细信息
            logger.debug(f"准备{action}文件: 源={src_path}, 目标={dest_path}")

            operation(src_path, dest_path)

            # 验证操作是否成功
            if not dest_path.exists():
                logger.error(f"文件操作后目标文件不存在: {dest_path}")
                raise Exception("文件操作后目标文件不存在")

            # 只显示最终操作类型，不显示具体转移方式
            logger.info(f"成功{action}: {src_path.name} -> {dest_path}")

            # 如果是移动操作，清理空文件夹
            if transfer_type == "move":
                # 确保源文件已成功移动（源文件不应再存在）
                if not src_path.exists():
                    self._clean_empty_dirs(src_parent_dir)
                else:
                    logger.warning(f"源文件仍然存在，跳过清理空文件夹: {src_path}")

            return True
        except Exception as e:
            logger.error(
                f"文件操作失败: {e}, 源文件: {src_path}, 目标文件: {dest_path}, 操作类型: {transfer_type}")
            return False

    def _log_audio_metadata(self, audio_path: Path):
        """
        安全记录音频原始元数据（使用标准化标签名映射）
        """
        try:
            audio = File(audio_path)
            if audio is None or not audio.tags:
                logger.debug(f"音频文件无可用元数据: {audio_path.name}")
                return

            tags_info = []

            # 检查文件格式，针对不同格式使用不同的标签名
            file_ext = audio_path.suffix.lower()

            if file_ext == '.mp3':
                # MP3格式使用ID3标签
                display_order = [
                    ('标题(TIT2)', 'TIT2'),
                    ('艺术家(TPE1)', 'TPE1'),
                    ('专辑(TALB)', 'TALB'),
                    ('年份(TDRC)', 'TDRC'),
                    ('音轨号(TRCK)', 'TRCK'),
                    ('碟号(TPOS)', 'TPOS'),
                    ('风格(TCON)', 'TCON'),
                    ('专辑艺术家(TPE2)', 'TPE2'),
                    ('作曲家(TCOM)', 'TCOM'),
                    ('作词家(TEXT)', 'TEXT'),  # MP3中TEXT是作词家
                    ('歌词(USLT)', 'USLT'),  # MP3中USLT是歌词内容
                    ('注释(COMM)', 'COMM'),
                    ('封面(APIC)', 'APIC')
                ]
            else:
                # 其他格式使用通用标签名
                display_order = [
                    ('标题(TITLE)', 'TITLE'),
                    ('艺术家(ARTIST)', 'ARTIST'),
                    ('专辑(ALBUM)', 'ALBUM'),
                    ('年份(DATE)', 'DATE'),
                    ('音轨号(TRACKNUMBER)', 'TRACKNUMBER'),
                    ('碟号(DISCNUMBER)', 'DISCNUMBER'),
                    ('风格(GENRE)', 'GENRE'),
                    ('专辑艺术家(ALBUMARTIST)', 'ALBUMARTIST'),
                    ('作曲家(COMPOSER)', 'COMPOSER'),
                    ('作词家(LYRICIST)', 'LYRICIST'),
                    ('注释(DESCRIPTION)', 'DESCRIPTION'),
                    ('歌词(LYRICS)', 'LYRICS'),
                    ('封面(METADATA_BLOCK_PICTURE)', 'METADATA_BLOCK_PICTURE')
                ]

            # 按照指定顺序获取并显示标签
            for display_name, tag_key in display_order:
                # 对于MP3的特殊处理（APIC和USLT）
                if file_ext == '.mp3' and tag_key in ['APIC', 'USLT']:
                    if tag_key == 'APIC':
                        # 检查封面
                        apic_frames = audio.tags.getall('APIC')
                        if apic_frames:
                            tags_info.append(
                                f"{display_name}: 图片数据[{len(apic_frames)}帧]")
                    elif tag_key == 'USLT':
                        # 检查歌词
                        uslt_frames = audio.tags.getall('USLT')
                        if uslt_frames:
                            tags_info.append(
                                f"{display_name}: 歌词数据[{len(uslt_frames)}帧]")
                    continue

                # 对于FLAC的特殊处理（METADATA_BLOCK_PICTURE）
                elif file_ext == '.flac' and tag_key == 'METADATA_BLOCK_PICTURE':
                    if hasattr(audio, 'pictures') and audio.pictures:
                        tags_info.append(
                            f"{display_name}: 图片数据[{len(audio.pictures)}张]")
                    continue

                # 对于其他标签使用常规方法
                value = self._get_audio_tag(audio, tag_key, "", audio_path)

                # 对于MP3文件，如果_get_audio_tag返回空值，尝试直接获取
                if not value and file_ext == '.mp3':
                    try:
                        # 对于MP3的ID3标签，尝试多种获取方式
                        direct_value = audio.get(tag_key)
                        if not direct_value:
                            # 尝试获取第一个匹配的标签
                            for possible_tag in [tag_key, tag_key.lower(), tag_key.upper()]:
                                direct_value = audio.get(possible_tag)
                                if direct_value:
                                    break

                        if direct_value:
                            # 处理ID3标签的特殊结构
                            if hasattr(direct_value, 'text'):
                                # ID3 Frame (如 TPE1)
                                if direct_value.text and len(direct_value.text) > 0:
                                    value = str(direct_value.text[0])
                            elif isinstance(direct_value, list) and len(direct_value) > 0:
                                # 列表类型
                                value = str(direct_value[0])
                            else:
                                # 直接转换
                                value = str(direct_value)
                    except:
                        pass

                # 对于FLAC文件，如果_get_audio_tag返回空值，尝试直接获取
                elif not value and file_ext == '.flac':
                    try:
                        direct_value = audio.get(tag_key)
                        if direct_value and isinstance(direct_value, list) and len(direct_value) > 0:
                            value = str(direct_value[0])
                    except:
                        pass

                # 判断是否显示此标签
                should_show = False
                if value and value.strip():
                    should_show = True
                elif not value and tag_key not in ['COMM', 'DESCRIPTION']:
                    # 对于空值，除了注释外都显示
                    should_show = True

                if should_show:
                    if not value:
                        value = ""  # 确保为空字符串而不是None

                    # 对于歌词标签，只显示开头部分
                    if (tag_key == 'LYRICS' or tag_key == 'USLT') and len(value) > 30:
                        lyrics_content = value[:20] + "..."
                        tags_info.append(f"{display_name}: {lyrics_content}")
                    else:
                        tags_info.append(f"{display_name}: {value}")

            # 添加FLAC图片信息（如果有）
            try:
                if hasattr(audio, 'pictures') and audio.pictures:
                    if not any("封面" in info for info in tags_info):  # 避免重复显示封面信息
                        tags_info.append(
                            f"封面(cover): 图片数据[{len(audio.pictures)}张]")
            except Exception as e:
                logger.debug(f"获取FLAC图片信息时出错: {e}")

            if tags_info:
                logger.debug(f"文件名：{audio_path.name}")
                for info in tags_info:
                    # 对于歌词标签，只显示开头部分
                    if "歌词(lyrics): " in info and len(info) > 30:
                        # 找到冒号位置
                        colon_pos = info.find(": ")
                        if colon_pos != -1:
                            label = info[:colon_pos+2]  # 包括": "
                            # 只取前20个字符
                            lyrics_content = info[colon_pos+2:colon_pos+22]
                            info = f"{label}{lyrics_content}..."

                    logger.debug(f"  {info}")
            else:
                logger.debug(f"音频文件无可用元数据: {audio_path.name}")

        except Exception as e:
            # 这里兜底，永远不抛失败日志
            logger.debug(f"记录音频元数据时忽略异常: {e}")

    def _clean_empty_dirs(self, start_dir: Path):
        """
        递归清理空文件夹
        :param start_dir: 开始清理的目录
        """
        try:
            current_dir = start_dir

            # 记录开始清理的日志
            logger.debug(f"开始清理空文件夹: {start_dir}")

            # 获取所有监控目录的路径（用于避免删除监控目录）
            monitor_paths = [Path(mon_path)
                             for mon_path in self._dirconf.keys()]

            # 从当前目录开始向上递归清理
            while current_dir and current_dir.exists():
                # 检查当前目录是否是监控目录（监控目录本身不应该被删除）
                if current_dir in monitor_paths:
                    logger.debug(f"跳过监控目录: {current_dir}")
                    break

                # 检查当前目录是否为空（排除隐藏文件和系统文件）
                try:
                    items = list(current_dir.iterdir())
                    # 过滤掉隐藏文件和系统文件，以及只保留文件和文件夹（不包含符号链接等）
                    visible_items = []
                    for item in items:
                        if item.name.startswith('.') or item.name.startswith('@'):
                            continue
                        if item.is_file() or item.is_dir():
                            visible_items.append(item)

                    logger.debug(
                        f"检查目录 {current_dir}: 发现 {len(visible_items)} 个可见项目")

                    if not visible_items:
                        # 目录为空，尝试删除
                        try:
                            current_dir.rmdir()
                            logger.info(f"已清理空文件夹: {current_dir}")
                        except OSError as e:
                            # 如果删除失败（可能因为权限或其他原因），记录详细信息
                            logger.debug(f"无法删除文件夹 {current_dir}: {e}")
                            break
                    else:
                        # 目录不为空，检查是否有其他音频文件
                        audio_files = [item for item in visible_items
                                       if item.is_file() and item.suffix.lower() in self.AUDIO_EXTENSIONS]

                        if not audio_files:
                            # 目录中没有其他音频文件，尝试删除
                            try:
                                current_dir.rmdir()
                                logger.info(f"已清理无音频文件的空文件夹: {current_dir}")
                            except OSError as e:
                                logger.debug(f"无法删除文件夹 {current_dir}: {e}")
                                break
                        else:
                            # 目录中有其他音频文件，停止清理
                            logger.debug(
                                f"目录 {current_dir} 包含 {len(audio_files)} 个音频文件，停止清理")
                            break

                except (OSError, PermissionError) as e:
                    logger.debug(f"无法访问文件夹 {current_dir}: {e}")
                    break

                # 向上移动到父目录
                parent_dir = current_dir.parent
                # 如果到达根目录，停止清理
                if parent_dir == current_dir or not parent_dir.exists():
                    break
                current_dir = parent_dir

        except Exception as e:
            logger.debug(f"清理空文件夹时出错: {e}")

    def _remove_comment_tags(self, audio):
        """
        删除音频文件标签中的description字段内容
        :param audio: 音频文件对象
        """
        try:
            if audio is None or not audio.tags:
                return

            cleaned = False

            # FLAC/OGG/OPUS/APE (VorbisComment) 格式处理
            if hasattr(audio.tags, 'items'):
                for key in list(audio.tags.keys()):
                    if key.lower() == 'description':
                        try:
                            # 直接删除description标签
                            del audio.tags[key]
                            cleaned = True
                            logger.debug("已删除 description 标签")
                        except Exception as e:
                            logger.debug(f"删除 description 标签失败: {e}")

            # MP3 (ID3) 格式处理
            elif hasattr(audio.tags, 'values'):
                # ID3 格式的 description 字段通常是 TXXX
                frames_to_delete = []
                for frame in audio.tags.values():
                    frame_id = getattr(frame, 'FrameID', '')
                    if frame_id == 'TXXX':
                        desc = getattr(frame, 'desc', '')
                        if str(desc).lower() == 'description':
                            frames_to_delete.append(frame_id)

                for frame_id in frames_to_delete:
                    try:
                        audio.tags.del_frame(frame_id)
                        cleaned = True
                        logger.debug("已删除 MP3 description 标签")
                    except Exception as e:
                        logger.debug(f"删除 MP3 description 标签失败: {e}")

            # 保存修改
            if cleaned:
                try:
                    audio.save()
                    logger.debug("标签保存完成，准备执行文件操作")
                    logger.info("已成功删除 description 标签")
                except Exception as e:
                    logger.error(f"保存标签修改失败: {e}")
                    raise  # 重新抛出异常，让上层处理

        except Exception as e:
            logger.error(f"处理description标签时出错: {e}")

    def _sanitize_filename(self, name: str) -> str:
        """
        清洗文件名中的非法字符
        :param name: 原始名称
        :return: 清洗后的名称
        """
        # 移除文件系统非法字符
        illegal_chars = r'<>:"/\\|?*'
        for char in illegal_chars:
            name = name.replace(char, '')
        # 移除首尾空格和点
        name = name.strip().strip('.')
        return name if name else 'Unknown'

    def _format_datetime(self, timestamp: str) -> str:
        """
        格式化时间戳为可读的日期时间格式
        :param timestamp: 时间戳字符串
        :return: 格式化后的时间字符串
        """
        if not timestamp:
            return ''
        try:
            # 如果是时间戳格式
            if timestamp.isdigit():
                dt = datetime.datetime.fromtimestamp(float(timestamp))
            else:
                # 尝试解析为ISO格式
                dt = datetime.datetime.fromisoformat(
                    timestamp.replace('Z', '+00:00'))

            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return timestamp

    def _truncate_path(self, path: str, max_length: int = 60) -> str:
        """
        截断路径字符串，超过长度显示省略号
        :param path: 路径字符串
        :param max_length: 最大长度
        :return: 截断后的路径字符串
        """
        if not path:
            return ''

        if len(path) <= max_length:
            return path

        # 保留开头和结尾部分
        head_length = max_length // 3
        tail_length = max_length - head_length - 3  # 3个点占位

        return f"{path[:head_length]}...{path[-tail_length:]}"

    def _get_transfer_type_text(self, transfer_type: str) -> str:
        """
        获取转移方式的中文描述
        :param transfer_type: 转移方式
        :return: 中文描述
        """
        type_map = {
            'move': '移动',
            'copy': '复制',
            'link': '硬链接'
        }
        return type_map.get(transfer_type, transfer_type)

    def _format_file_size(self, size_bytes: int) -> str:
        """
        格式化文件大小为可读格式
        :param size_bytes: 文件大小（字节）
        :return: 格式化后的文件大小字符串
        """
        if not size_bytes:
            return "0 B"

        # 定义单位
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_index = 0
        size = float(size_bytes)

        # 转换到合适的单位
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1

        # 格式化输出
        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        else:
            return f"{size:.2f} {units[unit_index]}"

    def _get_file_size(self, file_path: str) -> int:
        """
        获取文件大小
        :param file_path: 文件路径
        :return: 文件大小（字节），如果文件不存在返回0
        """
        try:
            if file_path and Path(file_path).exists():
                return Path(file_path).stat().st_size
            else:
                return 0
        except Exception:
            return 0

    def _build_song_index_key(self, artist: str, title: str) -> str:
        """
        构建歌曲索引键（标准化格式）
        :param artist: 歌手名
        :param title: 歌曲名
        :return: 标准化索引键
        """
        normalized_title = title.strip().lower()
        normalized_artist = artist.strip().lower()
        return f"{normalized_artist}||{normalized_title}"

    def _build_song_index(self):
        """
        构建歌曲查重索引
        """
        try:
            with self._index_lock:
                # 清空现有索引
                self._song_index.clear()

                # 获取历史记录
                history = self.get_data('organize_history') or []

                # 只索引成功整理的歌曲
                for record in history:
                    if not isinstance(record, dict) or record.get('status') != 'success':
                        continue

                    artist = record.get('artist', '')
                    title = record.get('title', '')

                    if artist and title:
                        index_key = self._build_song_index_key(artist, title)
                        self._song_index[index_key] = record

                logger.info(f"歌曲查重索引构建完成，共索引 {len(self._song_index)} 首成功整理的歌曲")

        except Exception as e:
            logger.error(f"构建歌曲索引失败: {e}")

    def _update_song_index(self, record: Dict[str, Any]):
        """
        更新歌曲索引（添加新记录）
        :param record: 新的历史记录
        """
        try:
            if not isinstance(record, dict) or record.get('status') != 'success':
                return

            artist = record.get('artist', '')
            title = record.get('title', '')

            if artist and title:
                index_key = self._build_song_index_key(artist, title)
                with self._index_lock:
                    self._song_index[index_key] = record
                    logger.debug(f"歌曲索引已更新 - 歌手: {artist}, 歌曲名: {title}")

        except Exception as e:
            logger.error(f"更新歌曲索引失败: {e}")

    def _is_song_already_organized(self, artist: str, title: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        检查歌曲是否已经整理过（使用索引加速查询）
        :param artist: 歌手名
        :param title: 歌曲名
        :return: (是否已整理, 对应的历史记录)
        """
        try:
            # 构建索引键
            index_key = self._build_song_index_key(artist, title)

            logger.debug(f"开始歌曲查重检查 - 索引键: '{index_key}'")
            logger.debug(f"待检查歌曲 - 歌手: '{artist}', 歌曲名: '{title}'")

            # 使用索引进行快速查询
            with self._index_lock:
                existing_record = self._song_index.get(index_key)

            if existing_record:
                # 检查记录状态是否为成功，只有成功的记录才认为是已整理过的
                record_status = existing_record.get('status', '')
                if record_status == 'success':
                    logger.info(f"歌曲查重：发现已整理的歌曲 - 歌手: {artist}, 歌曲名: {title}")
                    logger.info(
                        f"匹配的记录状态: {record_status}, 目标路径: {existing_record.get('target_path', '未知')}")
                    return True, existing_record
                else:
                    logger.debug(f"索引中找到匹配记录但状态为 {record_status}，继续查找...")
                    # 状态不是success，继续从数据库查找其他可能的成功记录

            # 索引中没有找到，尝试从数据库查找（兼容旧数据）
            logger.debug(f"索引中未找到，尝试从数据库查找...")
            history = self.get_data('organize_history') or []

            # 标准化歌曲名和歌手名进行比较
            normalized_title = title.strip().lower()
            normalized_artist = artist.strip().lower()

            # 遍历历史记录，查找匹配的歌曲
            match_count = 0
            for i, record in enumerate(history):
                if not isinstance(record, dict):
                    continue

                # 获取记录中的歌曲名和歌手名
                record_title = record.get('title', '').strip().lower()
                record_artist = record.get('artist', '').strip().lower()
                record_status = record.get('status', '')

                # 检查是否匹配（歌曲名和歌手名都相同）
                if record_title == normalized_title and record_artist == normalized_artist:
                    match_count += 1
                    # 检查记录状态是否为成功
                    if record_status == 'success':
                        logger.info(
                            f"数据库查重：发现已整理的歌曲 - 歌手: {artist}, 歌曲名: {title}")
                        logger.info(
                            f"匹配的记录状态: {record_status}, 目标路径: {record.get('target_path', '未知')}")
                        return True, record
                    else:
                        logger.debug(f"找到匹配记录但状态为 {record_status}，继续查找...")

            if match_count > 0:
                logger.debug(f"找到 {match_count} 条匹配记录，但状态都不是'success'")
            else:
                logger.debug(f"未找到匹配的歌曲记录")

            return False, None

        except Exception as e:
            logger.error(f"检查歌曲是否已整理时出错: {e}")
            return False, None

    def _add_organize_history(self, record: Dict[str, Any]):
        """
        添加整理记录到历史
        :param record: 记录信息
        """
        try:
            # 获取现有历史记录
            history = self.get_data('organize_history') or []

            # 添加时间戳
            record['timestamp'] = datetime.datetime.now().isoformat()

            # 确保文件大小字段存在（如果不存在，设置为0）
            if 'file_size' not in record:
                record['file_size'] = 0

            # 添加到历史记录
            history.append(record)

            # 限制历史记录数量，最多保留5000条
            if len(history) > 5000:
                history = history[-5000:]

            # 保存历史记录
            self.save_data('organize_history', history)

            # 如果记录状态为成功，更新索引
            if record.get('status') == 'success':
                self._update_song_index(record)

        except Exception as e:
            logger.error(f"保存整理记录失败: {e}")

    def _replace_organize_history(self, source_path: str, new_record: Dict[str, Any]) -> bool:
        """
        使用新记录完全替换现有的整理记录
        :param source_path: 源文件路径
        :param new_record: 新的记录信息，将完全替换旧记录
        """
        try:
            # 获取现有历史记录
            history = self.get_data('organize_history') or []

            # 查找匹配的记录（按源路径）
            for i, record in enumerate(history):
                if record.get('source_path') == source_path:
                    # 保存原有记录的时间戳，避免替换时创建新记录
                    original_timestamp = record.get('timestamp')

                    # 确保新记录包含源路径
                    if 'source_path' not in new_record:
                        new_record['source_path'] = source_path

                    # 保持原有时间戳，避免在历史记录中产生新条目
                    if original_timestamp:
                        new_record['timestamp'] = original_timestamp
                    else:
                        new_record['timestamp'] = datetime.datetime.now(
                        ).isoformat()

                    # 完全替换旧记录
                    history[i] = new_record

                    # 保存更新后的历史记录
                    self.save_data('organize_history', history)
                    logger.debug(f"已替换文件历史记录: {source_path}")
                    logger.debug(f"历史记录已保存，共{len(history)}条记录")

                    # 更新索引
                    if new_record.get('status') == 'success':
                        self._update_song_index(new_record)
                    return True

            # 如果没有找到匹配记录，创建新记录
            logger.debug(f"未找到文件历史记录，创建新记录: {source_path}")
            if 'source_path' not in new_record:
                new_record['source_path'] = source_path
            new_record['timestamp'] = datetime.datetime.now().isoformat()

            self._add_organize_history(new_record)
            return True

        except Exception as e:
            logger.error(f"替换历史记录失败: {e}")
            return False

    def _remove_all_and_add_organize_history(self, source_path: str, new_record: Dict[str, Any]) -> bool:
        """
        删除所有相同文件路径的记录，然后添加新记录
        :param source_path: 源文件路径
        :param new_record: 新的记录信息
        """
        try:
            # 获取现有历史记录
            history = self.get_data('organize_history') or []

            # 删除所有相同文件路径的记录
            history = [record for record in history if record.get(
                'source_path') != source_path]

            # 确保新记录包含源路径
            if 'source_path' not in new_record:
                new_record['source_path'] = source_path

            # 保持原有记录的时间戳（如果存在）
            existing_records = [record for record in self.get_data('organize_history') or []
                                if record.get('source_path') == source_path]
            if existing_records:
                # 使用最早的时间戳
                earliest_timestamp = min(record.get('timestamp', '')
                                         for record in existing_records)
                new_record['timestamp'] = earliest_timestamp
            else:
                new_record['timestamp'] = datetime.datetime.now().isoformat()

            # 添加新记录到历史记录末尾
            history.append(new_record)

            # 限制历史记录数量，最多保留5000条
            if len(history) > 5000:
                history = history[-5000:]

            # 保存更新后的历史记录
            self.save_data('organize_history', history)
            logger.debug(f"已删除并替换文件历史记录: {source_path}")

            # 更新索引
            if new_record.get('status') == 'success':
                self._update_song_index(new_record)
            return True

        except Exception as e:
            logger.error(f"删除并替换历史记录失败: {e}")
            return False

    def _update_organize_history(self, source_path: str, status: str, reason: str = None, target_path: str = None, transfer_type: str = None, has_lyrics: bool = None, has_cover: bool = None):
        """
        更新现有的整理记录
        :param source_path: 源文件路径
        :param status: 新状态 ('success' 或 'failed')
        :param reason: 失败原因（可选）
        :param target_path: 目标路径（可选）
        :param transfer_type: 转移方式（可选）
        :param has_lyrics: 是否有歌词（可选）
        :param has_cover: 是否有封面（可选）
        """
        try:
            # 获取现有历史记录
            history = self.get_data('organize_history') or []

            # 查找匹配的记录（按源路径）
            for i, record in enumerate(history):
                if record.get('source_path') == source_path:
                    # 更新记录
                    history[i]['status'] = status
                    if reason is not None:
                        history[i]['reason'] = reason
                    if target_path is not None:
                        history[i]['target_path'] = target_path
                    if transfer_type is not None:
                        history[i]['transfer_type'] = transfer_type
                    if has_lyrics is not None:
                        history[i]['has_lyrics'] = has_lyrics
                    if has_cover is not None:
                        history[i]['has_cover'] = has_cover

                    # 更新时间戳
                    history[i]['timestamp'] = datetime.datetime.now().isoformat()

                    # 保存更新后的历史记录
                    self.save_data('organize_history', history)
                    logger.debug(f"已更新文件历史记录: {source_path} -> {status}")

                    # 更新索引
                    if status == 'success':
                        self._update_song_index(history[i])
                    return True

            # 如果没有找到匹配记录，创建新记录
            logger.debug(f"未找到文件历史记录，创建新记录: {source_path}")
            self._add_organize_history({
                'filename': Path(source_path).name,
                'artist': '',
                'album': '',
                'title': '',
                'year': '',
                'source_path': source_path,
                'target_path': target_path or '',
                'status': status,
                'transfer_type': transfer_type or 'unknown',
                'file_size': self._get_file_size(source_path),
                'reason': reason or '',
                'has_lyrics': has_lyrics or False,
                'has_cover': has_cover or False
            })
            return True

        except Exception as e:
            logger.error(f"更新整理记录失败: {e}")
            return False

    def get_state(self) -> bool:
        """
        获取插件状态
        """
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        获取插件配置表单
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'require_tags',
                                            'label': '包含标签',
                                            'hint': '开启后只整理包含完整标签信息（标题、艺术家、专辑、封面、年份、歌词）的音频文件'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clear_history',
                                            'label': '清除历史记录',
                                            'hint': '开启后点击保存将清除所有历史记录'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'send_events',
                                            'label': '发送事件',
                                            'hint': '开启后检测到标签不完整的文件时发送事件通知'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mode',
                                            'label': '监控模式',
                                            'items': [
                                                {'title': '性能模式', 'value': 'fast'},
                                                {'title': '兼容模式',
                                                    'value': 'compatibility'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'batch_notify',
                                            'label': '聚合通知',
                                            'hint': '开启后同一歌手的歌曲将在时间窗口内聚合发送通知'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'batch_delay',
                                            'label': '聚合时间(秒)',
                                            'placeholder': '0',
                                            'type': 'number',
                                            'min': 0,
                                            'hint': '0表示不聚合，直接发送单个通知。大于0表示聚合时间窗口（秒）'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'conflict_strategy',
                                            'label': '覆盖方式',
                                            'items': [
                                                {'title': '跳过', 'value': 'skip'},
                                                {'title': '覆盖', 'value': 'overwrite'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled_scheduler',
                                            'label': '启用定时任务',
                                            'hint': '开启定时自动整理音乐文件功能'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '定时表达式',
                                            'hint': '设置定时任务的执行时间，如：0 8 * * * 表示每天8点执行'
                                        }
                                    }
                                ]
                            }
                        ]
                    },

                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'transfer_type',
                                            'label': '转移方式',
                                            'items': [
                                                {'title': '移动', 'value': 'move'},
                                                {'title': '复制', 'value': 'copy'},
                                                {'title': '硬链接', 'value': 'link'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'folder_rule',
                                            'label': '文件夹命名规则',
                                            'items': [
                                                {'title': '年份/艺术家/专辑',
                                                    'value': 'year_artist_album_title'},
                                                {'title': '年份/艺术家',
                                                    'value': 'year_artist'},
                                                {'title': '艺术家/专辑',
                                                    'value': 'artist_album'},
                                                {'title': '艺术家', 'value': 'artist'},
                                                {'title': '根目录', 'value': 'root'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {"cols": 12, "md": 4},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'filename_rule',
                                            'label': '文件命名规则',
                                            'items': [
                                                {'title': '保持不变',
                                                    'value': 'keep_original'},
                                                {'title': '艺术家-歌曲名',
                                                    'value': 'artist_title'},
                                                {'title': '歌曲名-艺术家',
                                                    'value': 'title_artist'},
                                                {'title': '艺术家-专辑-歌曲名',
                                                    'value': 'artist_album_title'}
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {"cols": 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'monitor_dirs',
                                            'label': '监控目录',
                                            'rows': 5,
                                            'placeholder': '每行一个目录，例如：\n/音乐/下载:/音乐/最终\n/音乐/下载:/音乐/最终#copy\n/音乐/下载:/音乐/最终#link'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {"cols": 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '监控目录配置方式：\n1. 监控目录:转移目录 或者 监控目录: 转移目录#转移方式(move/copy/link). 一行一个\n2. 只转移标签完整的文件'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "require_tags": True,
            "monitor_dirs": "/data/music_incoming:/data/music_library",
            "mode": "fast",
            "delay_seconds": 0,
            "folder_rule": "artist_album",
            "filename_rule": "artist_title",
            "transfer_type": "move",
            "conflict_strategy": "skip",
            "cron": "",
            "enabled_scheduler": False,
            "batch_notify": False,
            "batch_delay": 0,
            "send_events": False
        }

    def get_page(self) -> List[dict]:
        """
        显示整理记录页面
        """
        # 获取整理历史记录
        history_records = self.get_data('organize_history') or []

        if not history_records:
            return [
                {
                    'component': 'div',
                    'text': '暂无整理记录',
                    'props': {
                        'class': 'text-center text-grey',
                        'style': 'padding: 40px 0;'
                    }
                }
            ]

        # 统计各种状态记录数量
        failed_count = sum(
            1 for record in history_records if record.get('status') == 'failed')
        
        # 分别统计成功和跳过记录数量
        success_count = sum(
            1 for record in history_records if record.get('status') == 'success')
        skipped_count = sum(
            1 for record in history_records if record.get('status') == 'skipped')
        
        # 统计已整理记录数量（成功和跳过）
        organized_count = success_count + skipped_count

        # 显示最近100条记录，避免表格显示限制问题
        # 统计数据仍然基于所有记录，但表格只显示最近100条
        display_records = list(reversed(history_records))[:100]

        return [
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {'cols': 12},
                        'content': [
                            {
                                'component': 'script',
                                'props': {'type': 'text/javascript'},

                            }
                        ]
                    }
                ]
            },


            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {"cols": 12},
                        'content': [
                            {
                                'component': 'VCard',
                                'content': [
                                    {
                                        'component': 'VCardTitle',
                                        'props': {
                                            'class': 'd-flex align-center justify-space-between'
                                        },
                                        'content': [
                                            {
                                                'component': 'div',
                                                'text': f'整理记录历史 (共{len(history_records)}条  跳过{skipped_count}条  失败{failed_count}条)'
                                            },
                                            {
                                                'component': 'div',
                                                'props': {
                                                    'class': 'd-flex align-center gap-2'
                                                },
                                                'content': [
                                                    {
                                                        'component': 'VBtn',
                                                        'props': {
                                                            'color': 'warning',
                                                            'size': 'small',
                                                            'variant': 'outlined'
                                                        },
                                                        'events': {
                                                            'click': {
                                                                'type': 'request',
                                                                'api': 'plugin/MusicFileOrganizer/clear_failed_records',
                                                                'method': 'POST',
                                                                'params': {
                                                                    'apikey': settings.API_TOKEN
                                                                },
                                                                'success': '清除成功',
                                                                'fail': '清除失败'
                                                            }
                                                        },
                                                        'text': '清除失败记录'
                                                    },
                                                    {
                                                        'component': 'VBtn',
                                                        'props': {
                                                            'color': 'info',
                                                            'size': 'small',
                                                            'variant': 'outlined'
                                                        },
                                                        'events': {
                                                            'click': {
                                                                'type': 'request',
                                                                'api': 'plugin/MusicFileOrganizer/clear_skipped_records',
                                                                'method': 'POST',
                                                                'params': {
                                                                    'apikey': settings.API_TOKEN
                                                                },
                                                                'success': '清除成功',
                                                                'fail': '清除失败'
                                                            }
                                                        },
                                                        'text': '清除跳过记录'
                                                    },
                                                    {
                                                        'component': 'VBtn',
                                                        'props': {
                                                            'color': 'error',
                                                            'size': 'small',
                                                            'variant': 'outlined'
                                                        },
                                                        'events': {
                                                            'click': {
                                                                'type': 'request',
                                                                'api': 'plugin/MusicFileOrganizer/delete_failed_files',
                                                                'method': 'POST',
                                                                'params': {
                                                                    'apikey': settings.API_TOKEN
                                                                },
                                                                'success': '删除成功',
                                                                'fail': '删除失败'
                                                            }
                                                        },
                                                        'text': '删除失败源文件'
                                                    },
                                                    {
                                                        'component': 'VBtn',
                                                        'props': {
                                                            'color': 'warning',
                                                            'size': 'small',
                                                            'variant': 'outlined'
                                                        },
                                                        'events': {
                                                            'click': {
                                                                'type': 'request',
                                                                'api': 'plugin/MusicFileOrganizer/delete_skipped_files',
                                                                'method': 'POST',
                                                                'params': {
                                                                    'apikey': settings.API_TOKEN
                                                                },
                                                                'success': '删除成功',
                                                                'fail': '删除失败'
                                                            }
                                                        },
                                                        'text': '删除跳过源文件'
                                                    },
                                                    {
                                                        'component': 'VBtn',
                                                        'props': {
                                                            'color': 'info',
                                                            'size': 'small',
                                                            'variant': 'outlined'
                                                        },
                                                        'events': {
                                                            'click': {
                                                                'type': 'request',
                                                                'api': 'plugin/MusicScraperHelp/start_scrape',
                                                                'method': 'POST',
                                                                'params': {
                                                                    'apikey': settings.API_TOKEN
                                                                },
                                                                'success': '刮削任务已启动',
                                                                'fail': '刮削启动失败'
                                                            }
                                                        },
                                                        'text': '刮削失败文件'
                                                    }
                                                ]
                                            }
                                        ]
                                    },
                                    {
                                        'component': 'VCardText',
                                        'content': [
                                            {
                                                'component': 'VTable',
                                                'props': {
                                                    'density': 'comfortable',
                                                    'hover': True,
                                                    'fixed-header': True,
                                                    'height': '800px'
                                                },
                                                'content': [
                                                    {
                                                        'component': 'thead',
                                                        'content': [
                                                            {
                                                                'component': 'tr',
                                                                'content': [
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '标题',
                                                                        'props': {
                                                                            'width': '180px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '艺术家',
                                                                        'props': {
                                                                            'width': '120px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '专辑',
                                                                        'props': {
                                                                            'width': '180px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '歌词',
                                                                        'props': {
                                                                            'width': '80px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '封面',
                                                                        'props': {
                                                                            'width': '80px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '路径',
                                                                        'props': {
                                                                            'width': '350px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '转移方式',
                                                                        'props': {
                                                                            'width': '80px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '大小',
                                                                        'props': {
                                                                            'width': '80px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '时间',
                                                                        'props': {
                                                                            'width': '140px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '状态',
                                                                        'props': {
                                                                            'width': '70px'
                                                                        }
                                                                    },
                                                                    {
                                                                        'component': 'th',
                                                                        'text': '源文件',
                                                                        'props': {
                                                                            'width': '60px'
                                                                        }
                                                                    }
                                                                ]
                                                            }
                                                        ]
                                                    },
                                                    {
                                                        'component': 'tbody',
                                                        'content': [
                                                            {
                                                                'component': 'tr',
                                                                'content': [
                                                                    {
                                                                        'component': 'td',
                                                                        'text': record.get('title', record.get('filename', ''))
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'text': record.get('artist', '未知')
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'text': record.get('album', '未知')
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'content': [
                                                                            {
                                                                                'component': 'div',
                                                                                'props': {
                                                                                    'class': 'text-center'
                                                                                },
                                                                                'text': '有' if record.get('has_lyrics', False) else '没有'
                                                                            }
                                                                        ]
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'content': [
                                                                            {
                                                                                'component': 'div',
                                                                                'props': {
                                                                                    'class': 'text-center'
                                                                                },
                                                                                'text': '有' if record.get('has_cover', False) else '没有'
                                                                            }
                                                                        ]
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'content': [
                                                                            {
                                                                                'component': 'div',
                                                                                'text': f"{self._truncate_path(record.get('source_path', ''), 50)}",
                                                                                'props': {
                                                                                    'class': 'text-caption'
                                                                                }
                                                                            },
                                                                            {
                                                                                'component': 'div',
                                                                                'text': '=>',
                                                                                'props': {
                                                                                    'class': 'text-center text-grey'
                                                                                }
                                                                            },
                                                                            {
                                                                                'component': 'div',
                                                                                'text': f"{self._truncate_path(record.get('target_path', ''), 50)}",
                                                                                'props': {
                                                                                    'class': 'text-caption'
                                                                                }
                                                                            }
                                                                        ]
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'text': self._get_transfer_type_text(record.get('transfer_type', ''))
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'text': self._format_file_size(record.get('file_size', 0))
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'text': self._format_datetime(record.get('timestamp', ''))
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'content': [
                                                                            {
                                                                                'component': 'VChip',
                                                                                'props': {
                                                                                    'color': 'success' if record.get('status') == 'success' else 'warning' if record.get('status') == 'skipped' else 'error',
                                                                                    'size': 'small'
                                                                                },
                                                                                'text': '成功' if record.get('status') == 'success' else '已整' if record.get('status') == 'skipped' else '失败'
                                                                            }
                                                                        ]
                                                                    },
                                                                    {
                                                                        'component': 'td',
                                                                        'content': [
                                                                            {
                                                                                'component': 'VBtn',
                                                                                'props': {
                                                                                    'color': 'error',
                                                                                    'size': 'small',
                                                                                },
                                                                                'events': {
                                                                                    'click': {
                                                                                        'type': 'request',
                                                                                        'api': 'plugin/MusicFileOrganizer/delete_record',
                                                                                        'method': 'POST',
                                                                                        'params': {
                                                                                            'apikey': settings.API_TOKEN,
                                                                                            'source_path': record.get('source_path', '')
                                                                                        },
                                                                                        'success': '删除成功',
                                                                                        'fail': '删除失败'
                                                                                    }
                                                                                },
                                                                                'text': '删除'
                                                                            }
                                                                        ]
                                                                    }
                                                                ]
                                                            } for record in display_records
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
            }
        ]

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        :return: 命令关键字、事件、描述、附带数据
        """
        return [{
            "cmd": "/music_sync",
            "event": EventType.PluginAction,
            "desc": "整理音乐",
            "category": "音乐",
            "data": {
                "action": "music_sync"
            }
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        """注册API接口"""
        return [
            {
                "path": "/retry",
                "endpoint": self.retry_api,
                "methods": ["POST"],
                "summary": "重新整理文件",
                "description": "重新整理指定文件的音乐标签",
                "auth": "bear"  # 使用Bearer Token认证
            },
            {
                "path": "/delete_record",
                "endpoint": self.delete_record_api,
                "methods": ["POST"],
                "summary": "删除单条历史记录",
                "description": "根据源文件路径删除特定的历史记录",
                "auth": "bear"  # 使用Bearer Token认证
            },
            {
                "path": "/clear_failed_records",
                "endpoint": self.clear_failed_records_api,
                "methods": ["POST"],
                "summary": "清除失败记录",
                "description": "清除所有失败的历史记录",
                "auth": "bear"  # 使用Bearer Token认证
            },
            {
                "path": "/delete_failed_files",
                "endpoint": self.delete_failed_files_api,
                "methods": ["POST"],
                "summary": "删除失败源文件",
                "description": "删除所有失败记录的源文件",
                "auth": "bear"  # 使用Bearer Token认证
            },
            {
                "path": "/clear_skipped_records",
                "endpoint": self.clear_skipped_records_api,
                "methods": ["POST"],
                "summary": "清除跳过记录",
                "description": "清除所有跳过的历史记录",
                "auth": "bear"  # 使用Bearer Token认证
            },
            {
                "path": "/delete_skipped_files",
                "endpoint": self.delete_skipped_files_api,
                "methods": ["POST"],
                "summary": "删除跳过源文件",
                "description": "删除所有跳过记录的源文件，并将状态改为成功",
                "auth": "bear"  # 使用Bearer Token认证
            }
        ]

    def retry_api(self, req: RetryFileReq) -> schemas.Response:
        """
        API响应方法：重新整理文件
        :param req: 重新整理文件请求体
        :return: 响应结果
        """
        # 验证 API 密钥
        if req.apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")

        file_path = req.file_path
        if not file_path:
            return schemas.Response(success=False, message="缺少文件路径参数")

        logger.debug(f"API调用：重新整理文件 {file_path}")
        return self.retry_file(file_path)

    def delete_record_api(self, req: DeleteRecordReq) -> schemas.Response:
        """
        API响应方法：删除单条历史记录和源文件
        :param req: 删除记录请求体
        :return: 响应结果
        """
        try:
            # 验证 API 密钥
            if req.apikey != settings.API_TOKEN:
                return schemas.Response(success=False, message="API密钥错误")

            source_path = req.source_path
            logger.debug(f"开始删除记录，源文件路径: {source_path}")

            if not source_path:
                logger.error("缺少源文件路径参数")
                return schemas.Response(success=False, message="缺少源文件路径参数")

            # 获取历史记录
            history = self.get_data('organize_history') or []
            logger.debug(f"当前历史记录总数: {len(history)}")

            # 查找匹配的记录
            record_to_delete = None
            new_history = []
            for record in history:
                if record.get('source_path') == source_path:
                    record_to_delete = record
                    logger.debug(
                        f"找到匹配记录: {record.get('filename', '未知文件')}, 状态: {record.get('status', '未知')}, 转移方式: {record.get('transfer_type', '未知')}")
                else:
                    new_history.append(record)

            if not record_to_delete:
                logger.warning(f"未找到匹配的历史记录，源路径: {source_path}")
                return schemas.Response(success=False, message="未找到匹配的历史记录")

            filename = record_to_delete.get('filename', '未知文件')
            transfer_type = record_to_delete.get('transfer_type', 'move')
            target_path = record_to_delete.get('target_path', '')
            deleted_files = []
            failed_deletions = []

            logger.debug(f"开始删除文件: {filename}, 转移方式: {transfer_type}")
            logger.debug(f"源文件路径: {source_path}")
            logger.debug(f"目标文件路径: {target_path}")

            # 根据转移方式执行不同的删除策略

            # 检查文件状态：失败和跳过的文件只有源文件，成功的文件根据转移方式删除
            record_status = record_to_delete.get('status', '')

            if record_status == 'failed' or record_status == 'skipped':
                # 失败或跳过的文件：删除源文件
                logger.debug(f"文件状态为{record_status}，删除源文件")
                source_file = Path(source_path)
                if source_file.exists():
                    try:
                        source_file.unlink()
                        deleted_files.append("源文件")
                        logger.debug("源文件删除成功")
                    except Exception as e:
                        logger.error(f"源文件删除失败: {str(e)}")
                        failed_deletions.append(f"源文件: {str(e)}")
                else:
                    logger.warning(f"源文件不存在: {source_path}")
            else:
                # 成功文件：根据转移方式删除
                if transfer_type == 'move':
                    # 移动：删除目标文件
                    logger.debug("转移模式：移动，删除目标文件")
                    if target_path and Path(target_path).exists():
                        logger.info(f"目标文件存在，准备删除: {target_path}")
                        try:
                            Path(target_path).unlink()
                            deleted_files.append("目标文件")
                            logger.info("目标文件删除成功")
                        except Exception as e:
                            logger.error(f"目标文件删除失败: {str(e)}")
                            failed_deletions.append(f"目标文件: {str(e)}")
                    else:
                        logger.warning(f"目标文件不存在或路径为空: {target_path}")

                elif transfer_type == 'copy' or transfer_type == 'hardlink':
                    # 复制/硬链接：删除源文件和目标文件
                    logger.info(f"转移模式：{transfer_type}，删除源文件和目标文件")

                    # 删除源文件
                    source_file = Path(source_path)
                    if source_file.exists():
                        try:
                            source_file.unlink()
                            deleted_files.append("源文件")
                            logger.info("源文件删除成功")
                        except Exception as e:
                            logger.error(f"源文件删除失败: {str(e)}")
                            failed_deletions.append(f"源文件: {str(e)}")
                    else:
                        logger.warning(f"源文件不存在: {source_path}")

                    # 删除目标文件
                    if target_path and Path(target_path).exists():
                        try:
                            Path(target_path).unlink()
                            deleted_files.append("目标文件")
                            logger.info("目标文件删除成功")
                        except Exception as e:
                            logger.error(f"目标文件删除失败: {str(e)}")
                            failed_deletions.append(f"目标文件: {str(e)}")
                    else:
                        logger.warning(f"目标文件不存在或路径为空: {target_path}")

            # 保存更新后的历史记录
            self.save_data('organize_history', new_history)

            # 构建返回消息
            if deleted_files:
                message = f"已删除历史记录和{'、'.join(deleted_files)}"
                logger.info(f"{filename} - {message}")

            if failed_deletions:
                error_msg = f"，删除失败: {'、'.join(failed_deletions)}"
                if deleted_files:
                    message += error_msg
                else:
                    message = f"删除失败: {'、'.join(failed_deletions)}"
                logger.error(f"{filename} - 删除失败: {failed_deletions}")
                return schemas.Response(success=False, message=message)

            return schemas.Response(success=True, message=message if deleted_files else "仅删除历史记录")

        except Exception as e:
            logger.error(f"删除历史记录和源文件失败: {e}")
            return schemas.Response(success=False, message=f"删除历史记录和源文件失败: {str(e)}")

    def get_service(self) -> List[Dict[str, Any]]:
        """注册插件公共服务"""
        return []

    def sync(self) -> schemas.Response:
        """
        API调用目录同步
        """
        self.sync_all()
        return schemas.Response(success=True)

    def stop_service(self):
        """退出插件"""

        if self._observer:
            observer_count = len(self._observer)

            for i, observer in enumerate(self._observer, 1):
                try:
                    observer.stop()
                    observer.join()
                except Exception as e:
                    logger.error(f"停止监控服务失败: {str(e)}")

            self._observer = []

        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._event.set()
                self._scheduler.shutdown()
                self._event.clear()

            self._scheduler = None

        # 发送所有剩余的聚合通知（类似TV聚合的清理机制）
        if self._batch_notify and self._notify:
            try:
                logger.debug("插件停止，开始清理所有待发送的聚合通知")

                # 先取消所有定时器，避免定时器与清理逻辑竞争
                for artist_name, timer in list(self._artist_timers.items()):
                    try:
                        timer.cancel()
                        logger.debug(f"已取消歌手 {artist_name} 的定时器")
                    except Exception as e:
                        logger.debug(f"取消定时器时出错: {str(e)}")

                # 获取所有有未发送结果的歌手列表
                pending_artists = list(self._artist_batch_results.keys())

                # 使用超时机制发送剩余通知
                for artist_name in pending_artists:
                    try:
                        # 直接发送消息而不依赖定时器，但使用超时机制
                        self._send_artist_batch_notify(artist_name)
                    except Exception as e:
                        logger.error(f"发送歌手 {artist_name} 的聚合消息时出错: {str(e)}")
                        # 即使出错也继续清理其他歌手

                # 确保清理所有状态
                self._artist_timers.clear()
                self._artist_batch_results.clear()

                logger.info("插件停止，所有聚合通知已清理完成")

            except Exception as e:
                logger.error(f"清理聚合通知时发生错误: {str(e)}", exc_info=True)
                # 即使出错也要确保状态被清理
                self._artist_timers.clear()
                self._artist_batch_results.clear()

        # 清理标签不完整事件计时器
        try:
            with lock:
                if self._incomplete_tags_timer:
                    self._incomplete_tags_timer.cancel()
                    self._incomplete_tags_timer = None

                # 清空列表（发送事件后不再需要跟踪这些文件）
                self._incomplete_tags_files.clear()
                self._sent_incomplete_files.clear()
                self._last_file_event_time = None

            logger.info("标签不完整事件服务已停止")

        except Exception as e:
            logger.error(f"停止标签不完整事件服务时出错: {e}")

    def retry_file(self, file_path: str) -> schemas.Response:
        """
        重新整理失败的文件
        :param file_path: 文件路径
        :return: 响应结果
        """
        # 强制打印第一行日志
        logger.debug(f"【音乐整理】交互触发：开始处理 {file_path}")

        try:
            # 检查文件是否存在
            path = Path(file_path)
            if not path.exists():
                logger.error(f"文件不存在: {file_path}")
                return schemas.Response(success=False, message="文件不存在")

            # 查找该文件对应的监控目录
            mon_path = None
            for watch_path in self._dirconf.keys():
                if file_path.startswith(watch_path):
                    mon_path = watch_path
                    break

            if not mon_path:
                logger.error(f"文件不在监控目录中: {file_path}")
                return schemas.Response(success=False, message="文件不在监控目录中")

            # 执行重新整理
            logger.info(f"重新整理文件: {path.name}")
            result = self._organize_single_file(path, mon_path)

            # 检查result是否有效
            if result is None or not isinstance(result, dict):
                logger.error(f"重新整理失败: {file_path}, 原因: 文件处理返回无效结果")
                # 创建一个失败的记录
                failed_record = {
                    'filename': Path(file_path).name,
                    'artist': '',
                    'album': '',
                    'title': '',
                    'year': '',
                    'source_path': file_path,
                    'target_path': '',
                    'status': 'failed',
                    'transfer_type': 'unknown',
                    'file_size': self._get_file_size(file_path),
                    'reason': '文件处理返回无效结果',
                    'has_lyrics': False,
                    'has_cover': False
                }
                self._remove_all_and_add_organize_history(
                    file_path, failed_record)
                return schemas.Response(success=False, message="重新整理失败: 文件处理返回无效结果")

            # 确保result中包含必要的键
            if 'success' not in result or 'record' not in result or result['record'] is None:
                logger.error(f"重新整理失败: {file_path}, 原因: 文件处理结果不完整")
                # 创建一个失败的记录
                failed_record = {
                    'filename': Path(file_path).name,
                    'artist': '',
                    'album': '',
                    'title': '',
                    'year': '',
                    'source_path': file_path,
                    'target_path': '',
                    'status': 'failed',
                    'transfer_type': 'unknown',
                    'file_size': self._get_file_size(file_path),
                    'reason': '文件处理结果不完整',
                    'has_lyrics': False,
                    'has_cover': False
                }
                self._remove_all_and_add_organize_history(
                    file_path, failed_record)
                return schemas.Response(success=False, message="重新整理失败: 文件处理结果不完整")

            if result['success']:
                logger.info(f"重新整理成功: {file_path}")
                # 删除所有相同文件路径的记录，然后添加新记录
                self._remove_all_and_add_organize_history(
                    file_path,
                    result['record'])

                # 发送通知
                if self._notify:
                    if self._batch_notify:
                        # 聚合通知模式
                        self._add_batch_result(result)
                    else:
                        # 单个通知模式
                        self._send_single_notify(result)

                return schemas.Response(success=True, message="重新整理成功")
            else:
                # 获取更详细的失败信息
                record = result.get('record')
                if not record:
                    # 创建一个失败的记录
                    record = {
                        'filename': Path(file_path).name,
                        'artist': '',
                        'album': '',
                        'title': '',
                        'year': '',
                        'source_path': file_path,
                        'target_path': '',
                        'status': 'failed',
                        'transfer_type': 'unknown',
                        'file_size': self._get_file_size(file_path),
                        'reason': '处理结果异常',
                        'has_lyrics': False,
                        'has_cover': False
                    }
                # 删除所有相同文件路径的记录，然后添加新记录
                self._remove_all_and_add_organize_history(file_path, record)

                # 发送失败通知
                if self._notify:
                    failed_result = {
                        'success': False,
                        'record': record,
                        'filename': Path(file_path).name  # 确保包含filename字段
                    }
                    if self._batch_notify:
                        # 聚合通知模式
                        self._add_batch_result(failed_result)
                    else:
                        # 单个通知模式
                        self._send_single_notify(failed_result)

                return schemas.Response(success=False, message=f"重新整理失败: {record.get('reason', '未知原因')}")

        except Exception as e:
            logger.error(f"重新整理文件失败: {file_path}, 错误: {e}")
            return schemas.Response(success=False, message=f"重新整理失败: {str(e)}")

    def _get_file_failure_reason(self, file_path: Path) -> str:
        """
        获取文件失败的具体原因
        :param file_path: 文件路径
        :return: 失败原因描述
        """
        try:
            # 检查文件是否存在
            if not file_path.exists():
                return "文件不存在"

            # 检查文件大小
            try:
                file_size = file_path.stat().st_size
                if file_size == 0:
                    return "文件为空"
            except (OSError, PermissionError) as e:
                return f"无法访问文件: {str(e)}"

            # 检查文件格式支持
            audio_format = self._detect_audio_format(file_path)
            if audio_format == "UNKNOWN":
                return f"不支持的音频格式: {file_path.suffix}"

            # 检查文件是否损坏
            try:
                if audio_format == "MP3":
                    audio = EasyID3(str(file_path))
                elif audio_format == "FLAC":
                    audio = FLAC(str(file_path))
                else:
                    # 尝试通用方式读取
                    audio = File(str(file_path))
                    if not audio or audio is None:
                        return "文件可能已损坏或格式不支持"
            except Exception as e:
                return f"文件读取失败: {str(e)}"

            # 默认失败原因
            return "文件处理失败，可能是标签缺失或格式问题"

        except Exception as e:
            return f"无法确定失败原因: {str(e)}"

    def clear_failed_records_api(self, req: BaseApiReq) -> schemas.Response:
        """
        API响应方法：清除所有失败记录
        :param req: API请求体
        :return: 响应结果
        """
        try:
            # 验证 API 密钥
            if req.apikey != settings.API_TOKEN:
                return schemas.Response(success=False, message="API密钥错误")

            # 获取历史记录
            history = self.get_data('organize_history') or []

            # 过滤掉失败记录
            success_records = [
                record for record in history if record.get('status') != 'failed']
            failed_count = len(history) - len(success_records)

            if failed_count == 0:
                return schemas.Response(success=True, message="没有失败记录需要清除")

            # 保存更新后的历史记录
            self.save_data('organize_history', success_records)
            logger.info(f"已清除 {failed_count} 条失败记录")
            return schemas.Response(success=True, message=f"已清除 {failed_count} 条失败记录")

        except Exception as e:
            logger.error(f"清除失败记录失败: {e}")
            return schemas.Response(success=False, message=f"清除失败记录失败: {str(e)}")

    def clear_skipped_records_api(self, req: BaseApiReq) -> schemas.Response:
        """
        API响应方法：清除所有跳过记录
        :param req: API请求体
        :return: 响应结果
        """
        try:
            # 验证 API 密钥
            if req.apikey != settings.API_TOKEN:
                return schemas.Response(success=False, message="API密钥错误")

            # 获取历史记录
            history = self.get_data('organize_history') or []

            # 过滤掉跳过记录
            remaining_records = [
                record for record in history if record.get('status') != 'skipped']
            skipped_count = len(history) - len(remaining_records)

            if skipped_count == 0:
                return schemas.Response(success=True, message="没有跳过记录需要清除")

            # 保存更新后的历史记录
            self.save_data('organize_history', remaining_records)
            logger.info(f"已清除 {skipped_count} 条跳过记录")
            return schemas.Response(success=True, message=f"已清除 {skipped_count} 条跳过记录")

        except Exception as e:
            logger.error(f"清除跳过记录失败: {e}")
            return schemas.Response(success=False, message=f"清除跳过记录失败: {str(e)}")

    def delete_failed_files_api(self, req: BaseApiReq) -> schemas.Response:
        """
        API响应方法：删除所有失败记录的源文件
        :param req: API请求体
        :return: 响应结果
        """
        try:
            # 验证 API 密钥
            if req.apikey != settings.API_TOKEN:
                return schemas.Response(success=False, message="API密钥错误")

            logger.info("开始批量删除失败源文件")

            # 获取历史记录
            history = self.get_data('organize_history') or []
            logger.info(f"当前历史记录总数: {len(history)}")

            # 查找所有失败记录
            failed_records = [
                record for record in history if record.get('status') == 'failed']
            logger.info(f"找到失败记录数: {len(failed_records)}")

            if not failed_records:
                logger.info("没有失败记录需要删除")
                return schemas.Response(success=True, message="没有失败记录的源文件需要删除")

            deleted_count = 0
            failed_deletions = []

            for record in failed_records:
                filename = record.get('filename', '未知文件')
                source_path = record.get('source_path')

                if not source_path:
                    logger.warning(f"记录 {filename} 没有源文件路径")
                    failed_deletions.append(f"{filename}: 没有源文件路径")
                    continue

                source_file = Path(source_path)

                if not source_file.exists():
                    logger.warning(f"源文件不存在: {source_path}")
                    failed_deletions.append(f"{filename}: 源文件不存在")
                    continue

                try:
                    # 检查文件权限
                    if not source_file.is_file():
                        logger.warning(f"路径不是文件: {source_path}")
                        failed_deletions.append(f"{filename}: 路径不是文件")
                        continue

                    # 删除源文件
                    source_file.unlink()
                    deleted_count += 1
                    logger.info(f"已删除失败源文件: {filename}")

                except Exception as e:
                    logger.error(f"删除文件失败 {filename}: {str(e)}")
                    failed_deletions.append(f"{filename}: {str(e)}")

            # 删除所有失败记录，只保留非失败记录
            remaining_records = [
                record for record in history if record.get('status') != 'failed']

            # 保存更新后的历史记录
            self.save_data('organize_history', remaining_records)

            message = f"已删除 {deleted_count} 个失败源文件"
            if failed_deletions:
                message += f"，{len(failed_deletions)} 个文件删除失败"
                logger.warning(f"部分文件删除失败: {failed_deletions}")

            logger.info(message)
            return schemas.Response(success=True, message=message)

        except Exception as e:
            logger.error(f"删除失败源文件失败: {e}")
            return schemas.Response(success=False, message=f"删除失败源文件失败: {str(e)}")

    def delete_skipped_files_api(self, req: BaseApiReq) -> schemas.Response:
        """
        API响应方法：删除所有跳过记录的源文件
        :param req: API请求体
        :return: 响应结果
        """
        try:
            # 验证 API 密钥
            if req.apikey != settings.API_TOKEN:
                return schemas.Response(success=False, message="API密钥错误")

            logger.info("开始批量删除跳过源文件")

            # 获取历史记录
            history = self.get_data('organize_history') or []
            logger.info(f"当前历史记录总数: {len(history)}")

            # 查找所有跳过记录
            skipped_records = [
                record for record in history if record.get('status') == 'skipped']
            logger.info(f"找到跳过记录数: {len(skipped_records)}")

            if not skipped_records:
                logger.info("没有跳过记录需要删除")
                return schemas.Response(success=True, message="没有跳过记录的源文件需要删除")

            deleted_count = 0
            failed_deletions = []
            updated_count = 0

            for record in skipped_records:
                filename = record.get('filename', '未知文件')
                source_path = record.get('source_path')

                if not source_path:
                    logger.warning(f"记录 {filename} 没有源文件路径")
                    failed_deletions.append(f"{filename}: 没有源文件路径")
                    continue

                source_file = Path(source_path)

                if not source_file.exists():
                    logger.warning(f"源文件不存在: {source_path}")
                    failed_deletions.append(f"{filename}: 源文件不存在")
                    continue

                try:
                    # 检查文件权限
                    if not source_file.is_file():
                        logger.warning(f"路径不是文件: {source_path}")
                        failed_deletions.append(f"{filename}: 路径不是文件")
                        continue

                    # 删除源文件
                    source_file.unlink()
                    deleted_count += 1
                    logger.info(f"已删除跳过源文件: {filename}")

                    # 将记录状态改为成功
                    record['status'] = 'success'
                    updated_count += 1

                except Exception as e:
                    logger.error(f"删除文件失败 {filename}: {str(e)}")
                    failed_deletions.append(f"{filename}: {str(e)}")

            # 更新所有跳过记录的状态为成功，而不是删除记录
            updated_history = []
            for record in history:
                if record.get('status') == 'skipped':
                    # 如果文件删除成功，状态改为success；否则保持skipped
                    if any(f"{record.get('filename', '未知文件')}: " in failure for failure in failed_deletions):
                        # 文件删除失败，保持skipped状态
                        updated_history.append(record)
                    else:
                        # 文件删除成功，状态改为success
                        record['status'] = 'success'
                        updated_history.append(record)
                else:
                    updated_history.append(record)

            # 保存更新后的历史记录
            self.save_data('organize_history', updated_history)

            message = f"已删除 {deleted_count} 个跳过源文件，并将 {updated_count} 条记录状态改为成功"
            if failed_deletions:
                message += f"，{len(failed_deletions)} 个文件删除失败"
                logger.warning(f"部分文件删除失败: {failed_deletions}")

            logger.info(message)
            return schemas.Response(success=True, message=message)

        except Exception as e:
            logger.error(f"删除跳过源文件失败: {e}")
            return schemas.Response(success=False, message=f"删除跳过源文件失败: {str(e)}")

    def get_dashboard_meta(self) -> Optional[List[Dict[str, str]]]:
        """
        获取插件仪表盘元信息
        """
        return [{
            "key": "music_stats",
            "name": "音乐整理统计"
        }]

    def get_dashboard(self, key: str, **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[dict]]]:
        """
        获取音乐整理统计仪表盘
        """
        try:
            # 获取历史记录
            history = self.get_data('organize_history') or []

            # 统计各种状态的数量
            success_count = len(
                [r for r in history if r.get('status') == 'success'])
            failed_count = len(
                [r for r in history if r.get('status') == 'failed'])
            skipped_count = len(
                [r for r in history if r.get('status') == 'skipped'])
            total_count = len(history)

            # 计算成功率（基于成功和跳过的记录）
            success_rate = round(
                ((success_count + skipped_count) / total_count * 100), 2) if total_count > 0 else 0

            # 仪表板col配置
            col_config = {
                "cols": 12,
                "md": 6
            }

            # 全局配置
            global_config = {
                "refresh": 30,  # 30秒自动刷新
                "border": True,
                "title": "音乐整理统计"
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
                                        "icon": "mdi-music-note",
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
                                                    "text": "总记录"
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
                            "text": "最近处理记录(20条)"
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
                                        "height": "300px"
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
                                                            "text": "文件名",
                                                            "props": {
                                                                "width": "400px",
                                                                "class": "text-left"
                                                            }
                                                        },
                                                        {
                                                            "component": "th",
                                                            "text": "艺术家",
                                                            "props": {
                                                                "width": "120px",
                                                                "class": "text-left"
                                                            }
                                                        },
                                                        {
                                                            "component": "th",
                                                            "text": "专辑",
                                                            "props": {
                                                                "width": "150px",
                                                                "class": "text-left"
                                                            }
                                                        },
                                                        {
                                                            "component": "th",
                                                            "text": "状态",
                                                            "props": {
                                                                "width": "80px",
                                                                "class": "text-left"
                                                            }
                                                        },
                                                        {
                                                            "component": "th",
                                                            "text": "时间",
                                                            "props": {
                                                                "width": "150px",
                                                                "class": "text-left"
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
                                                            "text": record.get('filename', '未知文件')[:50] + '...' if len(record.get('filename', '')) > 50 else record.get('filename', '未知文件'),
                                                            "props": {
                                                                "class": "text-left"
                                                            }
                                                        },
                                                        {
                                                            "component": "td",
                                                            "text": record.get('artist', '未知')[:15] + '...' if len(record.get('artist', '')) > 15 else record.get('artist', '未知'),
                                                            "props": {
                                                                "class": "text-left"
                                                            }
                                                        },
                                                        {
                                                            "component": "td",
                                                            "text": record.get('album', '未知')[:15] + '...' if len(record.get('album', '')) > 15 else record.get('album', '未知'),
                                                            "props": {
                                                                "class": "text-left"
                                                            }
                                                        },
                                                        {
                                                            "component": "td",
                                                            "content": [
                                                                {
                                                                    "component": "VChip",
                                                                    "props": {
                                                                        "color": "success" if record.get('status') == 'success' else "warning" if record.get('status') == 'skipped' else "error",
                                                                        "size": "small",
                                                                        "class": "text-left"
                                                                    },
                                                                    "text": "成功" if record.get('status') == 'success' else "已整" if record.get('status') == 'skipped' else "失败"
                                                                }
                                                            ],
                                                            "props": {
                                                                "class": "text-left"
                                                            }
                                                        },
                                                        {
                                                            "component": "td",
                                                            "text": self._format_datetime(record.get('timestamp', '')),
                                                            "props": {
                                                                "class": "text-left"
                                                            }
                                                        }
                                                    ]
                                                } for record in reversed(history[-20:])  # 显示最近20条记录
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
            logger.error(f"获取仪表盘数据失败: {e}")
            return None

    def _handle_incomplete_tags_file(self, file_path: Path, artist: str, album: str, title: str, year: str):
        """
        处理标签不完整的文件，记录并准备发送事件
        :param file_path: 文件路径
        :param artist: 艺术家信息
        :param album: 专辑信息
        :param title: 歌曲标题
        :param year: 年份信息
        """
        try:
            current_time = datetime.datetime.now()

            # 添加文件到标签不完整集合
            file_info = {
                'path': str(file_path),
                'filename': file_path.name,
                'artist': artist,
                'album': album,
                'title': title,
                'year': year,
                'detected_time': current_time
            }

            with lock:
                # 添加文件信息（避免重复添加）
                file_path_str = str(file_path)
                # 检查是否已存在相同文件路径的记录
                existing_index = next((i for i, f in enumerate(
                    self._incomplete_tags_files) if f['path'] == file_path_str), None)
                if existing_index is not None:
                    # 更新现有记录
                    self._incomplete_tags_files[existing_index] = file_info
                else:
                    # 添加新记录
                    self._incomplete_tags_files.append(file_info)

                # 更新最后文件事件时间
                self._last_file_event_time = current_time

                # 取消之前的计时器（如果存在）
                if self._incomplete_tags_timer:
                    self._incomplete_tags_timer.cancel()
                    self._incomplete_tags_timer = None

                # 如果开启了发送事件功能，创建计时器30秒后发送事件
                if self._send_events:
                    self._incomplete_tags_timer = threading.Timer(
                        30.0, self._send_incomplete_tags_event)
                    self._incomplete_tags_timer.daemon = True
                    self._incomplete_tags_timer.start()

            logger.info("有需要刮削的音频文件")

        except Exception as e:
            logger.error(f"处理标签不完整文件时出错: {e}")

    def _send_incomplete_tags_event(self):
        """
        发送标签不完整事件，通知需要刮削（简化版，不传输文件名）
        """
        try:
            with lock:
                # 检查30秒内是否有新文件事件
                if self._last_file_event_time:
                    time_diff = (datetime.datetime.now() -
                                 self._last_file_event_time).total_seconds()
                    if time_diff < 30:
                        # 30秒内有新文件事件，不发送事件
                        logger.debug("30秒内有新文件事件，延迟发送刮削事件")
                        return

                # 获取所有标签不完整的文件信息
                incomplete_files = self._incomplete_tags_files.copy()

                if not incomplete_files:
                    logger.debug("没有需要刮削的音频文件")
                    return

                # 清空列表（发送事件后不再需要跟踪这些文件）
                self._incomplete_tags_files.clear()

                # 构建简化的事件消息（不包含文件名）
                file_count = len(incomplete_files)

                event_title = "有需要刮削的音频文件"
                event_text = f"检测到{file_count}个音频文件需要刮削"

                # 发送简化事件 - 使用官方模板格式
                from app.core.event import eventmanager
                eventmanager.send_event(EventType.MetadataScrape, {
                    "channel": None,  # 可选：消息通道
                    "type": None,     # 可选：通知类型
                    "title": event_title,
                    "text": event_text,
                    "image": "",      # 可选：图片URL
                    "userid": ""      # 可选：用户ID
                })

                logger.info("已发送刮削事件：有需要刮削的音频文件")

        except Exception as e:
            logger.error(f"发送刮削事件时出错: {e}")

    def _send_unified_notify(self, artist_name: str = None, results: List[Dict] = None,
                             title: str = None, text: str = None):
        """
        统一通知方法 - 所有通知使用相同样式
        """
        if not self._notify:
            return

        try:
            # 根据不同的情况构建通知内容
            if artist_name and results:
                # 聚合通知
                total_files = len(results)
                success_count = sum(1 for r in results if r.get('success'))
                failed_count = total_files - success_count

                # 构建统一的通知内容（按照通用模板）
                notify_text = ""

                # 如果有成功文件，显示成功部分
                if success_count > 0:
                    notify_text += "🎶 成功整理的歌曲:\n"
                    success_files = [r for r in results if r.get('success')]
                    for i, result in enumerate(success_files):
                        # 尝试获取歌曲标题和专辑信息
                        tags = result.get('record', {}).get('tags', {})
                        title_song = tags.get('title', '')
                        album = tags.get('album', '')

                        if not title_song:
                            # 从文件名提取
                            filename = result.get(
                                'record', {}).get('filename', '')
                            if ' - ' in filename:
                                title_song = filename.split(
                                    ' - ', 1)[1].rsplit('.', 1)[0]
                            else:
                                title_song = filename.rsplit('.', 1)[0]

                        # 显示格式：序号. 歌曲名 [专辑名]
                        if album:
                            notify_text += f"{i+1}. {title_song} - {album}\n"
                        else:
                            notify_text += f"{i+1}. {title_song}\n"
                    notify_text += "\n"

                # 如果有失败文件，显示失败部分
                if failed_count > 0:
                    notify_text += "❌ 处理失败的歌曲:\n"
                    failed_files = [r for r in results if not r.get('success')]
                    for i, result in enumerate(failed_files):
                        filename = result.get('record', {}).get(
                            'filename', '未知文件')
                        # 获取失败原因
                        reason = result.get('record', {}).get('reason', '未知原因')
                        notify_text += f"{i+1}. {filename} ({reason})\n"
                    notify_text += "\n"

                # 添加处理时间
                notify_text += f"\n⏰ 时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"

                # 构建标题
                if failed_count > 0:
                    notify_title = f"🎵 《{artist_name}》 {failed_count}首歌曲整理失败"
                else:
                    notify_title = f"🎵 《{artist_name}》 {total_files}首歌曲整理完成"

            else:
                # 单个通知或系统通知
                notify_title = title if title else "音频文件处理完成"
                notify_text = text if text else ""

                # 为单个通知也添加时间戳，保持样式一致
                if notify_text:
                    notify_text += f"\n\n⏰ 时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"

            # 发送通知
            self.post_message(
                mtype=NotificationType.Manual,
                title=notify_title,
                text=notify_text,
                image="https://gitee.com/gldl137/wechat-work-bot/raw/master/images/yyzl.jpg"
            )

            logger.info(f"通知发送成功: {notify_title}")

        except Exception as e:
            logger.error(f"发送通知时出错: {str(e)}", exc_info=True)
