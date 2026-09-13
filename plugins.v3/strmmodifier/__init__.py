import hashlib
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple

from app.core.cache import TTLCache
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

# 线程锁
lock = threading.Lock()


class StrmFileHandler(FileSystemEventHandler):
    """
    STRM文件监控处理器
    """

    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(StrmFileHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        """
        文件创建事件处理
        """
        # 检查插件是否启用
        if not self.sync._enabled:
            return

        if not event.is_directory:
            self.sync.event_handler(
                event=event,
                text="创建",
                mon_path=self._watch_path,
                event_path=event.src_path,
            )


class StrmModifier(_PluginBase):
    # 插件名称
    plugin_name = "STRM文件转换"
    # 插件描述
    plugin_desc = "监控STRM文件修改成OpenList地址"
    # 插件图标
    plugin_icon = "link.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "strmmodifier_"
    # 加载顺序
    plugin_order = 27
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _observer = []
    _enabled = False
    _onlyonce = False
    _clear_history = False  # 清理记录开关
    _notify = False
    _monitor_paths = ""  # 多个监控目录配置，用换行分隔
    _openlist_url = ""   # OpenList地址配置
    _exclude_paths = ""  # 多个排除目录配置，用换行分隔
    _mode = "fast"  # compatibility/fast
    _sync_interval = 0
    _processed_files_cache = None  # 缓存实例
    _sync_thread = None  # 异步同步线程
    _stop_requested = False  # 停止请求标志
    
    # 通知聚合相关属性
    _notification_buffer = {"success": [], "failed": []}  # 通知缓冲区
    _last_notification_time = 0  # 上次发送通知的时间戳
    _notification_timer = None  # 通知定时器

    def __init__(self):
        super().__init__()
        # 创建缓存实例，最大1000项，TTL 24小时
        self._processed_files_cache = TTLCache(
            region="strm_modifier_processed",
            maxsize=1000,
            ttl=86400
        )

    def init_plugin(self, config: dict = None):
        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._clear_history = config.get("clear_history")
            self._notify = config.get("notify")
            self._monitor_paths = config.get(
                "monitor_paths") or ""  # 读取多个监控目录配置
            self._openlist_url = config.get(
                "openlist_url") or ""    # 读取OpenList地址配置
            self._exclude_paths = config.get(
                "exclude_paths") or ""  # 读取多个排除目录配置
            self._mode = config.get("mode") or "compatibility"
            self._sync_interval = float(config.get("sync_interval") or 0)

            # 添加调试日志
            logger.debug(
                f"读取配置: enabled={self._enabled}, onlyonce={self._onlyonce}, clear_history={self._clear_history}")

        # 停止现有服务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 启动监控
            if self._enabled and self._monitor_paths:
                # 支持多个监控目录，每行一个目录
                monitor_paths = [
                    path.strip() for path in self._monitor_paths.split('\n') if path.strip()]
                if monitor_paths:
                    logger.info(f"准备启动 {len(monitor_paths)} 个监控目录")
                    for path in monitor_paths:
                        self.start_monitor(path)
                else:
                    logger.warning("未配置有效的监控目录")
            elif self._enabled:
                logger.warning("插件已启用但未配置监控目录")

            # 立即运行一次
            if self._onlyonce:
                logger.info("STRM URL修改服务启动，立即运行一次")
                # 如果选择了清理记录，则清空已处理文件记录缓存
                if self._clear_history:
                    cache_size = len(list(self._processed_files_cache.items()))
                    self._processed_files_cache.clear()
                    logger.info(f"已清理 {cache_size} 个处理记录缓存")

                # 异步执行全量同步，避免阻塞UI
                self._run_async_sync()

                # 关闭一次性开关和清理记录开关
                logger.debug(
                    f"运行前开关状态: onlyonce={self._onlyonce}, clear_history={self._clear_history}")
                self._onlyonce = False
                self._clear_history = False
                logger.debug(
                    f"运行后开关状态: onlyonce={self._onlyonce}, clear_history={self._clear_history}")
                self.__update_config()
            elif self._clear_history:
                # 如果只选择了清理记录但没有选择立即运行一次，也要关闭清理记录开关
                cache_size = len(list(self._processed_files_cache.items()))
                logger.debug(
                    f"清理记录开关状态: clear_history={self._clear_history}, 当前缓存项数: {cache_size}")
                self._clear_history = False
                logger.debug(f"清理记录开关已关闭: clear_history={self._clear_history}")
                self.__update_config()

    def start_monitor(self, source_dir: str):
        """
        启动文件监控
        """
        try:
            # 检查监控目录是否设置
            if not source_dir:
                logger.warning("监控目录未设置，无法启动监控服务")
                return

            # 创建监控目录
            monitor_dir = Path(source_dir)
            if not monitor_dir.exists():
                try:
                    monitor_dir.mkdir(parents=True, exist_ok=True)
                    logger.info(f"创建监控目录: {source_dir}")
                except Exception as e:
                    logger.error(f"创建监控目录失败 {source_dir}: {str(e)}")
                    return

            # 选择监控模式
            observer = None
            if str(self._mode) == "compatibility":
                # 兼容模式，目录同步性能降低但可以兼容挂载的远程共享目录
                observer = PollingObserver(timeout=10)
                logger.info(f"使用兼容模式启动监控: {source_dir}")
            else:
                # 性能模式，内部处理系统操作类型选择最优解
                observer = Observer(timeout=10)
                logger.info(f"使用性能模式启动监控: {source_dir}")

            # 启动监控
            if observer:
                self._observer.append(observer)
                observer.schedule(
                    StrmFileHandler(source_dir, self),
                    path=source_dir,
                    recursive=True
                )
                observer.daemon = True
                observer.start()
                logger.info(f"{source_dir} 的STRM文件监控服务启动成功")

        except Exception as e:
            err_msg = str(e)
            if "inotify" in err_msg and "reached" in err_msg:
                logger.warn(
                    f"文件监控服务启动出现异常：{err_msg}，请在宿主机上（不是docker容器内）执行以下命令并重启："
                    + """
                    echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
                    echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf
                    sudo sysctl -p
                    """
                )
            else:
                logger.error(f"{source_dir} 启动文件监控失败：{err_msg}")
                logger.error(traceback.format_exc())

    def __update_config(self):
        """
        更新配置
        """
        logger.debug(
            f"保存配置: enabled={self._enabled}, onlyonce={self._onlyonce}, clear_history={self._clear_history}")
        self.update_config(
            {
                "enabled": self._enabled,
                "onlyonce": self._onlyonce,
                "clear_history": self._clear_history,
                "notify": self._notify,
                "monitor_paths": self._monitor_paths,   # 保存多个监控目录配置
                "openlist_url": self._openlist_url,     # 保存OpenList地址配置
                "exclude_paths": self._exclude_paths,   # 保存多个排除目录配置
                "mode": self._mode,
                "sync_interval": self._sync_interval,
            }
        )

    def event_handler(self, event, mon_path: str, text: str, event_path: str):
        """
        处理文件变化
        :param event: 事件
        :param mon_path: 监控目录
        :param text: 事件描述
        :param event_path: 事件文件路径
        """
        # 再次检查插件是否启用（防止在监控运行期间插件被禁用）
        if not self._enabled:
            return

        if not event.is_directory:
            # 只处理strm文件
            if event_path.endswith('.strm'):
                logger.debug("文件%s：%s" % (text, event_path))
                # 在处理文件时，需要找到对应的监控目录来计算相对路径
                result = self.__handle_file(event_path=event_path, mon_path=mon_path)
                # 如果处理成功且开启了通知，添加到通知缓冲区
                if result and self._notify:
                    logger.debug(f"处理结果: 成功={result.get('success')}, 跳过={result.get('skipped')}, 错误={result.get('error')}, 文件名={result.get('filename')}")
                    if result.get("success"):
                        logger.debug(f"添加到成功缓冲区: {result['filename']}")
                        self._add_to_notification_buffer("success", result["filename"])
                    # 只有【有错误】且【不是正常跳过】才进失败缓冲区
                    elif result.get("error") and not result.get("skipped"):
                        logger.debug(f"添加到失败缓冲区: {result['filename']} - 错误: {result['error']}")
                        self._add_to_notification_buffer("failed", result["filename"])
                    # 如果是跳过状态，记录但不添加到缓冲区
                    elif result.get("skipped"):
                        logger.debug(f"文件被正常跳过，不添加到通知缓冲区: {result['filename']} - 原因: {result['error']}")

    def __handle_file(self, event_path: str, mon_path: str) -> Dict[str, Any]:
        """
        处理单个STRM文件
        :param event_path: 事件文件路径
        :param mon_path: 监控目录
        :return: 处理结果字典
        """
        file_path = Path(event_path)
        result = {
            "success": False,
            "skipped": False,  # 新增：标记是否为正常跳过
            "filename": file_path.name,
            "error": None,
            "old_url": "",
            "new_url": ""
        }
        try:
            # 检查是否在排除目录中
            if self._exclude_paths:
                exclude_paths = [
                    path.strip() for path in self._exclude_paths.split('\n') if path.strip()]
            for exclude_path_str in exclude_paths:
                exclude_path = Path(exclude_path_str)
                try:
                    file_path.relative_to(exclude_path)
                    logger.debug(f"文件 {file_path} 在排除目录中，跳过处理")
                    result["error"] = "文件在排除目录中"
                    result["skipped"] = True  # 标记为正常跳过
                    return result
                except ValueError:
                    # 文件不在当前检查的排除目录中，继续检查下一个
                    continue

            if not file_path.exists():
                logger.debug(f"文件不存在，跳过处理: {file_path}")
                result["error"] = "文件不存在"
                result["skipped"] = True  # 标记为正常跳过
                return result

            # 获取文件修改时间
            file_mtime = self.__get_file_mtime(str(file_path))
            if not file_mtime:
                logger.debug(f"无法获取文件修改时间，跳过处理: {file_path}")
                result["error"] = "无法获取文件修改时间"
                result["skipped"] = True  # 标记为正常跳过
                return result

            # 检查是否已处理过相同内容的文件（增强的去重机制）
            cached_mtime = self._processed_files_cache.get(str(file_path))
            if cached_mtime and cached_mtime == file_mtime:
                # 减少日志输出，只在调试模式下显示
                # logger.debug(f"文件已处理过，跳过: {file_path}")
                result["error"] = "文件已处理过"
                result["skipped"] = True  # 标记为正常跳过
                return result

            # 增加额外的去重检查，防止同一文件在极短时间内被重复处理
            current_time = time.time()
            last_processed_key = f"{str(file_path)}_last_processed"
            last_processed_time = self._processed_files_cache.get(
                last_processed_key)
            # 5秒内不重复处理（增加去重时间窗口）
            if last_processed_time and (current_time - last_processed_time) < 5.0:
                logger.debug(f"文件在5秒内已处理过，跳过: {file_path}")
                result["error"] = "文件在5秒内已处理过"
                result["skipped"] = True  # 标记为正常跳过
                return result

            # 全程加锁
            with lock:
                logger.debug(f"开始处理STRM文件: {file_path}")

                # 读取文件内容
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                except Exception as e:
                    logger.error(f"读取文件失败 {file_path}: {str(e)}")
                    result["error"] = f"读取文件失败: {str(e)}"
                    return result

                # 检查是否已经是OpenList地址，如果是则跳过处理
                openlist_base_url = self._openlist_url.rstrip(
                    '/') if self._openlist_url else ""
                if openlist_base_url and content.startswith(openlist_base_url):
                    # 减少日志输出，只在调试模式下显示
                    # logger.debug(f"文件 {file_path} 已经是OpenList地址，无需修改")
                    # 添加到已处理文件缓存，避免下次重复处理
                    self._processed_files_cache[str(
                        file_path)] = self.__get_file_mtime(str(file_path))
                    result["error"] = "文件已经是OpenList地址"
                    result["skipped"] = True  # 标记为正常跳过
                    return result

                # 解析原始URL
                # 例如: http://192.168.1.100:3005/proxy/cb26e61dfdb39d4346aed01c466f5dafecfb015912dc85ec02338c0c2db24395/海达 (2025).mkv
                url_parts = content.split('/')
                if len(url_parts) < 6:
                    logger.warning(
                        f"URL格式不正确，跳过处理: '{content}' (文件: {file_path})")
                    # 添加到已处理文件缓存，避免下次重复处理错误格式的文件
                    self._processed_files_cache[str(
                        file_path)] = self.__get_file_mtime(str(file_path))
                    result["error"] = "URL格式不正确"
                    result["skipped"] = True  # 标记为正常跳过
                    return result

                # 提取原始URL中的文件名
                original_filename = url_parts[-1]  # 海达 (2025).mkv
                if not original_filename:
                    logger.warning(
                        f"无法从URL中提取文件名: '{content}' (文件: {file_path})")
                    # 添加到已处理文件缓存，避免下次重复处理
                    self._processed_files_cache[str(
                        file_path)] = self.__get_file_mtime(str(file_path))
                    result["error"] = "无法从URL中提取文件名"
                    result["skipped"] = True  # 标记为正常跳过
                    return result

                # 获取penList地址
                #  http://192.168.1.100:5244
                new_base_url = self._openlist_url.rstrip(
                    '/') if self._openlist_url else ""

                # 获取相对路径（包含文件名）
                # 计算相对路径部分（相对于监控目录），包含文件名
                # 文件路径: /STRM影视/网盘/天翼云盘5/个人云盘5/电影/海达 (2025)/海达 (2025).strm
                # 监控目录: /STRM影视/网盘/
                # 相对路径结果: 天翼云盘5/个人云盘5/电影/海达 (2025)/海达 (2025).strm
                try:
                    relative_path = str(file_path.relative_to(
                        mon_path)).replace('\\', '/')
                    # 移除末尾的斜杠（如果有的话）
                    relative_path = relative_path.rstrip('/')
                    # 移除开头的斜杠（如果有的话）
                    relative_path = relative_path.lstrip('/')
                except ValueError as e:
                    logger.warning(
                        f"无法计算相对路径: {file_path} 相对于 {mon_path}，错误: {str(e)}")
                    # 添加到已处理文件缓存，避免下次重复处理
                    self._processed_files_cache[str(
                        file_path)] = self.__get_file_mtime(str(file_path))
                    result["error"] = "无法计算相对路径"
                    result["skipped"] = True  # 标记为正常跳过
                    return result

                # 获取文件扩展名并替换
                # 将.strm扩展名替换为原始文件的扩展名
                # 例如: 海达 (2025).strm -> 海达 (2025).mkv
                relative_path_with_extension = relative_path
                # 我们知道处理的文件一定是.strm文件，使用原始URL中的实际扩展名
                if relative_path.lower().endswith('.strm'):
                    # 从原始URL中提取扩展名
                    if '.' in original_filename:
                        original_extension = original_filename.split('.')[-1]
                        relative_path_with_extension = relative_path[:-
                                                                     4] + original_extension

                # 构建新URL
                # 例如: http://192.168.1.100:5244/d/天翼云盘5/个人云盘5/电影/海达 (2025)/海达 (2025).mkv
                new_url = f"{new_base_url}/d/{relative_path_with_extension}"

                # 写入到strm文件
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(new_url)
                except Exception as e:
                    logger.error(f"写入文件失败 {file_path}: {str(e)}")
                    result["error"] = "写入文件失败"
                    return result

                # 更新已处理文件缓存（使用新的文件修改时间）
                self._processed_files_cache[str(
                    file_path)] = self.__get_file_mtime(str(file_path))

                # 记录处理时间，防止短时间内重复处理
                current_time = time.time()
                last_processed_key = f"{str(file_path)}_last_processed"
                self._processed_files_cache[last_processed_key] = current_time

                logger.info(f"已修改STRM文件: {file_path}")
                logger.debug(f"原URL: {content}")
                logger.debug(f"新URL: {new_url}")

                # 保存处理记录
                history = self.get_data('history') or []
                history.append({
                    "filename": file_path.name,
                    "old_url": content,
                    "new_url": new_url,
                    "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                })
                # 只保留最近50条记录
                if len(history) > 50:
                    history = history[-50:]
                self.save_data('history', history)

                # 设置成功结果
                result["success"] = True
                result["old_url"] = content
                result["new_url"] = new_url
                
                # 返回成功结果
                return result

        except Exception as e:
            logger.error(f"处理STRM文件失败 {file_path}: {str(e)}")
            logger.error(traceback.format_exc())
            # 设置错误结果并返回
            result["error"] = f"处理异常: {str(e)}"
            return result

    def sync_all(self):
        """
        立即运行一次，全量同步目录中所有文件
        """
        try:
            logger.info("开始全量同步STRM文件 ...")
            if not self._monitor_paths:
                logger.warning("监控路径未设置，无法执行全量同步")
                return

            # 解析多个监控目录
            monitor_paths = [
                path.strip() for path in self._monitor_paths.split('\n') if path.strip()]
            if not monitor_paths:
                logger.warning("未配置有效的监控路径")
                return

            total_files = 0
            processed_count = 0  # 实际处理（修改）的文件数
            excluded_count = 0
            skipped_count = 0    # 跳过的文件数（已处理过或不需要处理）

            # 解析排除目录
            exclude_paths = []
            if self._exclude_paths:
                exclude_paths = [
                    Path(path.strip()) for path in self._exclude_paths.split('\n') if path.strip()]

            for monitor_path_str in monitor_paths:
                monitor_dir = Path(monitor_path_str)
                if not monitor_dir.exists():
                    logger.warn(f"监控目录不存在: {monitor_path_str}")
                    continue

                strm_files = list(monitor_dir.rglob("*.strm"))
                total_files += len(strm_files)
                logger.info(
                    f"在目录 {monitor_path_str} 中发现 {len(strm_files)} 个STRM文件")

                for strm_file in strm_files:
                    # 检查是否在排除目录中
                    excluded = False
                    for exclude_path in exclude_paths:
                        try:
                            strm_file.relative_to(exclude_path)
                            logger.debug(f"文件 {strm_file} 在排除目录中，跳过处理")
                            excluded_count += 1
                            excluded = True
                            break
                        except ValueError:
                            # 文件不在当前检查的排除目录中，继续检查下一个
                            continue

                    if excluded:
                        continue

                    file_path = str(strm_file)
                    # 获取文件修改时间
                    file_mtime = self.__get_file_mtime(file_path)
                    if not file_mtime:
                        logger.debug(f"无法获取文件修改时间，跳过处理: {strm_file}")
                        continue

                    # 只有当文件未处理过或者需要清理记录时才处理文件
                    cached_mtime = self._processed_files_cache.get(file_path)
                    if not cached_mtime or cached_mtime != file_mtime or self._clear_history:
                        # 注意：这里不要提前添加到_processed_files_cache中，应该在实际处理后再添加
                        # 先检查是否需要实际处理该文件
                        need_processing = self.__check_if_need_processing(
                            file_path)
                        if need_processing:
                            self.__handle_file(
                                event_path=file_path, mon_path=str(monitor_dir))
                            processed_count += 1
                        else:
                            logger.debug(f"文件 {strm_file} 不需要处理，跳过")
                            skipped_count += 1
                            # 即使不需要处理，也要添加到已处理文件缓存中，避免下次重复检查
                            self._processed_files_cache[file_path] = file_mtime
                    else:
                        logger.debug(f"文件已处理过，跳过: {strm_file}")
                        skipped_count += 1

                    # 等待间隔
                    if self._sync_interval > 0:
                        time.sleep(self._sync_interval)

            logger.info(f"全量同步STRM文件完成！共发现 {total_files} 个文件，"
                        f"实际修改 {processed_count} 个文件，"
                        f"排除 {excluded_count} 个文件，"
                        f"跳过 {skipped_count} 个文件")
        except Exception as e:
            logger.error(f"全量同步STRM文件失败：{str(e)}")
            logger.error(traceback.format_exc())

    def _run_async_sync(self):
        """
        异步执行全量同步
        """
        try:
            # 如果已经有同步线程在运行，先停止它
            if self._sync_thread and self._sync_thread.is_alive():
                logger.info("检测到已有同步线程在运行，请求停止")
                self._stop_requested = True
                self._sync_thread.join(timeout=5.0)  # 等待5秒让线程停止
                if self._sync_thread.is_alive():
                    logger.warning("同步线程未能及时停止，强制中断")
                else:
                    logger.info("已停止之前的同步线程")
                self._stop_requested = False

            # 使用线程异步执行全量同步，避免阻塞UI
            self._sync_thread = threading.Thread(
                target=self.sync_all_optimized)
            self._sync_thread.daemon = True
            self._sync_thread.start()
            logger.info("已启动异步全量同步线程（使用优化版本）")
        except Exception as e:
            logger.error(f"启动异步同步线程失败: {str(e)}")
            logger.error(traceback.format_exc())

    def sync_all_optimized(self):
        """
        优化的全量同步 - 使用多线程并行处理
        """
        try:
            logger.info("开始优化的全量同步STRM文件 ...")

            # 检查是否请求停止
            if self._stop_requested:
                logger.info("收到停止请求，取消同步操作")
                return

            if not self._monitor_paths:
                logger.warning("监控路径未设置，无法执行全量同步")
                return

            # 解析多个监控目录
            monitor_paths = [
                path.strip() for path in self._monitor_paths.split('\n') if path.strip()]
            if not monitor_paths:
                logger.warning("未配置有效的监控路径")
                return

            # 解析排除目录
            exclude_paths = []
            if self._exclude_paths:
                exclude_paths = [
                    Path(path.strip()) for path in self._exclude_paths.split('\n') if path.strip()]

            # 收集所有需要处理的文件
            all_files_to_process = []

            for monitor_path_str in monitor_paths:
                # 检查是否请求停止
                if self._stop_requested:
                    logger.info("收到停止请求，取消文件收集")
                    return

                monitor_dir = Path(monitor_path_str)
                if not monitor_dir.exists():
                    logger.warn(f"监控目录不存在: {monitor_path_str}")
                    continue

                strm_files = list(monitor_dir.rglob("*.strm"))
                logger.info(
                    f"在目录 {monitor_path_str} 中发现 {len(strm_files)} 个STRM文件")

                for strm_file in strm_files:
                    # 检查是否请求停止
                    if self._stop_requested:
                        logger.info("收到停止请求，取消文件处理")
                        return

                    # 检查是否在排除目录中
                    excluded = False
                    for exclude_path in exclude_paths:
                        try:
                            strm_file.relative_to(exclude_path)
                            excluded = True
                            break
                        except ValueError:
                            continue

                    if excluded:
                        continue

                    file_path = str(strm_file)
                    # 快速检查是否需要处理
                    if self._quick_check_need_processing(file_path):
                        all_files_to_process.append(
                            (file_path, str(monitor_dir)))

            logger.info(f"共找到 {len(all_files_to_process)} 个需要处理的文件")

            # 发送通知
            if len(all_files_to_process) == 0:
                # 没有需要处理的文件
                if self._notify:
                    self._send_notification("STRM文件同步", [], 0)
                return

            # 检查是否请求停止
            if self._stop_requested:
                logger.info("收到停止请求，取消并行处理")
                return

            # 使用多线程并行处理
            self._process_files_parallel(all_files_to_process)

        except Exception as e:
            logger.error(f"优化的全量同步STRM文件失败：{str(e)}")
            logger.error(traceback.format_exc())

    def _quick_check_need_processing(self, file_path: str) -> bool:
        """
        快速检查文件是否需要处理
        优化：避免频繁的文件读取和复杂的检查
        """
        try:
            # 首先检查缓存
            file_mtime = self.__get_file_mtime(file_path)
            if not file_mtime:
                logger.debug(f"文件修改时间获取失败，跳过: {file_path}")
                return False

            cached_mtime = self._processed_files_cache.get(file_path)
            if cached_mtime and cached_mtime == file_mtime:
                logger.debug(f"文件已处理过，跳过: {file_path}")
                return False

            # 检查是否已经配置了OpenList地址
            openlist_base_url = self._openlist_url.rstrip(
                '/') if self._openlist_url else ""
            if not openlist_base_url:
                logger.warning("未配置OpenList地址，所有文件都会被跳过")
                return False

            # 快速读取文件内容（只读取前200个字符用于检查）
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read(200).strip()
            except Exception as e:
                logger.debug(f"读取文件内容失败 {file_path}: {str(e)}")
                # 尝试其他编码
                try:
                    with open(file_path, 'r', encoding='gbk') as f:
                        content = f.read(200).strip()
                except:
                    # 如果还是失败，回退到详细检查
                    return self.__check_if_need_processing(file_path)

            # 检查是否已经是OpenList地址
            if openlist_base_url and content.startswith(openlist_base_url):
                # 减少日志输出，只在调试模式下显示
                # logger.debug(f"文件已经是OpenList地址，跳过: {file_path}")
                return False

            # 检查是否为有效的HTTP地址
            if content.startswith("http://") or content.startswith("https://"):
                logger.debug(f"文件需要处理: {file_path}")
                return True

            logger.debug(f"文件内容不是HTTP地址，跳过: {file_path}")
            logger.debug(f"文件内容预览: {content[:50]}...")
            return False
        except Exception as e:
            logger.debug(f"快速检查异常，回退到详细检查 {file_path}: {str(e)}")
            # 如果快速检查失败，回退到详细检查
            return self.__check_if_need_processing(file_path)

    def _process_files_parallel(self, files_to_process):
        """
        使用多线程并行处理文件，并收集处理结果发送通知
        """
        try:
            # 检查是否请求停止
            if self._stop_requested:
                logger.info("收到停止请求，取消并行处理")
                return

            # 根据CPU核心数确定线程数（最多8个线程）
            import os
            max_threads = min(os.cpu_count() or 4, 8, len(files_to_process))

            # 收集处理结果
            success_files = []
            failed_files = []

            if max_threads <= 1:
                # 单线程处理
                for file_path, monitor_dir in files_to_process:
                    # 检查是否请求停止
                    if self._stop_requested:
                        logger.info("收到停止请求，取消单线程处理")
                        return
                    result = self.__handle_file(file_path, monitor_dir)
                    if result and result.get("success"):
                        success_files.append(result["filename"])
                    # 只有【有错误】且【不是正常跳过】才进失败列表
                    elif result and result.get("error") and not result.get("skipped"):
                        failed_files.append(result["filename"])
                
                # 发送通知
                if success_files:
                    self._send_notification("STRM替换成功", success_files, len(success_files))
                if failed_files:
                    self._send_notification("STRM替换失败", failed_files, len(failed_files))
                
                return

            # 创建线程池
            from concurrent.futures import ThreadPoolExecutor, as_completed

            processed_count = 0
            total_count = len(files_to_process)

            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # 提交所有任务
                future_to_file = {}
                for file_path, monitor_dir in files_to_process:
                    # 检查是否请求停止
                    if self._stop_requested:
                        logger.info("收到停止请求，取消任务提交")
                        # 取消已提交的任务
                        for future in future_to_file:
                            future.cancel()
                        return
                    future = executor.submit(
                        self.__handle_file, file_path, monitor_dir)
                    future_to_file[future] = (file_path, monitor_dir)

                # 等待所有任务完成
                for future in as_completed(future_to_file):
                    # 检查是否请求停止
                    if self._stop_requested:
                        logger.info("收到停止请求，取消剩余任务")
                        # 取消未完成的任务
                        for remaining_future in future_to_file:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        return

                    try:
                        result = future.result()
                        if result and result.get("success"):
                            success_files.append(result["filename"])
                        # 只有【有错误】且【不是正常跳过】才进失败列表
                        elif result and result.get("error") and not result.get("skipped"):
                            failed_files.append(result["filename"])
                        
                        processed_count += 1
                        if processed_count % 100 == 0:
                            logger.info(
                                f"处理进度: {processed_count}/{total_count} ({processed_count/total_count*100:.1f}%)")
                    except Exception as e:
                        file_path, monitor_dir = future_to_file[future]
                        logger.error(f"处理文件失败 {file_path}: {str(e)}")
                        failed_files.append(Path(file_path).name)

            logger.info(f"并行处理完成！成功: {len(success_files)}, 失败: {len(failed_files)}")

            # 发送通知
            if success_files:
                self._send_notification("STRM替换成功", success_files, len(success_files))
            if failed_files:
                self._send_notification("STRM替换失败", failed_files, len(failed_files))

        except Exception as e:
            logger.error(f"并行处理文件失败: {str(e)}")
            logger.error(traceback.format_exc())

    def __check_if_need_processing(self, file_path: str) -> bool:
        """
        检查文件是否需要处理
        :param file_path: 文件路径
        :return: 是否需要处理
        """
        path_obj = Path(file_path)
        if not path_obj.exists():
            logger.debug(f"文件不存在，跳过详细检查: {file_path}")
            return False

        try:
            # 读取文件内容，尝试多种编码
            content = ""
            try:
                with open(path_obj, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
            except UnicodeDecodeError:
                try:
                    with open(path_obj, 'r', encoding='gbk') as f:
                        content = f.read().strip()
                except UnicodeDecodeError:
                    try:
                        with open(path_obj, 'r', encoding='latin-1') as f:
                            content = f.read().strip()
                    except Exception as e:
                        logger.error(f"无法读取文件内容 {file_path}: {str(e)}")
                        return False

            # 检查是否已经配置了OpenList地址，如果没有配置则不需要处理
            openlist_base_url = self._openlist_url.rstrip(
                '/') if self._openlist_url else ""
            if not openlist_base_url:
                logger.debug(f"未配置OpenList地址，跳过详细检查: {file_path}")
                return False

            # 检查是否已经是OpenList地址
            if openlist_base_url and content.startswith(openlist_base_url):
                # 减少日志输出，只在调试模式下显示
                # logger.debug(f"文件已经是OpenList地址，跳过详细检查: {file_path}")
                return False

            # 检查是否为有效的HTTP地址（原始代理地址），但不是OpenList地址
            if content.startswith("http://") or content.startswith("https://"):
                logger.debug(f"文件需要处理（详细检查）: {file_path}")
                logger.debug(f"文件内容: {content[:100]}...")
                return True

            logger.debug(f"文件内容不是HTTP地址，跳过详细检查: {file_path}")
            logger.debug(f"文件内容预览: {content[:100]}...")
            return False
        except Exception as e:
            logger.error(f"检查文件是否需要处理失败 {file_path}: {str(e)}")
            return False

    def __get_file_mtime(self, file_path: str) -> float:
        """
        获取文件的修改时间
        :param file_path: 文件路径
        :return: 文件的修改时间戳
        """
        try:
            return Path(file_path).stat().st_mtime
        except Exception as e:
            logger.debug(f"获取文件修改时间失败 {file_path}: {str(e)}")
            return 0

    def _add_to_notification_buffer(self, buffer_type: str, filename: str):
        """
        添加文件到通知缓冲区
        :param buffer_type: 缓冲区类型（"success" 或 "failed"）
        :param filename: 文件名
        """
        # 使用线程锁确保缓冲区操作安全
        with lock:
            # 检查文件是否已在缓冲区中，避免重复添加
            if filename in self._notification_buffer[buffer_type]:
                logger.debug(f"文件 {filename} 已存在于 {buffer_type} 缓冲区，跳过重复添加")
                return
                
            # 检查文件是否在另一个缓冲区中，如果存在则移除
            other_buffer = "failed" if buffer_type == "success" else "success"
            if filename in self._notification_buffer[other_buffer]:
                logger.warning(f"文件 {filename} 已存在于 {other_buffer} 缓冲区，移除后重新添加到 {buffer_type} 缓冲区")
                self._notification_buffer[other_buffer].remove(filename)
            
            # 添加到缓冲区
            self._notification_buffer[buffer_type].append(filename)
            
            # 重置定时器
            if self._notification_timer:
                self._notification_timer.cancel()
            
            # 设置10秒后发送通知的定时器
            self._notification_timer = threading.Timer(10.0, self._send_buffered_notifications)
            self._notification_timer.daemon = True
            self._notification_timer.start()
            
            logger.debug(f"文件 {filename} 已添加到 {buffer_type} 缓冲区，定时器已重置，当前缓冲区状态: 成功={len(self._notification_buffer['success'])}, 失败={len(self._notification_buffer['failed'])}")

    def _send_buffered_notifications(self):
        """
        发送缓冲区中的通知
        """
        try:
            # 使用线程锁确保缓冲区操作安全
            with lock:
                # 复制缓冲区数据，避免在发送过程中被修改
                success_files = self._notification_buffer["success"].copy()
                failed_files = self._notification_buffer["failed"].copy()
                
                # 检查缓冲区是否为空，避免发送空通知
                if not success_files and not failed_files:
                    logger.debug("通知缓冲区为空，跳过发送")
                    return
                
                # 清空缓冲区
                self._notification_buffer = {"success": [], "failed": []}
                self._last_notification_time = time.time()
            
            # 发送成功通知
            if success_files:
                self._send_notification("STRM替换成功", success_files, len(success_files))
                logger.info(f"发送聚合成功通知，共 {len(success_files)} 个文件")
            
            # 发送失败通知
            if failed_files:
                self._send_notification("STRM替换失败", failed_files, len(failed_files))
                logger.info(f"发送聚合失败通知，共 {len(failed_files)} 个文件")
            
        except Exception as e:
            logger.error(f"发送聚合通知失败: {str(e)}")

    def _send_notification(self, title: str, files: list, total_count: int):
        """
        发送统一格式的通知
        :param title: 通知标题
        :param files: 文件列表
        :param total_count: 总文件数
        """
        if not self._notify:
            return
            
        # 构建通知内容
        content_lines = []
        if title == "STRM替换成功":
            content_lines.append("成功文件如下")
        elif title == "STRM替换失败":
            content_lines.append("失败文件如下")
        else:
            content_lines.append("没有需要替换的STRM文件")
        
        # 列出前5个文件
        for i, filename in enumerate(files[:5], 1):
            content_lines.append(f"{i}. {filename}")
        
        # 如果有更多文件，显示省略号和总数
        if len(files) > 5:
            content_lines.append(f".....等共({len(files)})个")
        else:
            content_lines.append(f".....等共({len(files)})个")
        
        # 添加时间戳
        content_lines.append(f"时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        
        # 发送通知
        self.post_message(
            mtype=NotificationType.MediaServer,
            title=title,
            text="\n".join(content_lines),
            image="https://gitee.com/gldl137/wechat-work-bot/raw/master/images/xgstrm.jpg"
        )

    def get_state(self) -> bool:
        return self._enabled

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        注册远程命令
        :return: 命令关键字、事件、描述、附带数据
        """
        return [
            {
                "cmd": "/strm_modify",
                "event": EventType.PluginAction,
                "desc": "STRM文件转换",
                "category": "插件",
                "data": {"action": "strm_modify"}
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """拼装插件配置页面"""
        return [
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
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
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {
                                    'model': 'notify',
                                    'label': '开启通知',
                                }
                            }
                        ]
                    },
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
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
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
                        'content': [
                            {
                                'component': 'VSwitch',
                                'props': {
                                    'model': 'clear_history',
                                    'label': '清理记录',
                                }
                            }
                        ]
                    },
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
                        'content': [
                            {
                                'component': 'VSelect',
                                'props': {
                                    'model': 'mode',
                                    'label': '监控模式',
                                    'items': [
                                        {
                                            'title': '兼容模式',
                                            'value': 'compatibility',
                                        },
                                        {
                                            'title': '性能模式',
                                            'value': 'fast'
                                        },
                                    ],
                                }
                            }
                        ]
                    },
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12,
                            'md': 4
                        },
                        'content': [
                            {
                                'component': 'VTextField',
                                'props': {
                                    'model': 'sync_interval',
                                    'label': '同步间隔(秒)',
                                    'placeholder': '0'
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
                        'props': {
                            'cols': 12
                        },
                        'content': [
                            {
                                'component': 'VTextField',
                                'props': {
                                    'model': 'openlist_url',
                                    'label': 'OpenList地址',
                                    'placeholder': 'http://your-openlist-server:5244',
                                    'required': True
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
                        'props': {
                            'cols': 12
                        },
                        'content': [
                            {
                                'component': 'VTextarea',
                                'props': {
                                    'model': 'monitor_paths',
                                    'label': '监控目录',
                                    'placeholder': '/STRM影视/网盘/',
                                    'rows': 4
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
                        'props': {
                            'cols': 12
                        },
                        'content': [
                            {
                                'component': 'VTextarea',
                                'props': {
                                    'model': 'exclude_paths',
                                    'label': '排除目录',
                                    'placeholder': '/STRM影视/网盘/排除/',
                                    'rows': 4
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
                        'props': {
                            'cols': 12
                        },
                        'content': [
                            {
                                'component': 'VAlert',
                                'props': {
                                    'type': 'info',
                                    'variant': 'tonal',
                                    'density': 'compact',
                                    'class': 'mt-2',
                                },
                                'content': [
                                    {
                                        'component': 'div',
                                        'text': '监控目录配置:',
                                    },
                                    {
                                        'component': 'div',
                                        'text': 'OpenList目录:/天翼云盘5/个人5/电影/A.MP4.',
                                    },
                                    {
                                        'component': 'div',
                                        'text': '生成strm目录:/STRM影视/网盘/天翼云盘5/个人5/电影/A.MP4',
                                    },
                                    {
                                        'component': 'div',
                                        'text': '监控目录填写:/STRM影视/网盘.',
                                    },
                                    {
                                        'component': 'div',
                                        'text': '生成的STRM目录减掉和OpenList目录相同的部分.',
                                    },
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "clear_history": False,
            "notify": False,
            "monitor_paths": "",
            "exclude_paths": "",
            "openlist_url": "",
            "mode": "fast",
            "sync_interval": 0
        }

    def get_page(self) -> List[dict]:
        pass

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event):
        """
        处理远程命令事件
        :param event: 事件对象
        """
        if not self._enabled:
            logger.warning("STRM文件转换插件未启用，忽略命令")
            return

        if not event or not event.event_data:
            logger.error("事件数据为空，无法处理命令")
            return

        # 获取事件数据
        event_data = event.event_data
        action = event_data.get("action")

        if not action:
            logger.error("命令事件缺少action参数")
            return

        logger.info(f"收到STRM文件转换命令，action: {action}")

        try:
            if action == "strm_modify":
                # 立即运行一次STRM文件转换
                logger.info("开始执行STRM文件转换...")
                if hasattr(self, '_run_async_sync'):
                    self._run_async_sync()
                    logger.info("STRM文件转换命令执行完成")
                else:
                    logger.error("插件没有可用的立即运行方法")
            else:
                logger.warning(f"未知的命令action: {action}")

        except Exception as e:
            logger.error(f"处理STRM文件转换命令时发生错误: {str(e)}")
            logger.error(f"错误详情: {e}")

    def stop_service(self):
        """
        退出插件
        """
        # 停止文件监控
        stopped_count = 0
        if self._observer:
            for observer in self._observer:
                try:
                    observer.stop()
                    # 检查observer是否已启动，避免join未启动的线程
                    if hasattr(observer, '_thread') and observer._thread and observer._thread.is_alive():
                        observer.join()
                    stopped_count += 1
                except Exception as e:
                    logger.error(f"停止文件监控失败: {str(e)}")
            self._observer = []

        if stopped_count > 0:
            logger.info(f"已停止 {stopped_count} 个文件监控器")

        # 停止后台同步线程
        if self._sync_thread and self._sync_thread.is_alive():
            logger.info("检测到后台同步线程正在运行，请求停止")
            self._stop_requested = True
            # 等待线程停止，最多等待5秒
            self._sync_thread.join(timeout=5.0)
            if self._sync_thread.is_alive():
                logger.warning("同步线程未能及时停止，强制中断")
            else:
                logger.info("已停止后台同步线程")
            self._stop_requested = False

        # 清空处理记录缓存
        if hasattr(self, '_processed_files_cache') and self._processed_files_cache:
            processed_count = len(list(self._processed_files_cache.items()))
            self._processed_files_cache.clear()
            if processed_count > 0:
                logger.info(f"STRM URL修改器已停止，清理了 {processed_count} 个处理记录")

        # 停止通知定时器
        if self._notification_timer:
            self._notification_timer.cancel()
            self._notification_timer = None
        
        # 清空通知缓冲区
        self._notification_buffer = {"success": [], "failed": []}
        self._last_notification_time = 0
