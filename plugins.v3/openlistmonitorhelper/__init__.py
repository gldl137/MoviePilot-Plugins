"""
OpenList监测助手插件
通过HTTP API检测OpenList存储状态，监控存储异常情况
"""
import os
import time
import threading
import requests
from pathlib import Path
from typing import Any, Dict, List, Tuple

from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import NotificationType
from app.chain.storage import StorageChain


class OpenListMonitorHelper(_PluginBase):
    """
    OpenList监测助手插件主类 - 继承自_PluginBase基类
    通过API检测OpenList存储状态，监控存储异常情况
    """
    
    # === 插件元数据 ===
    plugin_name = "OpenList监测助手"
    plugin_desc = "OpenList存储状态监测工具 - 通过API检测存储异常"
    plugin_icon = "https://raw.githubusercontent.com/opentvmedia/OpenTV/main/static/logo.png"
    plugin_version = "1.0.0"
    plugin_author = "gldl137"
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "OpenListMonitorHelper"
    plugin_order = 12
    auth_level = 1

    # === 插件配置参数 ===
    _enabled = False
    _monitor_interval = "0 0 * * *"  # 默认每天凌晨执行
    _monitor_thread = None
    _monitor_running = False
    _enable_notify = False
    _storage_chain = None

    def init_plugin(self, config: dict = None):
        """初始化插件"""
        logger.info("[监测助手] 正在初始化OpenList监测助手插件...")

        if config:
            # 加载新配置
            new_enabled = config.get("enabled", False)
            new_interval = config.get("monitor_interval", "0 0 * * *")
            new_notify = config.get("enable_notify", False)

            # 检查配置是否真正改变
            config_changed = (
                self._enabled != new_enabled or
                self._monitor_interval != new_interval or
                self._enable_notify != new_notify
            )
            
            # 更新配置
            self._enabled = new_enabled
            self._monitor_interval = new_interval
            self._enable_notify = new_notify

            # 处理配置变化：如果插件被启用或配置改变，需要重新启动服务
            if config_changed:
                self.stop_service()
                
                if self._enabled:
                    # 异步初始化，避免阻塞配置保存
                    threading.Thread(target=self._async_init_plugin, daemon=True).start()
                else:
                    logger.info("[监测助手] 插件已禁用")
            else:
                logger.info("[监测助手] 配置未改变，保持当前状态")
        else:
            self.stop_service()
            self._enabled = False
            logger.info("[监测助手] 未提供配置，插件已禁用")
    
    def _async_init_plugin(self):
        """异步初始化插件"""
        try:
            # 初始化StorageChain
            if self._init_storage_chain():
                self._start_monitor_service()
            else:
                logger.error("[监测助手] ❌ StorageChain初始化失败，插件无法启动")
        except Exception as e:
            logger.error(f"[监测助手] ❌ 异步初始化失败: {e}")

    def _start_monitor_service(self):
        """启动存储监测服务"""
        # 测试API连接
        config = self._get_storage_config_from_system()
        if not config:
            logger.error("[监测助手] ❌ 无法获取OpenList配置，监测服务无法启动")
            return
        
        if not self._test_api_connection(config):
            logger.error("[监测助手] ❌ API连接测试失败，监测服务无法启动")
            return
        
        # 启动监测线程
        self._monitor_running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("[监测助手] ✅ 存储监测服务已启动")

    def _init_storage_chain(self) -> bool:
        """初始化存储链"""
        try:
            self._storage_chain = StorageChain()
            
            # 测试存储连接
            test_result = self._test_storage_connection()
            if test_result:
                logger.info("[监测助手] ✅ 存储链初始化成功")
                return True
            else:
                logger.error("[监测助手] ❌ 存储连接测试失败")
                self._storage_chain = None
                return False
                
        except Exception as e:
            logger.error(f"[监测助手] ❌ 存储链初始化失败: {e}")
            self._storage_chain = None
            return False
    
    def _test_storage_connection(self) -> bool:
        """测试存储连接"""
        try:
            # 使用StorageChain从系统获取OpenList配置并测试连接
            if not self._storage_chain:
                logger.error("[监测助手] ❌ 存储链未初始化")
                return False
                
            # 测试OpenList存储连接
            root_item = self._storage_chain.get_file_item(
                storage="openlist",
                path=Path("/")
            )
            
            if root_item:
                logger.info("[监测助手] ✅ 存储连接测试成功 - 系统OpenList配置可用")
                return True
            
            # 如果openlist失败，尝试alist（兼容性）
            root_item = self._storage_chain.get_file_item(
                storage="alist",
                path=Path("/")
            )
            
            if root_item:
                logger.info("[监测助手] ✅ 存储连接测试成功 - 系统AList配置可用")
                return True
                
            logger.warning("[监测助手] ❌ 存储连接测试返回空结果")
            return False
                    
        except Exception as e:
            logger.error(f"[监测助手] ❌ 存储连接测试异常: {e}")
            return False

    def _test_api_connection(self, config: Dict[str, Any]) -> bool:
        """测试API连接"""
        try:
            if not config:
                logger.error("[监测助手] ❌ API配置为空")
                return False
            
            api_url = config.get("api_url")
            token = config.get("token")
            
            if not api_url or not token:
                logger.error("[监测助手] ❌ API地址或Token为空")
                return False
            
            # 使用MoviePilot的RequestUtils进行API测试
            from app.utils.http import RequestUtils
            
            # 正确的Authorization格式（不要加Bearer）
            headers = {
                "Authorization": token,
                "Accept": "application/json"
            }
            
            # 使用MoviePilot封装的请求工具，设置超时避免阻塞
            resp = RequestUtils(headers=headers, timeout=10).get_res(api_url)
            
            if not resp:
                logger.error("[监测助手] ❌ API连接测试失败（无响应）")
                return False
            
            # 允许200（成功）或403（权限不足）说明API地址是正确的
            if resp.status_code in [200, 403]:
                logger.info("[监测助手] ✅ API连接测试成功")
                
                # 如果返回403，可能是账号权限问题
                if resp.status_code == 403:
                    logger.warning("[监测助手] ⚠️ API连接成功但权限不足，请检查是否为管理员账号")
                
                return True
            else:
                logger.error(f"[监测助手] ❌ API连接失败，状态码: {resp.status_code}")
                return False
                    
        except Exception as e:
            logger.error(f"[监测助手] ❌ API连接测试异常: {e}")
            return False

    def _monitor_loop(self):
        """监测循环"""
        # 第一次启动时立即执行一次
        self._perform_monitor_scan()
        
        while self._monitor_running:
            try:
                # 解析cron表达式，计算下次执行时间
                next_run_time = self._calculate_next_run_time()
                
                # 等待到下次执行时间
                wait_time = next_run_time - time.time()
                if wait_time > 0:
                    # 分阶段等待，可以响应停止信号
                    while wait_time > 0 and self._monitor_running:
                        sleep_time = min(wait_time, 60)  # 最多等待60秒
                        time.sleep(sleep_time)
                        wait_time -= sleep_time
                
                if self._monitor_running:
                    self._perform_monitor_scan()
                    
            except Exception as e:
                logger.error(f"[监测助手] ❌ 监测循环异常: {e}")
                # 出错后等待5分钟再重试
                time.sleep(300)

    def _calculate_next_run_time(self) -> float:
        """计算下次执行时间（精确解析完整 cron 表达式）"""
        try:
            from apscheduler.triggers.cron import CronTrigger
            from datetime import datetime
            trigger = CronTrigger.from_crontab(self._monitor_interval)
            next_fire = trigger.get_next_fire_time(None, datetime.now())
            if next_fire:
                return next_fire.timestamp()
        except Exception as e:
            logger.warning(f"[监测助手] cron 解析失败，回退到每天一次: {e}")
        # 解析失败时回退：每天执行一次
        return time.time() + 24 * 60 * 60

    def _perform_monitor_scan(self):
        """执行存储状态监测扫描"""
        try:
            logger.info("[监测助手] 🔍 开始存储状态监测扫描...")
            
            # 检查存储状态
            storage_status = self._check_storage_status()
            
            if storage_status.get("has_issues", False):
                # 发送通知
                if self._enable_notify:
                    self._send_notification("存储状态异常", storage_status.get("message", "检测到存储状态异常"))
                
                # 只显示错误信息，不显示完整的storage_status对象
                error_message = storage_status.get("message", "检测到存储状态异常")
                logger.info(f"[监测助手] ❌ 存储状态异常: {error_message}")
            else:
                logger.info("[监测助手] ✅ 存储状态正常")
                
        except Exception as e:
            logger.error(f"[监测助手] ❌ 存储状态监测扫描异常: {e}")

    def _get_storage_config_from_system(self) -> Dict[str, Any]:
        """从MoviePilot系统配置中获取AList服务器信息（使用MoviePilot封装的方法）"""
        try:
            # 使用MoviePilot封装的AList类来获取基础地址和Token
            from app.modules.filemanager.storages.alist import Alist
            
            alist = Alist()
            
            # 获取基础地址（标准化后的）
            base_url = alist._Alist__get_base_url
            
            # 获取可用的Token（自动缓存 & 刷新）
            token = alist._Alist__get_valuable_toke
            
            if not base_url or not token:
                logger.error("[监测助手] ❌ AList地址或令牌不可用")
                return {}
            
            # 使用MoviePilot风格的方法构建API URL
            api_url = alist._Alist__get_api_url("/api/admin/storage/list")
            
            logger.info(f"[监测助手] ✅ 从MoviePilot系统获取AList配置成功")
            logger.debug(f"[监测助手] 🔧 基础地址: {base_url}")
            
            return {
                "api_url": api_url,
                "token": token,
                "base_url": base_url
            }
            
        except Exception as e:
            logger.error(f"[监测助手] ❌ 从MoviePilot系统获取AList配置失败: {e}")
            return {}
    
    def _check_storage_status(self) -> Dict[str, Any]:
        """检查存储状态（使用MoviePilot封装的AList配置）"""
        try:
            # 从系统获取AList配置
            config = self._get_storage_config_from_system()
            if not config:
                return {"has_issues": True, "status": "配置获取失败", "message": "无法获取AList配置"}
            
            api_url = config.get("api_url")
            token = config.get("token")
            
            if not api_url or not token:
                return {"has_issues": True, "status": "配置不完整", "message": "API地址或Token为空"}
            
            # 使用MoviePilot的RequestUtils进行API调用
            from app.utils.http import RequestUtils
            
            # 正确的Authorization格式（不要加Bearer）
            headers = {
                "Authorization": token,
                "Accept": "application/json"
            }
            
            logger.debug("[监测助手] 🔧 使用MoviePilot封装的AList配置调用API")
            
            # 调用API获取存储状态
            resp = RequestUtils(headers=headers).get_res(api_url)
            
            if not resp:
                logger.error("[监测助手] ❌ API调用失败（无响应）")
                return {
                    "has_issues": True,
                    "status": "API调用失败",
                    "message": "API调用无响应"
                }
            
            if resp.status_code != 200:
                # 处理权限不足的情况
                if resp.status_code == 403:
                    logger.error("[监测助手] ❌ API调用失败，权限不足（403）")
                    try:
                        error_data = resp.json()
                        return {
                            "has_issues": True,
                            "status": "权限不足",
                            "message": f"权限不足: {error_data.get('message', '请检查是否为管理员账号')}",
                            "details": error_data
                        }
                    except:
                        return {
                            "has_issues": True,
                            "status": "权限不足",
                            "message": "API权限不足，请检查是否为管理员账号"
                        }
                
                logger.error(f"[监测助手] ❌ API调用失败，状态码: {resp.status_code}")
                return {
                    "has_issues": True,
                    "status": "API调用失败",
                    "message": f"API调用失败，状态码: {resp.status_code}"
                }
            
            # 解析API响应
            try:
                storage_data = resp.json()
                logger.debug(f"[监测助手] 🔧 API响应数据: {storage_data}")
            except Exception as e:
                logger.error(f"[监测助手] ❌ API响应解析失败: {e}")
                return {
                    "has_issues": True,
                    "status": "响应解析失败",
                    "message": f"API响应解析失败: {e}"
                }
            
            # 检查API响应格式是否正确
            if not isinstance(storage_data, (list, dict)):
                return {
                    "has_issues": True,
                    "status": "API响应格式错误",
                    "message": f"API返回数据格式不正确: {storage_data}",
                    "details": storage_data
                }
            
            # 如果返回的是字典，检查是否有data字段
            if isinstance(storage_data, dict):
                if "data" in storage_data:
                    storage_data = storage_data["data"]
                elif "code" in storage_data and storage_data.get("code") != 200:
                    return {
                        "has_issues": True,
                        "status": "API调用失败",
                        "message": f"API返回错误: {storage_data.get('message', '未知错误')}",
                        "details": storage_data
                    }
            
            # 检查存储状态
            issues = self._analyze_storage_status(storage_data)
            
            if issues:
                return {
                    "has_issues": True,
                    "status": "异常",
                    "message": f"检测到存储异常: {issues}",
                    "details": storage_data
                }
            
            # 存储状态正常
            return {
                "has_issues": False,
                "status": "正常",
                "message": "所有存储状态正常",
                "details": storage_data
            }
                
        except Exception as e:
            logger.error(f"[监测助手] ❌ 检查存储状态异常: {e}")
            return {"has_issues": True, "status": "检查异常", "message": f"存储状态检查异常: {e}"}
    
    def _analyze_storage_status(self, storage_data) -> str:
        """分析存储状态数据 - 只检查status字段是否为"work"""
        issues = []
        
        # 检查数据类型
        if not storage_data:
            return "无存储数据"
        
        # 根据API返回的数据结构提取存储列表
        storages = []
        
        if isinstance(storage_data, dict):
            # 如果返回的是字典，检查是否有content字段
            if "content" in storage_data:
                storages = storage_data["content"]
            elif "storages" in storage_data:
                storages = storage_data["storages"]
            elif "list" in storage_data:
                storages = storage_data["list"]
            else:
                # 如果是单个存储信息，包装成列表
                storages = [storage_data]
        elif isinstance(storage_data, list):
            storages = storage_data
        else:
            return f"不支持的数据格式: {type(storage_data)}"
        
        # 如果转换后仍然为空
        if not storages:
            return "无存储数据"
        
        for storage in storages:
            # 检查存储状态字段
            status = storage.get("status", "unknown")
            mount_path = storage.get("mount_path", "未知存储")
            disabled = storage.get("disabled", False)
            
            # 忽略已禁用的存储
            if disabled:
                continue
                
            # 简化逻辑：只检查status是否为"work"，不是就表示错误
            if status != "work":
                # 获取错误消息（如果有）
                error_msg = storage.get("msg", "")
                # 按照参考格式：储存：存储名称，原因：状态值, 错误消息
                issues.append(f"储存：{mount_path}，原因：{status}" + (f", {error_msg}" if error_msg else ""))
        
        return "; ".join(issues) if issues else ""

    def _send_notification(self, title: str, message: str):
        """发送通知"""
        try:
            self.post_message(
                mtype=NotificationType.Plugin,
                title=f"OpenList监测助手 - {title}",
                text=message
            )
        except Exception as e:
            logger.warning(f"[监测助手] 发送通知失败: {e}")

    def get_state(self) -> bool:
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """获取插件配置表单"""
        return [
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
                                    "label": "启动插件",
                                    "hint": "启用后插件开始工作并自动启动监测"
                                },
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
                                    "hint": "检测到存储异常时发送系统通知"
                                },
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
                                    "model": "monitor_interval",
                                    "label": "监测间隔",
                                    "hint": "设置监测执行的时间间隔，如：0 0 * * * 表示每天凌晨执行",
                                    "placeholder": "0 0 * * *",
                                    "dense": True
                                }
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": self._enabled,
            "enable_notify": self._enable_notify,
            "monitor_interval": self._monitor_interval
        }

    def get_page(self) -> List[dict]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self) -> List[Dict[str, Any]]:
        return []

    def stop_service(self):
        """停止插件服务"""
        # 使用线程安全的方式停止监测
        if self._monitor_running:
            self._monitor_running = False
            
            # 等待线程安全退出
            if self._monitor_thread and self._monitor_thread.is_alive():
                # 设置超时，避免无限等待
                self._monitor_thread.join(timeout=3)
                
                if self._monitor_thread.is_alive():
                    logger.warning("[监测助手] 监测线程未在超时时间内退出，强制停止")
                
                self._monitor_thread = None
            
            logger.info("[监测助手] 网盘监测服务已停止")
        else:
            logger.debug("[监测助手] 监测服务未运行，无需停止")

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册公共定时服务。

        注意：本插件采用「插件内线程轮询」方式定时执行（见 _monitor_loop），
        因此这里不向宿主调度器注册 cron 服务。原因：宿主 register_plugin_service
        内部以 add_job(trigger="cron", cron="表达式") 形式注册，而当前环境
        APScheduler 版本的 Crontrigger.__init__ 不接受 cron= 关键字参数，
        会导致 "CronTrigger.__init__() got an unexpected keyword argument 'cron'" 报错。
        改用线程轮询（配合 _calculate_next_run_time 精确解析 cron）可彻底绕开该兼容问题。
        """
        return []

    def _perform_monitor_service(self):
        """
        定时服务执行方法
        
        注意：此方法由APScheduler定时调用，无需手动启动监测线程
        """
        try:
            logger.info("[监测助手] ⏰ 定时服务开始执行存储状态监测...")
            
            # 检查存储链是否已初始化
            if not self._storage_chain:
                logger.warning("[监测助手] ⚠️ 存储链未初始化，尝试重新初始化")
                if not self._init_storage_chain():
                    logger.error("[监测助手] ❌ 存储链初始化失败，定时服务无法执行")
                    return
            
            # 执行存储状态监测
            self._perform_monitor_scan()
            
            logger.info("[监测助手] ✅ 定时服务执行完成")
            
        except Exception as e:
            logger.error(f"[监测助手] ❌ 定时服务执行异常: {e}")
            
            # 发送错误通知
            if self._enable_notify:
                self._send_notification("定时服务异常", f"定时服务执行失败: {e}")