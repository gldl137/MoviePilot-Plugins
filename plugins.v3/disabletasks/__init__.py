# ===== 必须导入的Python标准库 =====
import traceback
from typing import Any, Dict, List, Tuple

# ===== 必须导入的MoviePilot框架组件 =====
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.helper.message import MessageHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class DisableTasks(_PluginBase):
    """
    定时任务禁用插件
    用于禁用特定的系统定时任务
    """

    # 插件名称
    plugin_name = "定时任务禁用"
    # 插件描述
    plugin_desc = "禁用特定的系统定时任务"
    # 插件图标
    plugin_icon = "task.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "DisableTasks_"
    # 加载顺序
    plugin_order = 10
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _config = {}

    # 默认要禁用的任务列表
    _default_tasks_to_disable = ["new_subscribe_search"]
    
    # 任务描述映射
    _task_descriptions = {
        "subscribe_calendar_cache": "订阅日历缓存",
        "new_subscribe_search": "新增订阅搜索",
        "user_auth": "用户认证检查",
        "subscribe_follow": "关注的订阅分享",
        "subscribe_tmdb": "订阅元数据更新",
        "subscribe_refresh": "订阅刷新",
        "transfer": "下载文件整理"
    }

    def init_plugin(self, config: dict = None):
        """
        初始化插件
        """
        try:
            # 初始化配置
            self._config = {
                "enabled": True,
                "tasks_to_disable": self._default_tasks_to_disable.copy()
            }
            
            if config:
                self._config.update(config)
            
            self._enabled = self._config.get("enabled", True)

            # 保存配置时自动禁用选中的任务（仅在插件启用时）
            if self._enabled:
                tasks_to_disable = self._config.get("tasks_to_disable", [])
                if tasks_to_disable:
                    disabled_count = self._disable_tasks(tasks_to_disable)
                    if disabled_count > 0:
                        logger.info(f"插件初始化时已禁用 {disabled_count} 个任务")

            logger.info(f"插件 {self.plugin_name} 初始化完成")

        except Exception as e:
            logger.error(f"插件 {self.plugin_name} 初始化失败: {str(e)}")
            logger.error(traceback.format_exc())

    def get_state(self) -> bool:
        """
        获取插件启用状态
        """
        return self._enabled

    def get_command(self) -> List[Dict]:
        """
        获取插件命令配置
        """
        return [
            {
                "cmd": "/disable_tasks",
                "event": EventType.PluginAction,
                "desc": "禁用定时任务",
                "data": {
                    "action": "disable_tasks"
                }
            },
            {
                "cmd": "/task_status",
                "event": EventType.PluginAction,
                "desc": "查看任务状态",
                "data": {
                    "action": "task_status"
                }
            }
        ]

    def get_api(self) -> List[Dict]:
        """
        获取插件API配置
        """
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        获取插件配置表单
        """
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件"
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "tasks_to_disable",
                                            "label": "选择要禁用的任务",
                                            "multiple": True,
                                            "chips": True,
                                            "disabled": not self._config.get("enabled", True),
                                            "items": [
                                                {"title": "订阅日历缓存", "value": "subscribe_calendar_cache"},
                                                {"title": "新增订阅搜索", "value": "new_subscribe_search"},
                                                {"title": "用户认证检查", "value": "user_auth"},
                                                {"title": "关注的订阅分享", "value": "subscribe_follow"},
                                                {"title": "订阅元数据更新", "value": "subscribe_tmdb"},
                                                {"title": "订阅刷新", "value": "subscribe_refresh"},
                                                {"title": "下载文件整理", "value": "transfer"}
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "⚠️ 重要说明：还原定时任务,取消选中后需要重启MoviePilot才能生效。"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": self._config.get("enabled", True),
            "tasks_to_disable": self._config.get("tasks_to_disable", self._default_tasks_to_disable)
        }

    def get_page(self) -> List[dict]:
        """
        获取插件页面配置
        """
        pass

    def stop_service(self):
        """
        停止插件服务
        """
        try:
            logger.info(f"插件 {self.plugin_name} 服务已停止")
        except Exception as e:
            logger.error(f"停止插件 {self.plugin_name} 服务失败: {str(e)}")
            logger.error(traceback.format_exc())

    @eventmanager.register(EventType.PluginAction)
    def command_action(self, event: Event):
        """
        处理插件命令事件
        """
        try:
            if not self._enabled:
                return

            event_data = event.event_data
            if not event_data:
                return

            action = event_data.get("action")
            if not action:
                return

            # 创建消息助手
            message_helper = MessageHelper()

            if action == "disable_tasks":
                self._handle_disable_command(message_helper)
            elif action == "task_status":
                self._handle_status_command(message_helper)

        except Exception as e:
            logger.error(f"处理插件命令失败: {str(e)}")
            logger.error(traceback.format_exc())

    def update_config(self, config: dict, plugin_id: str = None):
        """
        更新配置时动态管理任务状态
        """
        try:
            # 保存旧配置用于比较
            old_enabled = self._config.get("enabled", True)
            old_tasks = set(self._config.get("tasks_to_disable", []))

            # 更新配置
            self._config.update(config)
            self._enabled = self._config.get("enabled", True)

            # 获取新选中的任务
            new_tasks = set(self._config.get("tasks_to_disable", []))

            disabled_count = 0
            restored_count = 0

            # 处理插件启用状态变化
            if not old_enabled and self._enabled:
                # 插件从禁用变为启用，禁用所有选中任务
                if new_tasks:
                    disabled_count = self._disable_tasks(list(new_tasks))
                    if disabled_count > 0:
                        logger.info(f"插件启用时已禁用 {disabled_count} 个任务")

            elif old_enabled and not self._enabled:
                # 插件从启用变为禁用，还原所有任务
                if old_tasks:
                    restored_count = self._restore_tasks(list(old_tasks))
                    if restored_count > 0:
                        logger.info(f"插件禁用时已还原 {restored_count} 个任务")

            elif self._enabled:
                # 插件保持启用状态，处理任务选择变化
                tasks_to_disable = list(new_tasks - old_tasks)
                tasks_to_restore = list(old_tasks - new_tasks)

                # 禁用新选中的任务
                if tasks_to_disable:
                    disabled_count = self._disable_tasks(tasks_to_disable)
                    if disabled_count > 0:
                        logger.info(f"已禁用 {disabled_count} 个新选中任务")

                # 还原取消选中的任务
                if tasks_to_restore:
                    restored_count = self._restore_tasks(tasks_to_restore)
                    if restored_count > 0:
                        logger.info(f"已还原 {restored_count} 个取消选中任务")

            # 发送通知
            message_parts = []
            if not old_enabled and self._enabled:
                message_parts.append("插件已启用")
            elif old_enabled and not self._enabled:
                message_parts.append("插件已禁用")

            if disabled_count > 0:
                message_parts.append(f"禁用 {disabled_count} 个任务")
            if restored_count > 0:
                message_parts.append(f"还原 {restored_count} 个任务")

            if message_parts:
                MessageHelper().put(
                    title="定时任务状态更新",
                    message="，".join(message_parts),
                    role="system"
                )

            return True

        except Exception as e:
            logger.error(f"更新配置时管理任务失败: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _disable_tasks(self, task_ids: List[str]) -> int:
        """
        禁用指定的任务

        Args:
            task_ids: 要禁用的任务ID列表

        Returns:
            int: 成功禁用的任务数量
        """
        try:
            from app.scheduler import Scheduler

            scheduler = Scheduler()
            disabled_count = 0

            for job_id in task_ids:
                try:
                    # 直接通过调度器移除系统任务
                    for job in list(scheduler._scheduler.get_jobs()):
                        if job.id == job_id or job.id.startswith(job_id + "|"):
                            scheduler._scheduler.remove_job(job.id)
                            disabled_count += 1
                            logger.info(f"已禁用任务: {job_id}")
                            break

                except Exception as e:
                    logger.error(f"禁用任务 {job_id} 失败: {str(e)}")

            return disabled_count

        except Exception as e:
            logger.error(f"禁用任务操作失败: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    def _restore_tasks(self, task_ids: List[str]) -> int:
        """
        还原指定的任务（重新启用）
        通过调用 on_config_changed 重新初始化系统定时任务

        Args:
            task_ids: 要还原的任务ID列表

        Returns:
            int: 成功还原的任务数量
        """
        try:
            from app.scheduler import Scheduler

            scheduler = Scheduler()

            # 调用配置变更方法，重新初始化所有系统定时任务
            scheduler.on_config_changed()

            logger.info(f"已重新初始化系统定时任务，共还原 {len(task_ids)} 个任务")
            return len(task_ids)

        except Exception as e:
            logger.error(f"还原任务操作失败: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    def _handle_disable_command(self, message_helper):
        """
        处理禁用任务命令
        """
        try:
            tasks_to_disable = self._config.get("tasks_to_disable", [])
            if not tasks_to_disable:
                message_helper.put(
                    title="定时任务禁用",
                    message="没有配置要禁用的任务",
                    role="system"
                )
                return

            # 禁用任务
            disabled_count = self._disable_tasks(tasks_to_disable)

            # 发送结果消息
            if disabled_count > 0:
                message = f"成功禁用 {disabled_count} 个任务:\n"
                for task_id in tasks_to_disable:
                    description = self._task_descriptions.get(task_id, task_id)
                    message += f"- {description}\n"

                message_helper.put(
                    title="定时任务禁用",
                    message=message,
                    role="system"
                )
            else:
                message_helper.put(
                    title="定时任务禁用",
                    message="没有成功禁用任何任务",
                    role="system"
                )

        except Exception as e:
            logger.error(f"处理禁用命令失败: {str(e)}")
            message_helper.put(
                title="定时任务禁用失败",
                message=str(e),
                role="system"
            )

    def _handle_status_command(self, message_helper):
        """
        处理状态查询命令
        """
        try:
            tasks_to_disable = self._config.get("tasks_to_disable", [])

            message = f"定时任务禁用状态:\n"
            message += f"配置禁用的任务数: {len(tasks_to_disable)}\n\n"

            if tasks_to_disable:
                message += "配置禁用的任务:\n"
                for task_id in tasks_to_disable:
                    description = self._task_descriptions.get(task_id, task_id)
                    message += f"- {description}\n"

            message_helper.put(
                title="定时任务状态",
                message=message,
                role="system"
            )

        except Exception as e:
            logger.error(f"处理状态命令失败: {str(e)}")
            message_helper.put(
                title="获取任务状态失败",
                message=str(e),
                role="system"
            )
