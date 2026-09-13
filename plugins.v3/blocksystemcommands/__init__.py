# ===== 必须导入的Python标准库 =====
import json  # 【必须】JSON数据编码和解码：处理配置文件和数据交换
import os  # 【必须】操作系统接口：文件路径操作、环境变量等
import time  # 【必须】时间相关功能：延时执行、时间戳处理等
from typing import (Any,  # 【必须】类型注解支持：List（列表）、Tuple（元组）、Dict（字典）、Any（任意类型）
                    Dict, List, Tuple)

# ===== 必须导入的MoviePilot框架组件 =====
from app import schemas  # 【必须】数据模式定义：包含通知、媒体信息、事件等标准化数据结构
from app.core.event import Event, eventmanager  # 事件管理
from app.log import logger  # 【必须】系统日志记录器：用于记录插件运行日志，支持不同级别的日志输出
from app.plugins import \
    _PluginBase  # 【必须】插件基类：所有MoviePilot插件都必须继承的基类，定义插件的基本接口和生命周期
from app.schemas import CommandRegisterEventData
from app.schemas.types import ChainEventType, EventType  # 事件类型


class BlockSystemCommands(_PluginBase):
    # 插件名称
    plugin_name = "系统命令屏蔽"
    # 插件描述
    plugin_desc = "屏蔽指定的系统命令，防止误操作"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/opentvmedia/OpenTV/main/static/logo.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "BlockSystemCommands_"
    # 加载顺序
    plugin_order = 10
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _config = {}
    _blocked_commands = []
    _block_record = []

    # 默认需要屏蔽的命令列表
    _default_blocked_commands = []

    def init_plugin(self, config: dict = None):
        """
        初始化插件
        """
        try:
            # 初始化配置
            self._config = {
                "enabled": False,
                "blocked_commands": self._default_blocked_commands.copy()
            }

            if config:
                self._config.update(config)

            self._enabled = self._config.get("enabled", False)
            blocked_commands_str = self._config.get("blocked_commands", "")
            self._blocked_commands = self._parse_blocked_commands(
                blocked_commands_str)
            # 加载屏蔽历史记录
            self._block_record = self.get_data("block_record") or []

            # 注册事件监听器
            if self._enabled:
                self._register_event_listeners()
                logger.info(
                    f"系统命令屏蔽插件已启用，共屏蔽 {len(self._blocked_commands)} 个命令")
                logger.info(f"屏蔽命令列表: {self._blocked_commands}")
            else:
                logger.info("系统命令屏蔽插件已禁用")

        except Exception as e:
            logger.error(f"插件 {self.plugin_name} 初始化失败: {str(e)}")

    def _parse_blocked_commands(self, commands_str: str) -> List[str]:
        """
        解析命令配置字符串为列表
        :param commands_str: 命令配置字符串，每行一个命令
        :return: 命令列表
        """
        commands = []
        if not commands_str:
            return commands

        for line in commands_str.splitlines():
            line = line.strip()
            # 跳过空行和注释行
            if not line or line.startswith("#"):
                continue
            # 确保命令以 / 开头
            if not line.startswith("/"):
                line = "/" + line
            commands.append(line)

        return commands

    def _format_blocked_commands(self) -> str:
        """
        格式化命令列表为字符串
        :return: 命令配置字符串
        """
        return "\n".join(self._blocked_commands)

    def get_state(self) -> bool:
        """
        获取插件启用状态,必须
        """
        return self._enabled

    def get_command() -> List[Dict]:
        """
        获取插件命令配置,必须
        """
        pass

    def get_api(self) -> List[Dict]:
        """
        获取插件API配置,必须
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
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "blocked_commands",
                                            "label": "需要屏蔽的命令列表",
                                            "rows": 10,
                                            "placeholder": "/cookiecloud",
                                            "disabled": not self._config.get("enabled", False)
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
                                            "text": "警告：屏蔽系统命令后，这些命令将无法在微信聊天界面中执行。请确保不会影响正常使用。"
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
                                            "text": "使用说明：每行填写一个需要屏蔽的命令（如 /cookiecloud），命令必须以 / 开头，可以添加注释行（以 # 开头），使用 /blocklist 命令查看当前屏蔽列表"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": self._config.get("enabled", False),
            "blocked_commands": self._config.get("blocked_commands", "")
        }

    def get_page(self) -> List[dict]:
        """
        获取插件页面配置,必须
        """
        pass

    def _register_event_listeners(self):
        """
        注册事件监听器
        """
        # 注册命令注册事件监听器
        self.eventmanager.add_event_listener(
            ChainEventType.CommandRegister, self._handle_command_register)

    @eventmanager.register(EventType.PluginAction)
    def plugin_action(self, event: Event):
        """
        处理插件动作事件
        :param event: 事件对象
        """
        data = event.event_data or {}
        action = data.get("action")

        channel = data.get("channel")
        userid = data.get("userid")

        if action == "show_block_list":
            self._show_block_list(channel, userid)

    def _show_block_list(self, channel: str, userid: str):
        """
        显示已屏蔽的命令列表
        :param channel: 消息通道
        :param userid: 用户ID
        """
        try:
            if not self._blocked_commands:
                message = "当前没有屏蔽任何系统命令"
            else:
                message = "已屏蔽的系统命令列表：\n\n"
                for i, cmd in enumerate(self._blocked_commands, 1):
                    message += f"{i}. {cmd}\n"

                if self._block_record:
                    message += f"\n总共已屏蔽 {len(self._block_record)} 次"

            self.post_message(
                channel=channel,
                userid=userid,
                title="系统命令屏蔽列表",
                text=message
            )

        except Exception as e:
            logger.error(f"显示屏蔽列表失败: {e}")
            self.post_message(
                channel=channel,
                userid=userid,
                title="查询失败",
                text=f"查询屏蔽列表时发生错误: {str(e)}"
            )

    def _handle_command_register(self, event: Event):
        """
        拦截命令注册事件，屏蔽指定的系统命令
        :param event: 命令注册事件对象
        """
        try:
            if not event.event_data:
                return

            event_data: CommandRegisterEventData = event.event_data
            commands = event_data.commands

            if not commands:
                return

            # 记录本次屏蔽的命令
            blocked_in_this_round = []

            # 遍历需要屏蔽的命令列表
            for blocked_cmd in self._blocked_commands:
                if blocked_cmd in commands:
                    # 从命令字典中删除该命令
                    commands.pop(blocked_cmd)
                    blocked_in_this_round.append(blocked_cmd)
                    logger.info(f"成功屏蔽系统命令: {blocked_cmd}")

            # 更新事件数据
            if blocked_in_this_round:
                event_data.commands = commands

                # 记录屏蔽历史
                for cmd in blocked_in_this_round:
                    self._block_record.append({
                        "command": cmd,
                        "timestamp": time.time(),
                        "blocked_at": time.strftime("%Y-%m-%d %H:%M:%S")
                    })

                # 保存到插件数据
                self.save_data("block_record", self._block_record)

                # 保持记录不超过100条
                if len(self._block_record) > 100:
                    self._block_record = self._block_record[-100:]
                    self.save_data("block_record", self._block_record)

                logger.info(
                    f"本次共屏蔽 {len(blocked_in_this_round)} 个命令: {blocked_in_this_round}")

        except Exception as e:
            logger.error(f"处理命令注册事件时发生错误: {e}")

    def stop_service(self):
        """
        停止插件服务,必须
        """
        # 取消事件监听器
        try:
            self.eventmanager.remove_event_listener(
                ChainEventType.CommandRegister, self._handle_command_register)
            logger.info(f"插件 {self.plugin_name} 服务已停止")
        except Exception as e:
            logger.error(f"停止插件 {self.plugin_name} 服务失败: {str(e)}")

    def update_config(self, config: dict, plugin_id: str = None):
        """
        更新配置时动态管理事件监听器
        """
        try:
            # 保存旧配置用于比较
            old_enabled = self._config.get("enabled", False)
            old_commands_str = self._config.get("blocked_commands", "")

            # 更新配置
            self._config.update(config)
            self._enabled = self._config.get("enabled", False)
            new_commands_str = self._config.get("blocked_commands", "")

            # 处理插件启用状态变化
            if not old_enabled and self._enabled:
                # 插件从禁用变为启用，注册事件监听器
                self._blocked_commands = self._parse_blocked_commands(
                    new_commands_str)
                self._register_event_listeners()
                logger.info(
                    f"插件 {self.plugin_name} 已启用，共屏蔽 {len(self._blocked_commands)} 个命令")

            elif old_enabled and not self._enabled:
                # 插件从启用变为禁用，取消事件监听器
                self.eventmanager.remove_event_listener(
                    ChainEventType.CommandRegister, self._handle_command_register)
                logger.info(f"插件 {self.plugin_name} 已禁用")

            elif self._enabled and old_commands_str != new_commands_str:
                # 插件保持启用，命令列表发生变化
                self._blocked_commands = self._parse_blocked_commands(
                    new_commands_str)
                logger.info(
                    f"插件 {self.plugin_name} 屏蔽命令列表已更新，共屏蔽 {len(self._blocked_commands)} 个命令")

            return True

        except Exception as e:
            logger.error(f"更新配置时失败: {str(e)}")
            return False
