import json  # 【必须】JSON数据编码和解码：处理配置文件和数据交换
import os  # 【必须】操作系统接口：文件路径操作、环境变量等
import time  # 【必须】时间相关功能：延时执行、时间戳处理等
from typing import Any  # 【必须】类型注解支持：List（列表）、Tuple（元组）、Dict（字典）、Any（任意类型）
from typing import Dict, List, Tuple

# ===== 必须导入的MoviePilot框架组件 =====
from app import schemas  # 【必须】数据模式定义：包含通知、媒体信息、事件等标准化数据结构
from app.core.event import Event, eventmanager  # 事件管理
from app.log import logger  # 【必须】系统日志记录器：用于记录插件运行日志，支持不同级别的日志输出
from app.plugins import _PluginBase
from app.schemas.types import EventType  # 事件类型


class CommandHelper(_PluginBase):  # 修正：继承自 _PluginBase
    # 插件名称
    plugin_name = "命令帮助"
    # 插件描述
    plugin_desc = "显示所有插件的交互命令，帮助用户快速了解可用功能"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/opentvmedia/OpenTV/main/static/logo.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "CommandHelper"
    # 加载顺序
    plugin_order = 0
    # 可使用的用户级别
    auth_level = 1

    def __init__(self):
        """初始化插件实例"""
        super().__init__()
        self._enabled = False
        self._trigger_command = "/ml"
        self._command_mappings = {}  # 动态命令映射表
        self._command_history = []  # 命令历史记录

    def init_plugin(self, config: dict = None):
        """
        初始化插件
        """
        if config:
            self._enabled = config.get("enabled", False)
            self._trigger_command = config.get("trigger_command", "/ml")

            # 加载命令映射配置
            command_mappings = config.get("command_mappings", "")
            self._command_mappings = self._parse_command_mappings(
                command_mappings)

            # 加载命令历史
            self._command_history = self.get_data("command_history") or []

            # 注册事件监听器
            if self._enabled:
                self._register_event_listeners()
                logger.info("命令帮助插件已启用")
            else:
                logger.info("命令帮助插件已禁用")

    def _parse_command_mappings(self, command_mappings: str) -> Dict:
        """解析命令映射配置"""
        mappings = {}
        if not command_mappings:
            return mappings

        for line in command_mappings.splitlines():
            line = line.strip()
            if not line:
                continue

            # 格式：序号#执行命令#描述
            parts = line.split("#", 2)
            if len(parts) >= 2:
                key = parts[0].strip()
                command = parts[1].strip()
                desc = parts[2].strip() if len(parts) > 2 else ""

                mappings[key] = {
                    "command": command,
                    "desc": desc
                }

        return mappings

    def _format_command_mappings(self) -> str:
        """格式化命令映射为文本"""
        lines = []
        for key, mapping in self._command_mappings.items():
            lines.append(f"{key}#{mapping['command']}#{mapping['desc']}")
        return "\n".join(lines)

    def get_state(self) -> bool:
        """
        获取插件启用状态,必须
        """
        return self._enabled

    def get_command(self) -> List[Dict]:
        """
        获取插件命令配置,必须
        """
        commands = [
            {
                "cmd": self._trigger_command,
                "event": EventType.PluginAction,
                "desc": "显示命令帮助",
                "category": "管理",
                "data": {"action": "help"}
            }
        ]

        # 动态添加配置的命令映射
        for key, mapping in self._command_mappings.items():
            commands.append({
                "cmd": f"/{key}",
                "event": EventType.PluginAction,
                "desc": f"执行：{mapping['command']}",
                "category": "插件",
                "data": {"action": "forward", "key": key}
            })

        return commands

    def get_api(self) -> List[Dict]:
        """
        获取插件API配置,必须
        """
        return []

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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "trigger_command",
                                            "label": "触发命令",
                                            "placeholder": "/ml"
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
                                            "model": "command_mappings",
                                            "label": "命令映射配置",
                                            "rows": 10,
                                            "placeholder": "每行一个\n1#/opt转移#全量同步到OpenList\n4#/opt清理#清理上传记录"
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
                                            "text": "📝 使用说明：格式：序号#执行命令#描述，通过 /1 到 /9 快速执行常用操作\n示例：1#/opt转移#全量同步到OpenList"
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
            "trigger_command": self._trigger_command,
            "command_mappings": self._format_command_mappings()
        }

    def get_page(self) -> List[dict]:
        """
        获取插件页面配置,必须
        """
        pass

    def _register_event_listeners(self):
        """注册事件监听器"""
        # 注意：plugin_action方法已经使用@eventmanager.register装饰器注册
        # 这里不需要重复注册
        pass

    @eventmanager.register(EventType.PluginAction)
    def plugin_action(self, event: Event):
        """
        处理插件动作事件
        """
        data = event.event_data or {}
        action = data.get("action")

        channel = data.get("channel")
        userid = data.get("userid")

        if action == "help":
            logger.info("接收到命令帮助请求")

            self.post_message(
                channel=channel,
                userid=userid,
                title="命令帮助",
                text=self._generate_help_message()
            )

        elif action == "forward":
            key = data.get("key")
            cmd_info = self._command_mappings.get(key)

            if not cmd_info:
                self.post_message(
                    channel=channel,
                    userid=userid,
                    title="执行失败",
                    text="无效的快捷命令"
                )
                return

            self._forward_command(
                channel,
                userid,
                cmd_info["command"],
                cmd_info["desc"]
            )

    def _record_command_history(self, command, userid):
        """记录命令历史"""
        try:

            # 确保历史记录是列表
            if not isinstance(self._command_history, list):
                self._command_history = []

            # 添加到历史记录
            self._command_history.append({
                "command": command,
                "userid": userid,
                "timestamp": time.time()
            })

            # 保持历史记录不超过20条
            if len(self._command_history) > 20:
                self._command_history = self._command_history[-20:]

            # 保存到插件数据
            self.save_data("command_history", self._command_history)

        except Exception as e:
            logger.error(f"记录命令历史失败: {e}")

    def _generate_help_message(self) -> str:
        """生成帮助信息"""
        try:
            help_text = "🔸 数字快捷命令：\n\n"

            # 显示命令映射表中的所有命令
            for key, cmd_info in self._command_mappings.items():
                command = cmd_info["command"]
                desc = cmd_info["desc"]
                help_text += f"   /{key} - {command} - {desc}\n"

            help_text += "• 在聊天窗口中直接输入序号加上/"

            return help_text

        except Exception as e:
            logger.error(f"生成帮助信息失败: {e}")
            return "生成帮助信息失败，请检查日志。"

    def _forward_command(self, channel, userid, command, desc):
        """命令转发器 - 模拟用户点击菜单命令"""
        try:
            # 1. 告知用户 - 只显示命令，不显示描述
            self.post_message(
                channel=channel,
                userid=userid,
                title="命令执行",
                text=f"{command}"
            )

            # 2. 直接调用Command实例执行命令
            # 避免事件系统可能的限制，直接调用命令执行器
            from app.command import Command

            # 获取命令实例
            cmd_manager = Command()

            # 提取命令和参数
            cmd_parts = command.split()
            cmd = cmd_parts[0] if cmd_parts else ""
            args = " ".join(cmd_parts[1:]) if len(cmd_parts) > 1 else ""

            # 检查命令是否已注册
            if cmd_manager.get(cmd):
                # 执行命令
                cmd_manager.execute(
                    cmd=cmd,
                    data_str=args,
                    channel=channel,
                    source="user",
                    userid=userid
                )
                logger.info(f"已转发命令: {command}")
            else:
                # 命令未注册，尝试作为消息发送
                self.post_message(
                    channel=channel,
                    userid=userid,
                    text=f"未找到命令 {command}，可能尚未注册或插件未启用"
                )
                logger.warning(f"未找到命令: {command}")

            # 记录命令历史
            self._record_command_history(command, userid)

        except Exception as e:
            logger.error(f"转发命令失败: {e}")
            self.post_message(
                channel=channel,
                userid=userid,
                title="命令转发失败",
                text=f"转发命令时发生错误: {str(e)}"
            )

    def stop_service(self):
        """
        停止插件服务,必须
        """
        pass
