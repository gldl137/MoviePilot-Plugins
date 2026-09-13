# ===== 必须导入的Python标准库 =====
import json  # 【必须】JSON数据编码和解码：处理配置文件和数据交换
import os  # 【必须】操作系统接口：文件路径操作、环境变量等
import sys  # 【必须】系统相关功能：系统退出等
import time  # 【必须】时间相关功能：延时执行、时间戳处理等
from typing import (Any,  # 【必须】类型注解支持：List（列表）、Tuple（元组）、Dict（字典）、Any（任意类型）
                    Dict, List, Tuple)

# ===== 必须导入的MoviePilot框架组件 =====
from app import schemas  # 【必须】数据模式定义：包含通知、媒体信息、事件等标准化数据结构
from app.core.config import settings  # 【可选】系统配置：获取API Token等系统配置信息
from app.core.event import Event, eventmanager  # 事件管理
from app.log import logger  # 【必须】系统日志记录器：用于记录插件运行日志，支持不同级别的日志输出
from app.plugins import \
    _PluginBase  # 【必须】插件基类：所有MoviePilot插件都必须继承的基类，定义插件的基本接口和生命周期
from app.schemas.types import EventType  # 事件类型
from app.schemas.types import NotificationType  # 通知类型
from app.utils.http import RequestUtils  # 【必须】HTTP请求工具：用于与音乐刮削服务进行通信
from pydantic import BaseModel  # 【可选】基础模型：API请求响应数据模型


logger.info("音乐刮削助手插件加载完成")


class BaseApiReq(BaseModel):
    """基础API请求模型"""
    apikey: str = settings.API_TOKEN


class MusicScraperHelp(_PluginBase):
    # 插件名称
    plugin_name = "音乐刮削助手"
    # 插件描述
    plugin_desc = "音乐刮削助手，支持自动刮削音乐元数据"
    # 插件图标
    plugin_icon = "music.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "gldl137"
    # 作者主页
    author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "musicscraperhelp_"
    # 加载顺序
    plugin_order = 10
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _base_url = ""
    _password = ""
    _scrape_path = ""  # 刮削路径
    _session_cookie = None
    _send_notify = True  # 是否发送通知
    _is_scraping = False  # 是否正在刮削
    _scrape_once = False  # 是否立即运行一次

    def init_plugin(self, config: dict = None):
        """
        初始化插件
        """
        # 设置默认值，确保即使config为None也能正常工作
        self._enabled = False
        self._send_notify = True
        # 服务地址与登录密码一律从插件配置页读取，源码中不内置任何凭据
        self._base_url = ""
        self._password = ""
        self._scrape_path = ""  # 默认空路径
        self._scrape_once = False
        self._is_scraping = False  # 初始化刮削状态为未运行

        # 如果提供了配置，则覆盖默认值
        if config:
            self._enabled = config.get("enabled", False)
            self._send_notify = config.get("send_notify", True)
            self._base_url = config.get("base_url", "")
            self._password = config.get("password", "")
            self._scrape_path = config.get("scrape_path", "")  # 刮削路径
            self._scrape_once = config.get("scrape_once", False)



            # 插件启用时自动登录获取session
            if self._enabled:
                self._session_cookie = self._login()
                if self._session_cookie:
                    logger.info("MusicScraperHelp插件初始化完成，session已获取")

                    # 检查是否需要立即运行一次（异步执行，避免阻塞配置保存）
                    if self._scrape_once:
                        # 异步执行，避免阻塞配置保存
                        import threading
                        thread = threading.Thread(
                            target=self._run_once_immediately)
                        thread.daemon = True
                        thread.start()
                else:
                    logger.error("MusicScraperHelp插件初始化失败，无法获取session")

    def get_command(self) -> List[Dict]:
        """
        获取插件命令配置,必须
        """
        return [
            {
                "cmd": "/start_musicscrape",
                "event": EventType.PluginAction,
                "desc": "开始音乐刮削",
                "category": "音乐",
                "data": {"action": "start_scrape"}
            }
        ]

    def handle_command(self, command: str, data: Dict = None) -> Any:
        """
        处理插件命令
        """
        if command == "/start_musicscrape":
            # 先检查是否已有刮削任务在运行
            if self._is_scraping:
                return "刮削任务已在运行中，请等待当前任务完成"
            
            # 调用启动刮削的API方法（与页面"启动刮削"按钮功能相同）
            from app.core.config import settings
            from app.api.schemas import BaseApiReq
            
            # 创建API请求对象
            request = BaseApiReq()
            request.apikey = settings.API_TOKEN
            
            # 执行刮削任务
            result = self.start_scrape_api(request)
            
            if result.get("success"):
                return "刮削任务已启动"
            else:
                return f"刮削启动失败: {result.get('message', '未知错误')}"
        
        return None

    def get_api(self) -> List[Dict]:
        """
        获取插件API配置,必须
        """
        return [
            {
                "path": "/start_scrape",
                "endpoint": self.start_scrape_api,
                "methods": ["POST"],
                "summary": "启动音乐刮削",
                "description": "启动音乐刮削任务",
                "auth": "bear"  # 使用Bearer Token认证
            }
        ]

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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件"
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
                                            "model": "send_notify",
                                            "label": "发送通知"
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
                                            "model": "scrape_once",
                                            "label": "立即运行一次",
                                            "hint": "保存配置后立即启动一次刮削，执行后自动关闭此开关"
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
                                            "model": "base_url",
                                            "label": "Music-Scraper服务地址",
                                            "placeholder": "http://your-music-scraper-server:7301"
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
                                            "model": "password",
                                            "label": "登录密码",
                                            "type": "password"
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "scrape_path",
                                            "label": "刮削路径",
                                            "placeholder": "整理（只能写一个路径名，如：整理）",
                                            "hint": "事件中的路径优先于此配置路径，只需写路径名"
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
            "send_notify": self._send_notify,
            "scrape_once": self._scrape_once,
            "base_url": self._base_url,
            "password": self._password,
            "scrape_path": self._scrape_path
        }

    def get_page(self) -> List[dict]:
        """
        页面 JSON，按钮直接写死，用于渲染前端操作界面
        """
        # 获取表单API的当前状态
        form_status = self.get_form_status()
        
        plugin_prefix = self.__class__.__name__  # 类名直接作为 API 前缀
        
        return [
            {
                "component": "VRow",  # 行布局
                "content": [
                    {
                        "component": "VCol",  # 列布局
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VCard",  # 卡片组件
                                "content": [
                                    {
                                        "component": "VCardTitle",
                                        "props": {'class': 'd-flex align-center justify-space-between'},
                                        "text": "音乐刮削助手"
                                    },
                                    {
                                        "component": "VCardText",  # 卡片文本区域
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {"class": "mb-4 text-center"},
                                                "text": "点击下方按钮启动音乐刮削任务"
                                            },
                                            # 按钮区域
                                            {
                                                "component": "div",
                                                "props": {"class": "d-flex gap-2 mb-4"},
                                                "content": [
                                                    # 按钮：启动刮削任务
                                                    {
                                                        "component": "VBtn",
                                                        "props": {
                                                            "color": "primary",
                                                            "variant": "elevated",
                                                            "size": "large",
                                                            "prepend-icon": "mdi-play",
                                                            "width": "100%"
                                                        },
                                                        "text": "启动刮削",
                                                        "events": {
                                                            "click": {
                                                                "type": "request",
                                                                "api": f"plugin/{plugin_prefix}/start_scrape",
                                                                "method": "POST",
                                                                "params": {"apikey": settings.API_TOKEN},
                                                                "success": "刮削任务已启动",
                                                                "fail": "刮削启动失败"
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
                    # 添加状态显示区域
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VCard",
                                "content": [
                                    {
                                        "component": "VCardText",
                                        "content": [
                                            # 日志显示区域
                                            {
                                                "component": "div",
                                                "props": {"class": "mt-4"},
                                                "content": [
                                                    {
                                                        "component": "div",
                                                        "props": {"class": "font-weight-bold mb-2"},
                                                        "text": "刮削日志:"
                                                    },
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "rows": 10,
                                                            "readonly": True,
                                                            "variant": "outlined",
                                                            "value": "\n".join(form_status.get("logs", [])),
                                                            "placeholder": "刮削日志将在这里显示..."
                                                        }
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

    def stop_service(self):
        """
        停止插件服务,必须
        """
        # 首先设置停止标志，防止异步任务继续发送通知
        self._enabled = False
        self._session_cookie = None
        self._is_scraping = False  # 重置刮削状态

        # 停止所有可能正在运行的异步任务
        # 这里可以添加更多的清理逻辑，比如取消定时任务等

        logger.info("MusicScraperHelp插件服务已停止")

    def get_state(self) -> bool:
        """
        获取插件启用状态,必须
        """
        return self._enabled

    def _send_notification(self, title: str, text: str, error: bool = False, warn: bool = False):
        """
        发送系统通知
        """
        try:
            # 检查是否启用通知
            if not hasattr(self, '_send_notify') or not self._send_notify:
                return

            # 确保插件对象已正确初始化
            if not hasattr(self, 'post_message'):
                logger.error("post_message方法不存在，无法发送通知")
                return

            if error:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=f"❌ {title}",
                    text=text
                )
            elif warn:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=f"⚠️ {title}",
                    text=text
                )
            else:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=f"✅ {title}",
                    text=text
                )

        except Exception as e:
            logger.error(f"发送通知失败: {str(e)}", exc_info=True)

    @eventmanager.register(EventType.PluginAction)
    def plugin_action(self, event: Event):
        """
        处理插件动作事件
        """
        data = event.event_data or {}
        action = data.get("action")

        mtype = data.get("mtype", NotificationType.Plugin)
        userid = data.get("userid", 1)

        if action == "start_scrape":
            logger.info("接收到启动音乐刮削请求，使用配置路径")
            # 异步执行刮削任务，避免阻塞命令处理
            import threading
            thread = threading.Thread(
                target=self._execute_scrape_command, kwargs={"notify": True})
            thread.daemon = True
            thread.start()

        elif action == "save_and_scrape":
            logger.info("接收到保存配置并启动刮削请求")
            # 先触发保存配置
            self._update_config()

            # 再启动刮削
            success, message = self.start_auto_scrape()
            if success:
                logger.info(f"保存配置并刮削成功: {message}")
            else:
                logger.error(f"保存配置并刮削失败: {message}")

    def _login(self) -> dict:
        """
        仅使用密码登录，获取所有必要的cookies
        """
        try:
            headers = {
                "Content-Type": "application/json",
                "Accept": "*/*",
                "Origin": self._base_url,
                "Referer": f"{self._base_url}/mobile/login.html",
                "User-Agent": "Mozilla/5.0"
            }

            resp = RequestUtils(headers=headers, timeout=15).post_res(
                url=f"{self._base_url}/api/auth/login",
                json={"password": self._password}
            )

            # 记录登录响应详细信息
            if resp:
                if resp.status_code == 200:
                    # 检查响应体中是否有token
                    try:
                        response_data = resp.json()
                        if response_data.get("success") and response_data.get("data", {}).get("token"):
                            token = response_data["data"]["token"]
                            logger.info(f"登录成功")
                            # 返回token信息
                            return {"token": token}
                    except Exception as json_error:
                        logger.error(f"解析响应JSON失败: {json_error}")
                    
                    logger.error("登录失败：未获取到有效的认证信息")
                    return None
                else:
                    logger.error(f"登录失败，状态码: {resp.status_code}")
                    return None
            else:
                logger.error("登录请求无响应，可能是网络连接或服务地址问题")
                return None

        except Exception as e:
            logger.error(f"登录过程中发生错误: {str(e)}")
            # 增加详细的错误信息
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            return None

    def start_auto_scrape(self) -> Tuple[bool, str]:
        """
        启动自动刮削
        """
        if self._is_scraping:
            logger.warning("刮削任务已在运行中")
            return False, "刮削任务已在运行中"

        if not self._session_cookie:
            self._session_cookie = self._login()
            if not self._session_cookie:
                return False, "登录失败，无法获取cookies"

        # 设置刮削状态为运行中
        self._is_scraping = True

        try:
            # 确定刮削路径：以配置界面为准
            final_path = self._scrape_path or ""
            
            # 检查路径是否为空，如果为空则取消运行
            if not final_path:
                self._is_scraping = False
                logger.error("刮削路径为空，取消运行")
                return False, "刮削路径为空，请在配置界面设置路径"
            
            # 记录刮削路径
            path_info = f"路径: {final_path}"
            logger.info(f"开始刮削任务 - {path_info}")

            payload = {
                "path": final_path,
                "sources": ["qqmusic", "netease", "kugou"],
                "fields": [
                    "cover",
                    "title",
                    "artist",
                    "album",
                    "year",
                    "genre",
                    "lyrics_embedded"
                ],
                "skip_existing": False,
                "use_local_cover": False,
                "remove_ad": True,
                "recursive": True,
                "min_confidence": 70
            }

            headers = {
                "Content-Type": "application/json",
                "Accept": "*/*",
                "Origin": self._base_url,
                "Referer": f"{self._base_url}/mobile/auto-scrape.html",
                "User-Agent": "Mozilla/5.0"
            }

            # 根据认证类型调整请求
            if isinstance(self._session_cookie, dict):
                # 如果是token认证
                if "token" in self._session_cookie:
                    headers["Authorization"] = f"Bearer {self._session_cookie['token']}"
                    cookies = {}
                else:
                    # 如果是cookies认证
                    cookies = self._session_cookie
            else:
                cookies = {}

            resp = RequestUtils(
                headers=headers,
                cookies=cookies,
                timeout=30
            ).post_res(
                url=f"{self._base_url}/api/auto-scrape/start",
                json=payload
            )

            if resp is None:
                self._is_scraping = False
                return False, "刮削请求未建立连接"

            # 只信 status_code，不解析 body
            status_code = getattr(resp, "status_code", None)
            logger.info(f"刮削HTTP状态码: {status_code}")

            if status_code == 200:
                # 刮削启动成功，但不立即设置状态，因为可能是异步任务
                # 这里可以添加任务状态检查逻辑，或者设置一个定时器检查任务状态
                # 为简化，这里假设任务是异步的，等待一段时间后重置状态
                import threading

                def reset_scrape_status():
                    import time
                    time.sleep(60)  # 1分钟后重置状态，让用户可以再次尝试
                    logger.info("自动重置刮削状态，允许再次启动")
                    self._is_scraping = False

                threading.Thread(target=reset_scrape_status,
                                 daemon=True).start()

                return True, f"刮削任务已启动 - 路径: {final_path if final_path else '默认路径'}"

            elif status_code == 400:
                self._is_scraping = False
                return False, "目录内没有音乐文件"

            elif status_code == 401:
                self._is_scraping = False
                return False, "登录已失效，请重新登录"

            elif status_code == 403:
                self._is_scraping = False
                return False, "没有权限执行刮削"

            else:
                self._is_scraping = False
                return False, f"刮削启动失败（HTTP {status_code}）"

        except Exception as e:
            logger.error(f"启动刮削过程中发生错误: {str(e)}")
            self._is_scraping = False
            return False, f"启动刮削过程中发生错误: {str(e)}"

    def relogin(self) -> bool:
        """
        重新登录获取新session
        """
        self._session_cookie = self._login()
        return self._session_cookie is not None

    def reset_scrape_status(self):
        """
        手动重置刮削状态，允许重新启动刮削任务
        """
        if self._is_scraping:
            logger.info("手动重置刮削状态")
            self._is_scraping = False

    def reset_status_api(self, request: BaseApiReq) -> Dict[str, Any]:
        """
        重置刮削状态API接口
        """
        try:
            logger.info(f"接收到重置状态请求: {request}")

            # 确保插件已启用
            if not self._enabled:
                logger.warning("插件未启用")
                return {
                    "success": False,
                    "message": "插件未启用",
                    "status": "插件未启用"
                }

            # 验证API Token
            from app.core.config import settings
            if hasattr(request, 'apikey') and request.apikey == settings.API_TOKEN:
                logger.info("API Token验证成功")
            else:
                logger.warning("API Token验证失败")
                return {
                    "success": False,
                    "message": "API Token验证失败",
                    "status": "验证失败"
                }

            # 重置状态
            old_status = "刮削中" if self._is_scraping else "未启动"
            self.reset_scrape_status()
            
            return {
                "success": True,
                "message": "刮削状态已重置",
                "status": "已重置",
                "logs": [f"状态已从 {old_status} 重置为未启动"]
            }

        except Exception as e:
            logger.error(f"重置状态API异常: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"重置状态失败: {str(e)}",
                "status": "重置失败"
            }

    def form_status_api(self, request: BaseApiReq) -> Dict[str, Any]:
        """
        获取表单API状态的接口
        """
        try:
            logger.info("接收到获取表单状态请求")

            # 确保插件已启用
            if not self._enabled:
                logger.warning("插件未启用")
                return {
                    "success": False,
                    "message": "插件未启用"
                }

            # 验证API Token
            from app.core.config import settings
            if hasattr(request, 'apikey') and request.apikey == settings.API_TOKEN:
                logger.info("API Token验证成功")
            else:
                logger.warning("API Token验证失败")
                return {
                    "success": False,
                    "message": "API Token验证失败"
                }

            # 获取表单状态
            return {
                "success": True,
                **self.get_form_status()
            }

        except Exception as e:
            logger.error(f"获取表单状态API异常: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"获取表单状态失败: {str(e)}"
            }

    def start_scrape_api(self, request: BaseApiReq) -> Dict[str, Any]:
        """
        启动音乐刮削API接口
        """
        try:
            logger.info(f"接收到API启动刮削请求: {request}")

            # 确保插件已启用
            if not self._enabled:
                logger.warning("插件未启用，无法执行刮削任务")
                return {
                    "success": False,
                    "message": "插件未启用，无法执行刮削任务",
                    "status": "插件未启用",
                    "logs": ["插件未启用，无法执行刮削任务"]
                }

            # 验证API Token
            from app.core.config import settings
            if hasattr(request, 'apikey') and request.apikey == settings.API_TOKEN:
                logger.info("API Token验证成功")
            else:
                logger.warning("API Token验证失败")
                return {
                    "success": False,
                    "message": "API Token验证失败",
                    "status": "验证失败",
                    "logs": [f"API Token验证失败: {request.apikey}"]
                }

            # 如果刮削任务已在运行中，直接返回错误，避免重复执行
            if self._is_scraping:
                logger.warning("刮削任务已在运行中，拒绝重复启动")
                logs = self.get_latest_logs()
                return {
                    "success": False,
                    "message": "刮削任务已在运行中",
                    "status": "刮削中",
                    "logs": logs
                }

            # 获取最新的日志
            logs = self.get_latest_logs()

            # 异步执行刮削任务，使用固定参数
            import threading
            logger.info("启动异步刮削任务")
            thread = threading.Thread(
                target=self._execute_scrape_command, kwargs={"notify": True})
            thread.daemon = True
            thread.start()

            return {
                "success": True,
                "message": "音乐刮削任务已启动",
                "status": "刮削中",
                "logs": logs + ["音乐刮削任务已启动，正在执行中..."],
                # 不添加刷新参数，避免页面自动刷新
            }

        except Exception as e:
            logger.error(f"启动刮削API异常: {e}", exc_info=True)
            return {
                "success": False,
                "message": f"启动刮削任务失败: {str(e)}",
                "status": "启动失败",
                "logs": [f"启动刮削任务失败: {str(e)}"]
            }

    def _execute_scrape_command(self, notify: bool = True):
        """
        执行刮削命令（异步执行）
        """
        logger.info(f"开始执行刮削命令，通知状态: {notify}")
        
        # 获取当前日志
        logs = self.get_latest_logs()
        
        # 添加新日志
        path_info = f"路径: {self._scrape_path if self._scrape_path else '默认路径'}"
        logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 开始执行刮削任务 - {path_info}")
        self.save_data("scrape_logs", logs)
        
        try:
            # 确保插件已启用
            if not self._enabled:
                logger.warning("插件未启用，无法执行刮削任务")
                logs.append("插件未启用，无法执行刮削任务")
                self.save_data("scrape_logs", logs)
                if notify:
                    self._send_notification(
                        "音乐刮削", "插件未启用，无法执行刮削任务", error=True)
                return

            logger.info("调用start_auto_scrape方法")
            success, message = self.start_auto_scrape()
            logger.info(f"刮削结果: success={success}, message={message}")

            # 记录结果到日志
            logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 刮削结果: {message}")
            
            if success:
                logger.info("刮削成功")
                logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 刮削任务完成")
                self.save_data("scrape_logs", logs)
                if notify:
                    # 使用标准的插件通知接口
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title="音乐刮削完成",
                        text="已完成音乐元数据刮削任务"
                    )
            else:
                # 检查是否是业务可接受的400错误（目录内没有音乐文件）
                if "没有音乐文件" in message:
                    logger.warning(f"⚠️ 刮削命令执行: {message}")
                    logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 警告: {message}")
                    self.save_data("scrape_logs", logs)
                    if notify:
                        self._send_notification(
                            "音乐刮削", message, warn=True)  # 警告通知
                else:
                    logger.error(f"❌ 刮削命令执行失败: {message}")
                    logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 错误: {message}")
                    self.save_data("scrape_logs", logs)
                    if notify:
                        self._send_notification(
                            "音乐刮削", message, error=True)  # 错误通知
                
                # 确保任务状态被正确重置，避免循环调用
                self._is_scraping = False

        except Exception as e:
            logger.error(f"❌ 刮削命令执行异常: {e}", exc_info=True)
            logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 异常: {str(e)}")
            self.save_data("scrape_logs", logs)
            if notify:
                self._send_notification(
                    "音乐刮削", f"刮削命令执行失败: {str(e)}", error=True)

    def _run_once_immediately(self):
        """
        立即运行一次刮削（异步执行）
        """
        try:
            # 确保插件已启用
            if not self._enabled:
                logger.warning("插件未启用，跳过立即运行")
                return

            # 检查是否已有刮削任务在运行
            if self._is_scraping:
                logger.warning("刮削任务已在运行中，跳过立即运行")
                return

            logger.info("立即刮削任务开始")

            success, message = self.start_auto_scrape()

            # 执行完成后更新配置，关闭开关
            self._scrape_once = False
            self._update_config()

            if success:
                self._send_notification("音乐刮削", message)  # 成功通知
            else:
                # 检查是否是业务可接受的400错误（目录内没有音乐文件）
                if "没有音乐文件" in message:
                    logger.warning(f"⚠️ 立即运行刮削: {message}")
                    # 400状态码是正常业务结果，但仍然发送通知（警告类型）
                    self._send_notification("音乐刮削", message, warn=True)  # 警告通知
                else:
                    logger.error(f"❌ 立即运行刮削失败: {message}")
                    # 失败时发送通知
                    self._send_notification(
                        "音乐刮削", message, error=True)  # 错误通知

        except Exception as e:
            logger.error(f"❌ 立即运行刮削异常: {e}")
            # 异常时发送通知
            self._send_notification("音乐刮削", f"立即运行刮削异常: {str(e)}", error=True)

    def _update_config(self):
        """
        更新插件配置
        """
        try:
            config = {
                "enabled": self._enabled,
                "send_notify": self._send_notify,
                "scrape_once": self._scrape_once,
                "base_url": self._base_url,
                "password": self._password,
                "scrape_path": self._scrape_path
            }
            self.update_config(config)
        except Exception as e:
            logger.error(f"更新配置失败: {e}")

    def get_latest_logs(self) -> List[str]:
        """
        获取最新的刮削日志（从新到旧排列）
        """
        try:
            # 从插件数据存储中获取日志
            logs_data = self.get_data("scrape_logs") or []
            if logs_data:
                # 只返回最近10条日志，并反转顺序（从新到旧）
                latest_logs = logs_data[-10:] if len(logs_data) > 10 else logs_data
                return list(reversed(latest_logs))
            return []
        except Exception as e:
            logger.error(f"获取日志失败: {e}")
            return []

    def get_form_status(self) -> Dict[str, Any]:
        """
        获取表单API的当前状态
        """
        try:
            # 检查刮削是否正在运行
            status = "未启动"
            if self._is_scraping:
                status = "刮削中"
            
            # 获取最新日志
            logs = self.get_latest_logs()
            
            return {
                "status": status,
                "logs": logs
            }
        except Exception as e:
            logger.error(f"获取表单状态失败: {e}")
            return {
                "status": "获取状态失败",
                "logs": [f"获取状态失败: {str(e)}"]
            }

    @eventmanager.register(EventType.MetadataScrape)
    def handle_metadata_scrape(self, event: Event):
        """
        处理 MetadataScrape 事件，触发音乐刮削
        """
        logger.info("监听到 MetadataScrape 事件，使用配置路径，准备执行音乐刮削任务")

        # 插件未启用
        if not self._enabled:
            logger.warning("插件未启用，忽略 MetadataScrape 事件")
            return

        # 防止重复执行
        if self._is_scraping:
            logger.info("刮削任务已在运行中，忽略 MetadataScrape 事件")
            return

        # 异步执行，避免阻塞事件总线
        import threading
        threading.Thread(
            target=self._execute_scrape_command,
            kwargs={"notify": True},
            daemon=True
        ).start()

        logger.info("音乐刮削任务已通过事件触发")
