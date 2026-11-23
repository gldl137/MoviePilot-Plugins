"""
CAS视频扩展（CasFileExtender）

让 MoviePilot 把 .cas 当作视频文件识别：
1. 把 .cas 注入 settings.RMT_MEDIAEXT（影响实时读取扩展名的各处，如扫描、下载判断等）；
2. 刷新 TransferChain 单例缓存的 _media_exts / _allowed_exts（整理链按此缓存判定媒体类型）；
3. 刷新 Monitor 单例的 dispatcher.all_exts（目录监控按此过滤并触发整理）；
4. 为 OpenList/AList 的上传回查加兜底：CAS 秒传在网盘端实际已成功，但 alist 按目标
   .cas 路径回查常报 object not found（.cas 被秒传端消费/改名或索引短时间不可见），
   宿主 Alist.upload() 会把这种「PUT 成功但回查不到」误判成上传失败。这里参照宿主
   create_folder/copy/move 的 __build_transfer_item 兜底模式，为 .cas 合成目标文件项。

关键点：TransferChain 与 Monitor 的扩展名都是在实例 __init__ 时一次性算好并缓存的，
并不会实时读 settings，所以仅修改 settings.RMT_MEDIAEXT 或在类上打补丁都不够，
必须直接刷新这些已实例化对象的缓存列表。
"""
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.sdk.logging import logger

try:  # 事件系统（整理重命名 Hook）
    from app.sdk.events import eventmanager, Event
    from app.schemas.types import ChainEventType
    _EVENT_OK = True
except ImportError:  # 兼容旧版宿主
    eventmanager = None  # type: ignore
    Event = None  # type: ignore
    ChainEventType = None  # type: ignore
    _EVENT_OK = False

try:  # 新版宿主
    from app.sdk.plugin.base import _PluginBase
except ImportError:
    try:
        from app.sdk.plugin import _PluginBase
    except ImportError:  # 兼容旧版宿主
        from app.sdk.plugin.base import _PluginBase  # type: ignore

__all__ = ["CasFileExtender"]

DEFAULT_EXT = "cas"

# 已知内层视频扩展名（白名单优先匹配）：.cas 前若带这些扩展名，整理重命名时保留。
# 同时 _cas_inner_video_ext 允许任意「纯字母」内层扩展名自动保留，以兼容未列出的格式。
INNER_VIDEO_EXTS = {
    ".mkv", ".mk3d", ".mp4", ".m4v", ".avi", ".divx", ".xvid", ".mov", ".qt",
    ".wmv", ".asf", ".mpg", ".mpeg", ".m2v", ".m1v", ".ts", ".m2ts", ".mts",
    ".tp", ".trp", ".vob", ".ifo", ".ogv", ".ogg", ".webm", ".flv", ".f4v",
    ".f4p", ".3gp", ".3g2", ".rm", ".rmvb", ".iso", ".img", ".nrg", ".dav",
    ".264", ".265", ".h264", ".h265", ".hevc", ".avc", ".drc", ".wtv",
    ".dvr-ms", ".amv", ".nsv", ".svi", ".gvi", ".dat", ".bdmv", ".evo",
    ".mxf", ".webm",
}


def _load_settings():
    """兼容不同宿主版本的 settings 导入路径。"""
    try:
        from app.sdk.config import settings
        return settings
    except Exception:
        try:
            from app.sdk.config import settings  # type: ignore
            return settings
        except Exception:
            return None


def _inject_media_ext() -> bool:
    """
    把带点的 .cas 注入 settings.RMT_MEDIAEXT。

    白名单默认值为带点格式（.mp4/.mkv 等），且整理链 _is_media_file 判定使用
    f".{extension}" 拼点匹配，因此必须注入带点的 ".cas"。
    """
    settings = _load_settings()
    if settings is None:
        logger.warning("CAS视频扩展：未定位到 settings，白名单注入失败（宿主导入路径可能变更）")
        return False
    ext = f".{DEFAULT_EXT}"
    exts = getattr(settings, "RMT_MEDIAEXT", None)
    if not isinstance(exts, list):
        logger.warning(f"CAS视频扩展：RMT_MEDIAEXT 不是列表（实际 {type(exts)}），注入失败")
        return False
    if ext not in exts:
        exts.append(ext)
        logger.info(f"CAS视频扩展：已将 {ext} 注入视频白名单 RMT_MEDIAEXT")
    else:
        logger.info(f"CAS视频扩展：视频白名单已含 {ext}")
    return True


def _patch_transfer_chain() -> None:
    """
    刷新 TransferChain 单例缓存的媒体扩展名，使 .cas 被识别为视频文件。

    TransferChain 是 Singleton，其 _media_exts / _allowed_exts 在 __init__ 时一次性算好，
    不会实时读 settings，因此必须直接刷新实例缓存。
    """
    try:
        from app.chain.transfer import TransferChain
        tc = TransferChain()
        ext = f".{DEFAULT_EXT}"
        changed = False
        for attr in ("_media_exts", "_allowed_exts"):
            cur = list(getattr(tc, attr, []) or [])
            if ext not in cur:
                cur.append(ext)
                setattr(tc, attr, cur)
                changed = True
        if changed:
            logger.info("CAS视频扩展：已将 .cas 注入 TransferChain 媒体扩展名缓存")
        else:
            logger.info("CAS视频扩展：TransferChain 媒体扩展名缓存已含 .cas")
    except Exception as err:
        logger.warning(f"CAS视频扩展：刷新 TransferChain 失败：{err}")


def _refresh_native_monitor() -> bool:
    """
    刷新 Monitor 单例的 dispatcher.all_exts，使目录监控把 .cas 当作候选媒体触发整理。
    返回 True 表示刷新成功（Monitor 已就绪）。
    """
    try:
        from app.foundation.singleton import SingletonClass
        from app.monitor.monitor import Monitor
        monitor = SingletonClass.get_existing_instance(Monitor)
        if not monitor:
            return False
        dispatcher = getattr(monitor, "_dispatcher", None)
        if dispatcher is None:
            return False
        ext = f".{DEFAULT_EXT}"
        exts = list(getattr(dispatcher, "all_exts", []) or [])
        if ext not in exts:
            exts.append(ext)
            dispatcher.all_exts = exts
            logger.info("CAS视频扩展：已将 .cas 注入目录监控分发器扩展名")
        else:
            logger.info("CAS视频扩展：目录监控分发器扩展名已含 .cas")
        return True
    except Exception as err:
        logger.warning(f"CAS视频扩展：刷新分发器失败：{err}")
        return False


def _background_refresh_monitor() -> None:
    """
    后台周期刷新 Monitor 分发器扩展名，处理 Monitor 晚于插件启动的情况。
    最多重试 5 分钟，成功即结束。
    """
    for _ in range(150):
        if _refresh_native_monitor():
            return
        time.sleep(2)


# ---------------- OpenList/AList 上传回查兜底 ----------------
# 背景：宿主 Alist.upload() 在 PUT /api/fs/put 返回 200 后会立刻调用
# _delay_get_item(target_path, refresh=True) 回查目标文件，2 次失败（各 sleep 2s）即
# 返回 None，被整理链翻译成「上传 alist 失败」。CAS 秒传场景里，网盘端实际已把视频
# 落盘（.cas 被消费/改名成真实视频名），所以按原始 .cas 路径回查必然 object not found。
# 宿主的 create_folder/copy/move 早已用 __build_transfer_item 对「操作成功但元数据短时
# 不可见」做了兜底，唯独 upload() 漏了。这里给 upload 补上同样的兜底。

_upload_fallback_enabled: bool = False  # 由插件实例启用状态驱动，禁用时不兜底
_alist_upload_patched: bool = False
_orig_alist_upload = None  # type: ignore


def _build_uploaded_fileitem(storage_oper: Any, target_dir: Any, path: Any,
                             new_name: Optional[str]) -> Optional[Any]:
    """
    参照宿主 Alist.__build_transfer_item，上传成功但回查不到时按目标路径合成 FileItem。

    返回 None 表示无法合成（调用方应保持原生失败行为）。
    """
    from types import SimpleNamespace

    src_path = Path(str(path or ""))
    target_name = new_name or src_path.name
    if not target_name:
        return None
    target_dir_str = str(getattr(target_dir, "path", "") or "")
    if not target_dir_str:
        return None
    target_path = Path(target_dir_str) / target_name

    try:
        stat = src_path.stat()
        size, mtime = stat.st_size, stat.st_mtime
    except Exception:
        size, mtime = None, None

    # 源文件项只需提供 文件类型/size/mtime/thumbnail 这几个属性，
    # 优先复用宿主私有方法（字段定义随宿主版本变化也不用我们操心）。
    source_item = SimpleNamespace(
        type="file", size=size, modify_time=mtime, thumbnail=None
    )
    builder = (getattr(storage_oper, "_Alist__build_transfer_item", None)
               or getattr(storage_oper, "__build_transfer_item", None))
    if callable(builder):
        return builder(source_item, target_path)

    # 降级：直接构造 FileItem（兼容宿主把私有方法改名/移除的情况）
    try:
        from app.schemas.file import FileItem
        schema = getattr(type(storage_oper), "schema", None)
        storage_value = getattr(schema, "value", "alist") if schema is not None else "alist"
        return FileItem(
            storage=storage_value,
            type="file",
            path=target_path.as_posix(),
            name=target_name,
            basename=Path(target_name).stem,
            extension=Path(target_name).suffix[1:] or None,
            size=size,
            modify_time=mtime,
        )
    except Exception as err:
        logger.warning(f"CAS视频扩展：合成上传文件项失败：{err}")
        return None


def _patch_alist_upload() -> None:
    """
    monkey-patch Alist.upload：PUT 成功但回查不到 .cas 时，用目标路径合成文件项返回，
    避免整理任务被误判为「上传 alist 失败」。类级 patch，幂等。
    """
    global _alist_upload_patched, _orig_alist_upload
    if _alist_upload_patched:
        return
    try:
        from app.modules.filemanager.storages.alist import Alist
    except Exception as err:
        logger.warning(f"CAS视频扩展：导入 Alist 失败，无法启用上传兜底：{err}")
        return

    _orig_alist_upload = Alist.upload

    def upload_with_cas_fallback(self, *args, **kwargs):
        result = _orig_alist_upload(self, *args, **kwargs)
        if result is not None or not _upload_fallback_enabled:
            return result

        # 兼容不同宿主签名：upload(fileitem, path, new_name=None, task=False)
        target_dir = args[0] if len(args) > 0 else kwargs.get("fileitem")
        path = args[1] if len(args) > 1 else kwargs.get("path")
        new_name = args[2] if len(args) > 2 else kwargs.get("new_name")
        if not path or not str(path).lower().endswith(f".{DEFAULT_EXT}"):
            return None

        try:
            item = _build_uploaded_fileitem(self, target_dir, path, new_name)
            if item is not None:
                logger.info(
                    f"CAS视频扩展：OpenList 上传 {Path(str(path)).name} 后按目标路径回查不到，"
                    "已合成文件项继续整理（网盘端秒传实际已成功）"
                )
            return item
        except Exception as err:
            logger.warning(f"CAS视频扩展：上传回查兜底异常：{err}")
            return None

    Alist.upload = upload_with_cas_fallback
    _alist_upload_patched = True
    logger.info("CAS视频扩展：已为 OpenList 上传加 .cas 回查兜底")


class CasFileExtender(_PluginBase):
    # 插件元信息
    plugin_name = "CAS视频扩展"
    plugin_desc = "把 .cas 注入 MoviePilot 视频白名单，刷新整理链与目录监控的扩展名缓存，并修复 CAS 秒传成功后 OpenList 上传回查误报「上传 alist 失败」的问题。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.30"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "casfileextender_"
    plugin_order = 27
    auth_level = 1

    _enabled: bool = False
    _keep_inner_ext: bool = True
    _upload_fallback: bool = True

    def init_plugin(self, config: dict = None):
        global _upload_fallback_enabled
        cfg = config or {}
        self._enabled = bool(cfg.get("enabled", False))
        self._keep_inner_ext = bool(cfg.get("keep_inner_ext", True))
        self._upload_fallback = bool(cfg.get("upload_fallback", True))
        # 兜底开关由插件启用状态共同决定：插件禁用时绝不动宿主 upload
        _upload_fallback_enabled = self._enabled and self._upload_fallback
        if not self._enabled:
            return
        # 启用时刷新各处扩展名缓存（.cas 识别是全局行为，但受启用开关控制）
        _inject_media_ext()
        _patch_transfer_chain()
        _refresh_native_monitor()
        # Monitor 可能晚于插件启动，后台重试直到刷新成功
        threading.Thread(target=_background_refresh_monitor, daemon=True).start()
        # 为 OpenList 上传加 .cas 回查兜底（类级 patch，幂等）
        if self._upload_fallback:
            _patch_alist_upload()

    # ---------------- 宿主基类要求 ----------------

    def get_state(self) -> bool:
        """卡片绿点状态：只有插件启用时才亮。"""
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """启用开关、保留视频扩展名、上传回查兜底三个开关。"""
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
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "keep_inner_ext",
                                            "label": "保留视频扩展名",
                                            "hint": "整理重命名时把 .mkv.cas 的内层视频扩展名（如 .mkv）补回，避免被改成 .cas",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "upload_fallback",
                                            "label": "上传回查兜底",
                                            "hint": "CAS 秒传在网盘已成功、但 alist 回查目标 .cas 报 object not found 时，按目标路径合成文件项继续整理，避免误报「上传 alist 失败」",
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ], {"enabled": False, "keep_inner_ext": True, "upload_fallback": True}

    def get_page(self) -> Optional[List[dict]]:
        """无自定义详情页（与系统命令屏蔽一致：点击卡片直接进设置）。"""
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    # ---------------- 整理重命名 Hook ----------------
    # 场景：源文件形如 `xxx.mkv.cas`，MoviePilot 整理时把扩展名算成 `.cas`，
    # 导致目标文件名丢掉内层 `.mkv`。这里在重命名渲染前后把扩展名补回成 `.mkv.cas`。

    @staticmethod
    def _cas_inner_video_ext(source_item: Any, source_path: Any) -> Optional[str]:
        """从 .cas 源文件名提取内层视频扩展名（如 `xxx.mkv.cas` -> `.mkv`）。"""
        name = ""
        if source_item is not None:
            name = getattr(source_item, "name", "") or ""
        if not name and source_path:
            name = Path(str(source_path)).name
        name = str(name or "").strip()
        if not name.lower().endswith(f".{DEFAULT_EXT}"):
            return None
        base = name[: -len(DEFAULT_EXT) - 1]  # 去掉末尾 .cas
        inner = Path(base).suffix.lower()
        if not inner:
            return None
        # 已知视频扩展名，或纯字母的未知内层扩展名（容错未来格式）；
        # 含数字的内层名（如 2160p）视为分辨率标签而非扩展名，不保留。
        if inner in INNER_VIDEO_EXTS or (inner[1:].isalpha() and 1 <= len(inner) - 1 <= 8):
            return inner
        return None

    if _EVENT_OK and getattr(ChainEventType, "TransferRenameBuild", None) is not None:

        @eventmanager.register(ChainEventType.TransferRenameBuild)
        def on_transfer_rename_build(self, event: Event) -> None:
            """渲染前把 rename_dict['fileExt'] 从 `.cas` 改为 `.mkv.cas`。"""
            if not self._enabled:
                return
            if not self._keep_inner_ext:
                return
            data = event.event_data
            if not data:
                return
            inner = self._cas_inner_video_ext(
                getattr(data, "source_item", None), getattr(data, "source_path", None)
            )
            if not inner:
                return
            rename_dict = getattr(data, "rename_dict", None)
            if not isinstance(rename_dict, dict):
                return
            old = str(rename_dict.get("fileExt") or "")
            new = f"{inner}.{DEFAULT_EXT}"
            if old.lower() != new.lower():
                rename_dict["fileExt"] = new
                logger.info(f"CAS视频扩展：重命名扩展名 {old or '空'} -> {new}")

    if _EVENT_OK and getattr(ChainEventType, "TransferRename", None) is not None:

        @eventmanager.register(ChainEventType.TransferRename)
        def on_transfer_rename(self, event: Event) -> None:
            """渲染后兜底：若最终文件名仍以 `.cas` 结尾，补回内层视频扩展名。"""
            if not self._enabled:
                return
            if not self._keep_inner_ext:
                return
            data = event.event_data
            if not data:
                return
            render_str = str(getattr(data, "render_str", "") or "")
            if not render_str.lower().endswith(f".{DEFAULT_EXT}"):
                return
            inner = self._cas_inner_video_ext(
                getattr(data, "source_item", None), getattr(data, "source_path", None)
            )
            if not inner:
                return
            if render_str.lower().endswith(f"{inner}.{DEFAULT_EXT}"):
                return  # 已是 .mkv.cas，无需再补
            new_str = render_str[: -len(DEFAULT_EXT) - 1] + f"{inner}.{DEFAULT_EXT}"
            data.updated = True
            data.updated_str = new_str
            data.source = "CAS视频扩展"
            logger.info(f"CAS视频扩展：修正目标文件名 {render_str} -> {new_str}")

    def stop_service(self):
        pass
