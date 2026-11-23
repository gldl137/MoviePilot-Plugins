"""
CAS视频扩展（CasFileExtender）

让 MoviePilot 把 .cas 当作视频文件识别：
1. 把 .cas 注入 settings.RMT_MEDIAEXT（影响实时读取扩展名的各处，如扫描、下载判断等）；
2. 刷新 TransferChain 单例缓存的 _media_exts / _allowed_exts（整理链按此缓存判定媒体类型）；
3. 刷新 Monitor 单例的 dispatcher.all_exts（目录监控按此过滤并触发整理）；
4. 修复 CAS 秒传消费 .cas 后的上传/核对误报：上传 .cas 后，主动查询 OpenList 目标目录里
   CAS 实际产出的视频文件（如 2160p.mkv）作为唯一成功判据与大小来源；查到才继续整理，
   并用实际文件的 size 让归档显示真实大小；查不到则不伪造、保留原生失败（数据安全）。
   全程不读取 .cas 内容，只以网盘上真实存在的文件为准。

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
    """兼容不同宿主版本的 settings 导入路径（旧版 app.sdk.config / 新版 app.runtime.config）。"""
    for module in ("app.sdk.config", "app.runtime.config"):
        try:
            import importlib
            return importlib.import_module(module).settings
        except Exception:
            continue
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
# 背景（CAS 秒传 + MoviePilot 新版整理执行器）：
#   1) 本地 .cas 作为跨存储 move 上传到 OpenList 目标路径 xxx.cas；
#   2) CAS 服务监控网盘目录、消费掉 xxx.cas 并恢复出真实视频 xxx.mkv；
#   3) 宿主 Alist.upload() 的 PUT 成功后回查 xxx.cas 必然 object not found；
#   4) 更致命的是跨存储 move 的「删本地源前核对」步骤会再查一次 xxx.cas，
#      查不到即抛「文件 xxx.cas 已不可见」→ 外部结果 conflict → 转人工复核。
# 对策：
#   - patch Alist.upload：回查不到时按目标路径合成 FileItem（沿用宿主兜底模式）；
#   - patch Alist.get_item_strict：对刚由本插件合成的 .cas 路径，若宿主查不到，
#     则核对同目录是否存在 CAS 产出的视频（basename 相同、非 .cas）作为证据，
#     存在则合成 .cas FileItem 通过核对；不存在照常返回 None（保留数据安全保护）。

_upload_fallback_enabled: bool = False  # 由插件实例启用状态驱动，禁用时不兜底
_alist_upload_patched: bool = False
_orig_alist_upload = None  # type: ignore
_orig_alist_get_item_strict = None  # type: ignore

# 本插件合成的目标 .cas 项缓存：path(as_posix) -> {size, basename, ts}
# 供 get_item_strict 兜底核对时复用 size（保证与 __assert_file_identity 的 expected 一致），
# 短时窗口后自动失效，避免长期干预宿主查询。
_cas_pending_lock = threading.Lock()
_cas_pending: Dict[str, Dict[str, Any]] = {}
_CAS_PENDING_TTL = 180.0  # 秒；跨存储 move 删源核对通常在数秒内完成

# 判断 CAS 产出文件是否为视频（basename 相同、非 .cas 即可，白名单优先）
_CAS_OUTPUT_EXTS = INNER_VIDEO_EXTS | {
    ".mkv", ".mp4", ".avi", ".mov", ".ts", ".flv", ".wmv", ".rmvb", ".webm",
}


def _name_without_cas(name: str) -> str:
    """CAS 端规则：从 xxx.mkv.cas 推导真实文件名 xxx.mkv（仅去末尾 .cas）。"""
    return str(name or "").strip()[:-4] if str(name or "").strip().lower().endswith(".cas") else str(name or "").strip()


def _remember_cas_target(target_path: Any, size: Optional[int], basename: str,
                         real_name: Optional[str] = None, real_size: Optional[int] = None) -> None:
    try:
        key = Path(str(target_path)).as_posix()
        with _cas_pending_lock:
            _cas_pending[key] = {
                "size": size, "basename": basename, "ts": time.time(),
                "real_name": real_name, "real_size": real_size,
            }
    except Exception:
        pass


def _forget_cas_target(target_path: Any) -> None:
    try:
        with _cas_pending_lock:
            _cas_pending.pop(Path(str(target_path)).as_posix(), None)
    except Exception:
        pass


def _get_cas_pending(target_path: Any) -> Optional[Dict[str, Any]]:
    try:
        key = Path(str(target_path)).as_posix()
        now = time.time()
        with _cas_pending_lock:
            info = _cas_pending.get(key)
            if not info:
                return None
            if now - float(info.get("ts", 0)) > _CAS_PENDING_TTL:
                _cas_pending.pop(key, None)
                return None
            return dict(info)
    except Exception:
        return None


def _build_uploaded_fileitem(storage_oper: Any, target_dir: Any, path: Any,
                             new_name: Optional[str],
                             size_override: Optional[int] = None) -> Optional[Any]:
    """
    参照宿主 Alist.__build_transfer_item，上传成功但回查不到时按目标路径合成 FileItem。

    size_override: 用 OpenList 实际查询到的 CAS 产出视频大小（真实值）；
                   为 None 时退回本地 .cas 文件大小。不读取 .cas 内容。
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
        cas_size, mtime = stat.st_size, stat.st_mtime
    except Exception:
        cas_size, mtime = None, None

    display_size = size_override if isinstance(size_override, int) and size_override > 0 else cas_size

    # 源文件项只需提供 文件类型/size/mtime/thumbnail 这几个属性，
    # 优先复用宿主私有方法（字段定义随宿主版本变化也不用我们操心）。
    source_item = SimpleNamespace(
        type="file", size=display_size, modify_time=mtime, thumbnail=None
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
            size=display_size,
            modify_time=mtime,
        )
    except Exception as err:
        logger.warning(f"CAS视频扩展：合成上传文件项失败：{err}")
        return None


# CAS 产出视频的高频扩展名，按命中概率排序，用于精确探测（避免列整个目录）
_CAS_OUTPUT_PROBE_EXTS = (
    ".mkv", ".mp4", ".avi", ".mov", ".ts", ".flv", ".rmvb", ".webm", ".m4v", ".iso",
)


def _list_openlist_dir(storage_oper: Any, parent: Path, verbose: bool = True) -> List[Any]:
    """
    实际查询 OpenList 目录内容，返回文件项列表；verbose=True 时把查到的真实内容与格式打到日志。

    这是判断 CAS 是否成功的直接依据：只有 list 出来的真实文件才算数。
    """
    entries: List[Any] = []
    try:
        parent_posix = parent.as_posix()
        # 构造目录 FileItem（兼容不同宿主的字段），调原生 list
        dir_item = None
        try:
            from app.schemas.file import FileItem as _FI
            schema = getattr(type(storage_oper), "schema", None)
            storage_value = getattr(schema, "value", "alist") if schema is not None else "alist"
            dir_item = _FI(
                storage=storage_value,
                type="dir",
                path=parent_posix,
                name=parent.name or "/",
                basename=parent.name or "",
            )
        except Exception as err:
            logger.debug(f"CAS视频扩展：构造目录项失败：{err}")
        if dir_item is not None:
            try:
                entries = list(storage_oper.list(dir_item) or [])
            except TypeError:
                entries = list(storage_oper.list(fileitem=dir_item) or [])
    except Exception as err:
        logger.warning(f"CAS视频扩展：查询 OpenList 目录失败：{err}")
        return []

    # 把实际查询到的内容与格式打到日志，供直接判断秒传是否成功
    if verbose:
        logger.info(f"CAS视频扩展：查询 OpenList 目录 {parent.as_posix()} 实际内容（共 {len(entries)} 项）：")
        for entry in entries:
            name = getattr(entry, "name", "") or ""
            ftype = getattr(entry, "type", "") or ""
            size = getattr(entry, "size", None)
            ext = Path(str(name)).suffix.lower() or "(无扩展名)"
            size_str = f"{size}B" if isinstance(size, int) else "?"
            logger.info(f"CAS视频扩展：  - {name} | 类型={ftype} | 格式={ext} | 大小={size_str}")
    return entries


def _find_cas_output(storage_oper: Any, target_path: Any, basename: str,
                     real_name: Optional[str] = None,
                     wait: bool = False, wait_rounds: int = 12,
                     wait_interval: float = 0.7) -> Optional[Any]:
    """
    查询 OpenList 目标目录里 CAS 实际产出的视频文件（唯一成功判据与大小来源）。

    直接 list 目录拿到真实文件清单（日志可见），再按真实文件名/basename 匹配产出视频。
    CAS 恢复是异步的：wait=True 时短轮询若干次直到查到真实文件；查不到返回 None
    （= CAS 未成功，宿主按原生失败处理）。
    """
    parent = Path(str(target_path)).parent
    base = str(basename or "").strip()
    real = str(real_name or "").strip()

    def match(entries: List[Any]) -> Optional[Any]:
        # 首选真实文件名（如 2160p.mkv，来自 .cas 文件名去后缀，不读内容）
        if real:
            for entry in entries:
                if str(getattr(entry, "name", "") or "").lower() == real.lower():
                    return entry
        # 退回：basename 相同、扩展名是视频、非 .cas
        if base:
            for entry in entries:
                name = str(getattr(entry, "name", "") or "")
                if not name or name.lower().endswith(f".{DEFAULT_EXT}"):
                    continue
                if Path(name).stem.lower() != base.lower():
                    continue
                if Path(name).suffix.lower() in _CAS_OUTPUT_EXTS:
                    return entry
        return None

    rounds = wait_rounds if wait else 1
    for i in range(rounds):
        # 第一轮与最后一轮打印实际内容，中间静默，避免轮询刷屏
        verbose = (i == 0 or i == rounds - 1)
        entries = _list_openlist_dir(storage_oper, parent, verbose=verbose)
        found = match(entries)
        if found is not None:
            if i > 0:
                logger.info(
                    f"CAS视频扩展：第 {i + 1} 次查询命中 CAS 产出 "
                    f"{getattr(found, 'name', '?')}（{getattr(found, 'size', '?')}B）"
                )
            return found
        if i + 1 < rounds:
            time.sleep(wait_interval)
    return None


def _patch_alist_upload() -> None:
    """
    monkey-patch Alist.upload / get_item_strict：
    - upload 回查不到 .cas 时用目标路径合成 FileItem；
    - get_item_strict 对已知 CAS 目标路径查不到时，凭同目录 CAS 产出视频合成文件项通过核对。
    类级 patch，幂等。
    """
    global _alist_upload_patched, _orig_alist_upload, _orig_alist_get_item_strict
    if _alist_upload_patched:
        return
    try:
        from app.modules.filemanager.storages.alist import Alist
    except Exception as err:
        logger.warning(f"CAS视频扩展：导入 Alist 失败，无法启用上传兜底：{err}")
        return

    _orig_alist_upload = Alist.upload
    # get_item_strict 在部分宿主版本不存在，缺失时不 patch 它，绝不让整个 patch 失败
    _orig_alist_get_item_strict = getattr(Alist, "get_item_strict", None)

    def upload_with_cas_fallback(self, *args, **kwargs):
        result = _orig_alist_upload(self, *args, **kwargs)

        # 兼容不同宿主签名：upload(fileitem, path, new_name=None, task=False)
        target_dir = args[0] if len(args) > 0 else kwargs.get("fileitem")
        path = args[1] if len(args) > 1 else kwargs.get("path")
        new_name = args[2] if len(args) > 2 else kwargs.get("new_name")
        if not path or not str(path).lower().endswith(f".{DEFAULT_EXT}"):
            return result
        if not _upload_fallback_enabled:
            return result

        src_name = Path(str(path)).name
        real_name = _name_without_cas(src_name) or None  # 由文件名推导，不读 .cas 内容
        try:
            target_dir_str = str(getattr(target_dir, "path", "") or "")
            target_name = new_name or src_name
            if not target_dir_str or not target_name:
                return result
            target_path = Path(target_dir_str) / target_name
            basename = Path(real_name or target_name).stem
        except Exception as err:
            logger.debug(f"CAS视频扩展：解析目标路径失败：{err}")
            return result

        # 唯一判据：查询 OpenList 目标目录里 CAS 实际产出的视频（短轮询等异步恢复）。
        # 查到 -> 成功，用查到的真实文件 size/name；查不到 -> 不伪造，保留原生结果。
        output = _find_cas_output(self, target_path, basename, real_name=real_name, wait=True)
        if output is None:
            logger.info(
                f"CAS视频扩展：OpenList 目标目录未发现 {src_name} 对应的 CAS 产出视频，"
                "保留原生上传结果（不做伪造）"
            )
            return result

        out_size = getattr(output, "size", None)
        out_name = getattr(output, "name", None) or real_name or target_name
        cas_size = getattr(result, "size", None) if result is not None else None
        _remember_cas_target(target_path, cas_size, basename,
                             real_name=out_name, real_size=out_size)

        # 用 OpenList 查到的真实产出文件信息修正上传结果：归档显示真实大小，
        # path/name 保留 .cas 目标（不破坏宿主以 frozen target_file 为核心的核对链路），
        # size 必须是查到的真实值，保证后续身份核对 expected/current 一致。
        if isinstance(out_size, int) and out_size > 0:
            try:
                if result is None:
                    result = _build_uploaded_fileitem(
                        self, target_dir, path, new_name, size_override=out_size
                    )
                elif hasattr(result, "model_copy"):
                    result = result.model_copy(update={"size": out_size})
                else:
                    setattr(result, "size", out_size)
                logger.info(
                    f"CAS视频扩展：已在 OpenList 查到 CAS 产出视频 {out_name}，"
                    f"上传结果大小采用实际文件 {out_size} bytes"
                )
            except Exception as err:
                logger.warning(f"CAS视频扩展：用实际文件修正上传结果失败：{err}")
        return result

    def get_item_strict_with_cas(self, path: Any, *args, **kwargs):
        result = _orig_alist_get_item_strict(self, path, *args, **kwargs)
        if result is not None or not _upload_fallback_enabled or path is None:
            return result

        # 只处理 .cas 目标路径，且宿主明确报不存在（None）
        path_str = str(path)
        if not path_str.lower().endswith(f".{DEFAULT_EXT}"):
            return result
        pending = _get_cas_pending(path)
        basename = str((pending or {}).get("basename") or Path(path_str).stem)

        # 查询 OpenList 目标目录里 CAS 实际产出的视频（唯一判据）
        real_name = (pending or {}).get("real_name")
        output = _find_cas_output(self, path, basename, real_name=real_name, wait=True)
        if output is None:
            if pending:
                logger.info(
                    f"CAS视频扩展：OpenList 目标目录未发现 {path_str} 对应的 CAS 产出视频，"
                    "保留原生结果（数据安全，不做伪造）"
                )
            return None

        # size 用 OpenList 实际查到的产出视频大小，与 upload 阶段修正的 new_item 一致
        size = getattr(output, "size", None)
        if size is None:
            size = (pending or {}).get("real_size") or (pending or {}).get("size")

        # 合成 .cas FileItem：size 必须与核对方的 expected 一致，否则身份核对失败
        from types import SimpleNamespace
        src = SimpleNamespace(
            type="file",
            size=size,
            modify_time=getattr(output, "modify_time", None),
            thumbnail=None,
        )
        target_path = Path(path_str)
        builder = (getattr(self, "_Alist__build_transfer_item", None)
                   or getattr(self, "__build_transfer_item", None))
        item = None
        if callable(builder):
            item = builder(src, target_path)
        if item is None:
            try:
                from app.schemas.file import FileItem
                schema = getattr(type(self), "schema", None)
                storage_value = getattr(schema, "value", "alist") if schema is not None else "alist"
                item = FileItem(
                    storage=storage_value,
                    type="file",
                    path=target_path.as_posix(),
                    name=target_path.name,
                    basename=target_path.stem,
                    extension=DEFAULT_EXT,
                    size=size,
                    modify_time=getattr(output, "modify_time", None),
                )
            except Exception as err:
                logger.warning(f"CAS视频扩展：严格查询兜底合成文件项失败：{err}")
                return None
        # 核对已收口，主动清理登记，避免同一路径后续整理读到旧 size
        _forget_cas_target(path)
        logger.info(
            f"CAS视频扩展：{path_str} 已被 CAS 消费，凭同目录产出视频 "
            f"{getattr(output, 'name', '?')} 合成文件项通过整理核对"
        )
        return item

    Alist.upload = upload_with_cas_fallback
    if _orig_alist_get_item_strict is not None:
        Alist.get_item_strict = get_item_strict_with_cas
    _alist_upload_patched = True
    logger.info(
        "CAS视频扩展：补丁已安装（Alist.upload"
        + (" + get_item_strict" if _orig_alist_get_item_strict is not None else "")
        + "）—— 上传 .cas 后将查询 OpenList 目录实际内容判断秒传是否成功"
    )


class CasFileExtender(_PluginBase):
    # 插件元信息
    plugin_name = "CAS视频扩展"
    plugin_desc = "把 .cas 注入 MoviePilot 视频白名单，刷新整理链扩展名缓存；上传后实际 list 查询 OpenList 目录并把查到的文件名/格式/大小打到日志，以真实文件判断秒传成败并用于归档显示，修复上传/核对误报。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.35"
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
        # 为 OpenList 上传/严格查询加 .cas 秒传兜底（类级 patch，幂等）
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
                                            "label": "秒传核对兜底",
                                            "hint": "上传 .cas 后查询 OpenList 目标目录里 CAS 实际产出的视频，以网盘真实文件判断秒传是否成功、并用其真实大小显示归档；查不到才判定失败（不读 .cas 内容、不伪造）。修复「上传 alist 失败」及跨存储 move 核对冲突转人工复核",
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
