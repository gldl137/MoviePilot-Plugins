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
import re
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
        logger.debug(f"CAS视频扩展：已将 {ext} 注入视频白名单 RMT_MEDIAEXT")
    else:
        logger.debug(f"CAS视频扩展：视频白名单已含 {ext}")
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
            logger.debug("CAS视频扩展：已将 .cas 注入 TransferChain 媒体扩展名缓存")
        else:
            logger.debug("CAS视频扩展：TransferChain 媒体扩展名缓存已含 .cas")
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
            logger.debug("CAS视频扩展：已将 .cas 注入目录监控分发器扩展名")
        else:
            logger.debug("CAS视频扩展：目录监控分发器扩展名已含 .cas")
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
                         real_name: Optional[str] = None, real_size: Optional[int] = None,
                         real_mtime: Optional[float] = None) -> None:
    try:
        key = Path(str(target_path)).as_posix()
        with _cas_pending_lock:
            _cas_pending[key] = {
                "size": size, "basename": basename, "ts": time.time(),
                "real_name": real_name, "real_size": real_size,
                "real_mtime": real_mtime,
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


def _is_openlist_storage(storage_oper: Any) -> bool:
    """
    判断是否 OpenList/Alist 网盘（宽进策略，宁可多查也不要漏查）。

    本补丁只挂在 Alist.upload / Alist.get_item_strict 上，能进到兜底函数的
    self 必然是宿主 Alist（OpenList 兼容接口）实例；本地 Local 存储走自己的
    upload，根本不会触发本补丁。因此这里只做「明确排除本地」的保守判断：

    - schema.value 明确是 local/u115/smb/rclone 等非网盘 -> False；
    - 无法判断（类路径/schema 变更）-> True，绝不因识别失败而跳过查询。

    绝不用 isinstance(self, Alist) 精确判断：宿主版本间 Alist 类导入路径/类对象
    可能不一致，误判为 False 会让网盘上传查询被跳过，导致上传明明成功却被宿主
    判为失败、界面报错（v1.39 的事故根因）。
    """
    try:
        schema = getattr(type(storage_oper), "schema", None)
        value = str(getattr(schema, "value", "") or "").lower()
        if value and value not in ("alist", "alistgo", "openlist"):
            return False
    except Exception:
        pass
    # 识别不出来时按网盘处理：补丁挂载点已保证这里只可能是 Alist 实例
    return True


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

    # 完整目录清单只在首次查询用 DEBUG 记录（排查用）：轮询时逐条 INFO 会严重刷屏。
    # 真正是否查到产出，由 _find_cas_output 命中时用一条 INFO 证明即可。
    if verbose:
        logger.debug(f"CAS视频扩展：查询 OpenList 目录 {parent.as_posix()} 实际内容（共 {len(entries)} 项）：")
        for entry in entries:
            name = getattr(entry, "name", "") or ""
            ftype = getattr(entry, "type", "") or ""
            size = getattr(entry, "size", None)
            ext = Path(str(name)).suffix.lower() or "(无扩展名)"
            size_str = f"{size}B" if isinstance(size, int) else "?"
            logger.debug(f"CAS视频扩展：  - {name} | 类型={ftype} | 格式={ext} | 大小={size_str}")
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

    def season_episode(text: str) -> Optional[str]:
        """提取 SxxExx（忽略大小写/分隔符），如 S01E02 -> 's01e02'。"""
        m = re.search(r"[sS](\d{1,2})[eE](\d{2,4})", str(text or ""))
        return f"s{int(m.group(1)):02d}e{int(m.group(2)):02d}" if m else None

    def is_video_output(name: str) -> bool:
        return bool(name) and not name.lower().endswith(f".{DEFAULT_EXT}") \
            and Path(name).suffix.lower() in _CAS_OUTPUT_EXTS

    def match(entries: List[Any]) -> Optional[Any]:
        # ① 首选真实文件名（如 2160p.mkv，来自目标 .cas 名去后缀，不读内容）
        if real:
            for entry in entries:
                if str(getattr(entry, "name", "") or "").lower() == real.lower():
                    return entry
        # ② 退回：basename 相同、扩展名是视频、非 .cas
        if base:
            for entry in entries:
                name = str(getattr(entry, "name", "") or "")
                if is_video_output(name) and Path(name).stem.lower() == base.lower():
                    return entry
        # ③ 兜底：按季集号（SxxExx）唯一匹配。
        # 场景：整理重命名后目标是中文名，但某些上传仍用英文源文件名
        # （One.of.a.Kind...S01E02... vs 独一无二的浪漫 - S01E02），
        # 两者只有季集号相同。必须「唯一命中」才采纳，避免同目录多集时认错。
        wanted_se = season_episode(real) or season_episode(base)
        if wanted_se:
            hits = [
                entry for entry in entries
                if is_video_output(str(getattr(entry, "name", "") or ""))
                and season_episode(getattr(entry, "name", "")) == wanted_se
            ]
            if len(hits) == 1:
                logger.debug(
                    f"CAS视频扩展：文件名不一致，按季集号 {wanted_se} 唯一匹配到产出 "
                    f"{getattr(hits[0], 'name', '?')}"
                )
                return hits[0]
            if len(hits) > 1:
                logger.debug(
                    f"CAS视频扩展：季集号 {wanted_se} 匹配到 {len(hits)} 个产出，无法唯一定位，跳过"
                )
        return None

    # 每次查询打一条匹配参数诊断（只一次，不刷屏），用于定位中英文名匹配问题
    logger.debug(
        f"CAS视频扩展：准备查询产出 real={real!r} base={base!r} "
        f"季集号={season_episode(real) or season_episode(base)!r}"
    )
    rounds = wait_rounds if wait else 1
    # 上传刚返回时 CAS 可能还没消费 .cas 生成产出，先给一点缓冲再开始查，
    # 避免「上传刚启动就查、查不到就判失败」的过早误判。
    if wait:
        time.sleep(wait_interval)
    for i in range(rounds):
        # 仅首轮把完整目录清单写到 DEBUG（排查用），其余轮次完全静默
        entries = _list_openlist_dir(storage_oper, parent, verbose=(i == 0))
        found = match(entries)
        if found is not None:
            # 命中只打一条 INFO，作为「查到 OpenList 实际文件」的直接证据
            logger.info(
                f"CAS视频扩展：OpenList 目录 {parent.as_posix()} 查到 CAS 产出 "
                f"{getattr(found, 'name', '?')}（{getattr(found, 'size', '?')}B），判定上传成功"
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
        # 查询 OpenList 的两个前提必须同时满足：① 上传的是 .cas 文件；② 目标是 OpenList 网盘。
        # 本地之间传送 .cas、或非 .cas 文件的上传，一律不查（直接返回原生结果）。
        if not _upload_fallback_enabled:
            return result
        if not path or not str(path).lower().endswith(f".{DEFAULT_EXT}"):
            return result
        if not _is_openlist_storage(self):
            return result

        src_name = Path(str(path)).name
        # 只认 MP 整理后的文件名 new_name。两种「非最终整理名」都要跳过、不判失败：
        #   ① new_name 为空：这次 upload 未带目标名（探测/中间态），退回 src_name
        #      会用英文源名去查、对不上网盘中文产出而误报失败（v1.44 漏掉的情况）；
        #   ② new_name 仍是源文件裸名：MP 还没重命名的中间态。
        # 交给后续真正带中文整理名的那次 upload 判定。
        if not new_name or str(new_name).lower() == src_name.lower():
            logger.debug(
                f"CAS视频扩展：上传 .cas（{src_name}）未带整理后目标名或仍是源名，"
                "属中间态，跳过查询（等待 MP 重命名后的上传）"
            )
            return result
        try:
            target_dir_str = str(getattr(target_dir, "path", "") or "")
            target_name = str(new_name)
            if not target_dir_str or not target_name:
                return result
            target_path = Path(target_dir_str) / target_name
            # CAS 产出视频名 = 实际落盘的 .cas 文件名（new_name，已被整理重命名 Hook
            # 改成中文名）去掉 .cas 后缀。必须用目标名推导，不能用源文件名 src_name：
            # 英文源名（One.of.a.Kind...mkv.cas）与中文落地名
            # （独一无二的浪漫 - S01E02...mkv.cas）不一致，用源名匹配必然查不到产出。
            real_name = _name_without_cas(target_name) or None
            basename = Path(real_name or target_name).stem
        except Exception as err:
            logger.debug(f"CAS视频扩展：解析目标路径失败：{err}")
            return result

        # 唯一判据（二值）：查询 OpenList 目标目录里 CAS 实际产出的视频（短轮询等异步恢复）。
        # 查到 -> 上传成功，用真实文件 size/name 合成结果；
        # 查不到 -> 上传失败，返回 None（宿主据此判定失败，不做任何伪造）。
        output = _find_cas_output(self, target_path, basename, real_name=real_name, wait=True)
        if output is None:
            # 最后兜底：用本地源文件名再查一次（源文件名含 SxxExx，能与网盘中文产出
            # 按季集号唯一匹配）。覆盖「MP 中间态目标名不匹配」导致的误判失败。
            src_real = _name_without_cas(src_name) or None
            if src_real and src_real.lower() != str(real_name or "").lower():
                logger.debug(
                    f"CAS视频扩展：常规目标名未命中，用源文件名兜底重试：{src_real}"
                )
                output = _find_cas_output(
                    self, target_path, Path(src_real).stem,
                    real_name=src_real, wait=False,
                )
        if output is None:
            logger.warning(
                f"CAS视频扩展：OpenList 目标目录未发现 {target_name} 对应的 CAS 产出视频，"
                "判定上传失败"
            )
            return None

        out_size = getattr(output, "size", None)
        out_name = getattr(output, "name", None) or real_name or target_name
        cas_size = getattr(result, "size", None) if result is not None else None
        _remember_cas_target(target_path, cas_size, basename,
                             real_name=out_name, real_size=out_size,
                             real_mtime=getattr(output, "modify_time", None))

        # 用 OpenList 实际查到的产出视频合成上传结果：path/name 用真实视频名、
        # size 用真实大小，使整理历史记录（dest / dest_fileitem）显示真实视频而非 .cas。
        # 用 __build_transfer_item 合成（不带 fileid），避免后续身份核对触发 fileid 冲突。
        try:
            from types import SimpleNamespace
            real_target_path = Path(target_dir_str) / out_name
            builder = (getattr(self, "_Alist__build_transfer_item", None)
                       or getattr(self, "__build_transfer_item", None))
            source_item = SimpleNamespace(
                type="file", size=out_size,
                modify_time=getattr(output, "modify_time", None), thumbnail=None,
            )
            new_item = None
            if callable(builder):
                new_item = builder(source_item, real_target_path)
            if new_item is None:
                from app.schemas.file import FileItem
                schema = getattr(type(self), "schema", None)
                storage_value = getattr(schema, "value", "alist") if schema is not None else "alist"
                new_item = FileItem(
                    storage=storage_value, type="file", path=real_target_path.as_posix(),
                    name=out_name, basename=Path(out_name).stem,
                    extension=Path(out_name).suffix[1:] or None,
                    size=out_size, modify_time=getattr(output, "modify_time", None),
                )
            # 保留 pending：delete_move_source 核对仍以 frozen 的 .cas 路径查询，
            # 需靠 get_item_strict 兜底返回一致 size 的合成项（核对只看 size/type/fileid）。
            logger.debug(
                f"CAS视频扩展：上传结果已替换为 OpenList 实际产出 {out_name}（{out_size} bytes）"
            )
            return new_item
        except Exception as err:
            logger.warning(f"CAS视频扩展：用实际文件替换上传结果失败：{err}")
            return result

    def get_item_strict_with_cas(self, path: Any, *args, **kwargs):
        result = _orig_alist_get_item_strict(self, path, *args, **kwargs)
        if result is not None or not _upload_fallback_enabled or path is None:
            return result

        # 与 upload 一致的双条件：仅 OpenList 网盘 + .cas 路径才做兜底，
        # 本地严格查询、非 .cas 路径一律交回原生结果。
        path_str = str(path)
        if not path_str.lower().endswith(f".{DEFAULT_EXT}"):
            return result
        if not _is_openlist_storage(self):
            return result
        pending = _get_cas_pending(path)
        # 关键：只对本插件 upload 过并登记的 .cas 路径兜底。
        # 没有 pending 说明不是我们上传的（或尚未上传），立即返回，绝不轮询目录，
        # 否则宿主任何一次普通 .cas 严格查询都会触发 12 轮扫描造成刷屏。
        if not pending:
            return result

        # 优先复用 upload 阶段已查到的真实产出，不再重新列目录（避免重复查询/刷屏）
        real_name = pending.get("real_name")
        real_size = pending.get("real_size")
        if real_name and isinstance(real_size, int) and real_size > 0:
            size = real_size
            modify_time = pending.get("real_mtime")
            logger.debug(
                f"CAS视频扩展：{path_str} 核对复用 upload 阶段查到的产出 "
                f"{real_name}（{size}B），跳过重复目录查询"
            )
        else:
            # 罕见：pending 存在但无缓存产出，做一次非轮询查询（wait=False）
            output = _find_cas_output(
                self, path, pending.get("basename") or Path(path_str).stem,
                real_name=real_name, wait=False,
            )
            if output is None:
                return None
            size = getattr(output, "size", None) or pending.get("size")
            modify_time = getattr(output, "modify_time", None)
            real_name = getattr(output, "name", None) or real_name

        # 合成 .cas FileItem：size 必须与核对方的 expected 一致，否则身份核对失败
        from types import SimpleNamespace
        src = SimpleNamespace(
            type="file",
            size=size,
            modify_time=modify_time,
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
                    modify_time=modify_time,
                )
            except Exception as err:
                logger.warning(f"CAS视频扩展：严格查询兜底合成文件项失败：{err}")
                return None
        # 核对已收口，主动清理登记，避免同一路径后续整理读到旧 size
        _forget_cas_target(path)
        logger.debug(
            f"CAS视频扩展：{path_str} 核对通过（凭产出视频 {real_name} 合成文件项）"
        )
        return item

    Alist.upload = upload_with_cas_fallback
    if _orig_alist_get_item_strict is not None:
        Alist.get_item_strict = get_item_strict_with_cas
    _alist_upload_patched = True
    logger.debug(
        "CAS视频扩展：补丁已安装（Alist.upload"
        + (" + get_item_strict" if _orig_alist_get_item_strict is not None else "")
        + "）"
    )


class CasFileExtender(_PluginBase):
    # 插件元信息
    plugin_name = "CAS视频扩展"
    plugin_desc = "把 .cas 注入 MoviePilot 视频白名单，刷新整理链扩展名缓存；上传后以 OpenList 目录里能否查到 CAS 产出的真实视频作为上传成败的唯一判据（查到=成功并显示真实文件名/大小，查不到=失败），修复秒传后上传/核对误报。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.46"
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
        # 启动横幅（INFO）：让日志页能直接确认当前加载的版本与匹配策略，
        # 避免「已更新但仍是旧代码」难以判断。平时只此一条，不刷屏。
        logger.info(
            f"CAS视频扩展：v{self.plugin_version} 已加载（上传 .cas 到 OpenList 后，"
            "按目标文件名匹配网盘真实产出判定成败）"
        )
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
