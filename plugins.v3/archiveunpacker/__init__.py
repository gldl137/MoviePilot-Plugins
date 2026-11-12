"""
压缩包自动解压（ArchiveUnpacker）

监控指定目录（递归）中的压缩包，基于**文件变化事件**（与 MoviePilot 目录监控同源的
watchfiles 写法）发现新增/修改的压缩包时，自动将其解压到「同目录下的 completed 子目录」（自动创建）。

默认**扁平化解压**：压缩包内的文件直接提到 completed/ 根下，不保留压缩包内部文件夹层级；
可在设置里改为「保留目录结构」。

支持格式：
- zip / tar 系列：使用 Python 标准库，开箱即用；
- rar / 7z：调用系统命令 unrar / 7z（需宿主已安装，未安装则跳过并告警）。

设置项：
- 监控目录（多行，每行一个；留空则直接使用 MoviePilot 的目录监控路径）
- 压缩包扩展名（逗号分隔）
- 同名文件：跳过 / 覆盖
- 解压后原压缩包：保留 / 删除
- 解压结构：仅提取文件(扁平) / 保留目录结构
- 启动时扫描存量压缩包（默认关）

详情页展示解压记录（时间/压缩包/状态/条目数），记录持久化保存，最多保留 300 条。
"""
import bz2
import gzip
import lzma
import shutil
import subprocess
import threading
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.log import logger

try:  # 新版宿主
    from app.sdk.plugin.base import _PluginBase
except ImportError:
    try:
        from app.sdk.plugin import _PluginBase
    except ImportError:  # 兼容旧版宿主
        from app.plugins import _PluginBase  # type: ignore


__all__ = ["ArchiveUnpacker"]


DEFAULT_EXTS = "zip,rar,7z,tar,gz,tgz,bz2,xz"
TAR_EXTS = (".tar", ".tgz", ".tar.gz", ".tar.bz2", ".tar.xz")
MAX_RECORDS = 300


def _safe_join(dest: Path, name: str) -> Optional[Path]:
    """安全拼接路径，防止压缩包内 ../ 路径遍历逃逸出 dest。"""
    dest = dest.resolve()
    if not name:
        return None
    target = (dest / name).resolve()
    if target == dest:
        return None
    if dest in target.parents:
        return target
    return None


class ArchiveUnpacker(_PluginBase):
    plugin_name = "压缩包自动解压"
    plugin_desc = ("监控指定目录的压缩包（zip/rar/7z/tar 等），基于文件变化事件自动解压到同目录下的 completed 子目录；"
                   "默认扁平化（只提取文件、不保留压缩包内文件夹），也可选保留目录结构。"
                   "zip/tar 标准库开箱即用，rar/7z 需系统装 unrar/7z。")
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.11"
    plugin_author = "gldl137"
    plugin_author_url = "https://github.com/gldl137/MoviePilot-Plugins"
    plugin_config_prefix = "archiveunpacker_"
    plugin_order = 28
    auth_level = 1

    # 运行态
    _enabled: bool = False
    _monitor_dirs: List[str] = []
    _exts: set = set()
    _conflict: str = "skip"
    _keep_archive: str = "keep"
    _flatten: bool = True
    _scan_existing: bool = False
    _processed: Dict[str, Tuple[int, float]] = {}
    _processing: set = set()
    _history: List[dict] = []
    _wf_watch = None
    _wf_change = None
    _wf_filter = None
    _stop_event: Optional[threading.Event] = None

    # ---------------- 生命周期 ----------------
    def init_plugin(self, config: dict = None):
        cfg = config or {}
        self._enabled = bool(cfg.get("enabled", False))
        self._scan_existing = bool(cfg.get("scan_existing", False))
        self._monitor_dirs = [d.strip() for d in str(cfg.get("monitor_dirs", "")).splitlines() if d.strip()]
        exts = str(cfg.get("archive_exts", DEFAULT_EXTS)).split(",")
        self._exts = {("." + e.strip().lstrip(".").lower()) for e in exts if e.strip()}
        self._conflict = cfg.get("conflict", "skip")
        self._keep_archive = cfg.get("keep_archive", "keep")
        self._flatten = str(cfg.get("flatten", "flat")) == "flat"
        self._processed = {}
        self._processing = set()
        try:
            hist = self.get_data("unpack_history")
            self._history = hist if isinstance(hist, list) else []
        except Exception:
            self._history = []
        self._wf_watch = None
        self._wf_change = None
        self._wf_filter = None
        self._stop_event = threading.Event()

        if not self._enabled:
            return
        if not self._exts:
            logger.warning("压缩包自动解压：未配置任何压缩包扩展名，已跳过启动")
            return

        logger.debug(f"压缩包自动解压：启用，监控目录配置 {self._monitor_dirs or '（空，将使用 MoviePilot 监控目录）'}，"
                     f"格式 {sorted(self._exts)}，同名 {self._conflict}，原包 {self._keep_archive}，"
                     f"结构 {'扁平' if self._flatten else '保留目录'}")

        if self._scan_existing:
            threading.Thread(target=self._scan_existing_task, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()

    def stop_service(self):
        if self._stop_event:
            self._stop_event.set()

    # ---------------- 宿主要求 ----------------
    def get_state(self) -> Any:
        return {
            "enabled": self._enabled,
            "monitor_dirs": "\n".join(self._monitor_dirs),
            "archive_exts": ",".join(sorted(e.lstrip('.') for e in self._exts)),
            "conflict": self._conflict,
            "keep_archive": self._keep_archive,
            "flatten": "flat" if self._flatten else "keepdir",
            "scan_existing": self._scan_existing,
        }

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSwitch", "props": {"model": "scan_existing", "label": "启动时扫描存量"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [
                                {"component": "VTextarea", "props": {
                                    "model": "monitor_dirs",
                                    "label": "监控目录（每行一个；留空则直接使用 MoviePilot 的目录监控路径）",
                                    "rows": "4"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [
                                {"component": "VTextarea", "props": {
                                    "model": "archive_exts",
                                    "label": "压缩包扩展名（逗号分隔）",
                                    "rows": "2"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "conflict", "label": "同名文件",
                                    "items": [
                                        {"title": "跳过已有", "value": "skip"},
                                        {"title": "覆盖已有", "value": "overwrite"}]}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "keep_archive", "label": "解压后原压缩包",
                                    "items": [
                                        {"title": "保留", "value": "keep"},
                                        {"title": "删除", "value": "delete"}]}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "flatten", "label": "解压结构",
                                    "items": [
                                        {"title": "仅提取文件(扁平)", "value": "flat"},
                                        {"title": "保留目录结构", "value": "keepdir"}]}}]},
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "scan_existing": False,
            "monitor_dirs": "",
            "archive_exts": DEFAULT_EXTS,
            "conflict": "skip",
            "keep_archive": "keep",
            "flatten": "flat",
        }

    def get_page(self) -> Optional[List[dict]]:
        items = self._history if isinstance(getattr(self, "_history", None), list) else []
        head = ["时间", "压缩包", "状态", "条目", "说明"]
        rows = []
        for it in items:
            status = str(it.get("status", ""))
            color = "success" if status == "成功" else ("error" if status == "失败" else "grey")
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "props": {"width": "170px"}, "text": str(it.get("time", ""))},
                    {"component": "td", "text": str(it.get("archive", ""))},
                    {"component": "td", "props": {"width": "90px"}, "content": [
                        {"component": "VChip", "props": {"color": color, "size": "small"}, "text": status or "未知"}
                    ]},
                    {"component": "td", "props": {"width": "70px"}, "text": str(it.get("count", ""))},
                    {"component": "td", "text": str(it.get("message", ""))},
                ],
            })
        if not items:
            rows = [{
                "component": "tr",
                "content": [
                    {"component": "td", "props": {"colspan": 5, "class": "text-center"},
                     "text": "暂无解压记录（启用后解压成功/失败会记录在此）"}
                ],
            }]
        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined"},
                "content": [
                    {"component": "VCardTitle", "text": "解压记录"},
                    {"component": "VCardText",
                     "text": f"共 {len(items)} 条（最多保留 {MAX_RECORDS} 条，最新在上）。未开启「启动时扫描存量」时只统计启用后的解压。"},
                ],
            },
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mt-4"},
                "content": [
                    {"component": "VCardTitle", "text": "记录列表"},
                    {"component": "VCardText", "content": [
                        {
                            "component": "VTable",
                            "props": {"density": "compact", "hover": True,
                                      "fixed-header": True, "height": "600px"},
                            "content": [
                                {"component": "thead", "content": [
                                    {"component": "tr", "content": [
                                        {"component": "th", "text": h} for h in head
                                    ]}
                                ]},
                                {"component": "tbody", "content": rows},
                            ],
                        }
                    ]},
                ],
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    # ---------------- 解压记录 ----------------
    def _record(self, archive: Path, status: str, count: int, message: str = "") -> None:
        try:
            if not isinstance(getattr(self, "_history", None), list):
                self._history = []
            self._history.insert(0, {
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "archive": archive.name,
                "status": status,
                "count": count,
                "message": message,
            })
            self._history = self._history[:MAX_RECORDS]
            self.save_data("unpack_history", self._history)
        except Exception as err:
            logger.warning(f"压缩包自动解压：保存解压记录失败：{err}")

    # ---------------- 监控（watchfiles 事件驱动，与 MoviePilot 同款写法） ----------------
    def _monitor_loop(self) -> None:
        try:
            from watchfiles import Change, DefaultFilter, watch
        except ImportError:
            logger.error("压缩包自动解压：未安装 watchfiles，无法进行事件监控（请确认宿主环境）。")
            return
        self._wf_watch = watch
        self._wf_change = Change
        self._wf_filter = DefaultFilter

        user_dirs = []
        for d in self._monitor_dirs:
            p = Path(d)
            if p.exists() and p.is_dir():
                user_dirs.append(p)
            else:
                logger.warning(f"压缩包自动解压：监控目录不存在或非目录，已忽略：{d}")

        if user_dirs:
            watch_dirs = user_dirs
        else:
            mp_dirs = self._get_mp_dirs()
            watch_dirs = [Path(m) for m in mp_dirs if Path(m).exists()]
            if not watch_dirs:
                logger.warning("压缩包自动解压：未配置监控目录，且未找到 MoviePilot 目录监控配置，无法启动事件监控。"
                               "请在插件卡片填写监控目录，或在 MoviePilot 中配置目录监控。")
                return
            logger.debug(f"压缩包自动解压：未配置监控目录，使用 MoviePilot 目录监控路径：{watch_dirs}")

        logger.debug(f"压缩包自动解压：开始事件监控 {watch_dirs}")
        for p in watch_dirs:
            threading.Thread(target=self._watch_one, args=(p,), daemon=True).start()

    def _watch_one(self, path: Path) -> None:
        """按 MoviePilot LocalDirectoryWatcher 的方式监控单个目录（watch(str(path), ...)）。"""
        watch = self._wf_watch
        Change = self._wf_change
        watch_filter = self._wf_filter
        try:
            for changes in watch(
                str(path),
                watch_filter=watch_filter() if watch_filter else None,
                stop_event=self._stop_event,
                rust_timeout=1000,
                yield_on_timeout=True,
                recursive=True,
                ignore_permission_denied=True,
            ):
                if self._stop_event.is_set():
                    break
                for change, raw in changes:
                    if change in (Change.added, Change.modified):
                        self._maybe_handle(Path(raw))
        except Exception as err:
            logger.error(f"压缩包自动解压：监控 {path} 异常：{err}")

    @staticmethod
    def _get_mp_dirs() -> List[str]:
        """获取 MoviePilot 当前配置的目录监控路径（监控目录留空时使用）。"""
        dirs: List[str] = []
        try:
            from app.helper.directory import DirectoryHelper
        except ImportError:
            try:
                from app.utils.directory_helper import DirectoryHelper
            except ImportError:
                DirectoryHelper = None
        if DirectoryHelper is None:
            return dirs
        try:
            obj = DirectoryHelper()
            lst = obj.get_download_dirs() or []
        except Exception as err:
            logger.warning(f"压缩包自动解压：读取 MoviePilot 监控目录失败：{err}")
            return dirs
        for d in lst:
            p = getattr(d, "download_path", None) or getattr(d, "path", None)
            if p:
                dirs.append(str(p))
        return dirs

    def _scan_existing_task(self) -> None:
        logger.debug("压缩包自动解压：开始扫描存量压缩包")
        for base in self._monitor_dirs or self._get_mp_dirs():
            bp = Path(base)
            if not bp.exists():
                continue
            for p in bp.rglob("*"):
                if "completed" in p.parts:
                    continue
                self._maybe_handle(p)
        logger.debug("压缩包自动解压：存量扫描完成")

    def _maybe_handle(self, path: Path) -> None:
        if not path.is_file():
            return
        if path.suffix.lower() not in self._exts:
            return
        key = str(path)
        if key in self._processing:
            return  # 正在处理，避免重复起线程
        try:
            st = path.stat()
        except OSError:
            return
        prev = self._processed.get(key)
        if prev == (st.st_size, st.st_mtime):
            return  # 已处理且未变化
        self._processing.add(key)
        threading.Thread(target=self._run_process, args=(path, key), daemon=True).start()

    def _run_process(self, path: Path, key: str) -> None:
        try:
            self._process_archive(path)
        finally:
            self._processing.discard(key)

    # ---------------- 处理单个压缩包 ----------------
    def _process_archive(self, archive: Path) -> None:
        if not self._wait_stable(archive):
            self._record(archive, "跳过", 0, "文件未就绪或已消失")
            return
        try:
            st = archive.stat()
        except OSError:
            self._record(archive, "跳过", 0, "文件已消失")
            return
        key = str(archive)
        prev = self._processed.get(key)
        if prev == (st.st_size, st.st_mtime):
            return  # 已处理过且未变化

        dest = archive.parent / "completed"
        logger.info(f"压缩包自动解压：开始处理 {archive} -> {dest}")
        count = self._unpack(archive, dest, self._conflict)
        if count <= 0:
            logger.warning(f"压缩包自动解压：未解出文件（格式不支持/全部跳过/失败）：{archive}")
            self._record(archive, "失败", 0, "未解出文件（格式不支持/全部跳过/异常）")
            return

        logger.info(f"压缩包自动解压：已解出 {count} 项：{archive}")
        self._processed[key] = (st.st_size, st.st_mtime)
        self._record(archive, "成功", count)

        if self._keep_archive == "delete":
            try:
                archive.unlink()
                logger.info(f"压缩包自动解压：已删除原压缩包 {archive}")
            except OSError as err:
                logger.warning(f"压缩包自动解压：删除原压缩包失败 {archive}: {err}")

    @staticmethod
    def _wait_stable(path: Path, timeout: int = 120, interval: int = 2) -> bool:
        """等待文件大小稳定（下载/写入完成），避免解压半截文件。"""
        last = None
        waited = 0
        while waited < timeout:
            try:
                size = path.stat().st_size
            except OSError:
                return False
            if last is not None and size == last:
                return True
            last = size
            time.sleep(interval)
            waited += interval
        try:
            return path.stat().st_size == last
        except OSError:
            return False

    # ---------------- 解压 ----------------
    def _unpack(self, archive: Path, dest: Path, conflict: str) -> int:
        ext = archive.suffix.lower()
        name = archive.name.lower()
        try:
            if ext == ".zip":
                return self._unpack_zip(archive, dest, conflict)
            if ext == ".rar":
                return self._unpack_cmd(archive, dest, conflict, "rar")
            if ext == ".7z":
                return self._unpack_cmd(archive, dest, conflict, "7z")
            if name.endswith(TAR_EXTS):
                return self._unpack_tar(archive, dest, conflict)
            if ext in (".gz", ".bz2", ".xz"):
                return self._unpack_single(archive, dest, ext)
            logger.warning(f"压缩包自动解压：不支持的扩展名 {ext}：{archive}")
            return 0
        except Exception as err:
            logger.error(f"压缩包自动解压：解压异常 {archive}: {err}")
            return 0

    @staticmethod
    def _remove_target(target: Path) -> None:
        try:
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
        except OSError:
            pass

    def _member_name(self, arcname: str) -> str:
        """扁平化时取文件名（不含目录层级），否则保留原相对路径。"""
        return Path(arcname).name if self._flatten else arcname

    def _unpack_zip(self, archive: Path, dest: Path, conflict: str) -> int:
        dest.mkdir(parents=True, exist_ok=True)
        n = 0
        with zipfile.ZipFile(archive) as zf:
            for info in zf.infolist():
                arcname = info.filename
                if info.is_dir():
                    if not self._flatten:
                        target = _safe_join(dest, arcname)
                        if target:
                            target.mkdir(parents=True, exist_ok=True)
                    continue
                rel = self._member_name(arcname)
                if not rel:
                    continue
                target = _safe_join(dest, rel)
                if target is None:
                    logger.warning(f"压缩包自动解压：跳过危险路径 {arcname}")
                    continue
                if target.exists():
                    if conflict == "skip":
                        continue
                    self._remove_target(target)
                try:
                    data = zf.read(info)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
                except Exception as err:
                    logger.warning(f"压缩包自动解压：解压成员失败 {arcname}: {err}")
                    continue
                n += 1
        return n

    def _unpack_tar(self, archive: Path, dest: Path, conflict: str) -> int:
        import tarfile
        dest.mkdir(parents=True, exist_ok=True)
        n = 0
        with tarfile.open(archive) as tf:
            for m in tf.getmembers():
                arcname = m.name
                if m.isdir():
                    if not self._flatten:
                        target = _safe_join(dest, arcname)
                        if target:
                            target.mkdir(parents=True, exist_ok=True)
                    continue
                if not m.isfile():
                    continue  # 跳过符号链接等
                rel = self._member_name(arcname)
                if not rel:
                    continue
                target = _safe_join(dest, rel)
                if target is None:
                    logger.warning(f"压缩包自动解压：跳过危险路径 {arcname}")
                    continue
                if target.exists():
                    if conflict == "skip":
                        continue
                    self._remove_target(target)
                try:
                    f = tf.extractfile(m)
                    if f is None:
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(f.read())
                except Exception as err:
                    logger.warning(f"压缩包自动解压：解压成员失败 {arcname}: {err}")
                    continue
                n += 1
        return n

    @staticmethod
    def _unpack_single(archive: Path, dest: Path, ext: str) -> int:
        """处理纯单文件压缩（.gz/.bz2/.xz），解开为去掉后缀的同名文件。"""
        dest.mkdir(parents=True, exist_ok=True)
        out = dest / archive.stem
        opener = {".gz": gzip.open, ".bz2": bz2.open, ".xz": lzma.open}.get(ext)
        if opener is None:
            return 0
        try:
            with opener(archive, "rb") as fin, open(out, "wb") as fout:
                shutil.copyfileobj(fin, fout)
            return 1
        except Exception as err:
            logger.warning(f"压缩包自动解压：单文件解压失败 {archive}: {err}")
            return 0

    def _unpack_cmd(self, archive: Path, dest: Path, conflict: str, kind: str) -> int:
        dest.mkdir(parents=True, exist_ok=True)
        # 扁平化用 e（不保留路径），保留结构用 x（保留路径）
        verb = "e" if self._flatten else "x"
        if kind == "rar":
            bin_name = "unrar"
            if shutil.which(bin_name) is None:
                logger.warning(f"压缩包自动解压：未找到 {bin_name} 命令，跳过 rar：{archive}（请安装 unrar）")
                return 0
            oflag = "-o+" if conflict == "overwrite" else "-o-"
            # unrar 在 cwd 内解压；e 命令会扁平化输出所有文件到 cwd
            cmd = [bin_name, verb, "-y", oflag, str(archive)]
            cwd = str(dest)
        else:
            bin_name = "7z"
            if shutil.which(bin_name) is None:
                logger.warning(f"压缩包自动解压：未找到 {bin_name} 命令，跳过 7z：{archive}（请安装 p7zip）")
                return 0
            aflag = "-aoa" if conflict == "overwrite" else "-aos"
            cmd = [bin_name, verb, "-y", aflag, str(archive), f"-o{dest}"]
            cwd = None
        try:
            res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=3600)
            if res.returncode != 0:
                logger.warning(f"压缩包自动解压：命令解压失败 {archive}（{bin_name} 返回 {res.returncode}）：{res.stderr[:400]}")
                return 0
            return 1
        except Exception as err:
            logger.error(f"压缩包自动解压：执行 {bin_name} 失败 {archive}: {err}")
            return 0
