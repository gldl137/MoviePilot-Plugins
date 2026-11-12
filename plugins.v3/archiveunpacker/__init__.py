"""
压缩包自动解压（ArchiveUnpacker）

监控指定目录（递归）中的压缩包，发现新增/修改的压缩包时，自动将其解压到
「同目录下的 completed 子目录」（自动创建）。

支持格式：
- zip / tar 系列：使用 Python 标准库，开箱即用；
- rar / 7z：调用系统命令 unrar / 7z（需宿主已安装，未安装则跳过并告警）。

监控方式：
- 实时（watchfiles）：基于文件系统事件，最及时；若 watchfiles 在该路径/宿主上
  初始化失败（如某些挂载方式、中文路径组合报 No path was found），会自动降级为轮询。
- 轮询：周期性扫描目录，纯 Python，兼容性好，适合 watchfiles 不可用的环境。

设置项：
- 监控目录（多行，每行一个，递归监控）
- 监控方式：实时 / 轮询
- 轮询间隔（秒，仅轮询模式，默认 30）
- 压缩包扩展名（逗号分隔）
- 同名文件：跳过 / 覆盖
- 解压后原压缩包：保留 / 删除
- 启动时扫描存量压缩包（默认关）
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
    plugin_desc = ("监控指定目录的压缩包（zip/rar/7z/tar 等），自动解压到同目录下的 completed 子目录；"
                   "支持同名跳过/覆盖、解压后保留/删除原包。zip/tar 标准库开箱即用，rar/7z 需系统装 unrar/7z。"
                   "实时监控（watchfiles）失败会自动降级为轮询。")
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.1"
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
    _scan_existing: bool = False
    _mode: str = "watch"
    _poll_interval: int = 30
    _processed: Dict[str, Tuple[int, float]] = {}
    _processing: set = set()
    _poll_started: bool = False
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
        self._mode = cfg.get("mode", "watch")
        try:
            self._poll_interval = max(5, int(cfg.get("poll_interval", 30)))
        except (TypeError, ValueError):
            self._poll_interval = 30
        self._processed = {}
        self._processing = set()
        self._poll_started = False
        self._stop_event = threading.Event()

        if not self._enabled:
            return
        if not self._monitor_dirs:
            logger.warning("压缩包自动解压：未配置监控目录，已跳过启动")
            return
        if not self._exts:
            logger.warning("压缩包自动解压：未配置任何压缩包扩展名，已跳过启动")
            return

        logger.info(f"压缩包自动解压：启用，监控 {len(self._monitor_dirs)} 个目录，"
                    f"格式 {sorted(self._exts)}，同名 {self._conflict}，"
                    f"原包 {self._keep_archive}，模式 {self._mode}")

        if self._scan_existing:
            threading.Thread(target=self._scan_existing_task, daemon=True).start()

        if self._mode == "poll":
            self._ensure_poll()
        else:
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
            "scan_existing": self._scan_existing,
            "mode": self._mode,
            "poll_interval": self._poll_interval,
        }

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                                {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                                {"component": "VSwitch", "props": {"model": "scan_existing", "label": "启动时扫描存量"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "mode", "label": "监控方式",
                                    "items": [
                                        {"title": "实时(watchfiles，失败自动轮询)", "value": "watch"},
                                        {"title": "轮询(周期性扫描)", "value": "poll"}]}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12}, "content": [
                                {"component": "VTextarea", "props": {
                                    "model": "monitor_dirs",
                                    "label": "监控目录（每行一个，递归监控其下所有子目录）",
                                    "rows": "4"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                                {"component": "VTextarea", "props": {
                                    "model": "archive_exts",
                                    "label": "压缩包扩展名（逗号分隔）",
                                    "rows": "2"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                                {"component": "VTextField", "props": {
                                    "model": "poll_interval", "label": "轮询间隔(秒，仅轮询模式)", "type": "number"}}]},
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "conflict", "label": "同名文件",
                                    "items": [
                                        {"title": "跳过已有", "value": "skip"},
                                        {"title": "覆盖已有", "value": "overwrite"}]}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                                {"component": "VSelect", "props": {
                                    "model": "keep_archive", "label": "解压后原压缩包",
                                    "items": [
                                        {"title": "保留", "value": "keep"},
                                        {"title": "删除", "value": "delete"}]}}]},
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "scan_existing": False,
            "mode": "watch",
            "monitor_dirs": "",
            "archive_exts": DEFAULT_EXTS,
            "poll_interval": 30,
            "conflict": "skip",
            "keep_archive": "keep",
        }

    def get_page(self) -> Optional[List[dict]]:
        return None

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    # ---------------- 监控 ----------------
    def _monitor_loop(self) -> None:
        try:
            from watchfiles import Change, watch
        except ImportError:
            logger.warning("压缩包自动解压：未安装 watchfiles，自动降级为轮询模式")
            self._ensure_poll()
            return

        valid = []
        for d in self._monitor_dirs:
            try:
                rp = Path(d).resolve()
            except Exception:
                rp = Path(d)
            if rp.exists() and rp.is_dir():
                valid.append(rp)
            else:
                logger.warning(f"压缩包自动解压：监控目录不存在或非目录，已忽略：{d}")
        if not valid:
            logger.warning("压缩包自动解压：无可用监控目录，自动降级为轮询模式（仍会周期扫描）")
            self._ensure_poll()
            return

        logger.info(f"压缩包自动解压：开始实时监控 {valid}")
        for p in valid:
            threading.Thread(target=self._watch_one, args=(watch, Change, p), daemon=True).start()

    def _watch_one(self, watch, Change, path: Path) -> None:
        try:
            try:
                gen = watch([path], stop_event=self._stop_event, recursive=True)
            except TypeError:
                gen = watch([path], recursive=True)
            for changes in gen:
                if self._stop_event.is_set():
                    break
                for change, raw in changes:
                    if change in (Change.Added, Change.Modified):
                        self._maybe_handle(Path(raw))
        except RuntimeError as err:
            em = str(err)
            if "no path" in em.lower() or "not found" in em.lower():
                logger.error(f"压缩包自动解压：watchfiles 无法监控 {path}（该路径在 watchfiles 下可能不兼容，"
                             f"常见原因：特殊挂载方式或中文路径组合）。已自动降级为轮询模式继续工作；"
                             f"若希望稳定实时，请在设置里把「监控方式」直接改为「轮询」，"
                             f"或确认该路径为容器内真实存在的本地目录 / 改用其父目录。")
                self._ensure_poll()
            else:
                logger.error(f"压缩包自动解压：监控 {path} 异常：{err}")
        except Exception as err:
            logger.error(f"压缩包自动解压：监控 {path} 异常：{err}")

    def _ensure_poll(self) -> None:
        if self._poll_started:
            return
        self._poll_started = True
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def _poll_loop(self) -> None:
        logger.info(f"压缩包自动解压：轮询模式启动，间隔 {self._poll_interval}s，监控 {self._monitor_dirs}")
        while not (self._stop_event and self._stop_event.is_set()):
            for base in self._monitor_dirs:
                bp = Path(base)
                if not bp.exists():
                    continue
                for p in bp.rglob("*"):
                    if "completed" in p.parts:
                        continue
                    self._maybe_handle(p)
            if self._stop_event:
                self._stop_event.wait(self._poll_interval)
            else:
                time.sleep(self._poll_interval)

    def _scan_existing_task(self) -> None:
        logger.info("压缩包自动解压：开始扫描存量压缩包")
        for base in self._monitor_dirs:
            bp = Path(base)
            if not bp.exists():
                continue
            for p in bp.rglob("*"):
                if "completed" in p.parts:
                    continue
                self._maybe_handle(p)
        logger.info("压缩包自动解压：存量扫描完成")

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
        # 轮询模式下去重：已处理且 size/mtime 未变则跳过，避免每轮重复提交
        prev = self._processed.get(key)
        if prev == (st.st_size, st.st_mtime):
            return
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
            return
        try:
            st = archive.stat()
        except OSError:
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
            return

        logger.info(f"压缩包自动解压：已解出 {count} 项：{archive}")
        self._processed[key] = (st.st_size, st.st_mtime)

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

    def _unpack_zip(self, archive: Path, dest: Path, conflict: str) -> int:
        dest.mkdir(parents=True, exist_ok=True)
        n = 0
        with zipfile.ZipFile(archive) as zf:
            for info in zf.infolist():
                target = _safe_join(dest, info.filename)
                if target is None:
                    logger.warning(f"压缩包自动解压：跳过危险路径 {info.filename}")
                    continue
                if target.exists():
                    if conflict == "skip":
                        continue
                    self._remove_target(target)
                try:
                    if info.is_dir():
                        target.mkdir(parents=True, exist_ok=True)
                    else:
                        data = zf.read(info)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(data)
                except Exception as err:
                    logger.warning(f"压缩包自动解压：解压成员失败 {info.filename}: {err}")
                    continue
                n += 1
        return n

    def _unpack_tar(self, archive: Path, dest: Path, conflict: str) -> int:
        import tarfile
        dest.mkdir(parents=True, exist_ok=True)
        n = 0
        with tarfile.open(archive) as tf:
            for m in tf.getmembers():
                target = _safe_join(dest, m.name)
                if target is None:
                    logger.warning(f"压缩包自动解压：跳过危险路径 {m.name}")
                    continue
                if target.exists():
                    if conflict == "skip":
                        continue
                    self._remove_target(target)
                try:
                    if m.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                    elif m.isfile():
                        f = tf.extractfile(m)
                        if f is None:
                            continue
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(f.read())
                    else:
                        continue  # 跳过符号链接等
                except Exception as err:
                    logger.warning(f"压缩包自动解压：解压成员失败 {m.name}: {err}")
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

    @staticmethod
    def _unpack_cmd(archive: Path, dest: Path, conflict: str, kind: str) -> int:
        dest.mkdir(parents=True, exist_ok=True)
        if kind == "rar":
            bin_name = "unrar"
            if shutil.which(bin_name) is None:
                logger.warning(f"压缩包自动解压：未找到 {bin_name} 命令，跳过 rar：{archive}（请安装 unrar）")
                return 0
            oflag = "-o+" if conflict == "overwrite" else "-o-"
            cmd = [bin_name, "x", "-y", oflag, str(archive)]
            cwd = str(dest)
        else:
            bin_name = "7z"
            if shutil.which(bin_name) is None:
                logger.warning(f"压缩包自动解压：未找到 {bin_name} 命令，跳过 7z：{archive}（请安装 p7zip）")
                return 0
            aflag = "-aoa" if conflict == "overwrite" else "-aos"
            cmd = [bin_name, "x", "-y", aflag, str(archive), f"-o{dest}"]
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
