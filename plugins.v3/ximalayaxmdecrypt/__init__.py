"""
喜马拉雅 XM 音频解密插件（MoviePilot V3）

功能：
- 在插件「配置」页填写「输入目录」与「输出目录」；
- 在插件「详情」页点击「开始解密」，把输入目录下所有 `.xm` 加密音频批量解密为
  可直接播放的音频文件（mp3 / m4a / flac / wav / wma / aac）并写入输出目录；
- 详情页实时展示解密日志、成功/失败数量与运行状态。

解密算法与 `xm_encryptor.wasm` 运行时来自开源项目 Ximalaya-XM-Decrypt，
仅供学习交流使用，请勿用于商业用途。
"""

from __future__ import annotations

import base64
import io
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

from app.plugins import _PluginBase

try:  # MoviePilot V3 稳定日志入口
    from app.sdk.logging import logger
except Exception:  # pragma: no cover - 兼容旧宿主
    from app.log import logger


# 插件自身目录：wasm 运行时与插件源码位于同一目录
PLUGIN_DIR = Path(__file__).resolve().parent
WASM_FILE = PLUGIN_DIR / "xm_encryptor.wasm"

# 详情页日志面板保留的最大行数
MAX_LOG_LINES = 400

# 喜马拉雅 XM 解密使用的固定 AES-256 密钥
XM_AES_KEY = b"ximalayaximalayaximalayaximalaya"

# 插件中文名与简介：配置页、详情页、插件列表统一引用这里，避免各处写死不一致
PLUGIN_NAME = "喜马拉雅XM解密"
PLUGIN_DESC = (
    "把喜马拉雅下载的 .xm 加密音频批量解密为可播放的音频文件（输入目录 → 输出目录）。"
)

# 输出文件名中需要替换掉的非法字符
_INVALID_CHARS = ("/", "\\", ":", "*", "?", '"', "<", ">", "|")

# wasm 运行时后端（按优先级排列，二者之一即可）
_WASM_BACKENDS = ("wasmtime", "wasmer")

# 插件内置运行时 wheel 所在目录（MoviePilot 官方支持的本地 wheel 机制）
_VENDOR_WHEELS_DIRNAME = "wheels"

# 点击「开始解密」后，接口最多同步等待多少秒再返回。
# 详情页只在 API 调用完成时重绘一次，等待一下可以让常见批量在返回时已经跑完，
# 日志一次性完整呈现；超过该时间则返回"仍在运行"由用户手动刷新。
# 注意：前端请求层存在 15 秒级的异常判定逻辑，等太久会被前端判为"响应无效"
# （后台仍会跑完），所以这里刻意压在 8 秒以内。
_SYNC_WAIT_SECONDS = 8

# 输出目录已存在同名文件时的处理策略
_EXISTING_ACTIONS = {
    "skip": "跳过（已存在就跳过，重复点击不会产生副本）",
    "overwrite": "覆盖（重新解密并替换同名文件）",
}
_DEFAULT_EXISTING_ACTION = "skip"


# ---------------------------------------------------------------------------
# WASM 运行时（xm_encryptor.wasm）
# ---------------------------------------------------------------------------
class _WasmtimeRuntime:
    """基于 wasmtime 的运行时封装（支持 Python 3.12+）。"""

    def __init__(self, wasm_path: Path):
        import wasmtime

        engine = wasmtime.Engine()
        module = wasmtime.Module.from_file(engine, str(wasm_path))
        linker = wasmtime.Linker(engine)
        self._store = wasmtime.Store(engine)
        instance = linker.instantiate(self._store, module)
        self._exports = instance.exports(self._store)
        self._memory = self._exports["i"]

    def a(self, value: int) -> int:
        return self._exports["a"](self._store, value)

    def c(self, size: int) -> int:
        return self._exports["c"](self._store, size)

    def g(self, sp: int, d: int, dl: int, t: int, tl: int) -> int:
        return self._exports["g"](self._store, sp, d, dl, t, tl)

    def mem_write(self, offset: int, data: bytes) -> None:
        self._memory.write(self._store, bytes(data), offset)

    def read_i32(self, offset: int) -> int:
        raw = self._memory.read(self._store, offset, offset + 4)
        return int.from_bytes(raw, "little")

    def read_bytes(self, offset: int, length: int) -> bytes:
        return self._memory.read(self._store, offset, offset + length)


class _WasmerRuntime:
    """基于 wasmer 的运行时封装（老版本 Python 或已安装 wasmer 时使用）。"""

    def __init__(self, wasm_path: Path):
        from wasmer import Instance, Module, Store, engine
        from wasmer_compiler_cranelift import Compiler

        instance = Instance(
            Module(Store(engine.Universal(Compiler)), wasm_path.read_bytes())
        )
        self._exports = instance.exports
        self._memory = self._exports.i

    def a(self, value: int) -> int:
        return self._exports.a(value)

    def c(self, size: int) -> int:
        return self._exports.c(size)

    def g(self, sp: int, d: int, dl: int, t: int, tl: int) -> int:
        return self._exports.g(sp, d, dl, t, tl)

    def mem_write(self, offset: int, data: bytes) -> None:
        view = self._memory.uint8_view(offset)
        view[:] = bytes(data)

    def read_i32(self, offset: int) -> int:
        return int(self._memory.int32_view(offset // 4)[0])

    def read_bytes(self, offset: int, length: int) -> bytes:
        return bytearray(self._memory.buffer)[offset:offset + length]


def _load_runtime(wasm_path: Path) -> Tuple[Any, str]:
    """按 wasmtime -> wasmer 顺序加载 xm 解密运行时，返回 (运行时, 名称)。"""
    # 先确保运行时可用：宿主已安装的直接用，否则退回插件内置 wheel
    available, backend, _ = ensure_runtime_available()
    factories = [("wasmtime", _WasmtimeRuntime), ("wasmer", _WasmerRuntime)]
    if available:
        preferred = backend.split("（")[0]
        factories.sort(key=lambda item: item[0] != preferred)

    errors: List[str] = []
    for name, factory in factories:
        try:
            return factory(wasm_path), name
        except Exception as err:  # noqa: BLE001 - 逐个后端尝试并汇总原因
            errors.append(f"{name}: {err}")
    raise RuntimeError(
        "无法加载 xm_encryptor.wasm 运行时：宿主没有可用的 wasm 运行时，"
        f"插件内置 wheel 也未能生效。请确认插件目录 {_VENDOR_WHEELS_DIRNAME}/ 中存在"
        f"匹配当前平台的 wasmtime wheel，或手动执行：{_install_hint('wasmtime')}"
        f"；尝试详情：{' | '.join(errors)}"
    )


def _probe_module(module_name: str) -> Optional[str]:
    """尝试真正导入模块：可用返回 None，否则返回失败原因。

    用真实导入而不是 find_spec，是为了区分三种情况：
    - 完全没装（缺失模块 == 顶层包名）
    - 装成了不完整的包（缺失的是包内部的子模块，例如空壳 wheel）
    - 装了但原生库加载失败（架构不匹配、缺少系统库）
    这三种的处理方式并不一样，所以这里把缺失的模块名原样暴露出来。
    """
    import importlib

    try:
        importlib.import_module(module_name)
        return None
    except ModuleNotFoundError as err:
        missing = err.name or module_name
        if missing == module_name or module_name.startswith(f"{missing}."):
            return "未安装"
        return f"安装不完整（缺失子模块 {missing}）"
    except Exception as err:  # noqa: BLE001 - 原生库加载失败等
        return f"导入失败：{type(err).__name__}: {err}"


def _wheel_matches_platform(filename: str, sys_platform: str, machine: str) -> bool:
    """判断内置 wheel 是否匹配当前运行平台（只比平台标签，不比 Python 版本）。"""
    name = filename.lower()
    machine = (machine or "").lower()
    if sys_platform == "win32":
        if machine in ("amd64", "x86_64"):
            return "win_amd64" in name
        if machine in ("arm64", "aarch64"):
            return "win_arm64" in name
        return False
    if sys_platform == "darwin":
        if machine in ("arm64", "aarch64"):
            return "macosx" in name and "arm64" in name
        return "macosx" in name and "x86_64" in name
    if sys_platform.startswith("linux"):
        if "manylinux" not in name and "musllinux" not in name:
            return False
        if machine in ("aarch64", "arm64"):
            return "aarch64" in name
        if machine in ("x86_64", "amd64"):
            return "x86_64" in name
        return False
    return False


def _runtime_cache_dir() -> Path:
    """内置运行时的解包目录（优先宿主插件数据目录，避免写回插件源码目录）。"""
    import tempfile

    try:
        from app.sdk.config import settings

        base = Path(settings.PLUGIN_DATA_PATH) / "XimalayaXMDecrypt"
        base.mkdir(parents=True, exist_ok=True)
        return base / "runtime"
    except Exception:  # noqa: BLE001 - 宿主不可用时退回临时目录
        fallback = Path(tempfile.gettempdir()) / "ximalayaxmdecrypt_runtime"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _ensure_vendored_runtime(prepend: bool = False) -> Optional[str]:
    """把插件内置 wheel 解包并加入 sys.path，返回解包目录。

    目的是让插件自带 wasm 运行时：宿主环境装不上 wasmtime 时（镜像站给的轮子
    不对、依赖没真正落到当前解释器、容器无法联网……），依然能跑起来，
    全程不依赖 pip / uv / 网络。
    """
    import platform
    import sys
    import zipfile

    wheels_dir = PLUGIN_DIR / _VENDOR_WHEELS_DIRNAME
    if not wheels_dir.is_dir():
        return None

    candidates = [
        wheel
        for wheel in sorted(wheels_dir.glob("*.whl"))
        if _wheel_matches_platform(wheel.name, sys.platform, platform.machine())
    ]
    if not candidates:
        return None

    for wheel in candidates:
        target = _runtime_cache_dir() / wheel.stem
        marker = target / ".extracted"
        try:
            if not marker.exists():
                target.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(wheel) as archive:
                    archive.extractall(target)
                marker.write_text(wheel.name, encoding="utf-8")
        except Exception as err:  # noqa: BLE001 - 解包失败则尝试下一个候选
            logger.warning(f"[XM解密] 解包内置运行时失败 {wheel.name}：{err}")
            continue
        path = str(target)
        if path not in sys.path:
            if prepend:
                sys.path.insert(0, path)
            else:
                sys.path.append(path)
        return path
    return None


def _resolve_backend() -> Tuple[bool, str, List[str]]:
    """探测可用的 wasm 运行时后端，返回 (是否可用, 后端名, 失败原因)。"""
    errors: List[str] = []
    for name in _WASM_BACKENDS:
        reason = _probe_module(name)
        if reason is None:
            return True, name, errors
        errors.append(f"{name} {reason}")
    return False, "", errors


def ensure_runtime_available() -> Tuple[bool, str, List[str]]:
    """确保 wasm 运行时可用：先试宿主已安装的，再退回插件内置 wheel。"""
    import importlib

    ok, name, errors = _resolve_backend()
    if ok:
        return True, name, errors

    if _ensure_vendored_runtime(prepend=True) is None:
        return False, "", errors

    importlib.invalidate_caches()
    ok, name, vendored_errors = _resolve_backend()
    if ok:
        return True, f"{name}（插件内置）", []
    return False, "", errors + vendored_errors


def _install_hint(package: str) -> str:
    """生成在「MoviePilot 当前解释器」中安装指定依赖的可执行命令。"""
    import sys

    return f"{sys.executable or 'python'} -m pip install {package}"


def environment_summary() -> str:
    """返回当前运行解释器信息，用于排查依赖被装到了别的 Python 环境。"""
    import sys

    return f"Python {sys.version.split()[0]} @ {sys.executable or '未知'}"


def check_dependencies() -> Tuple[List[str], List[str]]:
    """检查解密所需依赖，返回 (缺失依赖名, 处理建议)。"""
    missing: List[str] = []
    tips: List[str] = []

    backend_ok, _backend_name, backend_errors = ensure_runtime_available()
    if not backend_ok:
        missing.append("wasmtime")
        tips.append(
            "wasm 运行时不可用（"
            + "；".join(backend_errors)
            + f"），且插件目录 {_VENDOR_WHEELS_DIRNAME}/ 里没有匹配当前平台的 wheel，"
            + f"请执行：{_install_hint('wasmtime')}"
        )

    if _probe_module("mutagen") is not None:
        missing.append("mutagen")
        tips.append(f"缺少 mutagen，请执行：{_install_hint('mutagen')}")
    if _probe_module("Crypto") is not None:
        missing.append("pycryptodome")
        tips.append(f"缺少 pycryptodome，请执行：{_install_hint('pycryptodome')}")
    return missing, tips


# ---------------------------------------------------------------------------
# XM 解密核心逻辑
# ---------------------------------------------------------------------------
class _XMInfo:
    """从 XM 文件 ID3 标签中解析出的解密所需信息。"""

    __slots__ = (
        "title", "artist", "album", "tracknumber",
        "size", "header_size", "isrc", "encoded_by", "encoding_technology",
    )

    def __init__(self) -> None:
        self.title = ""
        self.artist = ""
        self.album = ""
        self.tracknumber = 0
        self.size = 0
        self.header_size = 0
        self.isrc = ""
        self.encoded_by = ""
        self.encoding_technology = ""

    def iv(self) -> bytes:
        """AES 初始向量：优先 TSRC(ISRC)，否则回退 TENC(编码者)。"""
        if self.isrc:
            return bytes.fromhex(self.isrc)
        if self.encoded_by:
            return bytes.fromhex(self.encoded_by)
        raise ValueError("ID3 标签缺少 TSRC/TENC，无法获取解密初始向量")


def _frame_text(id3: Any, key: str) -> str:
    """安全读取 ID3 文本帧，缺失时返回空字符串。"""
    frame = id3.get(key)
    return "" if frame is None else str(frame).strip()


def _read_xm_info(raw: bytes) -> _XMInfo:
    """解析 XM 文件头部的 ID3 标签。"""
    from mutagen.id3 import ID3

    id3 = ID3(io.BytesIO(raw), v2_version=3)
    info = _XMInfo()
    info.title = _frame_text(id3, "TIT2")
    info.album = _frame_text(id3, "TALB")
    info.artist = _frame_text(id3, "TPE1")
    info.isrc = _frame_text(id3, "TSRC")
    info.encoded_by = _frame_text(id3, "TENC")
    info.encoding_technology = _frame_text(id3, "TSSE")
    info.header_size = int(id3.size)

    track = _frame_text(id3, "TRCK")
    size = _frame_text(id3, "TSIZ")
    if not track or not size:
        raise ValueError("ID3 标签缺少 TRCK/TSIZ，不是有效的喜马拉雅 XM 文件")
    info.tracknumber = int(str(track).split("/")[0])
    info.size = int(size)
    return info


def _printable_count(data: bytes) -> int:
    """返回数据开头连续可打印 ASCII 字节的数量。"""
    for index, char in enumerate(data):
        if char < 0x20 or char > 0x7E:
            return index
    return len(data)


def _xm_decrypt(raw: bytes, runtime: Any) -> Tuple[_XMInfo, bytes]:
    """执行三阶段解密，返回 (元信息, 解密后的音频数据)。"""
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad

    info = _read_xm_info(raw)
    encrypted = raw[info.header_size:info.header_size + info.size]

    # 阶段 1：AES-256-CBC
    cipher = AES.new(XM_AES_KEY, AES.MODE_CBC, info.iv())
    decrypted = cipher.decrypt(pad(encrypted, 16))

    # 阶段 2：xmDecrypt（wasm 运行时）
    decrypted = decrypted[:_printable_count(decrypted)]
    track_id = str(info.tracknumber).encode()
    stack_pointer = runtime.a(-16)
    data_offset = runtime.c(len(decrypted))
    track_offset = runtime.c(len(track_id))
    runtime.mem_write(data_offset, decrypted)
    runtime.mem_write(track_offset, track_id)
    runtime.g(stack_pointer, data_offset, len(decrypted), track_offset, len(track_id))
    result_pointer = runtime.read_i32(stack_pointer)
    result_length = runtime.read_i32(stack_pointer + 4)
    status0 = runtime.read_i32(stack_pointer + 8)
    status1 = runtime.read_i32(stack_pointer + 12)
    if status0 != 0 or status1 != 0:
        raise RuntimeError(f"xmDecrypt 执行失败（status={status0},{status1}）")
    result_data = bytes(runtime.read_bytes(result_pointer, result_length)).decode()

    # 阶段 3：拼接 base64 片段并接回原始尾部数据
    audio = base64.b64decode(info.encoding_technology + result_data)
    audio += raw[info.header_size + info.size:]
    return info, audio


def _detect_extension(data: bytes) -> str:
    """根据音频文件头识别容器格式，返回扩展名（不依赖 libmagic）。"""
    if len(data) < 12:
        return "mp3"
    if data[:3] == b"ID3":
        return "mp3"
    if data[:4] == b"fLaC":
        return "flac"
    if data[:4] == b"RIFF" and data[8:12] == b"WAVE":
        return "wav"
    if data[:4] == b"OggS":
        return "ogg"
    if data[4:8] == b"ftyp":
        return "m4a"
    if data[:4] == b"\x30\x26\xb2\x75":  # ASF/WMA 头部 GUID
        return "wma"
    if data[0] == 0xFF:
        second = data[1]
        if (second & 0xF6) == 0xF0:  # ADTS AAC
            return "aac"
        if (second & 0xE0) == 0xE0:  # MPEG 音频帧同步
            return "mp3"
    return "mp3"


def _sanitize_name(name: str) -> str:
    """替换文件名中的非法字符。"""
    for char in _INVALID_CHARS:
        name = name.replace(char, " ")
    return name.strip().rstrip(".")


def _write_tags(audio: bytes, info: _XMInfo) -> bytes:
    """把解密出的元信息写回音频标签，失败时返回原始音频数据。"""
    try:
        import mutagen

        buffer = io.BytesIO(audio)
        tags = mutagen.File(buffer, easy=True)
        if tags is None:
            return audio
        tags["title"] = info.title
        tags["album"] = info.album
        tags["artist"] = info.artist
        if info.tracknumber:
            tags["tracknumber"] = str(info.tracknumber)
        buffer.seek(0)
        tags.save(buffer)
        buffer.seek(0)
        return buffer.read()
    except Exception as err:  # noqa: BLE001 - 写标签失败不应导致解密结果丢失
        logger.warning(f"[XM解密] 写入音频标签失败，已保留原始音频数据：{err}")
        return audio


def _decrypt_xm_file(
    src: Path,
    out_dir: Path,
    keep_album: bool,
    runtime: Any,
    existing_action: str = _DEFAULT_EXISTING_ACTION,
) -> Tuple[Path, bool]:
    """解密单个 .xm 文件并写入输出目录，返回 (目标路径, 是否跳过)。"""
    with open(src, "rb") as file:
        raw = file.read()
    info, audio = _xm_decrypt(raw, runtime)
    extension = _detect_extension(audio[:255])

    title = _sanitize_name(info.title) or _sanitize_name(src.stem) or "unknown"
    album = _sanitize_name(info.album)
    target_dir = out_dir / album if (keep_album and album) else out_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    target = target_dir / f"{title}.{extension}"
    if existing_action == "skip" and target.exists():
        return target, True

    with open(target, "wb") as file:
        file.write(_write_tags(audio, info))
    return target, False


# ---------------------------------------------------------------------------
# 插件主类
# ---------------------------------------------------------------------------
class XimalayaXMDecrypt(_PluginBase):
    """喜马拉雅 XM 批量解密插件。"""

    # ---- 插件元信息 ----
    plugin_name = PLUGIN_NAME
    plugin_desc = PLUGIN_DESC
    plugin_icon = "music.png"
    plugin_version = "1.0.0"
    plugin_author = "Diaoxiaozhang"
    author_url = "https://github.com/Diaoxiaozhang/Ximalaya-XM-Decrypt"
    plugin_config_prefix = "ximalayaxmdecrypt_"
    plugin_order = 30
    auth_level = 1

    # ---- 运行状态 ----
    _enabled = False
    _input_dir = ""
    _output_dir = ""
    _recursive = True
    _keep_album = True
    _delete_source = False
    _existing_action = _DEFAULT_EXISTING_ACTION

    _running = False
    _stop_requested = False

    def __init__(self) -> None:
        super().__init__()
        self._logs: Deque[str] = deque(maxlen=MAX_LOG_LINES)
        self._log_lock = threading.Lock()
        self._runtime_lock = threading.Lock()
        self._runtime: Optional[Any] = None
        self._runtime_name = ""
        self._thread: Optional[threading.Thread] = None
        self._last_summary = ""

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def init_plugin(self, config: Optional[dict] = None) -> None:
        """读取配置。允许重复调用。"""
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._input_dir = str(config.get("input_dir") or "").strip()
        self._output_dir = str(config.get("output_dir") or "").strip()
        self._recursive = bool(config.get("recursive", True))
        self._keep_album = bool(config.get("keep_album", True))
        self._delete_source = bool(config.get("delete_source", False))
        action = str(config.get("existing_action") or _DEFAULT_EXISTING_ACTION)
        self._existing_action = action if action in _EXISTING_ACTIONS else _DEFAULT_EXISTING_ACTION
        logger.info(
            f"[XM解密] 插件初始化完成：enabled={self._enabled}, "
            f"输入目录={self._input_dir or '未设置'}, 输出目录={self._output_dir or '未设置'}"
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def stop_service(self) -> None:
        """停止正在运行的解密任务并释放资源。"""
        self._stop_requested = True
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning("[XM解密] 解密线程未在 5 秒内退出，将在当前文件处理完后停止")
        self._thread = None
        # 线程仍在运行时保持运行标记，避免重复触发时启动第二个任务
        self._running = bool(thread and thread.is_alive())
        self._stop_requested = False

    # ------------------------------------------------------------------
    # 配置页
    # ------------------------------------------------------------------
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """插件配置页：启用开关 + 输入/输出目录等。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "density": "compact",
                            "class": "mb-3",
                            "text": f"{PLUGIN_NAME}：{PLUGIN_DESC}",
                        },
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "recursive",
                                            "label": "递归子目录",
                                            "hint": "包含输入目录下所有子目录里的 .xm 文件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "keep_album",
                                            "label": "按专辑分文件夹",
                                            "hint": "输出时按专辑名建立子文件夹（与原工具一致）",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "delete_source",
                                            "label": "解密后删除源文件",
                                            "hint": "危险：解密成功后才会删除对应 .xm 文件",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "input_dir",
                                            "label": "输入目录",
                                            "placeholder": "/mnt/media/xm",
                                            "hint": "存放喜马拉雅 .xm 加密文件的目录",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "output_dir",
                                            "label": "输出目录",
                                            "placeholder": "/mnt/media/music",
                                            "hint": "解密后的音频保存目录，留空则默认使用「输入目录/output」",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "existing_action",
                                            "label": "同名文件处理",
                                            "items": [
                                                {"title": label, "value": value}
                                                for value, label in _EXISTING_ACTIONS.items()
                                            ],
                                            "hint": "重复点击「开始解密」时的行为，默认跳过",
                                        },
                                    }
                                ],
                            },
                        ],
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
                                            "density": "compact",
                                            "class": "mt-2",
                                            "text": (
                                                "填写好目录并保存后，切换到插件「详情」页，"
                                                "点击「开始解密」即可批量转换，日志会实时显示在详情页。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": self._enabled,
            "input_dir": self._input_dir,
            "output_dir": self._output_dir,
            "recursive": self._recursive,
            "keep_album": self._keep_album,
            "delete_source": self._delete_source,
            "existing_action": self._existing_action,
        }

    # ------------------------------------------------------------------
    # 详情页
    # ------------------------------------------------------------------
    def get_page(self) -> List[dict]:
        """插件详情页：执行按钮 + 状态 + 日志面板。"""
        plugin_prefix = self.__class__.__name__
        output_dir = self._output_dir or (
            f"{self._input_dir}/output" if self._input_dir else "未设置"
        )
        missing_deps, dep_tips = check_dependencies()
        with self._log_lock:
            log_lines = list(self._logs)

        if log_lines:
            log_content = [
                {"component": "div", "text": line} for line in log_lines
            ]
        else:
            log_content = [
                {
                    "component": "div",
                    "props": {"class": "text-medium-emphasis"},
                    "text": "暂无日志，点击「开始解密」后这里会显示实时进度。",
                }
            ]

        return [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"variant": "flat"},
                                "content": [
                                    {
                                        "component": "VCardTitle",
                                        "props": {"class": "text-h6"},
                                        "text": self.plugin_name,
                                    },
                                    {
                                        "component": "VCardSubtitle",
                                        "props": {"class": "text-caption text-medium-emphasis"},
                                        "text": (
                                            f"v{self.plugin_version}"
                                            f"　作者：{self.plugin_author}"
                                            f"　{self.plugin_desc}"
                                        ),
                                    },
                                    {
                                        "component": "VCardText",
                                        "content": [
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": "success" if self._enabled else "warning",
                                                    "variant": "tonal",
                                                    "density": "compact",
                                                    "class": "mb-3",
                                                    "text": (
                                                        f"插件状态：{'已启用' if self._enabled else '未启用'}"
                                                        f"（请先在配置页启用并设置目录）；"
                                                        f"任务状态：{'正在解密…' if self._running else '空闲'}"
                                                    ),
                                                },
                                            },
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": "error" if missing_deps else "success",
                                                    "variant": "tonal",
                                                    "density": "compact",
                                                    "class": "mb-3",
                                                    "text": (
                                                        (
                                                            "依赖检查：缺失 "
                                                            + "、".join(missing_deps)
                                                            + "。"
                                                            + " ".join(dep_tips)
                                                            + f"（MoviePilot 当前运行环境：{environment_summary()}）"
                                                        )
                                                        if missing_deps
                                                        else "依赖检查：wasm 运行时与 mutagen、pycryptodome 均已就绪"
                                                    ),
                                                },
                                            },
                                            {
                                                "component": "div",
                                                "props": {"class": "text-subtitle-2 mb-1"},
                                                "text": "配置概览",
                                            },
                                            {
                                                "component": "div",
                                                "props": {"class": "text-body-2 mb-3"},
                                                "content": [
                                                    {"component": "div", "text": f"输入目录：{self._input_dir or '未设置'}"},
                                                    {"component": "div", "text": f"输出目录：{output_dir}"},
                                                    {"component": "div", "text": f"递归子目录：{'是' if self._recursive else '否'}；按专辑分文件夹：{'是' if self._keep_album else '否'}；解密后删除源文件：{'是' if self._delete_source else '否'}"},
                                                    {"component": "div", "text": f"同名文件处理：{_EXISTING_ACTIONS.get(self._existing_action, self._existing_action)}"},
                                                ],
                                            },
                                            {
                                                "component": "VRow",
                                                "content": [
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "md": 3},
                                                        "content": [
                                                            {
                                                                "component": "VBtn",
                                                                "props": {
                                                                    "color": "primary",
                                                                    "variant": "elevated",
                                                                    "block": True,
                                                                    "prepend-icon": "mdi-play",
                                                                },
                                                                "text": "开始解密",
                                                                "events": {
                                                                    "click": {
                                                                        "type": "request",
                                                                        "api": f"plugin/{plugin_prefix}/start",
                                                                        "method": "POST",
                                                                        "success": "已执行，详见下方日志",
                                                                        "fail": "解密任务启动失败",
                                                                    }
                                                                },
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "md": 3},
                                                        "content": [
                                                            {
                                                                "component": "VBtn",
                                                                "props": {
                                                                    "color": "info",
                                                                    "variant": "tonal",
                                                                    "block": True,
                                                                    "prepend-icon": "mdi-refresh",
                                                                },
                                                                "text": "刷新日志",
                                                                "events": {
                                                                    "click": {
                                                                        "type": "request",
                                                                        "api": f"plugin/{plugin_prefix}/status",
                                                                        "method": "GET",
                                                                    }
                                                                },
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "md": 3},
                                                        "content": [
                                                            {
                                                                "component": "VBtn",
                                                                "props": {
                                                                    "color": "secondary",
                                                                    "variant": "tonal",
                                                                    "block": True,
                                                                    "prepend-icon": "mdi-stethoscope",
                                                                },
                                                                "text": "环境自检",
                                                                "events": {
                                                                    "click": {
                                                                        "type": "request",
                                                                        "api": f"plugin/{plugin_prefix}/diagnose",
                                                                        "method": "POST",
                                                                    }
                                                                },
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "md": 3},
                                                        "content": [
                                                            {
                                                                "component": "VBtn",
                                                                "props": {
                                                                    "color": "error",
                                                                    "variant": "text",
                                                                    "block": True,
                                                                    "prepend-icon": "mdi-delete-sweep",
                                                                },
                                                                "text": "清空日志",
                                                                "events": {
                                                                    "click": {
                                                                        "type": "request",
                                                                        "api": f"plugin/{plugin_prefix}/clear_logs",
                                                                        "method": "POST",
                                                                    }
                                                                },
                                                            }
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"variant": "flat", "class": "mt-4"},
                                "content": [
                                    {
                                        "component": "VCardTitle",
                                        "props": {"class": "text-subtitle-1"},
                                        "text": "运行日志",
                                    },
                                    {
                                        "component": "VCardText",
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {"class": "text-caption text-medium-emphasis mb-2"},
                                                "text": (
                                                    f"最近一次：{self._last_summary or '尚未执行'}"
                                                    f"；「开始解密」会最多等待 {_SYNC_WAIT_SECONDS} 秒再刷新页面，"
                                                    "批量较大时可随时点「刷新日志」看最新进度。"
                                                ),
                                            },
                                            {
                                                "component": "div",
                                                "props": {"class": "text-body-2"},
                                                "content": log_content,
                                            }
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                ],
            }
        ]

    # ------------------------------------------------------------------
    # 插件 API（详情页按钮调用）
    # ------------------------------------------------------------------
    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/start",
                "endpoint": self.api_start,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "开始解密",
                "description": "把输入目录下的 .xm 文件批量解密到输出目录",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "查询状态",
                "description": "返回当前运行状态与日志（用于刷新详情页）",
            },
            {
                "path": "/diagnose",
                "endpoint": self.api_diagnose,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "环境自检",
                "description": "把 Python 运行环境、依赖注册与导入结果写入日志面板",
            },
            {
                "path": "/clear_logs",
                "endpoint": self.api_clear_logs,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "清空日志",
                "description": "清空详情页的日志面板",
            },
        ]

    def api_start(self) -> Dict[str, Any]:
        """启动一次批量解密任务（后台线程执行，不阻塞页面）。"""
        if self._running:
            return self._result(False, "解密任务正在进行中，请等待当前任务结束")
        problem = self._validate()
        if problem:
            self._append_log(f"[错误] {problem}")
            return self._result(False, problem)

        self._stop_requested = False
        self._running = True
        self._last_summary = ""
        try:
            self._thread = threading.Thread(
                target=self._worker, name="ximalayaxmdecrypt", daemon=True
            )
            self._thread.start()
        except Exception as err:  # noqa: BLE001 - 线程创建失败时回滚运行状态
            self._running = False
            self._append_log(f"[错误] 启动解密线程失败：{err}")
            return self._result(False, f"启动解密线程失败：{err}")

        # 详情页只在接口返回时重绘一次，这里同步等一小会儿：
        # 批量不大时任务会在等待窗口内跑完，页面刷新后即可看到完整日志。
        thread = self._thread
        if thread:
            thread.join(timeout=_SYNC_WAIT_SECONDS)

        if self._running:
            return self._result(
                True,
                f"任务仍在运行中（已等待 {_SYNC_WAIT_SECONDS} 秒），"
                "请点「刷新日志」查看最新进度",
            )
        return self._result(
            True, self._last_summary or "解密任务已完成，详见下方日志"
        )

    def api_status(self) -> Dict[str, Any]:
        """返回运行状态与日志，详情页在调用后会自动刷新。"""
        if self._running:
            return self._result(True, "正在解密…")
        return self._result(True, self._last_summary or "空闲")

    def api_clear_logs(self) -> Dict[str, Any]:
        with self._log_lock:
            self._logs.clear()
        return self._result(True, "日志已清空")

    def api_diagnose(self) -> Dict[str, Any]:
        """把运行环境与依赖安装情况写入日志面板，便于远程排查。

        重点回答三个问题：MoviePilot 到底用哪个解释器、依赖发行版是否已注册、
        真实导入失败是什么原因（区分「没装」和「装了但装不完整」）。
        """
        import importlib
        import importlib.metadata as metadata
        import os
        import shutil
        import site
        import sys

        self._append_log("===== 环境自检开始 =====")
        try:
            self._append_log(f"Python：{sys.version}")
            self._append_log(f"解释器：{sys.executable}")
            self._append_log(
                f"环境前缀：{sys.prefix}（base={getattr(sys, 'base_prefix', '?')}）"
            )
            self._append_log("sys.path：" + " | ".join(path for path in sys.path if path))

            site_packages: List[str] = []
            try:
                site_packages = list(site.getsitepackages() or [])
            except Exception as err:  # noqa: BLE001
                self._append_log(f"读取 site-packages 失败：{err}")
            self._append_log("site-packages：" + (" | ".join(site_packages) or "未知"))

            # 发行版元数据：能看出"装过但被清理"或"装到别处"的痕迹
            registered: List[str] = []
            for dist in metadata.distributions():
                name = dist.metadata.get("Name") or ""
                if "wasm" not in name.lower():
                    continue
                location = getattr(dist, "_path", None) or "?"
                registered.append(f"{name}=={dist.metadata.get('Version')} @ {location}")
            self._append_log(
                "已注册的 wasm 相关发行版：" + ("；".join(registered) if registered else "无")
            )

            # 先刷新导入缓存再重试，排除缓存导致的假阴性
            try:
                importlib.reload(site)
            except Exception as err:  # noqa: BLE001
                self._append_log(f"reload(site) 失败：{err}")
            importlib.invalidate_caches()

            for name in _WASM_BACKENDS:
                try:
                    module = importlib.import_module(name)
                    self._append_log(
                        f"import {name} 成功：{getattr(module, '__file__', '?')}"
                        f"（版本 {getattr(module, '__version__', '未知')}）"
                    )
                except Exception as err:  # noqa: BLE001
                    self._append_log(f"import {name} 失败：{type(err).__name__}: {err}")

            for base in site_packages:
                try:
                    entries = sorted(
                        item for item in os.listdir(base) if "wasm" in item.lower()
                    )
                except OSError:
                    continue
                self._append_log(
                    f"{base} 下的 wasm* 条目：" + (", ".join(entries) if entries else "无")
                )

            self._append_log(f"uv 可执行文件：{shutil.which('uv') or '未找到'}")
        except Exception as err:  # noqa: BLE001 - 自检本身不应抛出
            self._append_log(f"环境自检异常：{type(err).__name__}: {err}")
        self._append_log("===== 环境自检结束 =====")
        return self._result(True, "环境自检完成，请查看日志面板")

    # ------------------------------------------------------------------
    # 内部实现
    # ------------------------------------------------------------------
    def _result(self, success: bool, message: str) -> Dict[str, Any]:
        """按宿主统一三段式 envelope 返回，保证前端能一致解析。

        这里刻意不返回日志内容：详情页刷新时会由 get_page 直接渲染完整日志，
        接口只需要回报执行结果，避免返回体又大又多余。
        """
        return {
            "success": success,
            "message": message,
            "data": {
                "running": self._running,
                "summary": self._last_summary,
            },
        }

    def _append_log(self, message: str) -> None:
        """写入日志面板（同时在宿主日志中留痕）。"""
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        with self._log_lock:
            self._logs.append(line)
        logger.info(f"[XM解密] {message}")

    def _validate(self) -> Optional[str]:
        """校验配置，返回错误描述；配置正常返回 None。"""
        if not self._enabled:
            return "插件未启用，请先在插件配置页开启「启用插件」并保存"
        if not self._input_dir:
            return "未配置输入目录，请先在插件配置页填写"
        input_dir = Path(self._input_dir)
        if not input_dir.exists():
            return f"输入目录不存在：{self._input_dir}"
        if not input_dir.is_dir():
            return f"输入目录不是文件夹：{self._input_dir}"
        if not WASM_FILE.exists():
            return f"缺少解密运行时文件：{WASM_FILE}"
        missing_deps, dep_tips = check_dependencies()
        if missing_deps:
            return (
                "缺少依赖 " + "、".join(missing_deps) + "。" + dep_tips[0]
                + f"（MoviePilot 当前运行环境：{environment_summary()}）"
            )
        return None

    def _get_runtime(self) -> Any:
        """懒加载并复用 wasm 运行时（实例化开销较大）。"""
        with self._runtime_lock:
            if self._runtime is None:
                if not WASM_FILE.exists():
                    raise RuntimeError(f"缺少解密运行时文件：{WASM_FILE}")
                self._append_log("正在加载 xm_encryptor.wasm 运行时…")
                runtime, name = _load_runtime(WASM_FILE)
                self._runtime = runtime
                self._runtime_name = name
                self._append_log(f"运行时加载完成（{name}）")
            return self._runtime

    def _worker(self) -> None:
        """后台线程：遍历输入目录并逐个解密。"""
        start_time = time.time()
        ok = fail = skipped = 0
        try:
            runtime = self._get_runtime()
            input_dir = Path(self._input_dir)
            output_dir = Path(self._output_dir) if self._output_dir else input_dir / "output"
            output_dir.mkdir(parents=True, exist_ok=True)

            pattern = "**/*.xm" if self._recursive else "*.xm"
            files = sorted(path for path in input_dir.glob(pattern) if path.is_file())

            self._append_log(f"输入目录：{input_dir}")
            self._append_log(f"输出目录：{output_dir}")
            self._append_log(
                "同名文件处理："
                + _EXISTING_ACTIONS.get(self._existing_action, self._existing_action)
            )
            self._append_log(f"共找到 {len(files)} 个 .xm 文件")
            if not files:
                self._append_log("没有需要处理的文件，任务结束")
                return

            total = len(files)
            for index, file in enumerate(files, 1):
                if self._stop_requested:
                    self._append_log("收到停止请求，任务提前结束")
                    break
                try:
                    target, is_skipped = _decrypt_xm_file(
                        file,
                        output_dir,
                        self._keep_album,
                        runtime,
                        self._existing_action,
                    )
                except Exception as err:  # noqa: BLE001 - 单个文件失败不中断整批任务
                    fail += 1
                    self._append_log(f"[{index}/{total}] 失败：{file.name} —— {err}")
                    logger.error(f"[XM解密] 解密失败 {file}: {err}", exc_info=True)
                    continue

                if is_skipped:
                    skipped += 1
                    self._append_log(f"[{index}/{total}] 跳过（已存在）：{target}")
                    continue

                ok += 1
                self._append_log(f"[{index}/{total}] 成功：{file.name} -> {target}")
                if self._delete_source:
                    try:
                        file.unlink()
                        self._append_log(f"    已删除源文件：{file.name}")
                    except Exception as err:  # noqa: BLE001
                        self._append_log(f"    删除源文件失败：{file.name} —— {err}")
        except Exception as err:  # noqa: BLE001 - 兜底，保证线程退出并记录原因
            self._append_log(f"[错误] 任务异常终止：{err}")
            logger.error(f"[XM解密] 任务异常终止：{err}", exc_info=True)
        finally:
            self._running = False
            elapsed = time.time() - start_time
            self._last_summary = (
                f"任务结束：成功 {ok} 个，跳过 {skipped} 个，失败 {fail} 个，"
                f"用时 {elapsed:.1f} 秒"
            )
            self._append_log(self._last_summary)
