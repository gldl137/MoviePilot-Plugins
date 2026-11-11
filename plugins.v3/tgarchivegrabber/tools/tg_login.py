# -*- coding: utf-8 -*-
"""
TG 群组 / 频道压缩包抓取插件的登录助手（独立脚本，不依赖 MoviePilot 运行环境）。

作者：gldl137
仓库：MoviePilot-Plugins（https://github.com/gldl137/MoviePilot-Plugins）

用途：
    1. 用 Telegram 用户账号登录一次，生成可复制进插件配置页的 StringSession；
    2. 列出该账号已加入的群组（普通群 / 超级群）与频道及其可用 ID，便于填写「监控群组 / 频道」。

运行（本机或 MoviePilot 容器内均可）：
    pip install telethon
    python tg_login.py

如需代理，设置环境变量后再运行，例如：
    TG_PROXY=socks5://127.0.0.1:7890 python tg_login.py

注意：
    - api_id / api_hash 在 https://my.telegram.org  →  API development tools 申请。
    - StringSession 等同于账号登录凭据，请勿提交到仓库或发给他人。
    - 账号需要已加入（或有权限访问）要监控的群组 / 频道。
    - 若改用 Bot API 模式，需在 @BotFather 用 /setprivacy 关闭隐私模式，否则 Bot 收不到群组普通消息。
"""

import os
import sys
from urllib.parse import urlparse


def build_proxy():
    """把 TG_PROXY 环境变量转换成 Telethon 需要的代理元组。"""
    raw = (os.environ.get("TG_PROXY") or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw if "://" in raw else "http://" + raw)
    scheme = (parsed.scheme or "").lower()
    if scheme.startswith("socks5"):
        proxy_type, default_port = "socks5", 1080
    elif scheme.startswith("socks4"):
        proxy_type, default_port = "socks4", 1080
    elif scheme.startswith("http"):
        proxy_type, default_port = "http", 8080
    else:
        print(f"不支持的代理协议 {scheme}，已忽略")
        return None
    if not parsed.hostname:
        return None
    proxy = [proxy_type, parsed.hostname, parsed.port or default_port]
    if parsed.username:
        proxy += [True, parsed.username, parsed.password or ""]
    return tuple(proxy)


def main() -> int:
    """交互式登录并输出会话串与频道列表。"""
    try:
        from telethon import TelegramClient
        from telethon.errors import SessionPasswordNeededError
        from telethon.sessions import StringSession
    except ImportError:
        print("缺少 telethon，请先执行：pip install telethon")
        return 1

    api_id = input("API ID（my.telegram.org 申请）: ").strip()
    api_hash = input("API Hash: ").strip()
    if not api_id.isdigit() or not api_hash:
        print("API ID 必须是数字，且 API Hash 不能为空")
        return 1

    client = TelegramClient(StringSession(), int(api_id), api_hash, proxy=build_proxy())
    client.connect()
    try:
        if not client.is_user_authorized():
            phone = input("手机号（含国家码，如 +8613800000000）: ").strip()
            client.send_code_request(phone)
            code = input("Telegram 验证码: ").strip()
            try:
                client.sign_in(phone, code)
            except SessionPasswordNeededError:
                client.sign_in(password=input("两步验证密码: ").strip())

        session = client.session.save()
        print("\n===== StringSession（复制到插件配置页）=====")
        print(session)
        print("=========================================\n")

        try:
            print("已加入的群组 / 频道（→ 后面是可填入「监控群组 / 频道」的标识）：")
            for dialog in client.iter_dialogs():
                if not (getattr(dialog, "is_group", False)
                        or getattr(dialog, "is_channel", False)):
                    continue
                entity = dialog.entity
                username = getattr(entity, "username", None)
                entity_id = getattr(entity, "id", None)
                if username:
                    ident = f"@{username}"
                elif getattr(entity, "broadcast", False) or getattr(entity, "megagroup", False):
                    ident = f"-100{entity_id}"
                elif entity_id:
                    ident = f"-{entity_id}"  # 普通群组使用负数 chat id
                else:
                    continue
                if getattr(entity, "megagroup", False):
                    kind = "超级群"
                elif getattr(entity, "broadcast", False):
                    kind = "频道"
                else:
                    kind = "普通群"
                print(f"  [{kind}] {dialog.name}  →  {ident}")
        except Exception as error:  # noqa: BLE001 - 列会话失败不影响会话生成
            print(f"列出群组失败（不影响会话生成）：{error}")

        print("\n提示：有 @username 的公开群组/频道优先填 @username；私有会话填上面输出的 -100... 或 -数字 ID。")
        print("Bot 模式不需要本脚本：改用 @BotFather 的 Token，把 Bot 加入群组，")
        print("并用 /setprivacy 关闭隐私模式，否则收不到群组里的普通消息。")
        return 0
    finally:
        client.disconnect()


if __name__ == "__main__":
    sys.exit(main())
