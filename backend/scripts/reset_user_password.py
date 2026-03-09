#!/usr/bin/env python3
"""
在 AUTH_SALT 或 SM4_KEY 变更后，用当前密钥重算并更新用户密码，恢复登录。

用法:
  cd backend && python scripts/reset_user_password.py <手机号> <新密码>
  cd backend && python scripts/reset_user_password.py --user-id <用户ID> <新密码> [--phone <明文手机号>]

  --phone 仅在 --user-id 时有效：用当前 SM4_KEY 重写该用户的 phone，这样登录时按手机号能查到用户。

会加载 backend/.env 中的 MYSQL_DSN、AUTH_SALT、SM4_KEY。
"""
import asyncio
import os
import sys
from pathlib import Path

# 加载 backend/.env
_backend = Path(__file__).resolve().parent.parent
_env = _backend / ".env"
if _env.exists():
    from dotenv import load_dotenv
    load_dotenv(_env)

# 确保能导入 src
sys.path.insert(0, str(_backend))
os.chdir(_backend)


async def main() -> None:
    from src.services.auth_service import update_password_by_phone, update_password_by_user_id

    args = sys.argv[1:]
    by_id = len(args) >= 3 and args[0] == "--user-id"
    if by_id:
        user_id, password = args[1], args[2]
        phone_arg = None
        if "--phone" in args:
            i = args.index("--phone")
            if i + 1 < len(args):
                phone_arg = args[i + 1]
        ok = await update_password_by_user_id(user_id, password, phone=phone_arg)
        if ok:
            if phone_arg:
                print("已更新密码与手机号密文，请用手机号 {} 和新密码登录。".format(phone_arg))
            else:
                print("已按 user_id 更新密码；若仍无法用手机号登录，请加上 --phone <手机号> 再执行一次。")
        else:
            print("未找到该 user_id 或更新失败。")
        return

    if len(args) < 2:
        print("用法: python scripts/reset_user_password.py <手机号> <新密码>")
        print("      python scripts/reset_user_password.py --user-id <用户ID> <新密码> [--phone <手机号>]")
        sys.exit(1)
    phone, password = args[0], args[1]
    ok = await update_password_by_phone(phone, password)
    if ok:
        print("已更新密码，请用该手机号和新密码登录。")
    else:
        print("未找到该手机号用户。若曾修改过 SM4_KEY，请用 --user-id <用户ID> <新密码> --phone <手机号> 重置。")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
