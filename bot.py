# -*- coding: utf-8 -*-
"""
全自动 Telegram 群管理机器人（SQLite + 长轮询版）
================================================
适配 Render 免费层部署，无需域名、无需 Postgres。

运行：python bot.py
"""

import asyncio
import logging
import os
import re
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatPermissions,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ============================================================
# 配置（已填好）
# ============================================================

BOT_TOKEN = "8787960937:AAF_2zPwGX8aY61II-2-b1HZJ_akmIPaBPU"
ADMIN_ID = 8245865770
DB_PATH = "bot.db"

# 规则参数
FLOOD_WINDOW = 10
FLOOD_LIMIT = 5
NEW_USER_SECONDS = 300
WARN_MUTE_AT = 3
WARN_KICK_AT = 5
LINK_MUTE_SECONDS = 3600
VPN_MUTE_SECONDS = 86400

# 内置违禁词
DEFAULT_BAD_WORDS = [
    "机场", "节点", "订阅", "翻墙", "科学上网", "梯子", "加速器",
    "v2ray", "clash", "ssr", "trojan", "vmess", "shadowsocks",
    "免费节点", "付费节点", "稳定节点", "高速节点",
    "加微信", "加qq", "私聊我", "低价出售", "代购", "刷单",
]

VPN_WORDS = [
    "机场", "节点", "翻墙", "科学上网", "梯子", "加速器",
    "v2ray", "clash", "ssr", "trojan", "vmess", "shadowsocks",
]

URL_PATTERN = re.compile(r"(https?://|t\.me/|www\.)[^\s]+", re.IGNORECASE)

# ============================================================
# 日志
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("aiogram.event").setLevel(logging.WARNING)
logger = logging.getLogger("group-bot")

# ============================================================
# SQLite 封装
# ============================================================

class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn: aiosqlite.Connection | None = None

    async def connect(self):
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER,
                chat_id     INTEGER,
                username    TEXT,
                full_name   TEXT,
                joined_at   TEXT,
                warns       INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS bad_words (
                word TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS violations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                chat_id     INTEGER,
                reason      TEXT,
                action      TEXT,
                created_at  TEXT
            );
        """)
        await self.conn.commit()

        # 初始化默认违禁词
        for w in DEFAULT_BAD_WORDS:
            await self.conn.execute(
                "INSERT OR IGNORE INTO bad_words (word) VALUES (?)", (w.lower(),)
            )
        await self.conn.commit()
        logger.info("SQLite 已连接：%s", self.path)

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def upsert_user(self, user_id, chat_id, username, full_name):
        await self.conn.execute(
            """INSERT INTO users (user_id, chat_id, username, full_name, joined_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(user_id, chat_id) DO UPDATE SET
                 username=excluded.username,
                 full_name=excluded.full_name""",
            (user_id, chat_id, username or "", full_name, datetime.utcnow().isoformat()),
        )
        await self.conn.commit()

    async def get_user(self, user_id, chat_id):
        async with self.conn.execute(
            "SELECT * FROM users WHERE user_id=? AND chat_id=?",
            (user_id, chat_id),
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

    async def add_warn(self, user_id, chat_id):
        await self.conn.execute(
            "UPDATE users SET warns = warns + 1 WHERE user_id=? AND chat_id=?",
            (user_id, chat_id),
        )
        await self.conn.commit()
        u = await self.get_user(user_id, chat_id)
        return u["warns"] if u else 0

    async def reset_warns(self, user_id, chat_id):
        await self.conn.execute(
            "UPDATE users SET warns = 0 WHERE user_id=? AND chat_id=?",
            (user_id, chat_id),
        )
        await self.conn.commit()

    async def add_word(self, word: str):
        await self.conn.execute(
            "INSERT OR IGNORE INTO bad_words (word) VALUES (?)", (word.lower(),)
        )
        await self.conn.commit()

    async def del_word(self, word: str):
        await self.conn.execute("DELETE FROM bad_words WHERE word=?", (word.lower(),))
        await self.conn.commit()

    async def list_words(self):
        async with self.conn.execute("SELECT word FROM bad_words ORDER BY word") as cur:
            rows = await cur.fetchall()
            return [r["word"] for r in rows]

    async def log_violation(self, user_id, chat_id, reason, action):
        await self.conn.execute(
            """INSERT INTO violations (user_id, chat_id, reason, action, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (user_id, chat_id, reason, action, datetime.utcnow().isoformat()),
        )
        await self.conn.commit()

    async def stats(self):
        async with self.conn.execute("SELECT COUNT(*) AS c FROM violations") as cur:
            total = (await cur.fetchone())["c"]
        async with self.conn.execute(
            "SELECT reason, COUNT(*) AS c FROM violations GROUP BY reason ORDER BY c DESC LIMIT 5"
        ) as cur:
            rows = await cur.fetchall()
        return total, [dict(r) for r in rows]


db = DB(DB_PATH)

# ============================================================
# 内存状态
# ============================================================

flood_tracker: dict[int, dict[int, deque]] = defaultdict(lambda: defaultdict(deque))
BOT_ID: int = 0

# ============================================================
# 工具函数
# ============================================================

def normalize(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", text)
    text = re.sub(r"[\s\.\-_\*·•]+", "", text)
    return text


def contains_bad_word(text: str, words: list[str]) -> str | None:
    norm = normalize(text)
    for w in words:
        if normalize(w) in norm:
            return w
    return None


def contains_url(text: str) -> bool:
    return bool(URL_PATTERN.search(text or ""))


def is_flood(chat_id: int, user_id: int) -> bool:
    now = time.monotonic()
    q = flood_tracker[chat_id][user_id]
    q.append(now)
    while q and now - q[0] > FLOOD_WINDOW:
        q.popleft()
    return len(q) >= FLOOD_LIMIT


def mute_permissions() -> ChatPermissions:
    return ChatPermissions(
        can_send_messages=False, can_send_audios=False, can_send_documents=False,
        can_send_photos=False, can_send_videos=False, can_send_video_notes=False,
        can_send_voice_notes=False, can_send_polls=False,
        can_send_other_messages=False, can_add_web_page_previews=False,
    )


def unmute_permissions() -> ChatPermissions:
    return ChatPermissions(
        can_send_messages=True, can_send_audios=True, can_send_documents=True,
        can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
        can_send_voice_notes=True, can_send_polls=True,
        can_send_other_messages=True, can_add_web_page_previews=True,
    )


async def safe_delete(message: Message):
    try:
        await message.delete()
    except Exception as e:
        logger.warning("删除消息失败: %s", e)


async def notify_admin(bot: Bot, text: str, user_id: int, chat_id: int):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ 一键解封", callback_data=f"unban:{chat_id}:{user_id}")
        ]]
    )
    try:
        await bot.send_message(ADMIN_ID, text, reply_markup=kb)
    except Exception as e:
        logger.warning("通知管理员失败: %s", e)


# ============================================================
# Router
# ============================================================

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "👋 我是全自动群管理机器人。\n\n"
        "把我拉进群并设为管理员，我会自动拦截广告/违禁词/外链。\n"
        "管理员命令：/help"
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("只有管理员能查看。")
        return
    await message.answer(
        "🛠 **管理员命令**\n\n"
        "/addword 词 — 加违禁词\n"
        "/delword 词 — 删违禁词\n"
        "/listwords — 查看词库\n"
        "/unban — 解封（回复消息）\n"
        "/resetwarns — 重置警告（回复消息）\n"
        "/myid — 查看 ID\n"
        "/stats — 拦截统计",
        parse_mode="Markdown",
    )


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(
        f"你的 ID：`{message.from_user.id}`\n群 ID：`{message.chat.id}`",
        parse_mode="Markdown",
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    total, top = await db.stats()
    lines = [f"📊 **拦截统计**\n总计：{total} 次\n"]
    if top:
        lines.append("**TOP 5：**")
        for r in top:
            lines.append(f"• {r['reason']}：{r['c']} 次")
    await message.answer("\n".join(lines), parse_mode="Markdown")


@router.message(Command("addword"))
async def cmd_addword(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("用法：/addword 违禁词")
        return
    await db.add_word(args[1].strip())
    await message.answer(f"✅ 已添加：`{args[1].strip()}`", parse_mode="Markdown")


@router.message(Command("delword"))
async def cmd_delword(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("用法：/delword 违禁词")
        return
    await db.del_word(args[1].strip())
    await message.answer(f"🗑 已删除：`{args[1].strip()}`", parse_mode="Markdown")


@router.message(Command("listwords"))
async def cmd_listwords(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    words = await db.list_words()
    text = "、".join(words) if words else "（空）"
    if len(text) > 3500:
        text = text[:3500] + "..."
    await message.answer(f"📚 违禁词库（{len(words)} 个）：\n\n{text}")


async def get_target_user(message: Message):
    if message.reply_to_message and message.reply_to_message.from_user:
        u = message.reply_to_message.from_user
        return u.id, u.full_name
    args = message.text.split(maxsplit=1)
    if len(args) >= 2 and args[1].strip().isdigit():
        return int(args[1].strip()), args[1].strip()
    return None


@router.message(Command("unban"))
async def cmd_unban(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("请在群里使用。")
        return
    target = await get_target_user(message)
    if not target:
        await message.answer("请回复某人的消息，或 /unban 用户ID")
        return
    uid, name = target
    try:
        await message.bot.restrict_chat_member(
            chat_id=message.chat.id, user_id=uid,
            permissions=unmute_permissions(),
        )
        await message.answer(f"✅ 已解封 {name}")
        await db.log_violation(uid, message.chat.id, "管理员解封", "unban")
    except Exception as e:
        await message.answer(f"解封失败：{e}")


@router.message(Command("resetwarns"))
async def cmd_resetwarns(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("请在群里使用。")
        return
    target = await get_target_user(message)
    if not target:
        await message.answer("请回复某人的消息")
        return
    uid, name = target
    await db.reset_warns(uid, message.chat.id)
    await message.answer(f"✅ 已重置 {name} 的警告")


@router.callback_query(F.data.startswith("unban:"))
async def cb_unban(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("无权操作", show_alert=True)
        return
    _, chat_id, user_id = callback.data.split(":")
    chat_id, user_id = int(chat_id), int(user_id)
    try:
        await callback.bot.restrict_chat_member(
            chat_id=chat_id, user_id=user_id,
            permissions=unmute_permissions(),
        )
        await callback.message.edit_text(callback.message.text + "\n\n✅ 已解封")
        await callback.answer("已解封")
        await db.log_violation(user_id, chat_id, "一键解封", "unban")
    except Exception as e:
        await callback.answer(f"失败：{e}", show_alert=True)


@router.message(F.new_chat_members)
async def on_new_member(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    for member in message.new_chat_members:
        if member.id == BOT_ID:
            continue
        await db.upsert_user(member.id, message.chat.id, member.username, member.full_name)
        await message.answer(f"👋 欢迎 {member.full_name}！发广告/外链会被自动处理。")


@router.message(F.text, ~F.text.startswith("/"))
async def auto_moderate(message: Message):
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if message.from_user.id == ADMIN_ID:
        return

    user = message.from_user
    chat_id = message.chat.id
    text = message.text or ""

    u = await db.get_user(user.id, chat_id)
    if not u:
        await db.upsert_user(user.id, chat_id, user.username, user.full_name)
        u = await db.get_user(user.id, chat_id)

    # 1. 刷屏
    if is_flood(chat_id, user.id):
        await safe_delete(message)
        until = datetime.now() + timedelta(minutes=10)
        try:
            await message.bot.restrict_chat_member(
                chat_id=chat_id, user_id=user.id,
                permissions=mute_permissions(), until_date=until,
            )
        except Exception as e:
            logger.warning("禁言失败: %s", e)
        await message.answer(f"⚠️ {user.full_name} 刷屏，已禁言 10 分钟。")
        await db.log_violation(user.id, chat_id, "刷屏", "mute 10m")
        return

    # 2. VPN 广告词
    hit = contains_bad_word(text, VPN_WORDS)
    if hit:
        await safe_delete(message)
        until = datetime.now() + timedelta(seconds=VPN_MUTE_SECONDS)
        try:
            await message.bot.restrict_chat_member(
                chat_id=chat_id, user_id=user.id,
                permissions=mute_permissions(), until_date=until,
            )
        except Exception as e:
            logger.warning("禁言失败: %s", e)
        await message.answer(f"🚫 {user.full_name} 发送 VPN 广告，已禁言 24h。命中：`{hit}`")
        await db.log_violation(user.id, chat_id, f"VPN广告({hit})", "mute 24h")
        await notify_admin(
            message.bot,
            f"🚫 群 `{chat_id}` 用户 `{user.id}`（{user.full_name}）\n发送 VPN 广告，已禁言 24h",
            user.id, chat_id,
        )
        return

    # 3. 普通违禁词
    words = await db.list_words()
    hit = contains_bad_word(text, words)
    if hit:
        await safe_delete(message)
        warns = await db.add_warn(user.id, chat_id)
        await message.answer(
            f"⚠️ {user.full_name} 发送违禁词，已删除。命中：`{hit}`\n当前警告：{warns}/{WARN_KICK_AT}"
        )
        await db.log_violation(user.id, chat_id, f"违禁词({hit})", f"warn({warns})")

        if warns >= WARN_MUTE_AT and warns < WARN_KICK_AT:
            until = datetime.now() + timedelta(hours=24)
            try:
                await message.bot.restrict_chat_member(
                    chat_id=chat_id, user_id=user.id,
                    permissions=mute_permissions(), until_date=until,
                )
            except Exception as e:
                logger.warning("禁言失败: %s", e)
            await message.answer(f"🔇 {user.full_name} 警告满 {WARN_MUTE_AT} 次，已禁言 24h。")
            await notify_admin(message.bot, f"🔇 用户 `{user.id}` 警告满 3 次", user.id, chat_id)
        elif warns >= WARN_KICK_AT:
            try:
                await message.bot.ban_chat_member(chat_id, user.id)
                await message.bot.unban_chat_member(chat_id, user.id)
            except Exception as e:
                logger.warning("踢人失败: %s", e)
            await message.answer(f"👢 {user.full_name} 警告满 {WARN_KICK_AT} 次，已踢出。")
            await notify_admin(message.bot, f"👢 用户 `{user.id}` 警告满 5 次", user.id, chat_id)
        return

    # 4. 外链
    if contains_url(text):
        joined = u["joined_at"] if u and u.get("joined_at") else None
        is_new = False
        if joined:
            try:
                is_new = (datetime.utcnow() - datetime.fromisoformat(joined)).total_seconds() < NEW_USER_SECONDS
            except Exception:
                is_new = False

        await safe_delete(message)
        if is_new:
            try:
                await message.bot.ban_chat_member(chat_id, user.id)
                await message.bot.unban_chat_member(chat_id, user.id)
            except Exception as e:
                logger.warning("踢人失败: %s", e)
            await message.answer(f"👢 {user.full_name}（新号）发送链接，已踢出。")
            await db.log_violation(user.id, chat_id, "新号发链接", "kick")
            await notify_admin(message.bot, f"👢 新号 `{user.id}` 发链接", user.id, chat_id)
        else:
            until = datetime.now() + timedelta(seconds=LINK_MUTE_SECONDS)
            try:
                await message.bot.restrict_chat_member(
                    chat_id=chat_id, user_id=user.id,
                    permissions=mute_permissions(), until_date=until,
                )
            except Exception as e:
                logger.warning("禁言失败: %s", e)
            await message.answer(f"🔗 {user.full_name} 发送外链，已禁言 1h。")
            await db.log_violation(user.id, chat_id, "外链", "mute 1h")
            await notify_admin(message.bot, f"🔗 用户 `{user.id}` 发外链", user.id, chat_id)
        return


# ============================================================
# 定时任务
# ============================================================

scheduler = AsyncIOScheduler()


async def cleanup_flood_tracker():
    now = time.monotonic()
    for chat_id in list(flood_tracker.keys()):
        for user_id in list(flood_tracker[chat_id].keys()):
            q = flood_tracker[chat_id][user_id]
            while q and now - q[0] > FLOOD_WINDOW:
                q.popleft()
            if not q:
                del flood_tracker[chat_id][user_id]
        if not flood_tracker[chat_id]:
            del flood_tracker[chat_id]


scheduler.add_job(cleanup_flood_tracker, "interval", minutes=5, id="cleanup")


# ============================================================
# 全局错误处理
# ============================================================

@router.errors()
async def global_error_handler(event):
    logger.exception("未处理异常: %s", event.exception)
    try:
        update = event.update
        msg = None
        if update.message:
            msg = update.message
        elif update.callback_query and update.callback_query.message:
            msg = update.callback_query.message
        if msg:
            await msg.answer("⚠️ 机器人内部错误，已记录日志。")
    except Exception:
        pass
    return True


# ============================================================
# 启动
# ============================================================

async def main():
    global BOT_ID

    await db.connect()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    me = await bot.get_me()
    BOT_ID = me.id
    logger.info("机器人启动：@%s (ID=%s)", me.username, me.id)

    dp = Dispatcher()
    dp.include_router(router)

    scheduler.start()
    logger.info("定时任务已启动")

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        scheduler.shutdown(wait=False)
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("机器人已停止")