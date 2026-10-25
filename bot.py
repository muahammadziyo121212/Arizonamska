# -*- coding: utf-8 -*-
"""🍥 ANIME QIDIRUV BOTI

Foydalanuvchi anime NOMI yoki KODINI yozadi → bot unga o'sha anime joylangan
YOPIQ kanalga (yoki guruhga) bir martalik kirish havolasini beradi.

Admin hech qanday video yubormaydi: har bir anime uchun alohida yopiq kanal
ochiladi, bot unga admin qilinadi va bot shu kanalni anime bilan bog'laydi.

Bitta fayl · aiogram 3 · Turso (libSQL) · Render.com uchun tayyor.
"""

from __future__ import annotations

import asyncio
import difflib
import logging
import math
import os
import re
import time
from contextlib import suppress
from html import escape
from typing import Any, Awaitable, Callable, Sequence

import libsql_client
from aiohttp import ClientError, ClientSession, ClientTimeout, web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import BaseFilter, Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    TelegramObject,
)

try:  # lokal ishlatishda .env o'qiladi (Render'da shart emas)
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("aiogram.event").setLevel(logging.WARNING)
log = logging.getLogger("animebot")


# ══════════════════════════════════════════════════════════════
#                          SOZLAMALAR
# ══════════════════════════════════════════════════════════════

def _int_env(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, "").strip() or default)
    except ValueError:
        return default


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MAIN_ADMIN_ID = _int_env("MAIN_ADMIN_ID", 0)
TURSO_URL = os.getenv("TURSO_DATABASE_URL", "").strip()
TURSO_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "animebot.db").strip()
PORT = _int_env("PORT", 8080)
API_ID = _int_env("API_ID", 0)                         # my.telegram.org
API_HASH = os.getenv("API_HASH", "").strip()
TG_SESSION = os.getenv("TG_SESSION", "").strip()       # make_session.py beradi
LINK_TTL = max(5, _int_env("LINK_TTL_MINUTES", 60))   # bir martalik havola muddati (daqiqa)
SUB_CACHE_TTL = _int_env("SUB_CACHE_TTL", 600)         # obuna tekshiruvi keshi (soniya)
START_CODE = 10000                                     # birinchi avtomatik kod
PER_PAGE = 8
TZ_OFFSET = 5 * 3600                                   # Toshkent (UTC+5)


# ══════════════════════════════════════════════════════════════
#                        MA'LUMOTLAR BAZASI
# ══════════════════════════════════════════════════════════════
# Jadvallar `ab_` prefiksi bilan — eski botlar bazasi bilan aralashib ketmaydi.

SCHEMA: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS ab_users (
        telegram_id INTEGER PRIMARY KEY,
        username    TEXT,
        full_name   TEXT,
        joined_at   INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS ab_admins (
        telegram_id INTEGER PRIMARY KEY,
        added_at    INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS ab_anime (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        code          INTEGER UNIQUE NOT NULL,
        title         TEXT    NOT NULL,
        skey          TEXT    NOT NULL,
        descr         TEXT,
        poster        TEXT,
        chat_id       INTEGER NOT NULL,
        chat_title    TEXT,
        chat_username TEXT,
        views         INTEGER NOT NULL DEFAULT 0,
        created_at    INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS ab_subs (
        chat_id INTEGER PRIMARY KEY,
        title   TEXT,
        url     TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS ab_chats (
        chat_id    INTEGER PRIMARY KEY,
        title      TEXT,
        username   TEXT,
        can_invite INTEGER NOT NULL DEFAULT 1,
        added_at   INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS ab_kv (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",
    "INSERT OR IGNORE INTO ab_kv(key, value) VALUES ('code_counter', '9999')",
)

_RETRY_ERRORS = (ClientError, asyncio.TimeoutError, OSError)


class Database:
    """libSQL (Turso yoki lokal fayl) ustidagi yupqa qatlam."""

    def __init__(self) -> None:
        self.client: Any = None
        self._wlock = asyncio.Lock()

    async def connect(self) -> None:
        if TURSO_URL:
            url, token = TURSO_URL, (TURSO_TOKEN or None)
        else:
            url, token = f"file:{DB_PATH}", None
        # libsql:// (WebSocket) ba'zi tarmoqlarda bloklanadi — https ishonchliroq.
        if url.startswith("libsql://"):
            url = "https://" + url[len("libsql://"):]
        self.client = libsql_client.create_client(url=url, auth_token=token)
        await self._run(lambda: self.client.execute("SELECT 1"))
        for stmt in SCHEMA:
            await self._run(lambda s=stmt: self.client.execute(s))
        log.info("💾 Baza tayyor: %s", "Turso (bulut)" if TURSO_URL else "lokal fayl")

    async def close(self) -> None:
        if self.client is not None:
            with suppress(Exception):
                await self.client.close()

    async def _run(self, call: Callable[[], Awaitable[Any]]) -> Any:
        """Tarmoq xatolarida 3 martagacha qayta uriniladi (SQL xatolarida emas)."""
        for attempt in (1, 2, 3):
            try:
                return await call()
            except _RETRY_ERRORS as e:
                if attempt == 3:
                    raise
                log.warning("Baza tarmoq xatosi (%s), qayta urinish %d/3", e, attempt)
                await asyncio.sleep(0.6 * attempt)

    @staticmethod
    def _rows(rs: Any) -> list[dict[str, Any]]:
        cols = list(rs.columns)
        return [dict(zip(cols, list(r))) for r in rs.rows]

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        rs = await self._run(lambda: self.client.execute(sql, list(params)))
        return self._rows(rs)

    async def fetchval(self, sql: str, params: Sequence[Any] = (), default: Any = None) -> Any:
        rows = await self.fetchall(sql, params)
        if not rows:
            return default
        return next(iter(rows[0].values()))

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        async with self._wlock:
            await self._run(lambda: self.client.execute(sql, list(params)))

    async def executemany(self, sql: str, seq: Sequence[Sequence[Any]]) -> None:
        stmts = [(sql, list(p)) for p in seq]
        if not stmts:
            return
        async with self._wlock:
            await self._run(lambda: self.client.batch(stmts))


db = Database()


# ══════════════════════════════════════════════════════════════
#                     XOTIRADAGI KESH (tezlik uchun)
# ══════════════════════════════════════════════════════════════

ANIMES: dict[int, dict[str, Any]] = {}      # kod → anime
ADMINS: set[int] = set()
SUBS: dict[int, dict[str, Any]] = {}        # majburiy obuna kanallari
CHATS: dict[int, dict[str, Any]] = {}       # bot admin bo'lgan kanallar/guruhlar
USERS: set[int] = set()
KV: dict[str, str] = {}
VIEW_BUF: dict[int, int] = {}               # kod → qo'shilmagan ko'rishlar
PENDING: dict[int, str] = {}                # obunadan oldin yozilgan so'rov
_PASS: dict[int, float] = {}                # obunadan o'tgan foydalanuvchilar keshi
_LINKS: dict[tuple[int, int], tuple[str, float]] = {}   # (user, chat) → (havola, muddat)
_WARNED: dict[int, float] = {}              # admin ogohlantirish cheklovi
_code_lock = asyncio.Lock()
_tasks: set[asyncio.Task] = set()


async def load_cache() -> None:
    for r in await db.fetchall("SELECT * FROM ab_anime"):
        ANIMES[r["code"]] = r
    ADMINS.update(r["telegram_id"] for r in await db.fetchall("SELECT telegram_id FROM ab_admins"))
    if MAIN_ADMIN_ID:
        ADMINS.add(MAIN_ADMIN_ID)
    for r in await db.fetchall("SELECT * FROM ab_subs"):
        SUBS[r["chat_id"]] = r
    for r in await db.fetchall("SELECT * FROM ab_chats"):
        CHATS[r["chat_id"]] = r
    USERS.update(r["telegram_id"] for r in await db.fetchall("SELECT telegram_id FROM ab_users"))
    for r in await db.fetchall("SELECT key, value FROM ab_kv"):
        KV[r["key"]] = r["value"]


# ───────────────────────── yordamchilar ─────────────────────────

def now() -> int:
    return int(time.time())


def h(value: Any) -> str:
    """HTML uchun xavfsiz matn."""
    return escape(str(value if value is not None else ""), quote=False)


def trim(text: str, n: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= n else text[: n - 1].rstrip() + "…"


_STRIP = str.maketrans("", "", "'`’ʻʼ‘´ʹ′")
_NON_WORD = re.compile(r"[\W_]+", re.UNICODE)


def norm(s: str) -> str:
    """Qidiruv uchun normallash: kichik harf, apostroflarsiz (o', g' → o, g)."""
    return _NON_WORD.sub(" ", (s or "").lower().translate(_STRIP)).strip()


def search(query: str) -> list[dict[str, Any]]:
    key = norm(query)
    if not key:
        return []
    tokens = key.split()
    hits = [a for a in ANIMES.values() if all(t in a["skey"] for t in tokens)]
    hits.sort(key=lambda a: (a["skey"] != key, not a["skey"].startswith(key), -a["views"], a["skey"]))
    return hits


def suggest(query: str, n: int = 5) -> list[dict[str, Any]]:
    """Imlo xatosi bo'lsa ham taxminiy natijalar (difflib)."""
    key = norm(query)
    if not key:
        return []
    scored: list[tuple[float, dict[str, Any]]] = []
    for a in ANIMES.values():
        sk = a["skey"]
        best = difflib.SequenceMatcher(None, key, sk).ratio()
        for w in sk.split():
            best = max(best, difflib.SequenceMatcher(None, key, w).ratio())
        if best >= 0.6:
            scored.append((best, a))
    scored.sort(key=lambda x: -x[0])
    return [a for _, a in scored[:n]]


def _st(member: Any) -> str:
    s = getattr(member, "status", "")
    return str(getattr(s, "value", s))


def fire(coro: Awaitable[Any]) -> None:
    """Fon vazifasi (havola yo'qolib ketmasligi uchun saqlab turiladi)."""
    t = asyncio.create_task(coro)  # type: ignore[arg-type]
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


async def notify_admins(bot: Bot, text: str, kb: InlineKeyboardMarkup | None = None) -> None:
    for aid in list(ADMINS):
        with suppress(TelegramAPIError):
            await bot.send_message(aid, text, reply_markup=kb)


# ───────────────────────── anime amallari ─────────────────────────

def peek_code() -> int:
    n = int(KV.get("code_counter", "9999")) + 1
    n = max(n, START_CODE)
    while n in ANIMES:
        n += 1
    return n


async def alloc_code() -> int:
    async with _code_lock:
        n = peek_code()
        KV["code_counter"] = str(n)
        await db.execute("UPDATE ab_kv SET value = ? WHERE key = 'code_counter'", [str(n)])
        return n


async def add_anime(
    code: int, title: str, chat: dict[str, Any], descr: str | None, poster: str | None
) -> dict[str, Any]:
    a = {
        "code": code,
        "title": title,
        "skey": norm(title),
        "descr": descr or None,
        "poster": poster or None,
        "chat_id": chat["chat_id"],
        "chat_title": chat.get("title"),
        "chat_username": chat.get("username"),
        "views": 0,
        "created_at": now(),
    }
    await db.execute(
        "INSERT INTO ab_anime(code, title, skey, descr, poster, chat_id, chat_title, "
        "chat_username, views, created_at) VALUES (?,?,?,?,?,?,?,?,0,?)",
        [a["code"], a["title"], a["skey"], a["descr"], a["poster"], a["chat_id"],
         a["chat_title"], a["chat_username"], a["created_at"]],
    )
    ANIMES[code] = a
    return a


_EDITABLE = {"title", "skey", "descr", "poster", "chat_id", "chat_title", "chat_username", "code"}


async def edit_anime(old_code: int, /, **fields: Any) -> dict[str, Any]:
    a = ANIMES[old_code]
    if "title" in fields:
        fields["skey"] = norm(fields["title"])
    bad = set(fields) - _EDITABLE
    if bad:
        raise ValueError(f"ruxsat etilmagan maydon: {bad}")
    if "code" in fields:
        await flush_views()   # eski kod bo'yicha kutayotgan ko'rishlar yo'qolmasin
    cols = ", ".join(f"{k} = ?" for k in fields)
    await db.execute(f"UPDATE ab_anime SET {cols} WHERE code = ?", [*fields.values(), old_code])
    a.update(fields)
    if "code" in fields and fields["code"] != old_code:
        ANIMES.pop(old_code, None)
        ANIMES[fields["code"]] = a
    return a


async def delete_anime(code: int) -> None:
    await db.execute("DELETE FROM ab_anime WHERE code = ?", [code])
    ANIMES.pop(code, None)


def bump_views(a: dict[str, Any]) -> None:
    a["views"] = a.get("views", 0) + 1
    VIEW_BUF[a["code"]] = VIEW_BUF.get(a["code"], 0) + 1


async def flush_views() -> None:
    if not VIEW_BUF:
        return
    buf = dict(VIEW_BUF)
    VIEW_BUF.clear()
    try:
        await db.executemany(
            "UPDATE ab_anime SET views = views + ? WHERE code = ?",
            [(n, c) for c, n in buf.items()],
        )
    except Exception:  # noqa: BLE001 — keyingi safar qayta uriniladi
        for c, n in buf.items():
            VIEW_BUF[c] = VIEW_BUF.get(c, 0) + n
        log.exception("Ko'rishlarni saqlab bo'lmadi")


async def touch_user(u: Any) -> None:
    if u.id in USERS:
        return
    USERS.add(u.id)
    await db.execute(
        "INSERT OR IGNORE INTO ab_users(telegram_id, username, full_name, joined_at) VALUES (?,?,?,?)",
        [u.id, u.username, u.full_name, now()],
    )


# ══════════════════════════════════════════════════════════════
#                 KANALGA KIRISH HAVOLASI (asosiy mantiq)
# ══════════════════════════════════════════════════════════════

def open_url(a: dict[str, Any]) -> str:
    """Allaqachon a'zo bo'lgan foydalanuvchi kanalni ochishi uchun havola."""
    if a.get("chat_username"):
        return f"https://t.me/{a['chat_username']}"
    cid = str(a["chat_id"])
    if cid.startswith("-100"):
        return f"https://t.me/c/{cid[4:]}/1"
    return f"https://t.me/c/{cid.lstrip('-')}/1"


async def access(bot: Bot, a: dict[str, Any], uid: int) -> tuple[str | None, str]:
    """(havola, holat). Holat: join | member | open | error."""
    cid = a["chat_id"]

    # Ochiq (username'li) kanal — oddiy havola yetarli.
    if a.get("chat_username"):
        return f"https://t.me/{a['chat_username']}", "open"

    # Allaqachon a'zomi?
    with suppress(TelegramAPIError):
        m = await bot.get_chat_member(cid, uid)
        st = _st(m)
        if st in ("creator", "administrator", "member") or (
            st == "restricted" and getattr(m, "is_member", False)
        ):
            return open_url(a), "member"

    # Shu foydalanuvchi uchun yaqinda yaratilgan havola bormi?
    cached = _LINKS.get((uid, cid))
    if cached and cached[1] > time.time():
        return cached[0], "join"

    try:
        link = await bot.create_chat_invite_link(
            cid,
            name=f"u{uid}"[:32],
            expire_date=now() + LINK_TTL * 60,
            member_limit=1,
        )
    except TelegramAPIError as e:
        log.warning("Havola yaratilmadi (anime=%s chat=%s): %s", a["code"], cid, e)
        if _WARNED.get(a["code"], 0) < time.time():
            _WARNED[a["code"]] = time.time() + 600
            fire(
                notify_admins(
                    bot,
                    f"⚠️ <b>{h(a['title'])}</b> (kod {a['code']}) uchun kirish havolasi yaratib "
                    f"bo'lmadi.\n\nSabab: <code>{h(e)}</code>\n\nBot kanalda <b>admin</b> ekanini va "
                    f"«Foydalanuvchilarni taklif qilish» huquqi borligini tekshiring.",
                )
            )
        return None, "error"

    _LINKS[(uid, cid)] = (link.invite_link, time.time() + LINK_TTL * 60 - 30)
    return link.invite_link, "join"


def card_text(a: dict[str, Any], status: str) -> str:
    parts = [f"🍥 <b>{h(a['title'])}</b>", f"🔢 Kod: <code>{a['code']}</code>"]
    if a.get("descr"):
        parts += ["", h(trim(a["descr"], 500))]
    parts.append("")
    if status == "join":
        parts.append(
            f"🔐 Kanalga qo'shilish havolasi <b>faqat siz uchun</b> yaratildi va "
            f"{LINK_TTL} daqiqa amal qiladi. Uni boshqalarga yubormang."
        )
    elif status == "member":
        parts.append("✅ Siz bu kanalda allaqachon borsiz.")
    elif status == "open":
        parts.append("👇 Kanalga o'tish uchun tugmani bosing.")
    else:
        parts.append(
            "⚠️ Kechirasiz, hozir bu anime uchun havola tayyorlab bo'lmadi. "
            "Admin xabardor qilindi — birozdan so'ng qayta urinib ko'ring."
        )
    return "\n".join(parts)


async def deliver(bot: Bot, uid: int, a: dict[str, Any]) -> None:
    url, status = await access(bot, a, uid)
    if status != "error":
        bump_views(a)
    kb = None
    if url:
        label = "🍿 Kanalga kirish" if status == "join" else "📺 Kanalni ochish"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=label, url=url)]])
    text = card_text(a, status)
    protect = status == "join"   # havolali xabarni forward qilib bo'lmasin
    if a.get("poster"):
        with suppress(TelegramAPIError):
            await bot.send_photo(uid, a["poster"], caption=text, reply_markup=kb, protect_content=protect)
            return
    await bot.send_message(uid, text, reply_markup=kb, protect_content=protect)


# ══════════════════════════════════════════════════════════════
#                       MAJBURIY OBUNA
# ══════════════════════════════════════════════════════════════

async def is_member(bot: Bot, chat_id: int, uid: int) -> bool:
    try:
        m = await bot.get_chat_member(chat_id, uid)
    except TelegramAPIError as e:
        # Sozlama xatosi (bot admin emas va h.k.) foydalanuvchini to'smasin.
        log.warning("Obuna tekshiruvi xatosi chat=%s: %s", chat_id, e)
        return True
    st = _st(m)
    return st in ("creator", "administrator", "member") or (
        st == "restricted" and getattr(m, "is_member", False)
    )


async def missing_subs(bot: Bot, uid: int, force: bool = False) -> list[dict[str, Any]]:
    if not SUBS:
        return []
    if not force and _PASS.get(uid, 0) > time.time():
        return []
    chans = list(SUBS.values())
    results = await asyncio.gather(*(is_member(bot, c["chat_id"], uid) for c in chans))
    missing = [c for c, ok in zip(chans, results) if not ok]
    if not missing:
        _PASS[uid] = time.time() + SUB_CACHE_TTL
    return missing


def subs_kb(missing: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"📢 {trim(c['title'] or 'Kanal', 40)}", url=c["url"])] for c in missing]
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="chk")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


SUB_TEXT = (
    "📢 <b>Botdan foydalanish uchun quyidagi kanallarga obuna bo'ling</b>, "
    "so'ng «✅ Tekshirish» tugmasini bosing."
)


class Gate(BaseMiddleware):
    """Foydalanuvchini ro'yxatga oladi va majburiy obunani tekshiradi."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user is None or user.is_bot:
            return await handler(event, data)
        await touch_user(user)
        if user.id in ADMINS:
            return await handler(event, data)
        if isinstance(event, CallbackQuery) and event.data == "chk":
            return await handler(event, data)

        miss = await missing_subs(data["bot"], user.id)
        if not miss:
            return await handler(event, data)

        if isinstance(event, Message):
            txt = (event.text or "").strip()
            parts = txt.split(maxsplit=1)
            if parts and parts[0].startswith("/start"):
                if len(parts) > 1:
                    PENDING[user.id] = parts[1]
            elif txt and not txt.startswith("/"):
                PENDING[user.id] = txt
            await event.answer(SUB_TEXT, reply_markup=subs_kb(miss))
        elif isinstance(event, CallbackQuery):
            await event.answer("Avval kanallarga obuna bo'ling 👇", show_alert=True)
            if event.message:
                await event.message.answer(SUB_TEXT, reply_markup=subs_kb(miss))
        return None


# ══════════════════════════════════════════════════════════════
#                     KLAVIATURA YORDAMCHILARI
# ══════════════════════════════════════════════════════════════

def btn(text: str, cb: str | None = None, url: str | None = None) -> InlineKeyboardButton:
    if url:
        return InlineKeyboardButton(text=text, url=url)
    return InlineKeyboardButton(text=text, callback_data=cb or "noop")


def kb(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[list(r) for r in rows])


def nav_row(prefix: str, page: int, pages: int) -> list[InlineKeyboardButton]:
    row: list[InlineKeyboardButton] = []
    if page > 0:
        row.append(btn("⬅️", f"{prefix}:{page - 1}"))
    row.append(btn(f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        row.append(btn("➡️", f"{prefix}:{page + 1}"))
    return row


CANCEL_KB = kb([btn("✖️ Bekor qilish", "adm")])


async def show(cb: CallbackQuery, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
    """Xabarni tahrirlaydi; bo'lmasa (masalan rasmli xabar) yangisini yuboradi."""
    try:
        await cb.message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as e:
        if "not modified" in str(e):
            return
        with suppress(TelegramAPIError):
            await cb.message.delete()
        await cb.message.answer(text, reply_markup=markup)

USER_MENU = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📚 Barcha animelar"), KeyboardButton(text="🔥 Mashhurlar")]],
    resize_keyboard=True,
)


def welcome(name: str) -> str:
    return (
        f"👋 Salom, <b>{h(name)}</b>!\n\n"
        "🍥 <b>Anime Dunyosi</b> botiga xush kelibsiz.\n\n"
        "Anime <b>nomini</b> yoki <b>kodini</b> yuboring — men sizga uning yopiq kanalini "
        "topib beraman.\n\n"
        "Masalan: <code>Naruto</code> yoki <code>10001</code>"
    )


HELP_TEXT = (
    "ℹ️ <b>Qanday ishlaydi?</b>\n\n"
    "1️⃣ Anime <b>nomini</b> yoki <b>kodini</b> yozing\n"
    "2️⃣ Bot sizga yopiq kanal havolasini beradi\n"
    "3️⃣ «Kanalga kirish» tugmasini bosing va tomosha qiling 🍿\n\n"
    "📚 Barcha animelar ro'yxati — pastdagi tugmada."
)


# ══════════════════════════════════════════════════════════════
#                        FOYDALANUVCHI QISMI
# ══════════════════════════════════════════════════════════════

user_router = Router(name="user")
user_router.message.filter(F.chat.type == "private")
user_router.message.middleware(Gate())
user_router.callback_query.middleware(Gate())

# `/` bilan boshlanmaydigan matn (buyruqlar holatlarga tushib qolmasin)
TXT = F.text & ~F.text.startswith("/")


def list_view(page: int) -> tuple[str, InlineKeyboardMarkup | None]:
    items = sorted(ANIMES.values(), key=lambda a: a["skey"])
    if not items:
        return "😔 Hozircha animelar qo'shilmagan.", None
    pages = max(1, math.ceil(len(items) / PER_PAGE))
    page = max(0, min(page, pages - 1))
    rows = [[btn(f"🍥 {trim(a['title'], 50)}", f"a:{a['code']}")] for a in items[page * PER_PAGE:(page + 1) * PER_PAGE]]
    rows.append(nav_row("ls", page, pages))
    return f"📚 <b>Barcha animelar</b> ({len(items)} ta)\n\nTanlang:", kb(*rows)


def top_view() -> tuple[str, InlineKeyboardMarkup | None]:
    items = sorted(ANIMES.values(), key=lambda a: (-a["views"], a["skey"]))[:10]
    if not items:
        return "😔 Hozircha animelar qo'shilmagan.", None
    rows = [[btn(f"🔥 {trim(a['title'], 44)} · 👁 {a['views']}", f"a:{a['code']}")] for a in items]
    return "🔥 <b>Eng mashhur animelar</b>", kb(*rows)


async def run_query(bot: Bot, uid: int, text: str) -> None:
    """Nom yoki kod bo'yicha qidiradi va natijani foydalanuvchiga yuboradi."""
    text = text.strip()[:100]
    if text.isdigit() and len(text) <= 12 and int(text) in ANIMES:
        await deliver(bot, uid, ANIMES[int(text)])
        return

    hits = search(text)
    if len(hits) == 1:
        await deliver(bot, uid, hits[0])
        return
    if hits:
        shown = hits[:10]
        rows = [[btn(f"🍥 {trim(a['title'], 50)}", f"a:{a['code']}")] for a in shown]
        extra = f"\n\n…yana {len(hits) - len(shown)} ta bor, nomni aniqroq yozing." if len(hits) > len(shown) else ""
        await bot.send_message(
            uid, f"🔎 «<b>{h(trim(text, 40))}</b>» bo'yicha <b>{len(hits)}</b> ta natija. Birini tanlang:{extra}",
            reply_markup=kb(*rows),
        )
        return

    if text.isdigit():
        await bot.send_message(uid, f"😔 <code>{h(text)}</code> kodli anime topilmadi.", reply_markup=kb([btn("📚 Barcha animelar", "ls:0")]))
        return

    sug = await asyncio.to_thread(suggest, text)
    if sug:
        rows = [[btn(f"🍥 {trim(a['title'], 50)}", f"a:{a['code']}")] for a in sug]
        await bot.send_message(uid, "🤔 Aniq topilmadi. Shulardan birini nazarda tutdingizmi?", reply_markup=kb(*rows))
        return
    await bot.send_message(
        uid,
        f"😔 «<b>{h(trim(text, 40))}</b>» bo'yicha hech narsa topilmadi.\n\n"
        "Nomini boshqacha yozib ko'ring yoki kod yuboring.",
        reply_markup=kb([btn("📚 Barcha animelar", "ls:0")]),
    )


async def do_start(bot: Bot, m: Message, command: CommandObject, state: FSMContext) -> None:
    await state.clear()
    await touch_user(m.from_user)
    arg = (command.args or "").strip()
    if arg:   # t.me/bot?start=10001
        await run_query(bot, m.from_user.id, arg)
        return
    await m.answer(welcome(m.from_user.full_name), reply_markup=USER_MENU)


@user_router.message(CommandStart())
async def user_start(m: Message, command: CommandObject, state: FSMContext, bot: Bot):
    await do_start(bot, m, command, state)


@user_router.message(Command("help"))
async def user_help(m: Message):
    await m.answer(HELP_TEXT)


@user_router.message(Command("list"))
@user_router.message(F.text == "📚 Barcha animelar")
async def user_list(m: Message):
    text, markup = list_view(0)
    await m.answer(text, reply_markup=markup)


@user_router.message(Command("top"))
@user_router.message(F.text == "🔥 Mashhurlar")
async def user_top(m: Message):
    text, markup = top_view()
    await m.answer(text, reply_markup=markup)


@user_router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


@user_router.callback_query(F.data.startswith("ls:"))
async def cb_list(cb: CallbackQuery):
    await cb.answer()
    try:
        page = int(cb.data.split(":")[1])
    except (ValueError, IndexError):
        page = 0
    text, markup = list_view(page)
    await show(cb, text, markup)


@user_router.callback_query(F.data.startswith("a:"))
async def cb_anime(cb: CallbackQuery, bot: Bot):
    await cb.answer()
    try:
        code = int(cb.data.split(":")[1])
    except (ValueError, IndexError):
        return
    a = ANIMES.get(code)
    if a is None:
        await cb.message.answer("😔 Bu anime topilmadi (o'chirilgan bo'lishi mumkin).")
        return
    await deliver(bot, cb.from_user.id, a)


@user_router.callback_query(F.data == "chk")
async def cb_check(cb: CallbackQuery, bot: Bot):
    uid = cb.from_user.id
    miss = await missing_subs(bot, uid, force=True)
    if miss:
        await cb.answer(f"❌ Yana {len(miss)} ta kanalga obuna bo'lishingiz kerak", show_alert=True)
        with suppress(TelegramAPIError):
            await cb.message.edit_reply_markup(reply_markup=subs_kb(miss))
        return
    await cb.answer("✅ Rahmat!")
    with suppress(TelegramAPIError):
        await cb.message.delete()
    pending = PENDING.pop(uid, None)
    if pending:
        await run_query(bot, uid, pending)
    else:
        await bot.send_message(uid, welcome(cb.from_user.full_name), reply_markup=USER_MENU)


@user_router.message(TXT, StateFilter(None))
async def user_text(m: Message, bot: Bot):
    await run_query(bot, m.from_user.id, m.text)


@user_router.message(StateFilter(None))
async def user_other(m: Message):
    if m.text and m.text.startswith("/"):
        return
    await m.answer("✍️ Anime <b>nomini</b> yoki <b>kodini</b> matn ko'rinishida yuboring.")


# ══════════════════════════════════════════════════════════════
#                          ADMIN QISMI
# ══════════════════════════════════════════════════════════════

class IsAdmin(BaseFilter):
    async def __call__(self, event: TelegramObject) -> bool:
        u = getattr(event, "from_user", None)
        return bool(u and u.id in ADMINS)


class Add(StatesGroup):
    channel = State()
    title = State()
    code = State()
    poster = State()


class Edit(StatesGroup):
    value = State()


class SubAdd(StatesGroup):
    chat = State()


class AdminAdd(StatesGroup):
    uid = State()


class Bcast(StatesGroup):
    msg = State()
    confirm = State()


adm = Router(name="admin")
adm.message.filter(IsAdmin())
adm.message.filter(F.chat.type == "private")
adm.callback_query.filter(IsAdmin())


def panel_text() -> str:
    return (
        "🛠 <b>Admin panel</b>\n\n"
        f"🍥 Animelar: <b>{len(ANIMES)}</b>\n"
        f"👥 Foydalanuvchilar: <b>{len(USERS)}</b>\n"
        f"📢 Majburiy kanallar: <b>{len(SUBS)}</b>"
    )


def panel_kb(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [btn("🆕 Yopiq guruh yaratish (avto)", "adm:new")],
        [btn("➕ Anime qo'shish", "adm:add"), btn("📋 Animelar", "adm:list:0")],
        [btn("📡 Majburiy obuna", "adm:subs"), btn("📊 Statistika", "adm:stats")],
        [btn("📢 Xabar yuborish", "adm:bc")],
    ]
    if uid == MAIN_ADMIN_ID:
        rows.append([btn("👮 Adminlar", "adm:admins"), btn("🔑 Akkauntni ulash", "adm:login")])
    return kb(*rows)


# ───────────── buyruqlar (holatdan qat'i nazar ishlaydi) ─────────────

@adm.message(CommandStart())
async def adm_start(m: Message, command: CommandObject, state: FSMContext, bot: Bot):
    await do_start(bot, m, command, state)


@adm.message(Command("admin"))
async def cmd_admin(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(panel_text(), reply_markup=panel_kb(m.from_user.id))


@adm.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("✖️ Bekor qilindi.\n\n" + panel_text(), reply_markup=panel_kb(m.from_user.id))


@adm.callback_query(F.data == "adm")
async def cb_panel(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    await show(cb, panel_text(), panel_kb(cb.from_user.id))


# ───────────── kanalni aniqlash va tekshirish ─────────────

def chat_ref_from_message(m: Message) -> int | str | None:
    """Forward qilingan xabar, ID, @username yoki t.me havolasidan kanalni oladi."""
    fo = getattr(m, "forward_origin", None)
    if fo is not None:
        chat = getattr(fo, "chat", None) or getattr(fo, "sender_chat", None)
        return chat.id if chat else None
    t = (m.text or "").strip()
    if not t:
        return None
    if re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{3,}", t):
        return t
    mm = re.search(r"t\.me/c/(\d+)", t)
    if mm:
        return int("-100" + mm.group(1))
    mm = re.search(r"t\.me/([A-Za-z][A-Za-z0-9_]{3,})", t)
    if mm:
        return "@" + mm.group(1)
    if re.fullmatch(r"-?\d{5,20}", t):
        return int(t) if t.startswith("-") else int("-100" + t)
    return None


async def inspect_chat(bot: Bot, ref: int | str) -> tuple[dict[str, Any] | None, str]:
    """Kanal mavjudligi va botning huquqlarini tekshiradi → (ma'lumot, xato matni)."""
    try:
        chat = await bot.get_chat(ref)
    except TelegramAPIError:
        return None, ("❌ Kanal topilmadi yoki bot u yerda yo'q.\n\nAvval botni kanalga "
                      "<b>admin</b> qilib qo'shing, so'ng qayta urinib ko'ring.")
    ctype = str(getattr(chat.type, "value", chat.type))
    if ctype not in ("channel", "supergroup", "group"):
        return None, "❌ Bu kanal yoki guruh emas."
    me = await bot.me()
    try:
        mem = await bot.get_chat_member(chat.id, me.id)
    except TelegramAPIError:
        return None, "❌ Bot bu kanalda admin emas. Uni admin qiling va qayta urinib ko'ring."
    st = _st(mem)
    if st == "administrator":
        if not getattr(mem, "can_invite_users", False):
            return None, ("❌ Botga <b>«Foydalanuvchilarni taklif qilish»</b> huquqini bering "
                          "(kanal → Administratorlar → bot → huquqlar).")
    elif st != "creator":
        return None, "❌ Bot bu kanalda admin emas. Uni admin qiling va qayta urinib ko'ring."
    return {"chat_id": chat.id, "title": chat.title or str(chat.id), "username": chat.username}, ""


def free_chats() -> list[dict[str, Any]]:
    """Bot admin bo'lgan, lekin hali hech qaysi animega ulanmagan kanallar."""
    used = {a["chat_id"] for a in ANIMES.values()}
    return [c for c in CHATS.values() if c["chat_id"] not in used and c.get("can_invite", 1)]


def picker_rows(prefix: str, chats: list[dict[str, Any]]) -> list[list[InlineKeyboardButton]]:
    return [[btn(f"📡 {trim(c['title'] or c['chat_id'], 40)}", f"{prefix}:{c['chat_id']}")] for c in chats[:8]]


CHANNEL_HELP = (
    "Kanalni tanlang yoki yuboring:\n"
    "• yuqoridagi tugmalardan biri (bot admin qilingan kanallar), yoki\n"
    "• kanaldan <b>biror xabarni forward</b> qiling, yoki\n"
    "• kanal <b>ID</b> sini (<code>-100…</code>) / <code>@username</code> ni yuboring.\n\n"
    "⚠️ Bot kanalda <b>admin</b> bo'lishi va <b>«Foydalanuvchilarni taklif qilish»</b> huquqiga ega bo'lishi shart."
)


# ───────────── ANILIST: janr, qismlar, poster avtomatik ─────────────

ANILIST_QUERY = """
query ($q: String) {
  Media(search: $q, type: ANIME, sort: SEARCH_MATCH) {
    genres episodes seasonYear averageScore status
    coverImage { extraLarge large }
  }
}"""

GENRE_UZ = {
    "Action": "Jangari", "Adventure": "Sarguzasht", "Comedy": "Komediya", "Drama": "Drama",
    "Fantasy": "Fantastika", "Horror": "Qo'rqinchli", "Mahou Shoujo": "Sehrli qiz",
    "Mecha": "Mecha", "Music": "Musiqa", "Mystery": "Sirli", "Psychological": "Psixologik",
    "Romance": "Romantika", "Sci-Fi": "Ilmiy fantastika", "Slice of Life": "Kundalik hayot",
    "Sports": "Sport", "Supernatural": "Ruhiy/g'ayritabiiy", "Thriller": "Triller",
    "Ecchi": "Ecchi",
}
STATUS_UZ = {
    "FINISHED": "Tugagan", "RELEASING": "Davom etmoqda", "NOT_YET_RELEASED": "Hali chiqmagan",
    "CANCELLED": "Bekor qilingan", "HIATUS": "Pauzada",
}


async def anilist_lookup(title: str) -> dict[str, Any] | None:
    try:
        async with ClientSession(timeout=ClientTimeout(total=15)) as s:
            async with s.post(
                "https://graphql.anilist.co",
                json={"query": ANILIST_QUERY, "variables": {"q": title}},
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()
    except (ClientError, asyncio.TimeoutError, ValueError):
        return None
    return (data.get("data") or {}).get("Media")


def anilist_text(m: dict[str, Any]) -> str:
    lines = []
    genres = ", ".join(GENRE_UZ.get(g, g) for g in m.get("genres") or [])
    if genres:
        lines.append(f"🎭 Janr: {genres}")
    if m.get("episodes"):
        lines.append(f"🎞 Qismlar: {m['episodes']}")
    if m.get("seasonYear"):
        lines.append(f"📅 Yil: {m['seasonYear']}")
    if m.get("averageScore"):
        lines.append(f"⭐ Reyting: {m['averageScore'] / 10:.1f}/10")
    if STATUS_UZ.get(m.get("status") or ""):
        lines.append(f"📡 Holat: {STATUS_UZ[m['status']]}")
    lines.append("🇺🇿 Til: O'zbekcha")
    return "\n".join(lines)


# ───────────── ANIME QO'SHISH ─────────────

@adm.callback_query(F.data == "adm:add")
async def add_begin(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    await state.set_state(Add.channel)
    rows = picker_rows("ap", free_chats()) + [[btn("✖️ Bekor qilish", "adm")]]
    await show(cb, "➕ <b>Yangi anime</b> — 1/4\n\n📡 Anime joylangan <b>yopiq kanalni</b> ulang.\n\n" + CHANNEL_HELP, kb(*rows))


async def ask_title(bot: Bot, chat_id: int, state: FSMContext, chat: dict[str, Any]) -> None:
    await state.update_data(chat=chat)
    await state.set_state(Add.title)
    await bot.send_message(
        chat_id,
        f"✅ Kanal ulandi: <b>{h(chat['title'])}</b>\n\n✍️ 2/4 — <b>Anime nomini</b> yozing:",
        reply_markup=kb([btn(f"📡 Kanal nomini ishlatish: {trim(chat['title'], 28)}", "at:use")], [btn("✖️ Bekor qilish", "adm")]),
    )


async def ask_code(bot: Bot, chat_id: int, state: FSMContext, title: str) -> None:
    await state.update_data(title=title)
    await state.set_state(Add.code)
    sug = peek_code()
    await bot.send_message(
        chat_id,
        f"🍥 <b>{h(title)}</b>\n\n🔢 3/4 — <b>Kod</b> yuboring (faqat raqam) yoki avtomatik tanlang:",
        reply_markup=kb([btn(f"🔢 Avtomatik: {sug}", "ac:auto")], [btn("✖️ Bekor qilish", "adm")]),
    )


async def ask_poster(bot: Bot, chat_id: int, state: FSMContext, code: int | None) -> None:
    await state.update_data(code=code)
    await state.set_state(Add.poster)
    await bot.send_message(
        chat_id,
        "🖼 4/4 — <b>Poster</b> (rasm) yuboring. Rasm izohi anime <b>tavsifi</b> bo'ladi.\n"
        "Faqat matn yuborsangiz — u tavsif sifatida saqlanadi.\n\nYoki o'tkazib yuboring 👇",
        reply_markup=kb(
            [btn("🤖 Avto: janr, poster, kanal nomi", "as:auto")],
            [btn("⏭ O'tkazib yuborish", "as:skip")],
            [btn("✖️ Bekor qilish", "adm")],
        ),
    )


async def finish_add(bot: Bot, chat_id: int, state: FSMContext, descr: str | None, poster: str | None) -> None:
    d = await state.get_data()
    if not d.get("chat") or not d.get("title"):
        await state.clear()
        await bot.send_message(chat_id, "⚠️ Jarayon uzilib qoldi. Qaytadan boshlang: /admin")
        return
    code = d.get("code") or await alloc_code()
    if code in ANIMES:   # (juda kam) poyga holati
        code = await alloc_code()
    try:
        a = await add_anime(code, d["title"], d["chat"], descr, poster)
    except Exception:  # noqa: BLE001
        log.exception("Anime saqlanmadi")
        await state.clear()
        await bot.send_message(chat_id, "❌ Saqlashda xatolik chiqdi. Qaytadan urinib ko'ring: /admin")
        return
    await state.clear()
    me = await bot.me()
    await bot.send_message(
        chat_id,
        f"✅ <b>Anime qo'shildi!</b>\n\n🍥 {h(a['title'])}\n🔢 Kod: <code>{a['code']}</code>\n"
        f"📡 Kanal: {h(a['chat_title'])}\n\n🔗 Havola: <code>https://t.me/{me.username}?start={a['code']}</code>",
        reply_markup=kb([btn("➕ Yana qo'shish", "adm:add"), btn("📋 Animelar", "adm:list:0")], [btn("🛠 Panel", "adm")]),
    )


@adm.callback_query(F.data.startswith("ap:"), StateFilter(Add.channel))
async def add_pick(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    chat, err = await inspect_chat(bot, int(cb.data.split(":", 1)[1]))
    if chat is None:
        await cb.message.answer(err, reply_markup=CANCEL_KB)
        return
    await ask_title(bot, cb.message.chat.id, state, chat)


@adm.message(StateFilter(Add.channel))
async def add_channel_msg(m: Message, state: FSMContext, bot: Bot):
    ref = chat_ref_from_message(m)
    if ref is None:
        await m.answer("❓ Tushunmadim.\n\n" + CHANNEL_HELP, reply_markup=CANCEL_KB)
        return
    chat, err = await inspect_chat(bot, ref)
    if chat is None:
        await m.answer(err, reply_markup=CANCEL_KB)
        return
    await ask_title(bot, m.chat.id, state, chat)


@adm.callback_query(F.data.startswith("qa:"))
async def quick_add(cb: CallbackQuery, state: FSMContext, bot: Bot):
    """Bot kanalga admin qilinganda kelgan xabardagi «Anime qo'shish» tugmasi."""
    await cb.answer()
    chat, err = await inspect_chat(bot, int(cb.data.split(":", 1)[1]))
    if chat is None:
        await cb.message.answer(err)
        return
    await state.clear()
    await ask_title(bot, cb.message.chat.id, state, chat)


@adm.callback_query(F.data == "at:use", StateFilter(Add.title))
async def add_title_use(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    d = await state.get_data()
    await ask_code(bot, cb.message.chat.id, state, d["chat"]["title"][:120])


@adm.message(TXT, StateFilter(Add.title))
async def add_title(m: Message, state: FSMContext, bot: Bot):
    title = m.text.strip()[:120]
    if not norm(title):
        await m.answer("❗ Nom harf yoki raqamdan iborat bo'lsin. Qaytadan yozing:", reply_markup=CANCEL_KB)
        return
    await ask_code(bot, m.chat.id, state, title)


@adm.callback_query(F.data == "ac:auto", StateFilter(Add.code))
async def add_code_auto(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    await ask_poster(bot, cb.message.chat.id, state, None)


@adm.message(TXT, StateFilter(Add.code))
async def add_code(m: Message, state: FSMContext, bot: Bot):
    t = m.text.strip()
    if not (t.isdigit() and 1 <= len(t) <= 9 and int(t) > 0):
        await m.answer("❗ Kod faqat raqamdan iborat bo'lsin (1–9 xonali). Qaytadan yuboring:", reply_markup=CANCEL_KB)
        return
    if int(t) in ANIMES:
        await m.answer(f"❗ <code>{t}</code> kodi band (<b>{h(ANIMES[int(t)]['title'])}</b>). Boshqasini yuboring:", reply_markup=CANCEL_KB)
        return
    await ask_poster(bot, m.chat.id, state, int(t))


@adm.callback_query(F.data == "as:auto", StateFilter(Add.poster))
async def add_poster_auto(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer("🔎 AniList'dan qidirilmoqda…")
    d = await state.get_data()
    m = await anilist_lookup(d["title"])
    if not m:
        await cb.message.answer(
            "😔 AniList'dan topilmadi. Nomni inglizcha/romaji yozib ko'ring yoki poster yuboring.",
            reply_markup=kb([btn("⏭ O'tkazib yuborish", "as:skip")], [btn("✖️ Bekor qilish", "adm")]),
        )
        return
    cover = m.get("coverImage") or {}
    poster = cover.get("extraLarge") or cover.get("large")
    descr = anilist_text(m)
    chat = d["chat"]
    # Kanalni o'zi sozlaydi (botda «Kanal ma'lumotlarini o'zgartirish» huquqi kerak)
    with suppress(TelegramAPIError):
        await bot.set_chat_description(chat["chat_id"], re.sub(r"[🎭🎞📅⭐📡🇺🇿]\s?", "", descr)[:255])
    try:
        await bot.set_chat_title(chat["chat_id"], d["title"][:128])
        chat["title"] = d["title"][:128]
        await state.update_data(chat=chat)
    except TelegramAPIError:
        pass
    await finish_add(bot, cb.message.chat.id, state, descr, poster)


@adm.callback_query(F.data == "as:skip", StateFilter(Add.poster))
async def add_poster_skip(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    await finish_add(bot, cb.message.chat.id, state, None, None)


@adm.message(F.photo, StateFilter(Add.poster))
async def add_poster_photo(m: Message, state: FSMContext, bot: Bot):
    await finish_add(bot, m.chat.id, state, trim(m.caption or "", 900) or None, m.photo[-1].file_id)


@adm.message(TXT, StateFilter(Add.poster))
async def add_poster_text(m: Message, state: FSMContext, bot: Bot):
    await finish_add(bot, m.chat.id, state, trim(m.text, 900), None)


# ───────────── AVTO GURUH YARATISH (sizning akkauntingiz orqali) ─────────────

USERBOT: Any = None
_AUTO: set[int] = set()
_create_lock = asyncio.Lock()


async def start_userbot() -> None:
    global USERBOT
    session = TG_SESSION or KV.get("userbot_session", "")
    if not (API_ID and API_HASH and session):
        log.info("Userbot ulanmagan (botda «🔑 Akkauntni ulash» tugmasini bosing)")
        return
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
    except ImportError:
        log.warning("telethon o'rnatilmagan")
        return
    c = TelegramClient(StringSession(session), API_ID, API_HASH)
    await c.connect()
    if not await c.is_user_authorized():
        log.warning("TG_SESSION yaroqsiz — make_session.py bilan qayta oling")
        await c.disconnect()
        return
    USERBOT = c
    log.info("👤 Userbot ulandi")


async def create_private_group(bot: Bot, title: str, about: str, photo_url: str | None) -> int:
    from telethon import functions, types

    c = USERBOT
    res = await c(functions.channels.CreateChannelRequest(title=title[:128], about=about[:255], megagroup=True))
    ch = res.chats[0]
    chat_id = int(f"-100{ch.id}")
    _AUTO.add(chat_id)
    peer = await c.get_input_entity(ch)
    # hamma ruxsatlar ochiq (stiker, GIF, media, havola, so'rovnoma...)
    # Oddiy a'zo: FAQAT ko'rish. Yozish, stiker, media, havola, taklif — hammasi yopiq.
    import inspect
    flags = dict(
        view_messages=False, send_messages=True, send_media=True, send_stickers=True,
        send_gifs=True, send_games=True, send_inline=True, embed_links=True, send_polls=True,
        change_info=True, invite_users=True, pin_messages=True, manage_topics=True,
        send_photos=True, send_videos=True, send_roundvideos=True, send_audios=True,
        send_voices=True, send_docs=True, send_plain=True,
    )
    known = set(inspect.signature(types.ChatBannedRights.__init__).parameters)
    rights = types.ChatBannedRights(until_date=None, **{k: v for k, v in flags.items() if k in known})
    await c(functions.messages.EditChatDefaultBannedRightsRequest(peer=peer, banned_rights=rights))
    # Forward qilish, nusxalash va saqlashni taqiqlash
    with suppress(Exception):
        await c(functions.channels.ToggleNoForwardsRequest(channel=peer, enabled=True))
    # Yangi a'zolar oldingi qismlarni ham ko'rsin
    with suppress(Exception):
        await c(functions.channels.TogglePreHistoryHiddenRequest(channel=peer, enabled=False))
    if photo_url:
        try:
            async with ClientSession(timeout=ClientTimeout(total=20)) as s:
                async with s.get(photo_url) as r:
                    data = await r.read()
            f = await c.upload_file(data, file_name="poster.jpg")
            await c(functions.channels.EditPhotoRequest(
                channel=peer, photo=types.InputChatUploadedPhoto(file=f)))
        except Exception:  # noqa: BLE001 — rasm bo'lmasa ham guruh ishlayveradi
            log.exception("Guruh rasmi o'rnatilmadi")
    me = await bot.me()
    await c.edit_admin(peer, me.username, is_admin=True, invite_users=True,
                       change_info=True, delete_messages=True, pin_messages=True)
    my_id = (await c.get_me()).id
    for aid in list(ADMINS):   # bot adminlari guruhda yoza olsin
        if aid == my_id:
            continue
        try:
            await c.edit_admin(peer, aid, is_admin=True, change_info=True,
                               delete_messages=True, pin_messages=True, invite_users=True)
        except Exception as e:  # noqa: BLE001 — akkaunt tanish bo'lmasa o'tkazib yuboriladi
            log.warning("Admin %s guruhga qo'shilmadi: %s", aid, e)
    return chat_id


class Login(StatesGroup):
    phone = State()
    code = State()
    password = State()


_LOGIN: dict[int, dict[str, Any]] = {}


async def _del(m: Message) -> None:
    with suppress(TelegramAPIError):
        await m.delete()


async def _login_done(m: Message, state: FSMContext, client: Any) -> None:
    global USERBOT
    sess = client.session.save()
    KV["userbot_session"] = sess
    await db.execute("INSERT OR REPLACE INTO ab_kv(key, value) VALUES ('userbot_session', ?)", [sess])
    USERBOT = client
    _LOGIN.pop(m.from_user.id, None)
    await state.clear()
    await m.answer("✅ <b>Akkaunt ulandi!</b> Endi «🆕 Yopiq guruh yaratish» ishlaydi.",
                   reply_markup=kb([btn("🛠 Panel", "adm")]))


@adm.callback_query(F.data == "adm:login")
async def login_begin(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != MAIN_ADMIN_ID:
        await cb.answer("Faqat asosiy admin uchun", show_alert=True)
        return
    await cb.answer()
    await state.clear()
    if not (API_ID and API_HASH):
        await show(cb, "⚠️ Avval Render'da <code>API_ID</code> va <code>API_HASH</code> ni kiriting "
                       "(my.telegram.org → API development tools).", kb([btn("⬅️ Orqaga", "adm")]))
        return
    await state.set_state(Login.phone)
    await show(cb, "🔑 Guruhlar yaratiladigan <b>Telegram akkaunt raqamini</b> yuboring (masalan <code>+998901234567</code>).\n\n"
                   "⚠️ Bu akkauntga to'liq kirish beriladi, faqat o'z akkauntingizni ulang.", CANCEL_KB)


@adm.message(TXT, StateFilter(Login.phone))
async def login_phone(m: Message, state: FSMContext):
    if m.from_user.id != MAIN_ADMIN_ID:
        return
    phone = re.sub(r"[^\d+]", "", m.text)
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        sent = await client.send_code_request(phone)
    except Exception as e:  # noqa: BLE001
        await client.disconnect()
        await m.answer(f"❌ Kod yuborilmadi: <code>{h(str(e)[:200])}</code>\nRaqamni tekshirib qayta yuboring.", reply_markup=CANCEL_KB)
        return
    _LOGIN[m.from_user.id] = {"client": client, "phone": phone, "hash": sent.phone_code_hash}
    await state.set_state(Login.code)
    await m.answer(
        "📩 Telegram ilovangizga <b>kod</b> keldi.\n\n"
        "Telegram kodni oddiy yozsangiz bekor qilib yuboradi, shuning uchun <b>raqamlar orasiga tire</b> qo'yib yozing: "
        "<code>1-2-3-4-5</code>", reply_markup=CANCEL_KB)


@adm.message(TXT, StateFilter(Login.code))
async def login_code(m: Message, state: FSMContext):
    from telethon.errors import PhoneCodeExpiredError, PhoneCodeInvalidError, SessionPasswordNeededError
    d = _LOGIN.get(m.from_user.id)
    if m.from_user.id != MAIN_ADMIN_ID or not d:
        return
    await _del(m)
    code = re.sub(r"\D", "", m.text)
    try:
        await d["client"].sign_in(d["phone"], code, phone_code_hash=d["hash"])
    except SessionPasswordNeededError:
        await state.set_state(Login.password)
        await m.answer("🔐 Akkauntda ikki bosqichli parol bor. Parolni yuboring (xabar darhol o'chiriladi).", reply_markup=CANCEL_KB)
        return
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        await m.answer("❌ Kod noto'g'ri yoki eskirgan. /cancel qilib, «🔑 Akkauntni ulash» ni qaytadan bosing.", reply_markup=CANCEL_KB)
        return
    except Exception as e:  # noqa: BLE001
        await m.answer(f"❌ Xatolik: <code>{h(str(e)[:200])}</code>", reply_markup=CANCEL_KB)
        return
    await _login_done(m, state, d["client"])


@adm.message(TXT, StateFilter(Login.password))
async def login_password(m: Message, state: FSMContext):
    d = _LOGIN.get(m.from_user.id)
    if m.from_user.id != MAIN_ADMIN_ID or not d:
        return
    await _del(m)
    try:
        await d["client"].sign_in(password=m.text)
    except Exception as e:  # noqa: BLE001
        await m.answer(f"❌ Parol noto'g'ri yoki xato: <code>{h(str(e)[:150])}</code>\nQayta yuboring.", reply_markup=CANCEL_KB)
        return
    await _login_done(m, state, d["client"])


class NewGrp(StatesGroup):
    title = State()


@adm.callback_query(F.data == "adm:new")
async def new_begin(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    if USERBOT is None:
        await show(cb, "⚠️ Akkaunt ulanmagan. Panelda «🔑 Akkauntni ulash» tugmasini bosing.",
                   kb([btn("⬅️ Orqaga", "adm")]))
        return
    await state.set_state(NewGrp.title)
    await show(cb, "🆕 <b>Anime nomini</b> yozing. Bir nechta bo'lsa, <b>har qatorga bittadan</b> yozing (eng ko'pi 10 ta).\n\n"
                   "Inglizcha/romaji nom bo'lsa poster va janr aniqroq topiladi.", CANCEL_KB)


async def create_one(bot: Bot, title: str) -> dict[str, Any]:
    """Bitta anime uchun guruh yaratadi va bazaga qo'shadi."""
    info = await anilist_lookup(title)
    descr = anilist_text(info) if info else None
    cover = (info or {}).get("coverImage") or {}
    poster = cover.get("extraLarge") or cover.get("large")
    about = re.sub(r"[🎭🎞📅⭐📡🇺🇿]\s?", "", descr or "")
    chat_id = await create_private_group(bot, title, about, poster)
    chat = None
    for _ in range(5):   # bot admin bo'lishini kutamiz
        await asyncio.sleep(1.5)
        chat, _err = await inspect_chat(bot, chat_id)
        if chat:
            break
    if chat is None:
        raise RuntimeError("Guruh yaratildi, lekin bot admin bo'lmadi")
    return await add_anime(await alloc_code(), title, chat, descr, poster)


async def run_batch(bot: Bot, admin_id: int, titles: list[str]) -> None:
    done: list[str] = []
    failed: list[str] = []
    me = await bot.me()
    async with _create_lock:
        for i, title in enumerate(titles):
            try:
                a = await create_one(bot, title)
                done.append(f"✅ {h(a['title'])} — <code>{a['code']}</code>")
            except Exception as e:  # noqa: BLE001
                log.exception("Avto-yaratish xatosi: %s", title)
                wait = getattr(e, "seconds", None)   # Telegram FloodWait
                if wait:
                    rest = titles[i:]
                    failed += [f"⏸ {h(t)}" for t in rest]
                    failed.append(f"\n⏳ Telegram {wait} soniya kutishni so'radi. Shundan keyin qolganlarini qayta yuboring.")
                    break
                failed.append(f"❌ {h(title)} — {h(str(e)[:120])}")
            if i < len(titles) - 1:
                await asyncio.sleep(25)   # Telegram cheklovidan saqlanish
    text = "🏁 <b>Tugadi</b>\n\n" + "\n".join(done + failed)
    if done:
        text += f"\n\n🔗 Havola: <code>https://t.me/{me.username}?start=KOD</code>"
    with suppress(TelegramAPIError):
        await bot.send_message(admin_id, text, reply_markup=kb([btn("🆕 Yana yaratish", "adm:new")], [btn("🛠 Panel", "adm")]))


@adm.message(TXT, StateFilter(NewGrp.title))
async def new_title(m: Message, state: FSMContext, bot: Bot):
    titles = [t.strip()[:120] for t in m.text.splitlines() if norm(t)][:10]
    if not titles:
        await m.answer("❗ Nom noto'g'ri. Qaytadan yozing:", reply_markup=CANCEL_KB)
        return
    await state.clear()
    mins = max(1, len(titles) * 30 // 60)
    await m.answer(
        f"⏳ <b>{len(titles)}</b> ta guruh yaratilmoqda (taxminan {mins} daqiqa). "
        "Tugagach xabar beraman — bu orada botdan boshqa narsa qilishingiz mumkin."
    )
    fire(run_batch(bot, m.chat.id, titles))


# ───────────── RO'YXAT / TAFSILOT / TAHRIRLASH / O'CHIRISH ─────────────

@adm.callback_query(F.data.startswith("adm:list:"))
async def adm_list(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    try:
        page = int(cb.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    items = sorted(ANIMES.values(), key=lambda a: -a["created_at"])
    if not items:
        await show(cb, "📋 Hozircha anime yo'q.", kb([btn("➕ Anime qo'shish", "adm:add")], [btn("⬅️ Orqaga", "adm")]))
        return
    pages = max(1, math.ceil(len(items) / PER_PAGE))
    page = max(0, min(page, pages - 1))
    rows = [[btn(f"{a['code']} · {trim(a['title'], 40)}", f"adm:a:{a['code']}")] for a in items[page * PER_PAGE:(page + 1) * PER_PAGE]]
    rows.append(nav_row("adm:list", page, pages))
    rows.append([btn("⬅️ Orqaga", "adm")])
    await show(cb, f"📋 <b>Animelar</b> ({len(items)} ta) — yangilari birinchi:", kb(*rows))


async def detail(bot: Bot, a: dict[str, Any]) -> tuple[str, InlineKeyboardMarkup]:
    me = await bot.me()
    text = (
        f"🍥 <b>{h(a['title'])}</b>\n\n"
        f"🔢 Kod: <code>{a['code']}</code>\n"
        f"📡 Kanal: {h(a['chat_title'])} (<code>{a['chat_id']}</code>)\n"
        f"👁 Ko'rishlar: {a['views']}\n"
        f"🖼 Poster: {'bor' if a.get('poster') else 'yo‘q'} · 📝 Tavsif: {'bor' if a.get('descr') else 'yo‘q'}\n\n"
        f"🔗 <code>https://t.me/{me.username}?start={a['code']}</code>"
    )
    c = a["code"]
    markup = kb(
        [btn("✏️ Nomi", f"ed:title:{c}"), btn("🔢 Kod", f"ed:code:{c}")],
        [btn("📡 Kanal", f"ed:chan:{c}"), btn("🖼 Poster/tavsif", f"ed:post:{c}")],
        [btn("🧪 Kanalni tekshirish", f"adm:t:{c}")],
        [btn("🗑 O'chirish", f"adm:d:{c}")],
        [btn("⬅️ Ro'yxat", "adm:list:0")],
    )
    return text, markup


@adm.callback_query(F.data.startswith("adm:a:"))
async def adm_detail(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    await state.clear()
    a = ANIMES.get(int(cb.data.split(":")[2]))
    if not a:
        await show(cb, "😔 Anime topilmadi.", kb([btn("⬅️ Ro'yxat", "adm:list:0")]))
        return
    text, markup = await detail(bot, a)
    await show(cb, text, markup)


@adm.callback_query(F.data.startswith("adm:t:"))
async def adm_test(cb: CallbackQuery, bot: Bot):
    a = ANIMES.get(int(cb.data.split(":")[2]))
    if not a:
        await cb.answer("Topilmadi", show_alert=True)
        return
    chat, err = await inspect_chat(bot, a["chat_id"])
    if chat is None:
        await cb.answer(re.sub(r"<[^>]+>", "", err)[:190], show_alert=True)
    else:
        await cb.answer("✅ Hammasi joyida: bot kanalda admin va havola yarata oladi.", show_alert=True)


@adm.callback_query(F.data.startswith("adm:d:"))
async def adm_delete_ask(cb: CallbackQuery):
    await cb.answer()
    c = int(cb.data.split(":")[2])
    a = ANIMES.get(c)
    if not a:
        return
    await show(
        cb, f"🗑 <b>{h(a['title'])}</b> (kod {c}) o'chirilsinmi?\n\n<i>Kanalning o'zi o'chmaydi, faqat botdagi yozuv.</i>",
        kb([btn("✅ Ha, o'chirish", f"adm:dy:{c}"), btn("✖️ Yo'q", f"adm:a:{c}")]),
    )


@adm.callback_query(F.data.startswith("adm:dy:"))
async def adm_delete(cb: CallbackQuery):
    await cb.answer("O'chirildi")
    await delete_anime(int(cb.data.split(":")[2]))
    await show(cb, "🗑 Anime o'chirildi.", kb([btn("📋 Animelar", "adm:list:0")], [btn("🛠 Panel", "adm")]))


@adm.callback_query(F.data.startswith("ed:"))
async def edit_begin(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    _, field, code_s = cb.data.split(":")
    code = int(code_s)
    a = ANIMES.get(code)
    if not a:
        return
    await state.clear()
    await state.set_state(Edit.value)
    await state.update_data(code=code, field=field)
    back = btn("✖️ Bekor qilish", f"adm:a:{code}")
    if field == "title":
        await show(cb, f"✏️ Hozirgi nom: <b>{h(a['title'])}</b>\n\nYangi nomni yozing:", kb([back]))
    elif field == "code":
        await show(cb, f"🔢 Hozirgi kod: <code>{code}</code>\n\nYangi kodni yuboring (faqat raqam):", kb([back]))
    elif field == "chan":
        rows = picker_rows("ep", free_chats()) + [[back]]
        await show(cb, "📡 <b>Yangi kanalni</b> ulang.\n\n" + CHANNEL_HELP, kb(*rows))
    else:
        await show(
            cb,
            "🖼 <b>Poster</b> (rasm) yuboring — rasm izohi tavsif bo'ladi.\nFaqat matn yuborsangiz, u tavsif sifatida saqlanadi.",
            kb([btn("🧹 Poster va tavsifni tozalash", "ex:clear")], [back]),
        )


async def edit_done(bot: Bot, chat_id: int, state: FSMContext, code: int) -> None:
    await state.clear()
    a = ANIMES.get(code)
    if not a:
        return
    text, markup = await detail(bot, a)
    await bot.send_message(chat_id, "✅ Saqlandi.\n\n" + text, reply_markup=markup)


@adm.callback_query(F.data.startswith("ep:"), StateFilter(Edit.value))
async def edit_pick_chat(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    d = await state.get_data()
    chat, err = await inspect_chat(bot, int(cb.data.split(":", 1)[1]))
    if chat is None:
        await cb.message.answer(err)
        return
    await edit_anime(d["code"], chat_id=chat["chat_id"], chat_title=chat["title"], chat_username=chat["username"])
    await edit_done(bot, cb.message.chat.id, state, d["code"])


@adm.callback_query(F.data == "ex:clear", StateFilter(Edit.value))
async def edit_clear_post(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    d = await state.get_data()
    await edit_anime(d["code"], poster=None, descr=None)
    await edit_done(bot, cb.message.chat.id, state, d["code"])


@adm.message(StateFilter(Edit.value))
async def edit_value(m: Message, state: FSMContext, bot: Bot):
    d = await state.get_data()
    code, field = d.get("code"), d.get("field")
    a = ANIMES.get(code)
    if a is None:
        await state.clear()
        await m.answer("😔 Anime topilmadi.")
        return
    back = kb([btn("✖️ Bekor qilish", f"adm:a:{code}")])
    if m.text and m.text.startswith("/"):
        await m.answer("❗ Bu buyruq. Qiymatni yuboring yoki /cancel.", reply_markup=back)
        return

    if field == "title":
        if not (m.text and norm(m.text)):
            await m.answer("❗ Nomni matn ko'rinishida yozing:", reply_markup=back)
            return
        await edit_anime(code, title=m.text.strip()[:120])
    elif field == "code":
        t = (m.text or "").strip()
        if not (t.isdigit() and 1 <= len(t) <= 9 and int(t) > 0):
            await m.answer("❗ Kod faqat raqam bo'lsin (1–9 xonali):", reply_markup=back)
            return
        new = int(t)
        if new != code and new in ANIMES:
            await m.answer(f"❗ <code>{new}</code> kodi band. Boshqasini yuboring:", reply_markup=back)
            return
        await edit_anime(code, code=new)
        code = new
    elif field == "chan":
        ref = chat_ref_from_message(m)
        if ref is None:
            await m.answer("❓ Tushunmadim.\n\n" + CHANNEL_HELP, reply_markup=back)
            return
        chat, err = await inspect_chat(bot, ref)
        if chat is None:
            await m.answer(err, reply_markup=back)
            return
        await edit_anime(code, chat_id=chat["chat_id"], chat_title=chat["title"], chat_username=chat["username"])
    else:   # post
        if m.photo:
            upd: dict[str, Any] = {"poster": m.photo[-1].file_id}
            if m.caption:
                upd["descr"] = trim(m.caption, 900)
            await edit_anime(code, **upd)
        elif m.text:
            await edit_anime(code, descr=trim(m.text, 900))
        else:
            await m.answer("❗ Rasm yoki matn yuboring:", reply_markup=back)
            return
    await edit_done(bot, m.chat.id, state, code)


# ───────────── STATISTIKA ─────────────

@adm.callback_query(F.data == "adm:stats")
async def adm_stats(cb: CallbackQuery):
    await cb.answer()
    day0 = (now() + TZ_OFFSET) // 86400 * 86400 - TZ_OFFSET
    today = await db.fetchval("SELECT COUNT(*) FROM ab_users WHERE joined_at >= ?", [day0], 0)
    week = await db.fetchval("SELECT COUNT(*) FROM ab_users WHERE joined_at >= ?", [now() - 7 * 86400], 0)
    total_views = sum(a["views"] for a in ANIMES.values())
    top = sorted(ANIMES.values(), key=lambda a: -a["views"])[:5]
    top_txt = "\n".join(f"{i}. {h(trim(a['title'], 34))} — 👁 {a['views']}" for i, a in enumerate(top, 1)) or "—"
    await show(
        cb,
        "📊 <b>Statistika</b>\n\n"
        f"👥 Jami foydalanuvchilar: <b>{len(USERS)}</b>\n"
        f"🆕 Bugun qo'shildi: <b>{today}</b>\n"
        f"📅 So'nggi 7 kun: <b>{week}</b>\n\n"
        f"🍥 Animelar: <b>{len(ANIMES)}</b>\n"
        f"👁 Jami ko'rishlar: <b>{total_views}</b>\n\n"
        f"🔥 <b>Top 5:</b>\n{top_txt}",
        kb([btn("⬅️ Orqaga", "adm")]),
    )


# ───────────── MAJBURIY OBUNA ─────────────

def subs_admin_view() -> tuple[str, InlineKeyboardMarkup]:
    rows = [[btn(f"🗑 {trim(c['title'] or c['chat_id'], 36)}", f"sd:{c['chat_id']}")] for c in SUBS.values()]
    rows.append([btn("➕ Kanal qo'shish", "sa")])
    rows.append([btn("⬅️ Orqaga", "adm")])
    text = "📡 <b>Majburiy obuna kanallari</b>\n\n"
    text += ("Foydalanuvchi botdan foydalanishdan oldin quyidagilarga obuna bo'lishi shart.\n"
             "O'chirish uchun kanal tugmasini bosing.") if SUBS else "Hozircha yo'q. Kanal qo'shsangiz, foydalanuvchilar avval unga obuna bo'ladi."
    return text, kb(*rows)


@adm.callback_query(F.data == "adm:subs")
async def adm_subs(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    text, markup = subs_admin_view()
    await show(cb, text, markup)


@adm.callback_query(F.data.startswith("sd:"))
async def sub_delete(cb: CallbackQuery):
    await cb.answer("O'chirildi")
    cid = int(cb.data.split(":", 1)[1])
    await db.execute("DELETE FROM ab_subs WHERE chat_id = ?", [cid])
    SUBS.pop(cid, None)
    _PASS.clear()
    text, markup = subs_admin_view()
    await show(cb, text, markup)


@adm.callback_query(F.data == "sa")
async def sub_add_begin(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    await state.set_state(SubAdd.chat)
    chats = [c for c in CHATS.values() if c["chat_id"] not in SUBS]
    rows = picker_rows("sp", chats) + [[btn("✖️ Bekor qilish", "adm:subs")]]
    await show(cb, "📡 <b>Majburiy obuna kanali</b>\n\n" + CHANNEL_HELP, kb(*rows))


async def sub_save(bot: Bot, chat_id: int, state: FSMContext, ref: int | str) -> None:
    chat, err = await inspect_chat(bot, ref)
    if chat is None:
        await bot.send_message(chat_id, err, reply_markup=kb([btn("✖️ Bekor qilish", "adm:subs")]))
        return
    if chat["username"]:
        url = f"https://t.me/{chat['username']}"
    else:
        try:
            link = await bot.create_chat_invite_link(chat["chat_id"], name="Majburiy obuna")
            url = link.invite_link
        except TelegramAPIError as e:
            await bot.send_message(chat_id, f"❌ Havola yaratib bo'lmadi: <code>{h(e)}</code>")
            return
    await db.execute(
        "INSERT OR REPLACE INTO ab_subs(chat_id, title, url) VALUES (?,?,?)",
        [chat["chat_id"], chat["title"], url],
    )
    SUBS[chat["chat_id"]] = {"chat_id": chat["chat_id"], "title": chat["title"], "url": url}
    _PASS.clear()
    await state.clear()
    text, markup = subs_admin_view()
    await bot.send_message(chat_id, f"✅ <b>{h(chat['title'])}</b> majburiy obunaga qo'shildi.\n\n" + text, reply_markup=markup)


@adm.callback_query(F.data.startswith("sp:"))
async def sub_pick(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    await sub_save(bot, cb.message.chat.id, state, int(cb.data.split(":", 1)[1]))


@adm.callback_query(F.data.startswith("qs:"))
async def sub_quick(cb: CallbackQuery, state: FSMContext, bot: Bot):
    await cb.answer()
    await sub_save(bot, cb.message.chat.id, state, int(cb.data.split(":", 1)[1]))


@adm.message(StateFilter(SubAdd.chat))
async def sub_add_msg(m: Message, state: FSMContext, bot: Bot):
    ref = chat_ref_from_message(m)
    if ref is None:
        await m.answer("❓ Tushunmadim.\n\n" + CHANNEL_HELP, reply_markup=kb([btn("✖️ Bekor qilish", "adm:subs")]))
        return
    await sub_save(bot, m.chat.id, state, ref)


# ───────────── ADMINLAR (faqat asosiy admin) ─────────────

def admins_view() -> tuple[str, InlineKeyboardMarkup]:
    rows = [
        [btn(f"🗑 {aid}", f"ar:{aid}")] for aid in sorted(ADMINS) if aid != MAIN_ADMIN_ID
    ]
    rows.append([btn("➕ Admin qo'shish", "aa")])
    rows.append([btn("⬅️ Orqaga", "adm")])
    others = [a for a in sorted(ADMINS) if a != MAIN_ADMIN_ID]
    text = f"👮 <b>Adminlar</b>\n\n👑 Asosiy: <code>{MAIN_ADMIN_ID}</code>\n"
    text += ("\nBoshqalar (o'chirish uchun bosing):\n" + "\n".join(f"• <code>{a}</code>" for a in others)) if others else "\nBoshqa adminlar yo'q."
    return text, kb(*rows)


@adm.callback_query(F.data == "adm:admins")
async def adm_admins(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != MAIN_ADMIN_ID:
        await cb.answer("Faqat asosiy admin uchun", show_alert=True)
        return
    await cb.answer()
    await state.clear()
    text, markup = admins_view()
    await show(cb, text, markup)


@adm.callback_query(F.data == "aa")
async def admin_add_begin(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id != MAIN_ADMIN_ID:
        await cb.answer("Faqat asosiy admin uchun", show_alert=True)
        return
    await cb.answer()
    await state.set_state(AdminAdd.uid)
    await show(
        cb, "➕ Yangi adminning <b>Telegram ID</b> sini yuboring yoki uning biror xabarini forward qiling.\n\n"
            "ID ni @userinfobot orqali bilib olish mumkin.",
        kb([btn("✖️ Bekor qilish", "adm:admins")]),
    )


@adm.message(StateFilter(AdminAdd.uid))
async def admin_add_msg(m: Message, state: FSMContext):
    if m.from_user.id != MAIN_ADMIN_ID:
        return
    uid: int | None = None
    fo = getattr(m, "forward_origin", None)
    if fo is not None and getattr(fo, "sender_user", None):
        uid = fo.sender_user.id
    elif m.text and m.text.strip().isdigit():
        uid = int(m.text.strip())
    if not uid:
        await m.answer("❓ ID raqamini yuboring yoki foydalanuvchi xabarini forward qiling.",
                       reply_markup=kb([btn("✖️ Bekor qilish", "adm:admins")]))
        return
    await db.execute("INSERT OR IGNORE INTO ab_admins(telegram_id, added_at) VALUES (?,?)", [uid, now()])
    ADMINS.add(uid)
    await state.clear()
    text, markup = admins_view()
    await m.answer(f"✅ <code>{uid}</code> admin qilindi.\n\n" + text, reply_markup=markup)


@adm.callback_query(F.data.startswith("ar:"))
async def admin_remove(cb: CallbackQuery):
    if cb.from_user.id != MAIN_ADMIN_ID:
        await cb.answer("Faqat asosiy admin uchun", show_alert=True)
        return
    uid = int(cb.data.split(":", 1)[1])
    if uid == MAIN_ADMIN_ID:
        await cb.answer("Asosiy adminni o'chirib bo'lmaydi", show_alert=True)
        return
    await cb.answer("O'chirildi")
    await db.execute("DELETE FROM ab_admins WHERE telegram_id = ?", [uid])
    ADMINS.discard(uid)
    text, markup = admins_view()
    await show(cb, text, markup)


# ───────────── XABAR YUBORISH (broadcast) ─────────────

@adm.callback_query(F.data == "adm:bc")
async def bc_begin(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.clear()
    await state.set_state(Bcast.msg)
    await show(
        cb, f"📢 Barcha <b>{len(USERS)}</b> ta foydalanuvchiga yuboriladigan xabarni yuboring "
            "(matn, rasm, video — istalgan).",
        CANCEL_KB,
    )


@adm.message(StateFilter(Bcast.msg))
async def bc_msg(m: Message, state: FSMContext, bot: Bot):
    if m.text and m.text.startswith("/"):
        await m.answer("❗ Bu buyruq. Yuboriladigan xabarni yuboring yoki /cancel.")
        return
    await state.update_data(src_chat=m.chat.id, src_msg=m.message_id)
    await state.set_state(Bcast.confirm)
    with suppress(TelegramAPIError):
        await bot.copy_message(m.chat.id, m.chat.id, m.message_id)
    await m.answer(
        f"👆 Shu xabar <b>{len(USERS)}</b> ta foydalanuvchiga yuboriladi. Tasdiqlaysizmi?",
        reply_markup=kb([btn("✅ Yuborish", "bc:yes"), btn("✖️ Yo'q", "adm")]),
    )


async def send_one(bot: Bot, uid: int, src_chat: int, src_msg: int) -> bool:
    for _ in range(2):
        try:
            await bot.copy_message(uid, src_chat, src_msg)
            return True
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except (TelegramForbiddenError, TelegramBadRequest):
            return False
        except TelegramAPIError:
            return False
    return False


async def run_broadcast(bot: Bot, admin_id: int, src_chat: int, src_msg: int) -> None:
    users = list(USERS)
    ok = 0
    t0 = time.time()
    for i in range(0, len(users), 20):   # ~20 xabar/soniya — Telegram limitidan past
        res = await asyncio.gather(*(send_one(bot, u, src_chat, src_msg) for u in users[i:i + 20]))
        ok += sum(res)
        await asyncio.sleep(1.0)
    with suppress(TelegramAPIError):
        await bot.send_message(
            admin_id,
            f"📢 <b>Xabar yuborish tugadi</b>\n\n✅ Yetkazildi: <b>{ok}</b>\n"
            f"❌ Yetmadi (botni bloklagan): <b>{len(users) - ok}</b>\n⏱ {int(time.time() - t0)} soniya",
        )


@adm.callback_query(F.data == "bc:yes", StateFilter(Bcast.confirm))
async def bc_go(cb: CallbackQuery, state: FSMContext, bot: Bot):
    d = await state.get_data()
    await state.clear()
    await cb.answer("Yuborish boshlandi")
    await show(cb, f"🚀 Yuborish boshlandi ({len(USERS)} ta). Tugagach xabar beraman.", kb([btn("🛠 Panel", "adm")]))
    fire(run_broadcast(bot, cb.from_user.id, d["src_chat"], d["src_msg"]))


# ══════════════════════════════════════════════════════════════
#         BOT KANALGA ADMIN QILINGANDA / CHIQARILGANDA
# ══════════════════════════════════════════════════════════════

system_router = Router(name="system")


@system_router.my_chat_member()
async def on_my_chat_member(ev: ChatMemberUpdated, bot: Bot):
    chat = ev.chat
    ctype = str(getattr(chat.type, "value", chat.type))
    if ctype == "private":
        return
    st = _st(ev.new_chat_member)

    if st != "administrator":   # chiqarildi / bloklandi / oddiy a'zoga tushirildi
        if CHATS.pop(chat.id, None) is not None:
            await db.execute("DELETE FROM ab_chats WHERE chat_id = ?", [chat.id])
        return
    if chat.id in _AUTO:
        return   # avto-yaratilgan guruh, o'zi ro'yxatga olinadi
    if ev.from_user.id not in ADMINS:
        return   # begonalar qo'shgan kanallar e'tiborga olinmaydi

    can_invite = bool(getattr(ev.new_chat_member, "can_invite_users", False))
    rec = {"chat_id": chat.id, "title": chat.title or str(chat.id), "username": chat.username,
           "can_invite": int(can_invite), "added_at": now()}
    CHATS[chat.id] = rec
    await db.execute(
        "INSERT OR REPLACE INTO ab_chats(chat_id, title, username, can_invite, added_at) VALUES (?,?,?,?,?)",
        [chat.id, rec["title"], rec["username"], rec["can_invite"], rec["added_at"]],
    )
    if not can_invite:
        await bot.send_message(
            ev.from_user.id,
            f"⚠️ Bot <b>{h(rec['title'])}</b> kanaliga admin qilindi, lekin <b>«Foydalanuvchilarni taklif "
            f"qilish»</b> huquqi yo'q.\n\nBu huquqsiz bot kirish havolasi yarata olmaydi. Huquqni yoqing.",
        )
        return
    await bot.send_message(
        ev.from_user.id,
        f"✅ Bot <b>{h(rec['title'])}</b> kanaliga admin qilindi.\n\nNima qilamiz?",
        reply_markup=kb([btn("➕ Shu kanal uchun anime qo'shish", f"qa:{chat.id}")],
                        [btn("📢 Majburiy obunaga qo'shish", f"qs:{chat.id}")]),
    )


# ══════════════════════════════════════════════════════════════
#                  HEALTH-SERVER va FON VAZIFALARI
# ══════════════════════════════════════════════════════════════

async def _health(_: web.Request) -> web.Response:
    return web.Response(text="🍥 Anime bot ishlayapti ✅")


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/health", _health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("🌐 Health-server: 0.0.0.0:%d", PORT)
    return runner


async def views_loop() -> None:
    while True:
        await asyncio.sleep(60)
        await flush_views()


async def keepalive_loop() -> None:
    """Render bepul rejasi 15 daqiqa trafiksiz qolsa uxlaydi — o'zimizni ping qilamiz."""
    url = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("PING_URL")
    if not url:
        return
    async with ClientSession(timeout=ClientTimeout(total=20)) as s:
        while True:
            await asyncio.sleep(600)
            with suppress(Exception):
                async with s.get(url):
                    pass


# ══════════════════════════════════════════════════════════════
#                          ISHGA TUSHIRISH
# ══════════════════════════════════════════════════════════════

async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("❌ BOT_TOKEN topilmadi! Render → Environment bo'limiga kiriting.")
    if not MAIN_ADMIN_ID:
        raise SystemExit("❌ MAIN_ADMIN_ID topilmadi! O'zingizning Telegram ID'ngizni kiriting.")

    health = await start_health_server()   # Render darhol ochiq portni ko'rsin
    await db.connect()
    await load_cache()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(system_router)
    dp.include_router(adm)          # TARTIB MUHIM: admin birinchi, foydalanuvchi oxirida
    dp.include_router(user_router)

    me = await bot.me()
    log.info("🍥 @%s ishga tushdi | animelar: %d | foydalanuvchilar: %d", me.username, len(ANIMES), len(USERS))
    with suppress(TelegramAPIError):
        await bot.send_message(MAIN_ADMIN_ID, f"🚀 <b>Bot ishga tushdi</b> — @{me.username}\n🍥 Animelar: {len(ANIMES)}")

    await start_userbot()
    fire(views_loop())
    fire(keepalive_loop())
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await flush_views()
        if USERBOT is not None:
            with suppress(Exception):
                await USERBOT.disconnect()
        await db.close()
        await health.cleanup()
        await bot.session.close()
        log.info("👋 Bot to'xtadi")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("👋 Xayr!")
