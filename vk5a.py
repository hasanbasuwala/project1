"""
vk_bot.py - Dedicated VK Playlist Downloader Microservice
───────────────────────────────────────────────────────────────
FEATURES & ARCHITECTURE:
  • MTProto Pyrogram Engine (API_ID + API_HASH + BOT_TOKEN) for >1GB Uploads
  • Fixed Pause/Resume & Cancel Playlist State Management
  • Native yt-dlp Multi-threaded Engine (Silent & Fast, Termux safe)
  • Live Upload Progress Tracking (Up to 2 GB per file)
  • Throttled Dual UI: Termux (2s) / Telegram Dashboard (10% or Stage change)
  • Dynamic Job Cards per download with live tracking & cleanup

CRASH-RECOVERY MODEL (v2):
  • Single persistent SQLite connection, WAL journal, guarded by one asyncio.Lock
    (fixes the fd-leak-per-query bug that was the likely root cause of crashes
    at scale, since a fresh sqlite3.connect() was opened -- and never closed --
    on every single DB call, including per-second progress updates).
  • Item -> Job claim is a single atomic transaction (no window where a job
    exists but its source item is still "pending", which used to cause
    duplicate-insert crashes in the drip-feed loop after a restart).
  • Every job carries a bounded retry counter. Permanent failures are marked
    "failed" (not silently orphaned) so playlists can still complete instead
    of hanging forever at e.g. 599/600.
  • Startup recovery trusts the FILESYSTEM, not the last DB stage string, to
    decide where a job resumes: a crash mid-write to the DB can leave a stale
    stage, but the actual bytes on disk (partial download vs. verified
    encoded file) don't lie.
  • Encode step is idempotent: if a valid encoded file already exists on disk
    (e.g. we crashed right after encoding but before the DB was updated), we
    skip re-encoding instead of clobbering a good file or crashing on a
    missing (already-deleted) source.
  • Upload step is idempotent: a durable "uploaded" checkpoint is written
    immediately after a successful Telegram send, before any bookkeeping /
    cleanup, so a crash in that narrow window causes a finalize-only replay
    instead of a duplicate re-upload.
  • Orphaned job folders (crash between mkdir and DB insert, or after DB
    delete but before rmtree) are swept on boot.
  • Global concurrency cap + free-disk-space gate so a 600-video playlist on
    a 30GB disk can never over-commit storage.
───────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import socket
import subprocess
import time
import urllib.request
import uuid
import sys
import sqlite3
from pathlib import Path
import yt_dlp
from logging.handlers import RotatingFileHandler

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified
import config

try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# ──────────────────────────── CONFIGURATION & CONSTANTS ──────────────────

BASE_DIR = Path("SysCache_VK")
LOG_DIR = BASE_DIR / "logs"
DB_PATH = BASE_DIR / "vk_scheduler.db"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s – %(message)s",
    handlers=[
        RotatingFileHandler(LOG_DIR / "vk_engine.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logging.getLogger().handlers[1].setLevel(logging.CRITICAL)
log = logging.getLogger("vk_stealth_bot")
logging.getLogger("pyrogram").setLevel(logging.ERROR)

API_ID = config.API_ID
API_HASH = config.API_HASH
BOT_TOKEN = getattr(config, "VK_BOT_TOKEN", config.BOT_TOKEN)
CHANNEL_ID = config.CHANNEL_ID
OWNER_ID = int(config.OWNER_ID) if hasattr(config, "OWNER_ID") else 0

VK_COOKIES = None
if os.path.exists("extracted_cookies.txt"):
    with open("extracted_cookies.txt", "r", encoding="utf-8") as f:
        VK_COOKIES = f.read().strip()

JOBS_DIR = BASE_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# --- Storage / concurrency safety (works for playlists of any size) ---
MAX_RETRIES = 3                 # permanent failure after this many attempts of any stage
MAX_GLOBAL_CONCURRENT = 7       # total in-flight jobs across ALL playlists at once
MIN_FREE_GB = 3.0               # never let free disk space drop below this
EST_JOB_FOOTPRINT_GB = 1.5      # rough worst-case footprint of one in-flight job

# --- Termux UI & Dashboard State Constants ---
C_CYAN, C_YELLOW, C_RED, C_GREEN, C_RESET, C_BOLD = "\033[36m", "\033[33m", "\033[31m", "\033[32m", "\033[0m", "\033[1m"
_live_ui_text = {}
_last_completed = "—"
_dash_msg_id, _dash_chat_id = 0, 0
_stack_msg_id, _stack_chat_id = 0, 0
_dash_tab = "playlists"
_expanded_pl = None
_expanded_bucket = None
_expanded_jid = None


def make_bar(percent: float, width: int = 10) -> str:
    filled = int(max(0.0, min(percent, 100.0)) / (100.0 / width))
    return "█" * filled + "░" * (width - filled)


def get_free_space_gb() -> float:
    total, used, free = shutil.disk_usage(BASE_DIR)
    return free / (1024 ** 3)


def _job_tracker_text(job: dict, avg_speed: str = None, avg_eta: str = None) -> str:
    title = str(job.get('title', 'Unknown'))[:18]
    status_raw = str(job.get('stage', 'PROCESSING')).upper()

    speed, eta = "—", "—"
    if "|" in status_raw:
        parts = [p.strip() for p in status_raw.split("|")]
        status_raw = parts[0]
        if len(parts) >= 3:
            speed = parts[1]
            eta = parts[2]

    if avg_speed: speed = avg_speed
    if avg_eta: eta = avg_eta

    pct = job.get('pct')
    pct_float = float(pct) if pct is not None else 0.0
    bar = make_bar(pct_float, 10)

    return (
        f"`[❖] ＴＡＳＫ :` `{title}..`\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`⚙️ PHASE :` `{status_raw}`\n"
        f"`⚡ SPEED :` `{speed}`\n"
        f"`⏳ ETA   :` `{eta}`\n"
        f"`📊 PROG  :` `[{bar}] {pct_float:.1f}%`"
    )


def _stack_bucket(job: dict) -> str:
    stage = (job.get('stage') or '').lower()
    if stage.startswith('uploading') or stage.startswith('uploaded'):
        return 'up'
    if stage in ('encoding', 'encoded', 'process'):
        return 'enc'
    return 'dl'  # queued / downloading / downloaded


def render_stack_card(jobs: list[dict], max_per_bucket: int = 6) -> str:
    """
    One compact message covering every active job, grouped by stage, instead
    of a separate Telegram message per job. A 600-video playlist with the
    old per-job-message design would leave hundreds of stray cards behind;
    this stays at exactly one message no matter how large the playlist is.
    """
    groups: dict[str, list[dict]] = {'dl': [], 'enc': [], 'up': []}
    for j in jobs:
        groups[_stack_bucket(j)].append(j)

    def fmt_job(j: dict) -> str:
        pct = float(j.get('pct') or 0.0)
        title = str(j.get('title') or '?')[:14]
        bar = make_bar(pct, 8)
        speed = ""
        stage_val = j.get('stage') or ""
        if "|" in stage_val:
            parts = [p.strip() for p in stage_val.split("|")]
            if len(parts) >= 2 and parts[1] not in ("~", ""):
                speed = f" {parts[1]}"
        return f"`  ├ {title:<14} [{bar}] {pct:>3.0f}%{speed}`"

    lines = [f"📦 **ACTIVE JOBS** ({len(jobs)})", "`━━━━━━━━━━━━━━━━━━━━━━━━━━`"]
    if not jobs:
        lines.append("`  System idle.`")
    else:
        labels = (('dl', '📥 DOWNLOADING'), ('enc', '⚙️ ENCODING'), ('up', '📤 UPLOADING'))
        shown = 0
        for key, label in labels:
            bucket_jobs = groups[key]
            if not bucket_jobs:
                continue
            lines.append(f"`{label} ({len(bucket_jobs)})`")
            lines.extend(fmt_job(j) for j in bucket_jobs[:max_per_bucket])
            shown += min(len(bucket_jobs), max_per_bucket)
        extra = len(jobs) - shown
        if extra > 0:
            lines.append(f"`  …and {extra} more`")
    lines.append(f"`🏁 LAST : {_last_completed[:20]}`")
    return "\n".join(lines)


# ──────────────────────────── SUBSYSTEM 1: DATABASE ─────────────────────

class JobScheduler:
    """
    Single persistent connection guarded by one asyncio.Lock.
    WAL journal + synchronous=FULL: durable across crashes/power loss without
    leaking a file descriptor on every call (the old per-call
    sqlite3.connect() pattern never closed its connections).
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute('''CREATE TABLE IF NOT EXISTS playlists (
            id TEXT PRIMARY KEY, url TEXT, caption TEXT, total INTEGER,
            downloaded INTEGER, status TEXT, chat_id INTEGER
        )''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS playlist_items (
            id TEXT PRIMARY KEY, playlist_id TEXT, url TEXT, title TEXT,
            status TEXT, retries INTEGER
        )''')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, url TEXT, title TEXT, playlist_id TEXT, item_id TEXT,
            stage TEXT, pct REAL, retries INTEGER, chat_id INTEGER, tracker_id INTEGER,
            held INTEGER DEFAULT 0
        )''')
        # Best-effort migrations for DBs created by older versions of this script.
        for stmt in (
            'ALTER TABLE jobs ADD COLUMN tracker_id INTEGER',
            'ALTER TABLE jobs ADD COLUMN item_id TEXT',
            'ALTER TABLE jobs ADD COLUMN held INTEGER DEFAULT 0',
            'ALTER TABLE playlist_items ADD COLUMN retries INTEGER',
        ):
            try:
                self.conn.execute(stmt)
            except sqlite3.OperationalError:
                pass
        self.conn.commit()

    def log_trace(self, jid: str, msg: str):
        job_dir = JOBS_DIR / f"JOB_{jid}"
        job_dir.mkdir(parents=True, exist_ok=True)
        with open(job_dir / "trace.log", "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

    # ---------- playlists ----------

    async def create_playlist(self, pl_id: str, url: str, caption: str, total: int, chat_id: int):
        async with self.lock:
            self.conn.execute(
                'INSERT INTO playlists VALUES (?, ?, ?, ?, 0, "active", ?)',
                (pl_id, url, caption, total, chat_id)
            )
            self.conn.commit()

    async def add_playlist_items(self, items: list[tuple]):
        # items: (id, playlist_id, url, title)
        async with self.lock:
            self.conn.executemany(
                'INSERT INTO playlist_items VALUES (?, ?, ?, ?, "pending", 0)', items
            )
            self.conn.commit()

    async def get_active_playlists(self) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute(
                'SELECT * FROM playlists WHERE status != "completed" AND status != "cancelled"'
            ).fetchall()]

    async def get_playlist(self, pl_id: str) -> dict:
        async with self.lock:
            row = self.conn.execute('SELECT * FROM playlists WHERE id = ?', (pl_id,)).fetchone()
            return dict(row) if row else {}

    async def get_playlist_failed_count(self, pl_id: str) -> int:
        async with self.lock:
            row = self.conn.execute(
                'SELECT COUNT(*) FROM playlist_items WHERE playlist_id = ? AND status = "failed"', (pl_id,)
            ).fetchone()
            return row[0] if row else 0

    async def update_playlist(self, pl_id: str, **kwargs):
        async with self.lock:
            for k, v in kwargs.items():
                self.conn.execute(f'UPDATE playlists SET {k} = ? WHERE id = ?', (v, pl_id))
            self.conn.commit()

    async def cancel_playlist(self, pl_id: str):
        """Full purge: marks playlist cancelled, wipes pending items & active jobs, cleans disk."""
        async with self.lock:
            self.conn.execute('UPDATE playlists SET status = "cancelled" WHERE id = ?', (pl_id,))
            self.conn.execute('DELETE FROM playlist_items WHERE playlist_id = ? AND status = "pending"', (pl_id,))
            active_jobs = [dict(r) for r in self.conn.execute(
                'SELECT id FROM jobs WHERE playlist_id = ?', (pl_id,)
            ).fetchall()]
            self.conn.execute('DELETE FROM jobs WHERE playlist_id = ?', (pl_id,))
            self.conn.commit()

        for j in active_jobs:
            shutil.rmtree(JOBS_DIR / f"JOB_{j['id']}", ignore_errors=True)

    async def get_pending_items(self, pl_id: str, limit: int = 2) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute(
                'SELECT * FROM playlist_items WHERE playlist_id = ? AND status = "pending" LIMIT ?',
                (pl_id, limit)
            ).fetchall()]

    async def update_item_status(self, item_id: str, status: str):
        async with self.lock:
            self.conn.execute('UPDATE playlist_items SET status = ? WHERE id = ?', (status, item_id))
            self.conn.commit()

    # ---------- jobs ----------

    async def claim_item_as_job(self, item: dict, chat_id: int):
        """
        Atomically: mark the source item 'processing' AND create its job row,
        in one transaction. Eliminates the crash window where an item was
        already 'processing' but had no job (or vice versa), which used to
        cause duplicate-insert crashes in the drip-feed loop after a restart.
        The job id is reused as the item id, so item<->job mapping never
        needs a lookup and reconciliation is trivial.
        """
        jid = item['id']
        async with self.lock:
            self.conn.execute('UPDATE playlist_items SET status = "processing" WHERE id = ?', (jid,))
            self.conn.execute(
                'INSERT OR IGNORE INTO jobs '
                '(id, url, title, playlist_id, item_id, stage, pct, retries, chat_id, tracker_id) '
                'VALUES (?, ?, ?, ?, ?, "queued", 0.0, 0, ?, NULL)',
                (jid, item['url'], item['title'], item['playlist_id'], jid, chat_id)
            )
            self.conn.commit()
        root = JOBS_DIR / f"JOB_{jid}"
        for d in (root, root / "dl", root / "enc"):
            d.mkdir(parents=True, exist_ok=True)

    async def update_job(self, jid: str, **kwargs):
        async with self.lock:
            for k, v in kwargs.items():
                self.conn.execute(f'UPDATE jobs SET {k} = ? WHERE id = ?', (v, jid))
            self.conn.commit()

    async def delete_job(self, jid: str):
        async with self.lock:
            self.conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))
            self.conn.commit()

    async def get_active_jobs(self) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute('SELECT * FROM jobs').fetchall()]

    async def get_held_jobs(self, pl_id: str) -> list[dict]:
        async with self.lock:
            return [dict(r) for r in self.conn.execute(
                'SELECT * FROM jobs WHERE playlist_id = ? AND held = 1', (pl_id,)
            ).fetchall()]

    async def clear_held(self, jid: str):
        async with self.lock:
            self.conn.execute('UPDATE jobs SET held = 0 WHERE id = ?', (jid,))
            self.conn.commit()

    async def get_job(self, jid: str) -> dict:
        async with self.lock:
            row = self.conn.execute('SELECT * FROM jobs WHERE id = ?', (jid,)).fetchone()
            return dict(row) if row else {}

    async def get_item_status(self, item_id: str) -> str | None:
        async with self.lock:
            row = self.conn.execute('SELECT status FROM playlist_items WHERE id = ?', (item_id,)).fetchone()
            return row[0] if row else None

    async def global_in_flight_count(self) -> int:
        async with self.lock:
            row = self.conn.execute('SELECT COUNT(*) FROM jobs').fetchone()
            return row[0] if row else 0

    async def fail_or_retry(self, job: dict, reason: str):
        """
        Bounded retry instead of the old behaviour (delete job, leave the
        source item stuck at 'processing' forever). Under MAX_RETRIES the
        item goes back to 'pending' so the drip-feed picks it up again.
        At MAX_RETRIES it's marked 'failed' (terminal) and counted against
        the playlist total so the playlist can still reach "completed"
        instead of hanging at e.g. 599/600 forever.

        Crucially: on a retry-eligible failure we do NOT delete the job row
        or wipe its folder. aria2c's '-c' resume depends on the partial
        media file plus its '<file>.aria2' control file still being on disk
        with the same output path -- deleting them (as this used to do
        unconditionally) forced every retry, crash or not, to restart the
        download from zero. Keeping the same job id (== item id) means the
        next claim_item_as_job() call is a harmless no-op INSERT OR IGNORE
        against the same row, preserving tracker_id too (no duplicate
        Telegram job card on retry).
        """
        jid = job['id']
        item_id = job.get('item_id') or jid
        retries = int(job.get('retries') or 0) + 1
        self.log_trace(jid, f"FAILURE (attempt {retries}/{MAX_RETRIES}): {reason}")

        async with self.lock:
            if retries < MAX_RETRIES:
                self.conn.execute(
                    'UPDATE playlist_items SET status = "pending", retries = ? WHERE id = ?',
                    (retries, item_id)
                )
                # Reset progress display but deliberately leave the job row
                # (and its on-disk folder) alone otherwise -- that's what
                # lets aria2c resume instead of starting over.
                self.conn.execute(
                    'UPDATE jobs SET stage = "queued", pct = 0.0, retries = ? WHERE id = ?',
                    (retries, jid)
                )
                self.conn.commit()
                return

            self.conn.execute(
                'UPDATE playlist_items SET status = "failed", retries = ? WHERE id = ?',
                (retries, item_id)
            )
            pl = self.conn.execute(
                'SELECT * FROM playlists WHERE id = ?', (job['playlist_id'],)
            ).fetchone()
            if pl:
                new_done = pl['downloaded'] + 1
                status = "completed" if new_done >= pl['total'] else pl['status']
                self.conn.execute(
                    'UPDATE playlists SET downloaded = ?, status = ? WHERE id = ?',
                    (new_done, status, pl['id'])
                )
            self.conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))
            self.conn.commit()

        # Only reclaim disk space once we've permanently given up on this item.
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)

    async def force_fail_job(self, jid: str):
        """Manual KILL from the dashboard: mark the item terminally failed
        instead of silently orphaning it (the old bug)."""
        async with self.lock:
            row = self.conn.execute('SELECT * FROM jobs WHERE id = ?', (jid,)).fetchone()
            job = dict(row) if row else None
            if job:
                item_id = job.get('item_id') or jid
                self.conn.execute('UPDATE playlist_items SET status = "failed" WHERE id = ?', (item_id,))
                pl = self.conn.execute(
                    'SELECT * FROM playlists WHERE id = ?', (job['playlist_id'],)
                ).fetchone()
                if pl:
                    new_done = pl['downloaded'] + 1
                    status = "completed" if new_done >= pl['total'] else pl['status']
                    self.conn.execute(
                        'UPDATE playlists SET downloaded = ?, status = ? WHERE id = ?',
                        (new_done, status, pl['id'])
                    )
                self.conn.execute('DELETE FROM jobs WHERE id = ?', (jid,))
                self.conn.commit()
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)

    # ---------- crash recovery ----------

    async def reconcile_items(self):
        """Any playlist_item stuck at 'processing' with no matching job row
        (crash before the job was created, or a job that got wiped without
        resetting its item) goes back to 'pending' so it isn't lost."""
        async with self.lock:
            job_item_ids = {r[0] for r in self.conn.execute('SELECT item_id FROM jobs').fetchall()}
            stuck = self.conn.execute('SELECT id FROM playlist_items WHERE status = "processing"').fetchall()
            for (iid,) in stuck:
                if iid not in job_item_ids:
                    self.conn.execute('UPDATE playlist_items SET status = "pending" WHERE id = ?', (iid,))
            self.conn.commit()

    async def reconcile_on_startup(self) -> dict:
        """
        Trust the filesystem over a possibly-stale DB stage string after a
        crash. Also sweeps orphaned job folders (crash between mkdir and DB
        insert, or after DB delete but before rmtree).

        Jobs belonging to a playlist that's currently 'paused' are NOT
        pushed into any queue -- they're marked held=1 instead. Without
        this check, a reboot would silently resume every in-flight job
        regardless of pause, since nothing else in the recovery path knows
        or cares about the playlist's pause state. The drip-feed loop
        releases held jobs automatically once their playlist is resumed.

        Returns which queue each recovered job should re-enter (jobs held
        for a paused playlist are reported separately, not queued).
        """
        result = {"dl": [], "enc": [], "up": [], "held": []}

        async with self.lock:
            jobs = [dict(r) for r in self.conn.execute('SELECT * FROM jobs').fetchall()]
            playlist_status = {
                r[0]: r[1] for r in self.conn.execute('SELECT id, status FROM playlists').fetchall()
            }
        known_ids = {j['id'] for j in jobs}

        if JOBS_DIR.exists():
            for folder in JOBS_DIR.glob("JOB_*"):
                jid = folder.name.replace("JOB_", "", 1)
                if jid not in known_ids:
                    shutil.rmtree(folder, ignore_errors=True)

        for j in jobs:
            jid = j['id']
            root = JOBS_DIR / f"JOB_{jid}"
            dl_dir, enc_dir = root / "dl", root / "enc"
            for d in (root, dl_dir, enc_dir):
                d.mkdir(parents=True, exist_ok=True)

            enc_file = enc_dir / f"{jid}.mp4"
            enc_ok = enc_file.exists() and enc_file.stat().st_size > 0

            # aria2c leaves a "<file>.aria2" control file next to the partial
            # data while a download is in progress/interrupted, and removes
            # it automatically on success. yt-dlp's external-downloader path
            # also writes to a ".part"-suffixed temp name until the download
            # finishes. Either marker present means the download is NOT done
            # -- checking "any file exists in dl_dir" (the old logic) treated
            # a 5%-downloaded file as complete and shipped it straight to the
            # encoder, which is why a crash-restart never actually resumed:
            # the downloader (and aria2c's -c) never got invoked again.
            in_progress_markers = list(dl_dir.glob("*.aria2")) + list(dl_dir.glob("*.part"))
            complete_media_files = [
                f for f in dl_dir.rglob("*")
                if f.is_file() and f.suffix.lower() in (".mp4", ".mkv", ".ts", ".webm")
                and not f.name.endswith(".part")
            ]

            stage = (j.get('stage') or "").lower()

            if stage.startswith("uploaded"):
                bucket, new_stage = "up", None
            elif enc_ok:
                bucket, new_stage = "up", "encoded"
            elif complete_media_files and not in_progress_markers:
                bucket, new_stage = "enc", "downloaded"
            else:
                bucket, new_stage = "dl", "queued"

            if new_stage:
                await self.update_job(jid, stage=new_stage, pct=0.0)

            is_paused = playlist_status.get(j.get('playlist_id')) == "paused"
            if is_paused:
                async with self.lock:
                    self.conn.execute('UPDATE jobs SET held = 1 WHERE id = ?', (jid,))
                    self.conn.commit()
                result["held"].append(jid)
            else:
                result[bucket].append(jid)

        return result


# ──────────────────────────── DRIP-FEED ORCHESTRATOR ───────────────────

async def playlist_drip_feed_loop(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue,
                                   dl_pool: "WorkerPool", enc_pool: "WorkerPool", up_pool: "WorkerPool"):
    """Maintains bounded concurrent downloads, gated by both a global
    concurrency cap and free disk space, so a playlist of any size (600+
    videos) can never over-commit storage or memory."""
    while True:
        await asyncio.sleep(3)
        try:
            active_playlists = await db.get_active_playlists()

            # Release any jobs parked because their playlist was paused --
            # including ones parked at boot by reconcile_on_startup() after
            # a reboot. This runs independent of the disk/concurrency gate
            # below since these are already-existing jobs, not new claims.
            for pl in active_playlists:
                if pl['status'] in ('paused', 'cancelled', 'completed'):
                    continue
                held_jobs = await db.get_held_jobs(pl['id'])
                for hj in held_jobs:
                    await db.clear_held(hj['id'])
                    stage = (hj.get('stage') or '').lower()
                    if stage.startswith('uploaded') or stage == 'encoded':
                        await up_q.put(hj['id'])
                    elif stage == 'downloaded':
                        await enc_q.put(hj['id'])
                    else:
                        await dl_q.put(hj['id'])

            free_gb = get_free_space_gb()
            if free_gb < MIN_FREE_GB:
                log.warning(f"Low disk space ({free_gb:.2f}GB free) — pausing new claims this cycle.")
                continue

            # A job "waiting for processing" or "waiting for upload" still
            # counts toward the disk-safety budget (it still has a real file
            # sitting on disk) -- but it shouldn't be capped at a hardcoded
            # number that has no idea how many workers are actually
            # configured. If enc/up have fewer workers than dl, a backlog
            # naturally forms there; counting that backlog against a fixed
            # cap of 3 (regardless of /vk_workers) starves dl_workers that
            # have nothing to do with disk space at all. The cap now scales
            # with whatever worker capacity is currently configured, so
            # resizing via /vk_workers actually changes throughput, while
            # disk safety remains a fully separate check below.
            worker_capacity = dl_pool.target + enc_pool.target + up_pool.target
            effective_cap = max(MAX_GLOBAL_CONCURRENT, worker_capacity)

            total_in_flight = await db.global_in_flight_count()
            if total_in_flight >= effective_cap:
                continue

            slots_by_space = max(1, int((free_gb - MIN_FREE_GB) / EST_JOB_FOOTPRINT_GB))
            global_slots_free = min(effective_cap - total_in_flight, slots_by_space)
            if global_slots_free <= 0:
                continue

            for pl in active_playlists:
                if global_slots_free <= 0:
                    break
                if pl['status'] in ('paused', 'cancelled', 'completed'):
                    continue

                pending_items = await db.get_pending_items(pl['id'], limit=global_slots_free)
                for item in pending_items:
                    await db.claim_item_as_job(item, pl['chat_id'])
                    await dl_q.put(item['id'])
                    global_slots_free -= 1
                    if global_slots_free <= 0:
                        break
        except Exception as e:
            log.exception(f"Drip Feed Loop Error: {e}")


# ──────────────────────────── PIPELINE ENGINES ─────────────────────────

class DownloaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db
        self.app = app

    def _extract_vk_api(self, url: str, jid: str) -> str | None:
        """Ghost Protocol: Direct CDN extraction without cookies."""
        VK_TOKEN = getattr(config, "VK_TOKEN", None)
        if not VK_TOKEN: return None

        try:
            import vk_api
            vk_session = vk_api.VkApi(token=VK_TOKEN)
            vk = vk_session.get_api()

            video_id = None
            video_match = re.search(r'video(-?\d+_\d+)', url)
            if video_match:
                video_id = video_match.group(1)

            if video_id:
                vid_details = vk.video.get(videos=video_id)
                if vid_details and vid_details.get('items'):
                    files = vid_details['items'][0].get('files', {})
                    for q in ['mp4_1080', 'mp4_720', 'mp4_480', 'mp4_360', 'hls']:
                        if q in files:
                            self.db.log_trace(jid, f"[vk_api] Direct {q.upper()} CDN link extracted.")
                            return files[q]
        except Exception as e:
            self.db.log_trace(jid, f"[vk_api] Ghost Protocol Failed: {e}")
        return None

    @staticmethod
    def _get_free_port() -> int:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    @staticmethod
    def _aria2_rpc_call(port: int, secret: str, method: str, params: list | None = None) -> dict:
        payload = {
            "jsonrpc": "2.0", "id": "poll", "method": method,
            "params": [f"token:{secret}"] + (params or []),
        }
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/jsonrpc",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read())

    async def _poll_aria2_progress(self, jid: str, port: int, secret: str, stop_event: asyncio.Event):
        """
        Runs concurrently with the blocking aria2c download. aria2c never
        pushes progress into yt-dlp's hooks or logger -- it only prints to
        its own inherited stdio (which is why the terminal showed live
        speed/ETA while nothing else updated). Talking to aria2c's own
        JSON-RPC port sidesteps that entirely: we get real progress without
        giving up aria2c's multi-connection download speed.

        IMPORTANT: with --enable-rpc enabled, aria2c behaves as a persistent
        server and does NOT exit on its own once its download finishes (it
        stays up in case more tasks arrive via RPC). yt-dlp's external
        downloader wrapper blocks on that subprocess actually exiting before
        it considers the download done -- so without an explicit shutdown,
        the file finishes on disk but the whole job (and the worker holding
        it) hangs forever, frozen at 100%. This loop detects "was active,
        now isn't" and calls aria2.shutdown to let the process terminate,
        which is what actually lets yt-dlp move on.
        """
        # Wait for aria2c's RPC server to come up (it starts almost
        # instantly, but the exact process launch timing isn't guaranteed).
        for _ in range(30):
            if stop_event.is_set():
                return
            try:
                await asyncio.to_thread(self._aria2_rpc_call, port, secret, "aria2.tellActive")
                break
            except Exception:
                await asyncio.sleep(0.5)

        last_db_update = 0.0
        seen_active = False
        idle_ticks = 0
        # Grace window after downloads appear to have stopped, in case
        # yt-dlp launches a second aria2c pass (e.g. an audio-only stream
        # for an adaptive format) on the same port shortly after.
        MAX_IDLE_TICKS = 8

        while not stop_event.is_set():
            try:
                resp = await asyncio.to_thread(self._aria2_rpc_call, port, secret, "aria2.tellActive")
                active = resp.get("result", [])
            except Exception:
                active = None  # RPC unreachable: aria2c still starting, or between passes.

            if active:
                seen_active = True
                idle_ticks = 0
                completed = sum(int(d.get("completedLength", 0)) for d in active)
                total = sum(int(d.get("totalLength", 0)) for d in active)
                speed_bps = sum(int(d.get("downloadSpeed", 0)) for d in active)

                pct = (completed / total * 100.0) if total else 0.0
                speed_str = f"{speed_bps / (1024 * 1024):.2f}MiB/s" if speed_bps else "~"
                if speed_bps > 0 and total > completed:
                    eta_sec = int((total - completed) / speed_bps)
                    eta_str = f"{eta_sec // 60}m{eta_sec % 60}s"
                else:
                    eta_str = "~"

                global _live_ui_text
                _live_ui_text[jid] = f"[aria2] {pct:.1f}% at {speed_str} ETA {eta_str}"

                now = time.time()
                if now - last_db_update >= 1.0:
                    stage_str = f"downloading | {speed_str} | {eta_str}"
                    await self.db.update_job(jid, pct=pct, stage=stage_str)
                    last_db_update = now

            elif seen_active:
                # Was downloading, now nothing's active -- the transfer is
                # done (or errored out). aria2c won't exit by itself, so
                # kick it so yt-dlp's subprocess wait actually unblocks.
                try:
                    await asyncio.to_thread(self._aria2_rpc_call, port, secret, "aria2.shutdown")
                except Exception:
                    pass  # already gone, or between video/audio passes -- fine either way
                idle_ticks += 1
                if idle_ticks >= MAX_IDLE_TICKS:
                    return
                seen_active = False  # give a second (e.g. audio) pass a chance to start
            await asyncio.sleep(1.0)

    async def execute(self, job: dict):
        jid, original_url = job['id'], job['url']
        dl_dir = JOBS_DIR / f"JOB_{jid}" / "dl"

        await self.db.update_job(jid, stage="downloading | ~ | ~")

        self.db.log_trace(jid, "Querying VK API backend for direct CDN payload...")
        extracted_cdn = await asyncio.to_thread(self._extract_vk_api, original_url, jid)

        target_url = extracted_cdn if extracted_cdn else original_url
        if extracted_cdn:
            self.db.log_trace(jid, "Target bypassed! Proceeding with cookieless direct CDN stream.")

        rpc_port = self._get_free_port()
        rpc_secret = secrets.token_hex(8)

        opts = {
            "outtmpl": str(dl_dir / f"{jid}.%(ext)s"),
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "merge_output_format": "mp4",
            "quiet": False,
            "noprogress": True,
            "no_warnings": True,
            "compat_opts": {"allow-unsafe-ext"},
            # Guard against one oversized video blowing the whole storage budget.
            "max_filesize": getattr(config, "VK_MAX_FILESIZE_BYTES", 2 * 1024 * 1024 * 1024),

            "external_downloader": "aria2c",
            "external_downloader_args": [
                "-c", "-j", "16", "-x", "16", "-s", "16", "-k", "5M",
                "--connect-timeout=15", "--timeout=15", "--max-tries=5",
                "--summary-interval=0",
                # RPC is how we get real progress out of aria2c -- it never
                # forwards its console output through yt-dlp's own hooks.
                "--enable-rpc=true",
                f"--rpc-listen-port={rpc_port}",
                f"--rpc-secret={rpc_secret}",
                "--rpc-listen-all=false",
            ],
        }

        # --- DYNAMIC CDN SIGNATURE SPOOFING (COOKIELESS BYPASS) ---
        # Default to a highly standard Windows Chrome User-Agent
        custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        # Check the URL for VK's strict engine bindings
        if "srcAg=GECKO" in target_url:
            custom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
            self.db.log_trace(jid, "Ghost Protocol: Gecko CDN signature detected. Spoofing Firefox User-Agent.")
        elif "srcAg=SAFARI" in target_url:
            custom_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"
            self.db.log_trace(jid, "Ghost Protocol: Safari CDN signature detected. Spoofing Apple User-Agent.")
        elif "srcAg=CHROMIUM" in target_url:
            self.db.log_trace(jid, "Ghost Protocol: Chromium CDN signature detected. Using standard Chrome User-Agent.")

        # Inject the spoofed User-Agent directly into yt-dlp's network options
        opts.setdefault("http_headers", {})
        opts["http_headers"]["User-Agent"] = custom_ua

        # Disable yt-dlp's default impersonation if we are strictly spoofing a raw link
        if "impersonate" in opts and ("srcAg=" in target_url):
            del opts["impersonate"]
            self.db.log_trace(jid, "Ghost Protocol: Disabled curl_cffi impersonation to prevent header collisions.")
        # -----------------------------------------------------------------

        stop_event = asyncio.Event()
        poller_task = asyncio.create_task(self._poll_aria2_progress(jid, rpc_port, rpc_secret, stop_event))
        try:
            await asyncio.to_thread(self._run_ytdlp, target_url, jid, opts)
        finally:
            stop_event.set()
            poller_task.cancel()
            try:
                await poller_task
            except asyncio.CancelledError:
                pass

    def _run_ytdlp(self, url: str, jid: str, base_opts: dict):
        opts = base_opts.copy()
        opts["quiet"] = True
        opts["noprogress"] = True

        self.db.log_trace(jid, "Executing aria2c-backed downloader...")
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.extract_info(url, download=True)
        except Exception as e:
            self.db.log_trace(jid, f"Download Error: {e}")
            raise e


class EncoderEngine:
    async def execute(self, job: dict, db: JobScheduler):
        jid = job['id']
        dl_dir, enc_dir = JOBS_DIR / f"JOB_{jid}" / "dl", JOBS_DIR / f"JOB_{jid}" / "enc"
        dst = enc_dir / f"{jid}.mp4"

        # Idempotency: if a valid encoded file already exists (e.g. we crashed
        # right after encoding but before the DB stage was updated), the
        # filesystem is the source of truth — skip re-encoding entirely.
        if dst.exists() and dst.stat().st_size > 0:
            db.log_trace(jid, "Encoded output already present on disk — skipping re-encode (crash-safe resume).")
        else:
            files = [f for f in dl_dir.rglob("*") if f.is_file() and f.suffix.lower() in [".mp4", ".mkv", ".ts", ".webm"]]
            if not files:
                raise RuntimeError("No downloaded media found.")

            src = max(files, key=lambda p: p.stat().st_size)
            tmp_dst = enc_dir / f"{jid}.mp4.partial"
            tmp_dst.unlink(missing_ok=True)

            free_gb = get_free_space_gb()
            if free_gb < 0.5:
                # ffmpeg failing identically across unrelated files at the same
                # moment is the classic signature of the disk actually being
                # full (ENOSPC) rather than a per-video codec problem. Fail
                # fast with a clear reason instead of burning a retry on a
                # cryptic exit code.
                raise RuntimeError(f"Only {free_gb:.2f}GB free — refusing to encode (likely disk-full failure).")

            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-i", str(src),
                "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
                "-movflags", "+faststart",
                "-f", "mp4", str(tmp_dst),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            _stdout, stderr_data = await proc.communicate()

            if proc.returncode != 0 or not tmp_dst.exists() or tmp_dst.stat().st_size == 0:
                tmp_dst.unlink(missing_ok=True)
                err_text = (stderr_data or b"").decode(errors="ignore").strip()
                # Keep only the tail -- ffmpeg's actual error is almost always
                # in the last few lines, and trace.log shouldn't balloon.
                err_tail = "\n".join(err_text.splitlines()[-25:]) if err_text else "(ffmpeg produced no stderr output)"
                db.log_trace(jid, f"ffmpeg stderr (exit {proc.returncode}):\n{err_tail}")
                raise RuntimeError(f"ffmpeg encode failed (exit code {proc.returncode}) -- see trace log for stderr.")

            tmp_dst.rename(dst)  # only a fully-written file is ever visible as "encoded"

        # Only safe to reclaim source space now that the encoded output is
        # verified on disk. If we crash right after this line, reconcile()
        # will see enc_ok=True and correctly resume from "encoded", not
        # re-attempt an encode with no source.
        for f in dl_dir.rglob("*"):
            if f.is_file():
                try:
                    f.unlink()
                except Exception:
                    pass


async def extract_video_metadata(video_path):
    """Uses ffprobe to extract exact width, height, and duration."""
    try:
        cmd = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height:format=duration",
            "-of", "json", str(video_path)
        ]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = await proc.communicate()
        data = json.loads(stdout)

        width = int(data['streams'][0]['width']) if 'streams' in data and data['streams'] else 0
        height = int(data['streams'][0]['height']) if 'streams' in data and data['streams'] else 0
        duration = int(float(data['format']['duration'])) if 'format' in data and 'duration' in data['format'] else 0
        return width, height, duration
    except Exception:
        return 1280, 720, 0


async def generate_thumbnail(video_path, thumb_path):
    """Uses ffmpeg to take a high-quality frame at the 3-second mark."""
    try:
        cmd = [
            "ffmpeg", "-y", "-ss", "00:00:03", "-i", str(video_path),
            "-vframes", "1", "-q:v", "2", str(thumb_path)
        ]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        await proc.wait()
        return str(thumb_path) if Path(thumb_path).exists() else None
    except Exception:
        return None


class UploaderEngine:
    def __init__(self, db: JobScheduler, app: Client):
        self.db = db
        self.app = app

    async def execute(self, job: dict):
        jid = job['id']
        stage = (job.get('stage') or "").lower()
        enc_dir = JOBS_DIR / f"JOB_{jid}" / "enc"
        enc_file = enc_dir / f"{jid}.mp4"

        if not stage.startswith("uploaded"):
            if not enc_file.exists():
                raise RuntimeError("Encoded payload missing.")

            width, height, duration = await extract_video_metadata(enc_file)
            thumb_path = await generate_thumbnail(enc_file, enc_dir / f"{jid}_thumb.jpg")

            pl = await self.db.get_playlist(job['playlist_id'])
            caption = f"{pl['caption']}\n\n**{job['title']}**" if pl and pl.get('caption') else f"**{job['title']}**"

            last_db_up = 0

            async def upload_progress(current, total):
                nonlocal last_db_up
                now = time.time()
                if now - last_db_up >= 1.5:
                    pct = (current / total) * 100 if total else 0.0
                    curr_mb = current / (1024 * 1024)
                    tot_mb = total / (1024 * 1024)
                    stage_str = f"uploading | {curr_mb:.1f}MB/{tot_mb:.1f}MB | —"

                    global _live_ui_text
                    _live_ui_text[jid] = f"[upload] {curr_mb:.1f}/{tot_mb:.1f} MB ({pct:.1f}%)"

                    try:
                        active_loop = asyncio.get_running_loop()
                    except RuntimeError:
                        active_loop = loop
                    asyncio.run_coroutine_threadsafe(self.db.update_job(jid, pct=pct, stage=stage_str), active_loop)
                    last_db_up = now

            await self.app.send_video(
                chat_id=CHANNEL_ID,
                video=str(enc_file),
                caption=caption,
                width=width,
                height=height,
                duration=duration,
                thumb=thumb_path,
                supports_streaming=True,
                progress=upload_progress
            )

            # Durable checkpoint BEFORE any cleanup/bookkeeping. If we crash
            # right after this, restart sees stage="uploaded" and replays
            # only finalize() below instead of re-uploading to the channel.
            await self.db.update_job(jid, stage="uploaded", pct=100.0)
            job['stage'] = "uploaded"

        await self.finalize(job)

    async def finalize(self, job: dict):
        """
        Runs after a successful upload. Must be safe to call more than once:
        if this raises partway through, up_worker retries it (see below)
        without re-uploading, so every step here needs to tolerate being
        replayed on an already-'done' item instead of double-counting.
        """
        jid = job['id']
        item_id = job.get('item_id') or jid

        already_counted = (await self.db.get_item_status(item_id)) == "done"

        if not already_counted:
            global _last_completed
            _last_completed = job['title']

            pl = await self.db.get_playlist(job['playlist_id'])
            if pl:
                new_count = pl['downloaded'] + 1
                status = "completed" if new_count >= pl['total'] else pl['status']
                await self.db.update_playlist(pl['id'], downloaded=new_count, status=status)

            await self.db.update_item_status(item_id, "done")

        await self.db.delete_job(jid)
        shutil.rmtree(JOBS_DIR / f"JOB_{jid}", ignore_errors=True)


# ──────────────────────────── DASHBOARD & ROUTER ───────────────────────

async def safe_edit(app: Client, chat_id: int, msg_id: int, text: str, kb: InlineKeyboardMarkup | None):
    try: await app.edit_message_text(chat_id, msg_id, text, reply_markup=kb)
    except MessageNotModified: pass
    except FloodWait as e: await asyncio.sleep(e.value)
    except Exception: pass


async def render_dashboard(db: JobScheduler, tab: str = "playlists", exp_pl: str = None, exp_bucket: str = None, exp_jid: str = None) -> tuple[str, InlineKeyboardMarkup]:
    playlists = await db.get_active_playlists()
    active_jobs = await db.get_active_jobs()
    total_storage = sum(f.stat().st_size for f in JOBS_DIR.rglob("*") if f.is_file()) / (1024 ** 3)
    free_gb = get_free_space_gb()

    act_text_blocks = ["`[🔄] ACT  :`"]
    if not active_jobs:
        act_text_blocks = ["`[🔄] ACT  :` `SYSTEM IDLE`"]
    else:
        for i, j in enumerate(active_jobs[:7]):
            pct = float(j.get('pct', 0.0) or 0.0)
            stage_short = (j.get('stage') or '').split('|')[0].strip()[:4].upper()
            act_text_blocks.append(f"`  {chr(97+i)}. [{stage_short}] {j['title'][:12]}.. [{make_bar(pct, 8)}] {pct:.0f}%`")

    act_string = "\n".join(act_text_blocks)

    text = (
        f"💻 **VK PLAYLIST MAINFRAME**\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`\n"
        f"`[⚡] STAT :` `DRIP-FEED ACTIVE`\n"
        f"`[💾] USED :` `{total_storage:.2f} GB`  `[🆓] FREE :` `{free_gb:.2f} GB`\n"
        f"{act_string}\n"
        f"`[🏁] LAST :` `{_last_completed[:12]}`\n"
        f"`━━━━━━━━━━━━━━━━━━━━━━━━━━`"
    )

    kb = []
    is_root_open = (tab == "playlists")
    kb.append([InlineKeyboardButton(f"{'[-]' if is_root_open else '[+]'} 🎵 ACTIVE PLAYLISTS ({len(playlists)})", callback_data=f"dash|{'root' if is_root_open else 'playlists'}")])

    def _base(stage_str):
        if not stage_str: return "queued"
        return stage_str.split("|")[0].strip().lower()

    if is_root_open:
        if not playlists:
            kb.append([InlineKeyboardButton("└ System Idle (Send Link)", callback_data="noop")])
        else:
            for pl in playlists:
                pl_id = pl['id']
                is_this_pl_exp = (exp_pl == pl_id)
                pl_status_icon = "⏸" if pl['status'] == "paused" else "▶️"

                kb.append([InlineKeyboardButton(
                    f" {'[-]' if is_this_pl_exp else '[+]'} {pl_status_icon} {pl['caption'][:15] or 'Playlist'} [{pl['downloaded']}/{pl['total']}]",
                    callback_data=f"dash|playlists:{pl_id}" if not is_this_pl_exp else "dash|playlists"
                )])

                if is_this_pl_exp:
                    pl_jobs = [j for j in active_jobs if j.get('playlist_id') == pl_id]
                    buckets = {
                        "dl": [j for j in pl_jobs if _base(j['stage']) in ["queued", "downloading"]],
                        "dl_done": [j for j in pl_jobs if _base(j['stage']) == "downloaded"],
                        "enc": [j for j in pl_jobs if _base(j['stage']) in ["encoding", "process"]],
                        "enc_done": [j for j in pl_jobs if _base(j['stage']) == "encoded"],
                        "up": [j for j in pl_jobs if _base(j['stage']) in ["uploading", "uploaded"]]
                    }

                    def build_bucket(bucket_id, label, icon, job_list):
                        is_b_open = (exp_bucket == bucket_id)
                        b_pref = "[-]" if is_b_open else "[+]"
                        kb.append([InlineKeyboardButton(f"    ├ {b_pref} {icon} {label} ({len(job_list)})", callback_data=f"dash|playlists:{pl_id}:{bucket_id}" if not is_b_open else f"dash|playlists:{pl_id}")])

                        if is_b_open:
                            if not job_list: kb.append([InlineKeyboardButton("      └ Empty", callback_data="noop")])
                            for j in job_list:
                                jid = j['id']
                                is_j_open = (exp_jid == jid)

                                if is_j_open:
                                    speed, eta = "—", "—"
                                    stage_val = j.get('stage') or ""
                                    if "|" in stage_val:
                                        p = [x.strip() for x in stage_val.split("|")]
                                        if len(p) >= 3: speed, eta = p[1], p[2]
                                        elif len(p) == 2: speed = p[1]
                                    pct = float(j.get('pct', 0.0) or 0.0)
                                    bar = make_bar(pct, 8)

                                    kb.append([InlineKeyboardButton(f"🪪 ISOLATED JOB CARD: {jid}", callback_data="noop")])
                                    kb.append([InlineKeyboardButton(f"📁 {j['title'][:15]}...", callback_data="noop")])
                                    kb.append([InlineKeyboardButton(f"⚡ {speed}  |  ⏳ {eta}", callback_data="noop")])
                                    kb.append([InlineKeyboardButton(f"📊 [{bar}] {pct:.1f}%", callback_data="noop")])
                                    kb.append([
                                        InlineKeyboardButton("📄 LOGS", callback_data=f"joblog|{jid}"),
                                        InlineKeyboardButton("❌ KILL", callback_data=f"kill_job|{jid}")
                                    ])
                                    kb.append([InlineKeyboardButton("🔙 CLOSE CARD", callback_data=f"dash|playlists:{pl_id}:{bucket_id}")])
                                else:
                                    pct = float(j.get('pct', 0.0) or 0.0)
                                    kb.append([InlineKeyboardButton(f"      ├ ⚡ {j['title'][:10]}.. | {pct:.0f}%", callback_data=f"dash|playlists:{pl_id}:{bucket_id}:{jid}")])

                    build_bucket("dl", "DOWNLOADING", "📥", buckets["dl"])
                    build_bucket("dl_done", "WAITING PROC", "⏳", buckets["dl_done"])
                    build_bucket("enc", "PROCESSING", "⚙️", buckets["enc"])
                    build_bucket("enc_done", "WAITING UP", "⏳", buckets["enc_done"])
                    build_bucket("up", "UPLOADING", "📤", buckets["up"])

                    if pl['status'] == "paused":
                        kb.append([
                            InlineKeyboardButton("▶️ RESUME PLAYLIST", callback_data=f"res|{pl['id']}"),
                            InlineKeyboardButton("❌ CANCEL PLAYLIST", callback_data=f"kill|{pl['id']}")
                        ])
                    else:
                        kb.append([
                            InlineKeyboardButton("⏸ PAUSE PLAYLIST", callback_data=f"pause|{pl['id']}"),
                            InlineKeyboardButton("❌ CANCEL PLAYLIST", callback_data=f"kill|{pl['id']}")
                        ])
                    kb.append([InlineKeyboardButton("───────────────────", callback_data="noop")])

    kb.append([InlineKeyboardButton("🔄 REFRESH SYSTEM", callback_data="refresh")])
    return text, InlineKeyboardMarkup(kb)


def setup_router(app: Client, db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue,
                  dl_pool: "WorkerPool", enc_pool: "WorkerPool", up_pool: "WorkerPool"):

    @app.on_message(filters.text & filters.user(OWNER_ID) & ~filters.command(["vk_dash"]))
    async def auto_catch_playlist(_, msg: Message):
        text = msg.text.strip()
        if not ("http://" in text or "https://" in text): return

        parts = text.split("#", 1)
        url = parts[0].strip()
        caption = f"#{parts[1].strip()}" if len(parts) > 1 else ""

        url = re.sub(r'vk\.ru', 'vk.com', url, flags=re.IGNORECASE)
        m = await msg.reply("🔍 `Querying VK API for Playlist items...`")

        def extract_via_vk_api(playlist_url: str):
            VK_TOKEN = getattr(config, "VK_TOKEN", None)
            if not VK_TOKEN: return None

            try:
                import vk_api
                vk_session = vk_api.VkApi(token=VK_TOKEN)
                vk = vk_session.get_api()

                match = re.search(r'playlist/(-?\d+)_(\d+)', playlist_url)
                if not match: match = re.search(r'album_(-?\d+)_(\d+)', playlist_url)

                if match:
                    owner_id, album_id = int(match.group(1)), int(match.group(2))
                    all_videos = []
                    offset, count = 0, 100

                    while True:
                        res = vk.video.get(owner_id=owner_id, album_id=album_id, count=count, offset=offset)
                        items = res.get('items', [])
                        if not items: break

                        for v in items:
                            v_url = f"https://vk.com/video{v['owner_id']}_{v['id']}"
                            all_videos.append({'url': v_url, 'title': v.get('title', 'VK Video')})

                        offset += count
                        if offset >= res.get('count', 0): break
                    return all_videos
            except Exception as e:
                log.error(f"VK API Extraction failed: {e}")
            return None

        def extract_via_vk_wallpost(wall_url: str):
            """
            Handles vk.com/vk.ru wall post links (e.g. wall-223870924_29924),
            which the playlist/album extractor above never matches (its
            regexes only recognize 'playlist/' and 'album_' URLs). Resolves
            the video(s) attached to the post via the VK API using the same
            token-based Ghost Protocol approach as the playlist path -- this
            avoids yt-dlp's cookie-dependent 'vk:wallpost' extractor, which
            fails outright ("only available for registered users") on any
            video VK gates behind a logged-in session unless valid, current
            cookies happen to be loaded.
            """
            VK_TOKEN = getattr(config, "VK_TOKEN", None)
            if not VK_TOKEN: return None

            match = re.search(r'wall(-?\d+)_(\d+)', wall_url)
            if not match: return None

            try:
                import vk_api
                vk_session = vk_api.VkApi(token=VK_TOKEN)
                vk = vk_session.get_api()

                owner_id, post_id = match.group(1), match.group(2)
                posts = vk.wall.getById(posts=f"{owner_id}_{post_id}")
                if not posts:
                    return None

                videos = []
                for att in posts[0].get('attachments', []):
                    if att.get('type') == 'video':
                        v = att['video']
                        v_url = f"https://vk.com/video{v['owner_id']}_{v['id']}"
                        videos.append({'url': v_url, 'title': v.get('title', 'VK Video')})
                return videos or None
            except Exception as e:
                log.error(f"VK Wallpost API extraction failed: {e}")
            return None

        def extract_via_ytdlp(playlist_url: str):
            cookie_path = "vk_temp_cookies.txt"
            if VK_COOKIES:
                with open(cookie_path, "w", encoding="utf-8") as f:
                    f.write("# Netscape HTTP Cookie File\n")
                    for item in VK_COOKIES.strip().split(';'):
                        if '=' in item:
                            k, v = item.strip().split('=', 1)
                            f.write(f".vk.com\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")
                            f.write(f".vkvideo.ru\tTRUE\t/\tTRUE\t2147483647\t{k}\t{v}\n")

            opts = {'extract_flat': True, 'quiet': True, 'cookiefile': cookie_path if VK_COOKIES else None}
            with yt_dlp.YoutubeDL(opts) as ydl:
                data = ydl.extract_info(playlist_url, download=False)
                entries = data.get('entries', [])
                items = []
                for e in entries:
                    v_url = e.get('url') or e.get('webpage_url')
                    if v_url: items.append({'url': v_url, 'title': e.get('title', 'VK Video')})
                return items

        try:
            entries = await asyncio.to_thread(extract_via_vk_api, url)
            if entries is None:
                entries = await asyncio.to_thread(extract_via_vk_wallpost, url)
            if entries is None:
                entries = await asyncio.to_thread(extract_via_ytdlp, url)

            if not entries:
                return await m.edit("❌ No videos found in playlist link.")

            pl_id = str(uuid.uuid4())[:8]
            await db.create_playlist(pl_id, url, caption, len(entries), msg.chat.id)

            items = [(str(uuid.uuid4())[:8], pl_id, item['url'], item['title']) for item in entries]
            await db.add_playlist_items(items)
            await m.edit(f"✅ **PLAYLIST LOCKED**\nFound `{len(items)}` videos.\nDrip-feeding queued.")

            global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
            if _dash_msg_id and _dash_chat_id:
                dash_text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
                await safe_edit(app, _dash_chat_id, _dash_msg_id, dash_text, kb)

        except Exception as e:
            await m.edit(f"❌ Extraction error: `{e}`")

    @app.on_message(filters.command(["vk_dash"]) & filters.user(OWNER_ID))
    async def cmd_dash(_, msg: Message):
        global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
        text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
        m = await msg.reply(text, reply_markup=kb)
        _dash_msg_id, _dash_chat_id = m.id, m.chat.id

    @app.on_message(filters.command(["vk_workers"]) & filters.user(OWNER_ID))
    async def cmd_workers(_, msg: Message):
        args = msg.command[1:]

        if not args:
            return await msg.reply(
                "`WORKER POOLS`\n"
                f"`  DL  : {dl_pool.current_count()}/{dl_pool.target}`\n"
                f"`  ENC : {enc_pool.current_count()}/{enc_pool.target}`\n"
                f"`  UP  : {up_pool.current_count()}/{up_pool.target}`\n\n"
                "Usage: `/vk_workers dl=5 enc=3 up=2`\n"
                "(you can set just one, e.g. `/vk_workers dl=1` to slow downloads only)"
            )

        changes = {}
        for arg in args:
            if "=" in arg:
                k, v = arg.split("=", 1)
                if k.lower() in ("dl", "enc", "up") and v.lstrip("-").isdigit():
                    changes[k.lower()] = int(v)

        if not changes:
            return await msg.reply("Couldn't parse that. Usage: `/vk_workers dl=5 enc=3 up=2`")

        lines = []
        pools = {"dl": dl_pool, "enc": enc_pool, "up": up_pool}
        for key, new_target in changes.items():
            pool = pools[key]
            before = pool.current_count()
            await pool.adjust(new_target)
            arrow = "→" if new_target >= before else "↘"
            lines.append(f"{key.upper()} {before} {arrow} {new_target}")

        await msg.reply(
            "✅ " + " | ".join(lines) +
            "\n\nGrowing takes effect immediately. Shrinking takes effect as "
            "current jobs finish -- nothing gets killed mid-download/encode/upload."
        )

    @app.on_callback_query()
    async def handle_callbacks(_, cb: CallbackQuery):
        global _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid

        if cb.data == "noop": return await cb.answer()

        if cb.data.startswith("delmsg|"):
            msg_id = int(cb.data.split("|")[1])
            try:
                await app.delete_messages(cb.message.chat.id, msg_id)
                await cb.answer("Task card dismissed.")
            except Exception:
                await cb.answer("Failed to delete.", show_alert=True)
            return

        elif cb.data.startswith("dash|"):
            parts = cb.data.split("|")[1].split(":")
            _dash_tab = parts[0]
            _expanded_pl = parts[1] if len(parts) > 1 else None
            _expanded_bucket = parts[2] if len(parts) > 2 else None
            _expanded_jid = parts[3] if len(parts) > 3 else None
            await cb.answer()

        elif cb.data == "refresh":
            await cb.answer("Refreshed.")

        elif cb.data.startswith("joblog|"):
            jid = cb.data.split("|")[1]
            log_path = JOBS_DIR / f"JOB_{jid}" / "trace.log"
            if not log_path.exists(): return await cb.answer("No logs found.", show_alert=True)
            with open(log_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
                recent = "\n".join(lines[-15:]) if lines else "No data."
            return await cb.answer(f"--- TRACE LOGS ---\n{recent}", show_alert=True)

        elif cb.data.startswith("kill_job|"):
            jid = cb.data.split("|")[1]
            # Marks the source item terminally 'failed' instead of silently
            # orphaning it — a manual kill used to leave the item stuck at
            # 'processing' forever with no job to ever revive it.
            await db.force_fail_job(jid)
            _expanded_jid = None
            await cb.answer("Task terminated.", show_alert=True)

        elif cb.data.startswith("pause|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="paused")
            _expanded_pl = pl_id

            pl = await db.get_playlist(pl_id)
            all_jobs = await db.get_active_jobs()
            pl_jobs = [j for j in all_jobs if j.get('playlist_id') == pl_id]
            completed = pl['downloaded'] if pl else 0
            total = pl['total'] if pl else 0
            in_flight = len(pl_jobs)
            still_waiting = max(0, total - completed - in_flight)

            await cb.answer(
                "⏸ PLAYLIST PAUSED\n\n"
                f"✅ Completed: {completed}/{total}\n"
                f"🔄 In progress right now (will finish naturally, not killed): {in_flight}\n"
                f"⏳ Not yet started (frozen until Resume): {still_waiting}\n\n"
                "No new videos will start -- including after a crash/reboot -- "
                "until you hit Resume.",
                show_alert=True
            )

        elif cb.data.startswith("res|"):
            pl_id = cb.data.split("|")[1]
            await db.update_playlist(pl_id, status="active")
            _expanded_pl = pl_id
            held_count = len(await db.get_held_jobs(pl_id))
            await cb.answer(
                f"▶️ Playlist Resumed."
                + (f" Releasing {held_count} held job(s) within a few seconds." if held_count else ""),
                show_alert=True
            )

        elif cb.data.startswith("kill|"):
            pl_id = cb.data.split("|")[1]
            await db.cancel_playlist(pl_id)
            _expanded_pl = None
            await cb.answer("❌ Playlist Terminated & Wiped.", show_alert=True)

        text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
        await safe_edit(app, cb.message.chat.id, cb.message.id, text, kb)


# ──────────────────────────── WORKER & TERMUX LOOPS ────────────────────

async def terminal_loop(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue):
    sys.stdout.write("\033[2J")
    while True:
        await asyncio.sleep(2)
        sys.stdout.write("\033[H")
        sys.stdout.write(f"{C_CYAN}{C_BOLD}=== VK MAINFRAME [LIVE] ==={C_RESET}\n")
        sys.stdout.write(f"QUEUES | DL: {dl_q.qsize()} | ENC: {enc_q.qsize()} | UP: {up_q.qsize()}\n{'─' * 40}\n")

        jobs = await db.get_active_jobs()
        if not jobs:
            sys.stdout.write(f"{C_GREEN}System Idle. Awaiting playlist vectors.{C_RESET}\033[K\n")
        else:
            for j in jobs[:5]:
                stage_val = j.get('stage') or ""
                col = C_YELLOW if "download" in stage_val else C_CYAN if "enc" in stage_val else C_GREEN
                pct = float(j.get('pct', 0.0) or 0.0)
                sys.stdout.write(f"{C_BOLD}[{j['title'][:15]}]{C_RESET} {col}{stage_val}{C_RESET} | [{make_bar(pct, 10)}] {pct:.1f}%\033[K\n")

                log_path = JOBS_DIR / f"JOB_{j['id']}" / "trace.log"
                last_log = "Initializing..."
                if log_path.exists():
                    try:
                        with open(log_path, "r", encoding="utf-8") as f:
                            lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
                            if lines: last_log = re.sub(r"^\[.*?\]\s*", "", lines[-1])
                    except Exception: pass
                sys.stdout.write(f"  ├ 📄 \033[2m{last_log[:70]}\033[0m\033[K\n")

                live_text = _live_ui_text.get(j['id'], "Awaiting data stream...")
                sys.stdout.write(f"  └ 📡 \033[36m{live_text[:75]}\033[0m\033[K\n")

        sys.stdout.write("\033[J")
        sys.stdout.flush()


class WorkerPool:
    """
    Lets worker counts per stage be adjusted at runtime (via /vk_workers)
    without ever killing a worker mid-job. Growing the pool spawns new
    worker tasks right away. Shrinking it just increments a retirement
    counter -- each worker checks that counter only after it has finished
    its current job and called task_done(), so nothing is ever cancelled
    while holding a download/encode/upload (which could orphan a subprocess
    or corrupt a partial file).
    """
    def __init__(self, name: str, worker_factory):
        self.name = name
        self._factory = worker_factory  # callable(pool) -> coroutine
        self.tasks: list[asyncio.Task] = []
        self.target = 0
        self._retire_count = 0

    def current_count(self) -> int:
        self.tasks = [t for t in self.tasks if not t.done()]
        return len(self.tasks)

    async def adjust(self, new_target: int):
        new_target = max(0, new_target)
        current = self.current_count()
        if new_target > current:
            for _ in range(new_target - current):
                self.tasks.append(asyncio.create_task(self._factory(self)))
        elif new_target < current:
            self._retire_count += (current - new_target)
        self.target = new_target

    def should_retire(self) -> bool:
        """Workers call this after finishing a job (task_done() already
        called) and before fetching the next one."""
        if self._retire_count > 0:
            self._retire_count -= 1
            return True
        return False


async def worker_pipeline(db: JobScheduler, dl_q: asyncio.Queue, enc_q: asyncio.Queue, up_q: asyncio.Queue, app: Client):
    dl_engine = DownloaderEngine(db, app)
    enc_engine = EncoderEngine()
    up_engine = UploaderEngine(db, app)

    async def dl_worker(pool: WorkerPool):
        while True:
            jid = await dl_q.get()
            try:
                jobs = await db.get_active_jobs()
                j_data = next((j for j in jobs if j['id'] == jid), None)
                if j_data:
                    try:
                        await dl_engine.execute(j_data)
                        await db.update_job(jid, stage="downloaded")
                        await enc_q.put(jid)
                    except Exception as e:
                        db.log_trace(jid, f"DL Error: {e}")
                        await db.fail_or_retry(j_data, str(e))
            except Exception as e:
                log.exception(f"dl_worker unexpected failure for {jid}: {e}")
            finally:
                dl_q.task_done()
            if pool.should_retire():
                return

    async def enc_worker(pool: WorkerPool):
        while True:
            jid = await enc_q.get()
            try:
                jobs = await db.get_active_jobs()
                j_data = next((j for j in jobs if j['id'] == jid), None)
                if j_data:
                    try:
                        await db.update_job(jid, stage="encoding")
                        await enc_engine.execute(j_data, db)
                        await db.update_job(jid, stage="encoded")
                        await up_q.put(jid)
                    except Exception as e:
                        db.log_trace(jid, f"Enc Error: {e}")
                        await db.fail_or_retry(j_data, str(e))
            except Exception as e:
                log.exception(f"enc_worker unexpected failure for {jid}: {e}")
            finally:
                enc_q.task_done()
            if pool.should_retire():
                return

    async def up_worker(pool: WorkerPool):
        while True:
            jid = await up_q.get()
            try:
                jobs = await db.get_active_jobs()
                j_data = next((j for j in jobs if j['id'] == jid), None)
                if j_data:
                    try:
                        if not (j_data.get('stage') or "").lower().startswith("uploaded"):
                            await db.update_job(jid, stage="uploading")
                            j_data['stage'] = "uploading"
                        await up_engine.execute(j_data)
                    except Exception as e:
                        db.log_trace(jid, f"UP Error: {e}")
                        latest = await db.get_job(jid)
                        already_uploaded = latest and (latest.get('stage') or "").lower().startswith("uploaded")
                        if already_uploaded:
                            # The video is already posted to the channel -- only
                            # finalize()'s bookkeeping/cleanup failed. Do NOT run
                            # fail_or_retry here: it would reset the source item
                            # to 'pending' and delete the job, which would trigger
                            # a full re-download + re-encode + re-upload next pass
                            # (a duplicate post). Instead just retry finalize a
                            # bounded number of times.
                            retries = int(latest.get('retries') or 0) + 1
                            if retries < MAX_RETRIES:
                                await db.update_job(jid, retries=retries)
                                await asyncio.sleep(2)
                                await up_q.put(jid)
                            else:
                                log.error(
                                    f"Job {jid} uploaded successfully but finalize() keeps "
                                    f"failing after {retries} attempts: {e}. Leaving job visible "
                                    f"in dashboard for manual review instead of discarding it."
                                )
                        else:
                            await db.fail_or_retry(j_data, str(e))
            except Exception as e:
                log.exception(f"up_worker unexpected failure for {jid}: {e}")
            finally:
                up_q.task_done()
            if pool.should_retire():
                return

    dl_pool = WorkerPool("dl", dl_worker)
    enc_pool = WorkerPool("enc", enc_worker)
    up_pool = WorkerPool("up", up_worker)

    await dl_pool.adjust(3)
    await enc_pool.adjust(2)
    await up_pool.adjust(2)

    return dl_pool, enc_pool, up_pool


# ──────────────────────────── BOOTSTRAP & REFRESHER ────────────────────

async def dashboard_refresher(app: Client, db: JobScheduler):
    global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
    global _stack_msg_id, _stack_chat_id
    last_state_hash = {}

    while True:
        await asyncio.sleep(2)
        try:
            jobs = await db.get_active_jobs()
            playlists = await db.get_active_playlists()

            needs_update = False
            current_hash = {}

            pl_summary = str([(p['id'], p['status'], p['downloaded']) for p in playlists])
            if last_state_hash.get("playlists") != pl_summary:
                needs_update = True
                last_state_hash["playlists"] = pl_summary

            for j in jobs:
                jid = j['id']
                stage_val = j.get('stage') or ""
                stage_base = stage_val.split('|')[0].strip()
                pct = float(j.get('pct', 0.0) or 0.0)
                pct_bucket = int(pct // 10) * 10

                state_str = f"{stage_base}_{pct_bucket}"
                current_hash[jid] = state_str

                if last_state_hash.get(jid) != state_str:
                    needs_update = True

            active_jids = set(current_hash.keys())
            known_jids = set(k for k in last_state_hash.keys() if k != "playlists")
            if active_jids != known_jids:
                needs_update = True

            if needs_update:
                if _stack_msg_id and _stack_chat_id:
                    try:
                        await safe_edit(app, _stack_chat_id, _stack_msg_id, render_stack_card(jobs), None)
                    except Exception:
                        pass

                for k in list(last_state_hash.keys()):
                    if k != "playlists" and k not in current_hash:
                        del last_state_hash[k]
                for k, v in current_hash.items():
                    last_state_hash[k] = v

            if _dash_msg_id and _dash_chat_id and needs_update:
                text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
                await safe_edit(app, _dash_chat_id, _dash_msg_id, text, kb)

        except Exception:
            log.exception("dashboard_refresher error")


async def main():
    app = Client("vk_stealth_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    db = JobScheduler(DB_PATH)

    dl_q, enc_q, up_q = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()

    # --- CRASH RECOVERY: reconcile DB against disk truth before anything else runs ---
    await db.reconcile_items()
    recovered = await db.reconcile_on_startup()
    for jid in recovered["dl"]:
        await dl_q.put(jid)
    for jid in recovered["enc"]:
        await enc_q.put(jid)
    for jid in recovered["up"]:
        await up_q.put(jid)

    if recovered["dl"] or recovered["enc"] or recovered["up"] or recovered["held"]:
        log.info(
            f"Recovered from previous session: "
            f"{len(recovered['dl'])} to (re)download, "
            f"{len(recovered['enc'])} to (re)encode, "
            f"{len(recovered['up'])} to (re)upload/finalize, "
            f"{len(recovered['held'])} held (paused playlist -- will resume when unpaused)."
        )
    # --- END CRASH RECOVERY ---

    async with app:
        log.info("VK Playlist Bot Online via MTProto.")
        dl_pool, enc_pool, up_pool = await worker_pipeline(db, dl_q, enc_q, up_q, app)
        setup_router(app, db, dl_q, enc_q, up_q, dl_pool, enc_pool, up_pool)
        asyncio.create_task(playlist_drip_feed_loop(db, dl_q, enc_q, up_q, dl_pool, enc_pool, up_pool))
        asyncio.create_task(terminal_loop(db, dl_q, enc_q, up_q))
        asyncio.create_task(dashboard_refresher(app, db))

        if OWNER_ID:
            global _dash_msg_id, _dash_chat_id, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid
            global _stack_msg_id, _stack_chat_id
            text, kb = await render_dashboard(db, _dash_tab, _expanded_pl, _expanded_bucket, _expanded_jid)
            m = await app.send_message(OWNER_ID, text, reply_markup=kb)
            _dash_msg_id, _dash_chat_id = m.id, m.chat.id

            stack_msg = await app.send_message(OWNER_ID, render_stack_card(await db.get_active_jobs()))
            _stack_msg_id, _stack_chat_id = stack_msg.id, stack_msg.chat.id

            try:
                await app.unpin_all_chat_messages(m.chat.id)
                await m.pin(disable_notification=True, both_sides=True)
            except Exception as e:
                log.error(f"Failed to pin dashboard: {e}")

        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: sys.exit(0)
