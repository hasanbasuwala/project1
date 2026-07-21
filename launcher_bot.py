"""
launcher_bot.py — Telegram UI for picking which Stealth Mainframe version runs.

Stays running 24/7 (separate bot token from the worker scripts). Sends an
inline keyboard listing every script in this folder; tapping one stops
whatever's currently running and launches the new one as a subprocess.

SETUP (config.py additions):
    LAUNCHER_BOT_TOKEN = "123456:ABC..."   # new bot from @BotFather
    # API_ID / API_HASH / OWNER_ID are reused from your existing config

RUN (keep it alive persistently, e.g. inside tmux or a systemd service):
    python3.13 launcher_bot.py
"""
import ast
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import config
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

SCRIPT_DIR = Path(__file__).resolve().parent
EXCLUDE = {"run.py", "config.py", "launcher_bot.py"}
STATE_FILE = SCRIPT_DIR / ".launcher_state.json"
LOG_DIR = SCRIPT_DIR / "SysCache" / "launcher_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

OWNER_ID = int(config.OWNER_ID)

# message_id -> set of filenames currently checked in the /delete menu
DELETE_SELECTIONS: dict[int, set] = {}


# ─────────────────────────── script discovery ───────────────────────────

def natural_key(name: str):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", name)]


def discover():
    scripts = []
    for p in sorted(SCRIPT_DIR.glob("*.py"), key=lambda p: natural_key(p.name)):
        if p.name in EXCLUDE or p.name.startswith("_"):
            continue
        desc = ""
        try:
            tree = ast.parse(p.read_text(encoding="utf-8"), filename=str(p))
            doc = ast.get_docstring(tree)
            if doc:
                for line in doc.strip().splitlines():
                    line = line.strip(" -─\t")
                    if line:
                        desc = line
                        break
        except Exception:
            pass
        scripts.append((p, desc))
    return scripts


# ─────────────────────────── process state ───────────────────────────

def _load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_state(state):
    STATE_FILE.write_text(json.dumps(state))


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def running_script():
    state = _load_state()
    pid = state.get("pid")
    if pid and _is_alive(pid):
        return state
    return None


def stop_current(timeout: float = 8.0) -> bool:
    """Ask the running script to shut down (SIGINT → SIGTERM → SIGKILL)."""
    state = _load_state()
    pid = state.get("pid")
    pgid = state.get("pgid")
    if not pid or not _is_alive(pid):
        _save_state({})
        return False

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            break
        except Exception:
            os.kill(pid, sig)

        deadline = time.time() + (timeout if sig != signal.SIGKILL else 3.0)
        while time.time() < deadline:
            if not _is_alive(pid):
                _save_state({})
                return True
            time.sleep(0.3)

    _save_state({})
    return True


def launch(script_path: Path):
    stop_current()
    log_path = LOG_DIR / f"{script_path.stem}.log"
    logf = open(log_path, "ab", buffering=0)
    proc = subprocess.Popen(
        [sys.executable, str(script_path)],
        cwd=str(script_path.parent),
        stdout=logf,
        stderr=logf,
        stdin=subprocess.DEVNULL,
        start_new_session=True,   # own process group, so we can signal it cleanly
    )
    _save_state({
        "pid": proc.pid,
        "pgid": os.getpgid(proc.pid),
        "script": script_path.name,
        "started": time.time(),
    })
    return proc.pid


# ─────────────────────────── bot ───────────────────────────

app = Client(
    "stealth_launcher",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.LAUNCHER_BOT_TOKEN,
)

owner_only = filters.user(OWNER_ID)


def build_menu() -> InlineKeyboardMarkup:
    scripts = discover()
    state = running_script()
    active = state["script"] if state else None

    rows = []
    for p, desc in scripts:
        mark = "🟢 " if p.name == active else "▫️ "
        rows.append([InlineKeyboardButton(f"{mark}{p.name}", callback_data=f"run:{p.name}")])

    if active:
        rows.append([InlineKeyboardButton("🛑 Stop running script", callback_data="stop")])
    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="refresh")])
    return InlineKeyboardMarkup(rows)


def status_text() -> str:
    state = running_script()
    if not state:
        return "**Stealth Mainframe Launcher**\n\nNothing running. Pick a version:"
    uptime = int(time.time() - state["started"])
    return (
        "**Stealth Mainframe Launcher**\n\n"
        f"🟢 Running: `{state['script']}` (pid {state['pid']}, up {uptime}s)\n\n"
        "Pick a version to switch, or stop it:"
    )


@app.on_message(filters.command(["start", "launch"]) & owner_only)
async def cmd_launch(client, message):
    await message.reply(status_text(), reply_markup=build_menu())


@app.on_message(filters.command("status") & owner_only)
async def cmd_status(client, message):
    await message.reply(status_text(), reply_markup=build_menu())


def build_delete_menu(selected: set) -> InlineKeyboardMarkup:
    scripts = discover()
    state = running_script()
    active = state["script"] if state else None

    rows = []
    for p, desc in scripts:
        if p.name == active:
            continue  # can't delete the one that's running
        mark = "✅ " if p.name in selected else "⬜ "
        rows.append([InlineKeyboardButton(f"{mark}{p.name}", callback_data=f"deltoggle:{p.name}")])

    if not rows:
        rows.append([InlineKeyboardButton("(nothing deletable)", callback_data="delcancel")])
    else:
        label = f"🗑 Delete selected ({len(selected)})" if selected else "🗑 Delete selected"
        rows.append([InlineKeyboardButton(label, callback_data="delgo")])
    rows.append([InlineKeyboardButton("Cancel", callback_data="delcancel")])
    return InlineKeyboardMarkup(rows)


@app.on_message(filters.command("delete") & owner_only)
async def cmd_delete(client, message):
    scripts = [p for p, _ in discover()]
    state = running_script()
    active = state["script"] if state else None
    deletable = [p for p in scripts if p.name != active]
    if not deletable:
        await message.reply("Nothing to delete." + (f" (`{active}` is running — stop it first if you want to remove it.)" if active else ""))
        return
    sent = await message.reply(
        "Tap to select scripts, then tap Delete selected:",
        reply_markup=build_delete_menu(set()),
    )
    DELETE_SELECTIONS[sent.id] = set()


@app.on_callback_query(owner_only)
async def on_callback(client, cq: CallbackQuery):
    data = cq.data

    if data == "refresh":
        await cq.message.edit_text(status_text(), reply_markup=build_menu())
        await cq.answer("Refreshed")
        return

    if data == "stop":
        await cq.answer("Stopping…")
        ok = stop_current()
        await cq.message.edit_text(status_text(), reply_markup=build_menu())
        await cq.message.reply("🛑 Stopped." if ok else "Nothing was running.")
        return

    if data.startswith("run:"):
        name = data.split("run:", 1)[1]
        target = SCRIPT_DIR / name
        if not target.exists():
            await cq.answer("Script not found (was it moved?)", show_alert=True)
            return
        await cq.answer(f"Launching {name}…")
        pid = launch(target)
        await cq.message.edit_text(status_text(), reply_markup=build_menu())
        await cq.message.reply(f"🚀 Launched `{name}` (pid {pid}).")
        return

    if data == "delcancel":
        await cq.answer("Cancelled")
        await cq.message.edit_text("Delete cancelled.")
        return

    if data.startswith("delask:"):
        name = data.split("delask:", 1)[1]
        target = SCRIPT_DIR / name
        state = running_script()
        if state and state["script"] == name:
            await cq.answer("That one's running — stop it first.", show_alert=True)
            return
        if not target.exists():
            await cq.answer("Already gone.", show_alert=True)
            return
        await cq.answer()
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, delete it", callback_data=f"delyes:{name}"),
            InlineKeyboardButton("❌ No", callback_data="delcancel"),
        ]])
        await cq.message.edit_text(f"Delete `{name}` permanently? This cannot be undone.", reply_markup=kb)
        return

    if data.startswith("delyes:"):
        name = data.split("delyes:", 1)[1]
        target = SCRIPT_DIR / name
        state = running_script()
        if state and state["script"] == name:
            await cq.answer("That one's running — stop it first.", show_alert=True)
            return
        try:
            target.unlink()
            log_path = LOG_DIR / f"{target.stem}.log"
            log_path.unlink(missing_ok=True)
            await cq.answer("Deleted")
            await cq.message.edit_text(f"🗑 Deleted `{name}`.")
        except FileNotFoundError:
            await cq.answer("Already gone.", show_alert=True)
        except Exception as e:
            await cq.answer("Delete failed", show_alert=True)
            await cq.message.reply(f"Failed to delete `{name}`: `{e}`")
        return


if __name__ == "__main__":
    print(f"Launcher bot up. Scripts folder: {SCRIPT_DIR}")
    app.run()
