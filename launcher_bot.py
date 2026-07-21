"""
launcher_bot.py — Telegram UI for picking which Stealth Mainframe version runs.

Stays running 24/7 (separate bot token from the worker scripts). Sends an
inline keyboard listing every script in this folder; tapping one stops
whatever's running and opens the new one inside a tmux session named
"stealth_run" — so it gets the actual Termux terminal UI (terminal_loop,
live logs, dashboard prints) instead of a silent background process.
Picking a different script kills that tmux session and opens a fresh one.

REQUIRES: tmux installed in Termux (`pkg install tmux`).

SETUP (config.py additions):
    LAUNCHER_BOT_TOKEN = "123456:ABC..."   # new bot from @BotFather
    # API_ID / API_HASH / OWNER_ID are reused from your existing config

RUN (keep it alive persistently, e.g. inside its own tmux/systemd unit):
    python3.13 launcher_bot.py

To watch a running script's live terminal:
    tmux attach -t stealth_run
    (detach without killing it: Ctrl-b then d)
"""
import ast
import asyncio
import json
import re
import shlex
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

TMUX_SESSION = "stealth_run"

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


def _tmux_alive() -> bool:
    return subprocess.run(
        ["tmux", "has-session", "-t", TMUX_SESSION],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode == 0


def running_script():
    """Returns {'script': name, 'started': ts} if the tmux session is alive, else None."""
    if not _tmux_alive():
        if STATE_FILE.exists():
            _save_state({})
        return None
    state = _load_state()
    if not state.get("script"):
        return None
    return state


def stop_current(timeout: float = 8.0) -> bool:
    """Ctrl-C into the pane (graceful, same as pressing it yourself), then kill-session if it won't die."""
    if not _tmux_alive():
        _save_state({})
        return False

    subprocess.run(
        ["tmux", "send-keys", "-t", TMUX_SESSION, "C-c"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _tmux_alive():
            _save_state({})
            return True
        time.sleep(0.3)

    subprocess.run(
        ["tmux", "kill-session", "-t", TMUX_SESSION],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    _save_state({})
    return True


def launch(script_path: Path):
    stop_current()
    log_path = LOG_DIR / f"{script_path.stem}.log"
    # tee mirrors output to a log file too, in case you want to check it
    # after the tmux session has already closed.
    cmd = (
        f"cd {shlex.quote(str(script_path.parent))} && "
        f"{shlex.quote(sys.executable)} {shlex.quote(str(script_path))} "
        f"2>&1 | tee -a {shlex.quote(str(log_path))}"
    )
    result = subprocess.run(["tmux", "new-session", "-d", "-s", TMUX_SESSION, cmd])
    if result.returncode != 0:
        raise RuntimeError("Failed to start tmux session — is tmux installed? (`pkg install tmux`)")
    _save_state({"script": script_path.name, "started": time.time()})


def capture_pane(lines: int = 40):
    """Grab the last N lines of the tmux pane, so we can show the actual
    Termux logger output inside Telegram instead of making you go attach."""
    if not _tmux_alive():
        return None
    r = subprocess.run(
        ["tmux", "capture-pane", "-t", TMUX_SESSION, "-p", "-S", f"-{lines}"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return None
    return r.stdout.strip()


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
        rows.append([InlineKeyboardButton("📟 View live log", callback_data="viewlog")])
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
        f"🟢 Running: `{state['script']}` (up {uptime}s)\n"
        f"Watch it live: `tmux attach -t {TMUX_SESSION}` (detach with Ctrl-b then d)\n\n"
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


async def _safe_edit(message, text, reply_markup=None):
    """edit_text but ignore Telegram's 'message not modified' error, which
    happens when a toggle lands back on a state identical to the current one."""
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            raise


@app.on_callback_query(owner_only)
async def on_callback(client, cq: CallbackQuery):
    data = cq.data
    try:
        await _handle_callback(cq, data)
    except Exception as e:
        # Guarantee the tap always resolves instead of spinning forever,
        # even if something above threw.
        try:
            await cq.answer(f"Error: {e}", show_alert=True)
        except Exception:
            pass


async def _handle_callback(cq: CallbackQuery, data: str):
    if data == "refresh":
        await cq.answer("Refreshed")
        await _safe_edit(cq.message, status_text(), build_menu())
        return

    if data == "stop":
        await cq.answer("Stopping…")
        ok = stop_current()
        await _safe_edit(cq.message, status_text(), build_menu())
        await cq.message.reply("🛑 Stopped." if ok else "Nothing was running.")
        return

    if data.startswith("run:"):
        name = data.split("run:", 1)[1]
        target = SCRIPT_DIR / name
        if not target.exists():
            await cq.answer("Script not found (was it moved?)", show_alert=True)
            return
        await cq.answer(f"Launching {name}…")
        launch(target)
        await _safe_edit(cq.message, status_text(), build_menu())

        # give it a moment to print its startup banner, then show the actual
        # terminal output right here instead of making you go attach tmux
        await asyncio.sleep(2)
        log = capture_pane()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh log", callback_data="viewlog")]])
        text = f"📟 Live log — `{name}`:\n```\n{(log or '(no output yet)')[-3500:]}\n```"
        await cq.message.reply(text, reply_markup=kb)
        return

    if data == "viewlog":
        state = running_script()
        if not state:
            await cq.answer("Nothing running.", show_alert=True)
            return
        await cq.answer("Refreshed")
        log = capture_pane()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh log", callback_data="viewlog")]])
        text = f"📟 Live log — `{state['script']}`:\n```\n{(log or '(no output yet)')[-3500:]}\n```"
        await _safe_edit(cq.message, text, kb)
        return

    if data == "delcancel":
        DELETE_SELECTIONS.pop(cq.message.id, None)
        await cq.answer("Cancelled")
        await _safe_edit(cq.message, "Delete cancelled.")
        return

    if data.startswith("deltoggle:"):
        name = data.split("deltoggle:", 1)[1]
        selected = DELETE_SELECTIONS.setdefault(cq.message.id, set())
        state = running_script()
        if state and state["script"] == name:
            await cq.answer("That one's running — stop it first.", show_alert=True)
            return
        if not (SCRIPT_DIR / name).exists():
            await cq.answer("Already gone.", show_alert=True)
            return
        if name in selected:
            selected.discard(name)
        else:
            selected.add(name)
        await cq.answer()
        await _safe_edit(
            cq.message,
            "Tap to select scripts, then tap Delete selected:",
            build_delete_menu(selected),
        )
        return

    if data == "delgo":
        selected = DELETE_SELECTIONS.get(cq.message.id, set())
        if not selected:
            await cq.answer("Select at least one script first.", show_alert=True)
            return
        await cq.answer()
        names = "\n".join(f"• `{n}`" for n in sorted(selected))
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"✅ Yes, delete {len(selected)}", callback_data="delyesall"),
            InlineKeyboardButton("❌ No", callback_data="delcancel"),
        ]])
        await _safe_edit(
            cq.message,
            f"Delete these {len(selected)} script(s) permanently? This cannot be undone.\n\n{names}",
            kb,
        )
        return

    if data == "delyesall":
        selected = DELETE_SELECTIONS.pop(cq.message.id, set())
        if not selected:
            await cq.answer("Nothing selected.", show_alert=True)
            return
        state = running_script()
        active = state["script"] if state else None
        deleted, skipped, failed = [], [], []
        for name in sorted(selected):
            if name == active:
                skipped.append(name)
                continue
            target = SCRIPT_DIR / name
            try:
                target.unlink()
                (LOG_DIR / f"{target.stem}.log").unlink(missing_ok=True)
                deleted.append(name)
            except FileNotFoundError:
                skipped.append(name)
            except Exception as e:
                failed.append(f"{name} ({e})")
        await cq.answer("Done")
        lines = []
        if deleted:
            lines.append("🗑 Deleted: " + ", ".join(f"`{n}`" for n in deleted))
        if skipped:
            lines.append("⏭ Skipped (running/missing): " + ", ".join(f"`{n}`" for n in skipped))
        if failed:
            lines.append("⚠️ Failed: " + ", ".join(failed))
        await _safe_edit(cq.message, "\n".join(lines) or "Nothing happened.")
        return


async def _startup():
    await app.start()
    print("🚀 Launcher Online!!")
    try:
        await app.send_message(
            OWNER_ID,
            "🚀 **Launcher Online!!**\n\n" + status_text(),
            reply_markup=build_menu(),
        )
    except Exception as e:
        print(f"Could not DM owner on startup: {e}")

    from pyrogram import idle
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.run(_startup())
