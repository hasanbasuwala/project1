"""
launcher_bot.py — Telegram UI for running Stealth Mainframe scripts.

Stays running 24/7 (separate bot token from the worker scripts). Sends an
inline keyboard listing every script in this folder. Tapping a STOPPED
script starts it in its OWN tmux session — it does not touch anything
else that's already running. Tapping a RUNNING script opens a per-script
menu (view log / restart / stop). A "🎛 Dashboard" view lists everything
currently running at once with quick log/stop buttons, and "🛑 Stop All"
kills everything in one tap.

Each script gets a dedicated tmux session named `stealth_<script_stem>`,
so it gets the actual Termux terminal UI (terminal_loop, live logs,
dashboard prints) instead of a silent background process, and you can
run as many of them side by side as you want.

REQUIRES: tmux installed in Termux (`pkg install tmux`).

SETUP (config.py additions):
    LAUNCHER_BOT_TOKEN = "123456:ABC..."   # new bot from @BotFather
    # API_ID / API_HASH / OWNER_ID are reused from your existing config

RUN (keep it alive persistently, e.g. inside its own tmux/systemd unit):
    python3.13 launcher_bot.py

Termux-side visibility (also shown in the bot's /help and dashboard footer):
    tmux ls                          # list every running session
    tmux attach -t stealth_vk_bot    # watch one live
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
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

SCRIPT_DIR = Path(__file__).resolve().parent
EXCLUDE = {"run.py", "config.py", "launcher_bot.py"}
STATE_FILE = SCRIPT_DIR / ".launcher_state.json"
LOG_DIR = SCRIPT_DIR / "SysCache" / "launcher_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

SESSION_PREFIX = "stealth_"

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


def session_name(script_name: str) -> str:
    """Deterministic tmux session name for a given script filename."""
    stem = Path(script_name).stem
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", stem)
    return f"{SESSION_PREFIX}{safe}"


# ─────────────────────────── process / tmux state ───────────────────────────

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state))


def _tmux_alive(session: str) -> bool:
    return subprocess.run(
        ["tmux", "has-session", "-t", session],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode == 0


def _list_tmux_sessions() -> set:
    r = subprocess.run(
        ["tmux", "ls", "-F", "#{session_name}"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return set()
    return {line.strip() for line in r.stdout.splitlines() if line.strip()}


def reconcile_state() -> dict:
    """Drop any script whose tmux session has died, keep the rest. Called
    before every menu render so the UI never lies about what's running.
    Also runs on launcher startup, so scripts survive a launcher restart."""
    scripts = {p.name for p, _ in discover()}
    alive_sessions = _list_tmux_sessions()
    state = _load_state()
    changed = False

    for name in list(state.keys()):
        if name not in scripts or session_name(name) not in alive_sessions:
            del state[name]
            changed = True

    # Pick up sessions that are alive but weren't tracked (e.g. launcher
    # was restarted while a script kept running).
    for p, _ in discover():
        sess = session_name(p.name)
        if sess in alive_sessions and p.name not in state:
            state[p.name] = {"started": time.time(), "recovered": True}
            changed = True

    if changed:
        _save_state(state)
    return state


def is_running(script_name: str) -> bool:
    return script_name in reconcile_state()


def stop_script(script_name: str, timeout: float = 8.0) -> bool:
    """Ctrl-C into the pane (graceful, same as pressing it yourself), then
    kill-session if it won't die. Only touches this one script's session."""
    sess = session_name(script_name)
    if not _tmux_alive(sess):
        state = _load_state()
        if script_name in state:
            del state[script_name]
            _save_state(state)
        return False

    subprocess.run(
        ["tmux", "send-keys", "-t", sess, "C-c"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _tmux_alive(sess):
            break
        time.sleep(0.3)
    else:
        subprocess.run(
            ["tmux", "kill-session", "-t", sess],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    state = _load_state()
    state.pop(script_name, None)
    _save_state(state)
    return True


def stop_all() -> list:
    stopped = []
    for name in list(reconcile_state().keys()):
        if stop_script(name):
            stopped.append(name)
    return stopped


def launch(script_path: Path, restart: bool = False):
    sess = session_name(script_path.name)
    if _tmux_alive(sess):
        if not restart:
            return  # already running, leave it alone
        stop_script(script_path.name)

    log_path = LOG_DIR / f"{script_path.stem}.log"
    # tee mirrors output to a log file too, in case you want to check it
    # after the tmux session has already closed.
    cmd = (
        f"cd {shlex.quote(str(script_path.parent))} && "
        f"{shlex.quote(sys.executable)} {shlex.quote(str(script_path))} "
        f"2>&1 | tee -a {shlex.quote(str(log_path))}"
    )
    result = subprocess.run(["tmux", "new-session", "-d", "-s", sess, cmd])
    if result.returncode != 0:
        raise RuntimeError("Failed to start tmux session — is tmux installed? (`pkg install tmux`)")

    state = _load_state()
    state[script_path.name] = {"started": time.time()}
    _save_state(state)


def capture_pane(script_name: str, lines: int = 40):
    """Grab the last N lines of a script's tmux pane, so we can show the
    actual Termux logger output inside Telegram instead of making you go attach."""
    sess = session_name(script_name)
    if not _tmux_alive(sess):
        return None
    r = subprocess.run(
        ["tmux", "capture-pane", "-t", sess, "-p", "-S", f"-{lines}"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return None
    return r.stdout.strip()


def fmt_uptime(started: float) -> str:
    secs = int(time.time() - started)
    if secs < 60:
        return f"{secs}s"
    mins, secs = divmod(secs, 60)
    if mins < 60:
        return f"{mins}m{secs:02d}s"
    hrs, mins = divmod(mins, 60)
    return f"{hrs}h{mins:02d}m"


# ─────────────────────────── bot ───────────────────────────

app = Client(
    "stealth_launcher",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.LAUNCHER_BOT_TOKEN,
)

owner_only = filters.user(OWNER_ID)

TMUX_HINT = (
    "`tmux ls` to list sessions · `tmux attach -t <session>` to watch one live "
    "(detach with `Ctrl-b` then `d`)"
)


def build_menu() -> InlineKeyboardMarkup:
    scripts = discover()
    state = reconcile_state()

    rows = []
    for p, _ in scripts:
        running = p.name in state
        mark = "🟢 " if running else "▫️ "
        cb = f"manage:{p.name}" if running else f"run:{p.name}"
        rows.append([InlineKeyboardButton(f"{mark}{p.name}", callback_data=cb)])

    if state:
        rows.append([InlineKeyboardButton(f"🎛 Dashboard ({len(state)} running)", callback_data="dashboard")])
        rows.append([InlineKeyboardButton("🛑 Stop All", callback_data="stopall")])
    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="refresh")])
    return InlineKeyboardMarkup(rows)


def status_text() -> str:
    state = reconcile_state()
    if not state:
        return "**Stealth Mainframe Launcher**\n\nNothing running. Pick a script to start it:"
    lines = [f"🟢 `{name}` — up {fmt_uptime(info['started'])}" for name, info in sorted(state.items())]
    return (
        "**Stealth Mainframe Launcher**\n\n"
        f"{len(state)} script(s) running:\n" + "\n".join(lines) +
        f"\n\n{TMUX_HINT}\n\nTap ▫️ to start something new, or 🟢 to manage a running one:"
    )


def build_manage_menu(script_name: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📟 View live log", callback_data=f"viewlog:{script_name}")],
        [InlineKeyboardButton("🔁 Restart", callback_data=f"restart:{script_name}")],
        [InlineKeyboardButton("🛑 Stop", callback_data=f"stop:{script_name}")],
        [InlineKeyboardButton("⬅️ Back", callback_data="refresh")],
    ]
    return InlineKeyboardMarkup(rows)


def manage_text(script_name: str) -> str:
    state = reconcile_state()
    info = state.get(script_name)
    if not info:
        return f"`{script_name}` is not running."
    sess = session_name(script_name)
    return (
        f"🟢 `{script_name}`\n"
        f"Session: `{sess}` · up {fmt_uptime(info['started'])}\n\n"
        f"`tmux attach -t {sess}`"
    )


def build_dashboard_menu(state: dict) -> InlineKeyboardMarkup:
    rows = []
    for name in sorted(state):
        info = state[name]
        rows.append([
            InlineKeyboardButton(f"{name} · {fmt_uptime(info['started'])}", callback_data=f"viewlog:{name}"),
            InlineKeyboardButton("🛑", callback_data=f"stop:{name}"),
        ])
    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="dashboard")])
    rows.append([InlineKeyboardButton("🛑 Stop All", callback_data="stopall")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="refresh")])
    return InlineKeyboardMarkup(rows)


def dashboard_text(state: dict) -> str:
    if not state:
        return "**🎛 Dashboard**\n\nNothing running."
    return f"**🎛 Dashboard** — {len(state)} running\n\n{TMUX_HINT}\n\nTap a script for its log, or 🛑 to stop it:"


@app.on_message(filters.command(["start", "launch"]) & owner_only)
async def cmd_launch(client, message):
    await message.reply(status_text(), reply_markup=build_menu())


@app.on_message(filters.command("status") & owner_only)
async def cmd_status(client, message):
    await message.reply(status_text(), reply_markup=build_menu())


@app.on_message(filters.command("dashboard") & owner_only)
async def cmd_dashboard(client, message):
    state = reconcile_state()
    await message.reply(dashboard_text(state), reply_markup=build_dashboard_menu(state))


@app.on_message(filters.command("help") & owner_only)
async def cmd_help(client, message):
    await message.reply(
        "**Commands**\n"
        "/launch or /status — main menu, tap a script to start/manage it\n"
        "/dashboard — everything currently running, quick log/stop buttons\n"
        "/delete — remove script files (running ones are protected)\n\n"
        f"**In Termux**\n{TMUX_HINT}"
    )


def build_delete_menu(selected: set) -> InlineKeyboardMarkup:
    scripts = discover()
    state = reconcile_state()

    rows = []
    for p, _ in scripts:
        if p.name in state:
            continue  # can't delete something that's running
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
    state = reconcile_state()
    deletable = [p for p in scripts if p.name not in state]
    if not deletable:
        await message.reply("Nothing to delete — every script is currently running. Stop one first.")
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

    if data == "dashboard":
        state = reconcile_state()
        await cq.answer("Refreshed")
        await _safe_edit(cq.message, dashboard_text(state), build_dashboard_menu(state))
        return

    if data == "stopall":
        await cq.answer("Stopping everything…")
        stopped = stop_all()
        await _safe_edit(cq.message, status_text(), build_menu())
        if stopped:
            await cq.message.reply("🛑 Stopped: " + ", ".join(f"`{n}`" for n in stopped))
        else:
            await cq.message.reply("Nothing was running.")
        return

    if data.startswith("run:"):
        name = data.split("run:", 1)[1]
        target = SCRIPT_DIR / name
        if not target.exists():
            await cq.answer("Script not found (was it moved?)", show_alert=True)
            return
        if is_running(name):
            await cq.answer("Already running.", show_alert=True)
            return
        await cq.answer(f"Launching {name}…")
        launch(target)
        await _safe_edit(cq.message, status_text(), build_menu())

        # give it a moment to print its startup banner, then show the actual
        # terminal output right here instead of making you go attach tmux
        await asyncio.sleep(2)
        log = capture_pane(name)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh log", callback_data=f"viewlog:{name}")]])
        text = f"📟 Live log — `{name}` (`{session_name(name)}`):\n```\n{(log or '(no output yet)')[-3500:]}\n```"
        await cq.message.reply(text, reply_markup=kb)
        return

    if data.startswith("manage:"):
        name = data.split("manage:", 1)[1]
        if not is_running(name):
            await cq.answer("Not running anymore.", show_alert=True)
            await _safe_edit(cq.message, status_text(), build_menu())
            return
        await cq.answer()
        await _safe_edit(cq.message, manage_text(name), build_manage_menu(name))
        return

    if data.startswith("restart:"):
        name = data.split("restart:", 1)[1]
        target = SCRIPT_DIR / name
        if not target.exists():
            await cq.answer("Script not found (was it moved?)", show_alert=True)
            return
        await cq.answer(f"Restarting {name}…")
        launch(target, restart=True)
        await asyncio.sleep(2)
        await _safe_edit(cq.message, manage_text(name), build_manage_menu(name))
        return

    if data.startswith("viewlog:"):
        name = data.split("viewlog:", 1)[1]
        if not is_running(name):
            await cq.answer("Not running anymore.", show_alert=True)
            return
        await cq.answer("Refreshed")
        log = capture_pane(name)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh log", callback_data=f"viewlog:{name}")],
            [InlineKeyboardButton("⬅️ Back", callback_data=f"manage:{name}")],
        ])
        text = f"📟 Live log — `{name}` (`{session_name(name)}`):\n```\n{(log or '(no output yet)')[-3500:]}\n```"
        await _safe_edit(cq.message, text, kb)
        return

    if data.startswith("stop:"):
        name = data.split("stop:", 1)[1]
        await cq.answer("Stopping…")
        ok = stop_script(name)
        await _safe_edit(cq.message, status_text(), build_menu())
        await cq.message.reply(f"🛑 Stopped `{name}`." if ok else f"`{name}` wasn't running.")
        return

    if data == "delcancel":
        DELETE_SELECTIONS.pop(cq.message.id, None)
        await cq.answer("Cancelled")
        await _safe_edit(cq.message, "Delete cancelled.")
        return

    if data.startswith("deltoggle:"):
        name = data.split("deltoggle:", 1)[1]
        selected = DELETE_SELECTIONS.setdefault(cq.message.id, set())
        if is_running(name):
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
        state = reconcile_state()
        deleted, skipped, failed = [], [], []
        for name in sorted(selected):
            if name in state:
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
    async with app:
        reconcile_state()  # pick up sessions that survived a launcher restart
        print("🚀 Launcher Online!!")
        try:
            await app.send_message(
                OWNER_ID,
                "🚀 **Launcher Online!!**\n\n" + status_text(),
                reply_markup=build_menu(),
            )
        except Exception as e:
            print(f"Could not DM owner on startup: {e}")
        await idle()


if __name__ == "__main__":
    app.run(_startup())
