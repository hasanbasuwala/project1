import asyncio
import os
import glob
import requests
from playwright.async_api import async_playwright
import config

# --- CONFIGURATION ---
TARGET_URL = "https://vk.com/wall-223870924_29871"
COOKIE_FILE = "extracted_cookies.txt"
VIDEO_DIR = "videos"

BOT_TOKEN = getattr(config, 'BOT_TOKEN', None)
OWNER_ID = getattr(config, 'OWNER_ID', None)
VK_USER = getattr(config, 'VK_USERNAME', None)
VK_PASS = getattr(config, 'VK_PASSWORD', None)
# ---------------------

def send_video_to_telegram(video_path, caption):
    """Uploads the recorded Playwright video clip to Telegram."""
    if not BOT_TOKEN or not OWNER_ID:
        print("[!] WARNING: BOT_TOKEN or OWNER_ID missing in config.py. Skipping Telegram upload.")
        return

    print(f"[*] Uploading video {video_path} to Telegram...")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo"
    
    try:
        with open(video_path, 'rb') as video_file:
            files = {'video': video_file}
            data = {'chat_id': OWNER_ID, 'caption': caption}
            response = requests.post(url, files=files, data=data)
            
            if response.status_code == 200:
                print(f"[+] Successfully sent video clip to Telegram!")
            else:
                print(f"[-] Telegram API Error: {response.text}")
    except Exception as e:
        print(f"[-] Failed to send video to Telegram: {e}")


async def run_test():
    mobile_url = TARGET_URL.replace("vk.com", "m.vk.com").replace("vk.ru", "m.vk.com")
    os.makedirs(VIDEO_DIR, exist_ok=True)
    print(f"[*] Targeting Mobile Site: {mobile_url}")

    async with async_playwright() as p:
        print("[*] Launching Playwright with Live Screen Recording...")
        mobile_device = p.devices['iPhone 13']
        
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage"
            ]
        )
        
        # Enable video recording directly in context
        context = await browser.new_context(
            **mobile_device,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            record_video_dir=VIDEO_DIR,
            record_video_size={"width": 390, "height": 844}
        )

        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        if os.path.exists(COOKIE_FILE):
            print(f"[*] Loading cookies from {COOKIE_FILE}...")
            with open(COOKIE_FILE, 'r', encoding='utf-8') as f:
                cookie_str = f.read().strip()
            
            pw_cookies = []
            domains = [".vk.com", "m.vk.com", ".vk.ru", "m.vk.ru"]
            for item in cookie_str.split(';'):
                if '=' in item:
                    k, v = item.strip().split('=', 1)
                    for d in domains:
                        pw_cookies.append({"name": k, "value": v, "domain": d, "path": "/"})
            
            if pw_cookies:
                await context.add_cookies(pw_cookies)
                print(f"[*] Injected {len(pw_cookies)} cookies into browser context.")

        page = await context.new_page()
        found_media = []

        # 1. WIRETAP BROWSER CONSOLE: Catch JavaScript errors 
        page.on("console", lambda msg: print(f"[JS {msg.type.upper()}] {msg.text}") if msg.type in ["error", "warning"] else None)

        # 2. WIRETAP FAILED REQUESTS: Catch connections VK aggressively drops
        page.on("requestfailed", lambda req: print(f"[NETWORK DROP] {req.url[:120]} - {req.failure}"))

        # 3. NETWORK SNIFFER: Updated to catch HTTP error codes
        async def handle_response(response):
            try:
                req = response.request
                url_lower = req.url.lower()
                content_type = response.headers.get("content-type", "").lower()
                
                # ---> DETECT API BLOCKS (403 Forbidden, 429 Too Many Requests) <---
                if response.status >= 400:
                    # Ignore harmless ad-block/tracker failures
                    if not any(bad in url_lower for bad in ["tracker", "log", "stats"]):
                        print(f"[HTTP {response.status}] Blocked Request: {req.url[:120]}...")
                
                # Normal Media Sniffing
                bad_keywords = ["google", "analytics", "ad", "beacon", "vast", "blank", "trailer", "promo", ".mp3", "audio"]
                if any(bad in url_lower for bad in bad_keywords): return
                
                if "video/" in content_type or ".mp4" in url_lower or ".ts" in url_lower or ".m3u8" in url_lower:
                    print(f"\n[+] SNIFFER CAUGHT MEDIA: {content_type}")
                    print(f"    URL: {req.url[:150]}...")
                    found_media.append(req.url)
            except Exception:
                pass

        page.on("response", handle_response)

        # Execution Sequence
        print("\n[*] Navigating to target post...")
        await page.goto(mobile_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(4000)

        login_btn = page.locator("a[href*='login'], .a_login, button:has-text('Log in')")
        if await login_btn.count() > 0 and await login_btn.first.is_visible():
            print("[*] Guest wall hit. Clicking Log In...")
            await login_btn.first.click()
            await page.wait_for_timeout(3000)

        login_input = page.locator("input[name='login'], input[name='email'], input[type='text']")
        if await login_input.count() > 0 and await login_input.first.is_visible():
            print("[*] Submitting credentials...")
            if VK_USER:
                await login_input.first.fill(VK_USER)
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(3000)
            
            pass_input = page.locator("input[name='password'], input[type='password']")
            if await pass_input.count() > 0 and await pass_input.first.is_visible() and VK_PASS:
                await pass_input.first.fill(VK_PASS)
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(6000)
                
                print("[*] Re-navigating to target post...")
                await page.goto(mobile_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(4000)

        print("[*] Scrolling to trigger video player rendering...")
        await page.mouse.wheel(0, 400)
        await page.wait_for_timeout(5000)

        play_btn = page.locator(".VideoIcon, .MediaGrid__item, div[aria-label*='Play'], .vv_inline_video")
        if await play_btn.count() > 0 and await play_btn.first.is_visible():
            print("[*] Play button detected! Clicking...")
            await play_btn.first.click(force=True)
            await page.wait_for_timeout(5000)

        print("\n" + "="*50)
        print(f"TEST COMPLETE | Media Links Captured: {len(found_media)}")
        for i, link in enumerate(found_media):
            print(f"{i+1}. {link[:100]}...")
        print("="*50)

        # CLOSE CONTEXT FIRST so Playwright flushes and saves the video file
        await context.close()
        await browser.close()

    # Find and upload the generated video file
    video_files = glob.glob(os.path.join(VIDEO_DIR, "*.webm"))
    if video_files:
        latest_video = max(video_files, key=os.path.getctime)
        send_video_to_telegram(latest_video, "🎥 Playwright Live Recording of Mobile Navigation")


if __name__ == "__main__":
    asyncio.run(run_test())