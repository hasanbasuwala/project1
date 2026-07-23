import asyncio
import os
import glob
import requests
from playwright.async_api import async_playwright
from playwright_stealth import Stealth  # <-- THIS MUST BE HERE
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
    if not BOT_TOKEN or not OWNER_ID:
        print("[!] WARNING: BOT_TOKEN or OWNER_ID missing. Skipping Telegram upload.")
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

    # --- THE NEW v2.x STEALTH WRAPPER ---
    async with Stealth().use_async(async_playwright()) as p:
        print("[*] Launching Playwright (HEADED mode via Xvfb with Stealth v2)...")
        mobile_device = p.devices['iPhone 13']
        
        browser = await p.chromium.launch(
            executable_path='/usr/bin/chromium',
            headless=False,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--use-gl=swiftshader",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security"
            ]
        )
        
        context = await browser.new_context(
            **mobile_device,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            record_video_dir=VIDEO_DIR,
            record_video_size={"width": 390, "height": 844}
        )
        
        # ... [Keep your cookie injection code exactly the same here] ...

        page = await context.new_page()
        
        # REMOVE the old manual stealth = Stealth() and await stealth.apply_stealth_async(page) lines!
        # The new wrapper above already handles it automatically for every page.
        
        found_media = []

        # ... [Keep the rest of your page.on sniffer and navigation sequence exactly the same] ...

        # Network Sniffer with API Block Detection
        async def handle_response(response):
            try:
                req = response.request
                url_lower = req.url.lower()
                content_type = response.headers.get("content-type", "").lower()
                
                if response.status >= 400 and not any(bad in url_lower for bad in ["tracker", "log", "stats", "analytics"]):
                    print(f"[HTTP {response.status}] Blocked Request: {req.url[:120]}...")
                
                bad_keywords = ["google", "analytics", "ad", "beacon", "vast", "blank", "trailer", "promo", ".mp3", "audio"]
                if any(bad in url_lower for bad in bad_keywords): return
                
                if "video/" in content_type or ".mp4" in url_lower or ".ts" in url_lower or ".m3u8" in url_lower:
                    print(f"\n[+] SNIFFER CAUGHT MEDIA: {content_type}")
                    print(f"    URL: {req.url[:150]}...")
                    found_media.append(req.url)
            except Exception:
                pass

        page.on("response", handle_response)
        page.on("console", lambda msg: print(f"[JS {msg.type.upper()}] {msg.text}") if msg.type in ["error", "warning"] else None)

        print("\n[*] Navigating to target post...")
        # Generous timeout for Termux environments
        await page.goto(mobile_url, wait_until="domcontentloaded", timeout=90000) 
        await page.wait_for_timeout(6000)

        login_btn = page.locator("a[href*='login'], .a_login, button:has-text('Log in')")
        if await login_btn.count() > 0 and await login_btn.first.is_visible():
            print("[*] Guest wall hit. Clicking Log In...")
            await login_btn.first.click()
            await page.wait_for_timeout(4000)

        login_input = page.locator("input[name='login'], input[name='email'], input[type='text']")
        if await login_input.count() > 0 and await login_input.first.is_visible():
            print("[*] Submitting credentials...")
            if VK_USER:
                # Typing slowly mimics human behavior
                await login_input.first.fill(VK_USER)
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(3000)
            
            pass_input = page.locator("input[name='password'], input[type='password']")
            if await pass_input.count() > 0 and await pass_input.first.is_visible() and VK_PASS:
                await pass_input.first.fill(VK_PASS)
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(8000)
                
                print("[*] Re-navigating to target post...")
                await page.goto(mobile_url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(6000)

        print("[*] Scrolling to trigger video player rendering...")
        await page.mouse.wheel(0, 500)
        await page.wait_for_timeout(6000)

        play_btn = page.locator(".VideoIcon, .MediaGrid__item, div[aria-label*='Play'], .vv_inline_video")
        if await play_btn.count() > 0 and await play_btn.first.is_visible():
            print("[*] Play button detected! Clicking...")
            await play_btn.first.click(force=True)
            await page.wait_for_timeout(8000)

        print("\n" + "="*50)
        print(f"TEST COMPLETE | Media Links Captured: {len(found_media)}")
        for i, link in enumerate(found_media):
            print(f"{i+1}. {link[:100]}...")
        print("="*50)

        await context.close()
        await browser.close()

    # Upload video result
    video_files = glob.glob(os.path.join(VIDEO_DIR, "*.webm"))
    if video_files:
        latest_video = max(video_files, key=os.path.getctime)
        send_video_to_telegram(latest_video, "🎥 Playwright Xvfb Recording (Stealth Mode)")

if __name__ == "__main__":
    asyncio.run(run_test())