import re
import logging
import socket
import asyncio
import aiodns
import warnings
from async_timeout import timeout
from urllib.parse import urlparse
from playwright.async_api import Route, Request
from playwright._impl._errors import TargetClosedError
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError
)
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging configuration to match the main application's log settings
LOG_FILE_PATH = "./logs/service.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ScraperService")

# Set Coinbase Wallet Browser User-Agent (Example based on known data)
IPHONE_FIREFOX_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) FxiOS/131.0 Mobile/15E148 Safari/605.1.15"
)

# Set to track already requested URLs
loaded_resources = set()


async def dns_resolve_async(domain, timeout_duration=5):
    """
    Asynchronously resolves a domain name to check if it is reachable.
    """
    resolver = aiodns.DNSResolver()

    try:
        async with timeout(timeout_duration):
            result = await resolver.gethostbyname(domain, socket.AF_INET)
            if result and result.addresses:
                return result.addresses[0]
            return None
    except (aiodns.error.DNSError, asyncio.TimeoutError) as e:
        logger.debug(f"DNS resolution failed for {domain}: {e}")
        return None
    except Exception as e:
        logger.debug(f"Unexpected error during DNS resolution for {domain}: {e}")
        return None


def is_redirected_to_different_domain(initial_url, final_url):
    """
    Compare domains to check if redirected to a different domain.
    """
    initial_domain = urlparse(initial_url).netloc
    final_domain = urlparse(final_url).netloc
    return final_domain if initial_domain != final_domain else False


def detect_obfuscation(js_code, threshold=5, density_threshold=0.05):
    """
    Detect obfuscation by analyzing the density of hexadecimal patterns.
    """
    hex_pattern = r'0x[0-9a-fA-F]+'
    hex_matches = re.findall(hex_pattern, js_code)
    hex_count = len(hex_matches)
    code_length = len(js_code)
    density = hex_count / code_length if code_length > 0 else 0
    return hex_count > threshold and density > density_threshold


async def scrape_website_async(
        url, max_wait_time=14000, check_interval=400,
        no_change_limit=3, change_limit=5):
    if not url.startswith("http"):
        url = "https://" + url

    redirect_domain = False
    has_obfuscation = False
    js_filepaths = []
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    script_urls = []
    script_contents = []
    script_capture_tasks = []
    html_content = ''

    resolved_ip = await dns_resolve_async(domain)
    if not resolved_ip:
        logger.debug(f"Domain {domain} does not exist or DNS resolution failed.")
        return {
            "domain": url,
            "status_code": "DNS Error",
            "ip": None,
            "obfuscation": has_obfuscation,
            "script_paths": str(js_filepaths),
            "redirect_domain": False,
            "html_content": "",
            "error": f"DNS resolution failed for {domain}"
        }

    page, context, browser = None, None, None

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True, args=["--ignore-certificate-errors"]
            )
            context = await browser.new_context(
                user_agent=IPHONE_FIREFOX_AGENT,
                viewport={"width": 390, "height": 844},
                java_script_enabled=True,
                locale="en-GB",
                timezone_id="America/New_York"
            )
            page = await context.new_page()

            await page.route(
                "**/*",
                lambda route, request: asyncio.create_task(handle_request(route, request))
            )
            
            page.on(
                "response",
                lambda response: script_capture_tasks.append(
                    asyncio.create_task(
                        capture_scripts_async(response, script_urls, script_contents)
                    )
                )
            )

            await page.goto(url, timeout=max_wait_time)
            html_content = await page.content()
            await page.wait_for_selector("body", timeout=max_wait_time)

            await asyncio.sleep(1)

            final_url = page.url
            redirect_domain = is_redirected_to_different_domain(url, final_url)
            unchanged_iterations, changed_iterations = 0, 0
            previous_content = None

            while True:
                current_content = await page.content()
                if current_content != previous_content:
                    previous_content = current_content
                    unchanged_iterations = 0
                    changed_iterations += 1
                    if changed_iterations >= change_limit:
                        break
                else:
                    unchanged_iterations += 1
                    changed_iterations = 0
                if unchanged_iterations >= no_change_limit:
                    break
                await asyncio.sleep(check_interval / 800.0)

            html_content = current_content or ""

            try:
                status_code = await page.evaluate("() => document.readyState")
                status_code = "complete" if status_code == "complete" else "loading"
            except asyncio.TimeoutError:
                status_code = "loading"
            except Exception as e:
                logger.error(f"Error during page evaluation: {e}")
                status_code = "error"

            await asyncio.gather(*script_capture_tasks)

            js_filepaths = [urlparse(url).path for url in script_urls]

            for script in script_contents:
                if detect_obfuscation(script):
                    has_obfuscation = True
                    break

    except asyncio.CancelledError:
        logger.info(f"Task was cancelled while processing domain {domain}. Cleaning up.")
        return {
            "domain": url,
            "status_code": "Cancelled",
            "ip": None,
            "obfuscation": False,
            "script_paths": [],
            "redirect_domain": False,
            "html_content": "",
            "error": f"Task was cancelled for {domain}"
        }

    except PlaywrightTimeoutError as timeout_error:
        logger.debug(f"Timeout occurred while loading the page {url}: {timeout_error}")
        return {
            "domain": url,
            "status_code": "Timeout",
            "ip": resolved_ip,
            "obfuscation": has_obfuscation,
            "script_paths": str(js_filepaths),
            "redirect_domain": redirect_domain,
            "html_content": html_content or "",
            "error": "Timeout occurred."
        }

    except Exception as e:
        logger.debug(f"Error while scraping {url}: {e}")
        return {
            "domain": url,
            "status_code": "Failed",
            "ip": resolved_ip,
            "obfuscation": has_obfuscation,
            "script_paths": str(js_filepaths),
            "redirect_domain": redirect_domain,
            "html_content": html_content or "",
            "error": str(e)
        }

    finally:
        try:
            if context:
                await context.close()
        except Exception as e:
            logger.debug(f"Error closing context: {e}")

        if page:
            await page.close()
        if browser:
            await browser.close()

        logger.debug(f"Scraping completed for {url}")

    return {
        "domain": url,
        "status_code": status_code,
        "ip": resolved_ip,
        "obfuscation": has_obfuscation,
        "script_paths": str(js_filepaths),
        "redirect_domain": redirect_domain,
        "html_content": html_content or "",
    }

async def capture_scripts_async(response, script_urls, script_contents):
    try:
        if "javascript" in response.headers.get("content-type", "") and response.status == 200:
            try:
                js_code = await response.text()
                script_urls.append(response.url)
                script_contents.append(js_code)
            except Exception as e:
                logger.debug(f"Failed to capture script content from {response.url}: {e}")
        elif response.status != 200:
            logger.debug(f"Non-200 status for script: {response.url} with status: {response.status}")
    except Exception as e:
        logger.debug(f"Error capturing scripts from {response.url}: {e}")

async def handle_request(route: Route, request: Request):
    try:
        url = request.url

        # If resource was already loaded before, abort it
        if url in loaded_resources:
            logger.debug(f"Aborting repeated request to {url}")
            try:
                await route.abort()
            except TargetClosedError:
                logger.debug(f"Failed to abort request for {url} because the target was already closed.")
            except Exception as e:
                logger.debug(f"Unexpected error while aborting request for {url}: {e}")
        else:
            loaded_resources.add(url)
            try:
                await route.continue_()
            except TargetClosedError:
                logger.debug(f"Failed to continue request for {url} because the target was already closed.")
            except Exception as e:
                logger.debug(f"Unexpected error while continuing request for {url}: {e}")

    except asyncio.CancelledError:
        # Log and simply exit; do not attempt any route fulfillment or continuation
        logger.debug(f"Request was cancelled: {request.url}")

    except TargetClosedError:
        # Just log the fact that target is already closed
        logger.debug(f"Target closed for request: {request.url}")

    except Exception as e:
        # If any general error occurs, log it and do nothing else
        logger.debug(f"Error in handling request: {request.url} - {e}")







