import asyncio
import pytest
from unittest.mock import patch, AsyncMock, MagicMock, Mock
from app.scraper.scraper_service import dns_resolve_async, is_redirected_to_different_domain, detect_obfuscation, scrape_website_async, capture_scripts_async, handle_request, loaded_resources
from app.processing.data_builder import from_scraper_to_parsed_data
from app.processing.html_sublimation import HtmlSublimator
import pulsar
import os
import time
from collections import deque
import logging

# Configure logging
LOG_FILE_PATH = "./logs/service.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ScraperService")

PULSAR_URL = "pulsar://localhost:6650"

@pytest.mark.asyncio
@patch('playwright.async_api.Route', new_callable=MagicMock)
async def test_handle_request(mock_route):
    # Create a more realistic mock request object
    mock_request = Mock()
    mock_request.url = "https://example.com/script.js"

    loaded_resources.clear()
    mock_route.continue_ = AsyncMock()
    mock_route.abort = AsyncMock()

    # First time loading the resource
    await handle_request(mock_route, mock_request, loaded_resources)
    assert "https://example.com/script.js" in loaded_resources  # Use the imported loaded_resources
    mock_route.continue_.assert_called_once()

    # Second time should be aborted
    await handle_request(mock_route, mock_request, loaded_resources)
    mock_route.abort.assert_called_once()


@pytest.mark.asyncio
@patch('aiodns.DNSResolver.gethostbyname', new_callable=AsyncMock)
async def test_dns_resolve_async(mock_gethostbyname):
    # Mock the DNS response
    mock_gethostbyname.return_value = type('DNSResult', (object,), {'addresses': ['1.2.3.4']})()

    result = await dns_resolve_async("example.com")
    assert result == '1.2.3.4'

    # Test DNS resolution failure
    mock_gethostbyname.side_effect = Exception("DNS Error")
    result = await dns_resolve_async("nonexistent.com")
    assert result is None


def test_is_redirected_to_different_domain():
    # Test domain comparison for redirection
    initial_url = "https://example.com"
    final_url = "https://different.com"
    assert is_redirected_to_different_domain(initial_url, final_url) == "different.com"

    final_url = "https://example.com/some-page"
    assert is_redirected_to_different_domain(initial_url, final_url) is False


def test_detect_obfuscation():
    # Test detecting obfuscation in JavaScript code
    js_code = "var a = 0x1; var b = 0x2;"
    assert detect_obfuscation(js_code) is False

    js_code = "0x1 0x2 0x3 0x4 0x5 0x6 0x7 0x8"
    assert detect_obfuscation(js_code) is True


@pytest.mark.asyncio
@patch('playwright.async_api.async_playwright')
async def test_scrape_website_async(mock_playwright):
    # Set up mocks for Playwright browser, context, and page
    mock_browser = AsyncMock()
    mock_context = AsyncMock()
    mock_page = AsyncMock()
    
    mock_playwright.return_value.__aenter__.return_value.chromium.launch.return_value = mock_browser
    mock_browser.new_context.return_value = mock_context
    mock_context.new_page.return_value = mock_page
    mock_page.goto.return_value = None
    mock_page.content.return_value = "<!DOCTYPE html><html><head>\n    <title>Example Domain</title>\n    <meta charset=\"utf-8\">\n    <meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\">\n    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n    <style type=\"text/css\">\n    body { background-color: #f0f0f2; margin: 0; padding: 0; font-family: -apple-system, system-ui, BlinkMacSystemFont, \"Segoe UI\", \"Open Sans\", \"Helvetica Neue\", Helvetica, Arial, sans-serif; }\n    div { width: 600px; margin: 5em auto; padding: 2em; background-color: #fdfdff; border-radius: 0.5em; box-shadow: 2px 3px 7px 2px rgba(0,0,0,0.02); }\n    a:link, a:visited { color: #38488f; text-decoration: none; }\n    @media (max-width: 700px) { div { margin: 0 auto; width: auto; } }\n    </style>\n</head>\n<body>\n<div>\n    <h1>Example Domain</h1>\n    <p>This domain is for use in illustrative examples in documents. You may use this domain in literature without prior coordination or asking for permission.</p>\n    <p><a href=\"https://www.iana.org/domains/example\">More information...</a></p>\n</div>\n</body></html>"
    mock_page.evaluate.return_value = "complete"

    url = "https://example.com"
    result = await scrape_website_async(url)

    assert result['domain'] == url
    assert result['status_code'] == 'complete'
    assert "<title>Example Domain</title>" in result['html_content']


@pytest.mark.asyncio
@patch('playwright.async_api.Response')
async def test_capture_scripts_async(mock_response):
    # Mock a JavaScript response
    mock_response.headers = {'content-type': 'application/javascript'}
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="var x = 10;")
    mock_response.url = "https://example.com/script.js"
    
    script_urls = []
    script_contents = []
    await capture_scripts_async(mock_response, script_urls, script_contents)

    assert len(script_urls) == 1
    assert len(script_contents) == 1
    assert script_urls[0] == mock_response.url
    assert script_contents[0] == "var x = 10;"


def test_html_sublimator_extract_readable_text():
    html_content = """
    <html>
        <head>
            <title>Test Title</title>
            <meta name="description" content="This is a test meta description.">
        </head>
        <body>
            <div>Test Body Content</div>
        </body>
    </html>
    """
    sublimator = HtmlSublimator(html_content)
    expected_text = "[TITLE] Test Title [/TITLE] [META] This is a test meta description. [/META] [BODY] Test Title Test Body Content [/BODY]"
    assert sublimator.extract_readable_text() == expected_text


def test_html_sublimator_extract_simhash():
    html_content = "<html><body>Sample content for simhash</body></html>"
    sublimator = HtmlSublimator(html_content)
    simhash = sublimator.extract_simhash()
    assert isinstance(simhash, int)  # Simhash should return an integer value


def test_html_sublimator_get_tag_sequence():
    html_content = """
    <html>
        <div>
            <h1>Header 1</h1>
            <nav>Navigation</nav>
            <footer>Footer content</footer>
        </div>
    </html>
    """
    sublimator = HtmlSublimator(html_content)
    expected_sequence = "068c"
    assert sublimator.get_tag_sequence() == expected_sequence


def test_html_sublimator_detect_membership():
    html_content = """
    <html>
        <body>
            <form action="/signup">
                <button>Sign up</button>
            </form>
        </body>
    </html>
    """
    sublimator = HtmlSublimator(html_content)
    assert sublimator.detect_membership() is True

    html_content = """
    <html>
        <body>
            <div>No membership here</div>
        </body>
    </html>
    """
    sublimator = HtmlSublimator(html_content)
    assert sublimator.detect_membership() is False
