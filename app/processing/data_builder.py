import asyncio
import msgpack
import zstandard as zstd
from app.processing.html_sublimation import HtmlSublimator
import logging

logger = logging.getLogger("DataBuilder")


async def from_scraper_to_parsed_data(scraped_data):
    # Get or create the event loop
    loop = asyncio.get_event_loop()

    html_content = scraped_data.pop("html_content", None)
    sublimator = HtmlSublimator(html_content)

    # Run the methods in parallel using a thread executor to avoid blocking
    logger.debug("Starting membership detection...")
    has_membership_future = loop.run_in_executor(None, sublimator.detect_membership)

    logger.debug("Starting readable text extraction...")
    readable_text_future = loop.run_in_executor(None, sublimator.extract_readable_text)

    logger.debug("Starting simhash extraction...")
    simhash_future = loop.run_in_executor(None, sublimator.extract_simhash)

    logger.debug("Starting tag sequence extraction...")
    dom_tag_sequence_future = loop.run_in_executor(None, sublimator.get_tag_sequence)

    # Gather the results, handling errors in individual tasks
    results = await asyncio.gather(
        has_membership_future,
        readable_text_future,
        simhash_future,
        dom_tag_sequence_future,
        return_exceptions=True
    )

    # Unpack results with error handling
    has_membership, readable_text, simhash, dom_tag_sequence = results

    # Check for exceptions and log them
    if isinstance(has_membership, Exception):
        logger.error(f"Error in detecting membership: {has_membership}")
        has_membership = None
    if isinstance(readable_text, Exception):
        logger.error(f"Error in extracting readable text: {readable_text}")
        readable_text = None
    if isinstance(simhash, Exception):
        logger.error(f"Error in extracting simhash: {simhash}")
        simhash = None
    if isinstance(dom_tag_sequence, Exception):
        logger.error(f"Error in extracting tag sequence: {dom_tag_sequence}")
        dom_tag_sequence = None

    # Remove redundant data from scraped_data
    domain = scraped_data.pop("domain", None)
    status_code = scraped_data.pop("status_code", None)

    # Update the scraped_data dictionary with newly processed information
    scraped_data.update({
        "membership": has_membership,
        "text_orig": readable_text,
        "simhash": simhash,
        "dom_tag_sequence": dom_tag_sequence
    })

    # Serialize the updated scraped_data using msgpack
    try:
        serialized_data = msgpack.packb(scraped_data)
        logger.debug(f"Serialized data size: {len(serialized_data)} bytes")
    except Exception as e:
        logger.error(f"Error during serialization: {e}")
        return None

    # Compress the serialized data using Zstandard
    try:
        compressor = zstd.ZstdCompressor(level=3)  # Level can be adjusted (default is 3)
        compressed_data = compressor.compress(serialized_data)
        logger.debug(f"Compressed data size: {len(compressed_data)} bytes")
    except Exception as e:
        logger.error(f"Error during compression: {e}")
        return None

    # Prepare and return the final response without redundancy
    return {
        "domain": domain,
        "status_code": status_code,
        "data": compressed_data
    }
