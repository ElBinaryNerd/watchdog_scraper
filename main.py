import asyncio
import os
import logging
import pulsar
import coolname
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from app.scraper.scraper_service import scrape_website_async
from app.processing.data_builder import from_scraper_to_parsed_data
import multiprocessing
import threading
from collections import deque
import time

# Load environment variables from .env file
load_dotenv()

# Configure logging
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

# Load Pulsar configurations from .env
PULSAR_IP = os.getenv('PULSAR_IP', 'localhost')
PULSAR_PORT = os.getenv('PULSAR_PORT', '6650')
DOMAIN_TOPIC = os.getenv('DOMAIN_TOPIC', 'domains-to-scrape')
RESULT_TOPIC = os.getenv('RESULT_TOPIC', 'scraped-results')

# Pulsar client setup
PULSAR_URL = f'pulsar://{PULSAR_IP}:{PULSAR_PORT}'

# Load the number of concurrent tasks from .env
CONCURRENT_TASKS = int(os.getenv('CONCURRENT_TASKS', multiprocessing.cpu_count()))

# Generate or load client name
CLIENT_NAME_FILE = "./client_name.txt"
if os.path.exists(CLIENT_NAME_FILE):
    with open(CLIENT_NAME_FILE, 'r') as file:
        client_name = file.read().strip()
else:
    client_name = "-".join(coolname.generate())
    with open(CLIENT_NAME_FILE, 'w') as file:
        file.write(client_name)

logger.info(f"Service started with client name: {client_name}")

# Deque to store timestamps of processed URLs
processed_urls_timestamps = deque()

# Shared counter for parsed domains
parsed_domains_counter = multiprocessing.Value('i', 0)
counter_lock = threading.Lock()

def process_scrape_task(domain):
    try:
        logger.info(f"Starting scraping for domain: {domain}")
        scraped_data = asyncio.run(scrape_website_async(domain))  # Now runs in its own event loop

        html_content = scraped_data.get("html_content")
        if not html_content:
            logger.debug("No HTML content found for domain: %s", domain)
            return None

        analyzed_data = asyncio.run(from_scraper_to_parsed_data(scraped_data))
        return analyzed_data
    except Exception as e:
        logger.error(f"Error while processing domain {domain}: {e}")
        return None

async def consume_and_process():
    try:
        client = pulsar.Client(PULSAR_URL)
        consumer = client.subscribe(
            DOMAIN_TOPIC,
            subscription_name='my-subscription'
        )
        producer = client.create_producer(RESULT_TOPIC)

        logger.info(f"Connected to Pulsar broker at {PULSAR_IP}:{PULSAR_PORT}")
        logger.info(f"Subscribed to topic '{DOMAIN_TOPIC}', ready to consume messages.")

        # Use ThreadPoolExecutor for better compatibility with logging
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=CONCURRENT_TASKS)

        semaphore = asyncio.Semaphore(CONCURRENT_TASKS)

        async def process_domain(domain):
            async with semaphore:
                try:
                    logger.info(f"Starting to process domain {domain}")
                    # Run the scrape in a separate thread to maximize CPU utilization
                    result = await loop.run_in_executor(executor, process_scrape_task, domain)
                    if result:
                        result['processor'] = client_name
                        producer.send(str(result).encode('utf-8'))
                        logger.info(f"Processed and sent result for domain: {result.get('domain')}")
                        # Record the timestamp when the domain is processed
                        processed_urls_timestamps.append(time.time())
                        # Increment shared counter
                        with counter_lock:
                            parsed_domains_counter.value += 1
                except Exception as e:
                    logger.error(f"Error processing domain {domain}: {e}")

        async def track_scraping_count():
            logger.info("Tracking scraping count started.")
            while True:
                # Calculate the number of URLs processed in the last 60 seconds
                current_time = time.time()
                while processed_urls_timestamps and (current_time - processed_urls_timestamps[0]) > 60:
                    processed_urls_timestamps.popleft()

                with counter_lock:
                    total_scraped = parsed_domains_counter.value

                logger.info(f"Total URLs scraped in the last 60 seconds: {len(processed_urls_timestamps)}")
                logger.info(f"Total URLs scraped overall: {total_scraped}")
                await asyncio.sleep(60)

        # Start tracking the scraping count in a separate task
        asyncio.create_task(track_scraping_count())

        tasks = []

        while True:
            try:
                msg = consumer.receive(timeout_millis=5000)  # Add a timeout to avoid blocking indefinitely
                domain = msg.data().decode('utf-8')

                # Start processing the domain while respecting the semaphore limit
                task = asyncio.create_task(process_domain(domain))
                tasks.append(task)
                consumer.acknowledge(msg)

            except pulsar.Timeout:
                logger.debug("No messages received, checking again...")

            # To avoid unbounded task growth, remove completed tasks from the list
            tasks = [t for t in tasks if not t.done()]

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
        if 'consumer' in locals():
            consumer.close()
        if 'client' in locals():
            client.close()
        logger.info("Pulsar client closed.")

if __name__ == "__main__":
    try:
        asyncio.run(consume_and_process())
    except KeyboardInterrupt:
        logger.info("Service interrupted and shutting down.")
    except Exception as e:
        logger.error(f"Service encountered an unexpected error: {e}")
        sys.exit(1)
