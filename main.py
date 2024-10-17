import asyncio
import os
import logging
import pulsar
from dotenv import load_dotenv
from app.scraper.scraper_service import scrape_website_async
from app.processing.data_builder import from_scraper_to_parsed_data
import coolname
import sys

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

# Limit the number of concurrent tasks
CONCURRENT_TASKS = 5

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


async def process_scrape_task(domain):
    try:
        scraped_data = await scrape_website_async(domain)
        html_content = scraped_data.get("html_content")

        if not html_content:
            logger.debug("No HTML content found for domain: %s", domain)
            return None

        analyzed_data = await from_scraper_to_parsed_data(scraped_data)
        return analyzed_data
    except Exception as e:
        logger.error(f"Error while processing domain {domain}: {e}")
        return None


async def consume_and_process():
    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        DOMAIN_TOPIC, 
        subscription_name='my-subscription')
    producer = client.create_producer(RESULT_TOPIC)

    logger.info(f"Connected to Pulsar broker at {PULSAR_IP}:{PULSAR_PORT}")
    logger.info(f"Subscribed to topic '{DOMAIN_TOPIC}', ready to consume messages.")

    semaphore = asyncio.Semaphore(CONCURRENT_TASKS)

    async def process_domain(domain):
        async with semaphore:
            result = await process_scrape_task(domain)
            if result:
                result['processor'] = client_name
                producer.send(str(result).encode('utf-8'))
                logger.debug(
                    f"Processed and sent result for domain: {result.get('domain')}"
                )

    try:
        while True:
            msg = consumer.receive()
            domain = msg.data().decode('utf-8')
            logger.debug(f"Received domain: {domain}")

            # Start processing the domain while respecting the semaphore limit
            asyncio.create_task(process_domain(domain))
            consumer.acknowledge(msg)

            # Delay before fetching the next domain
            await asyncio.sleep(1)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
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
