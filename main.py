import asyncio
import os
import pulsar
from dotenv import load_dotenv
from app.scraper.scraper_service import scrape_website_async
from app.processing.data_builder import from_scraper_to_parsed_data
import coolname
import sys
import time
from collections import deque
import warnings
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
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
CONCURRENT_TASKS = int(os.getenv('CONCURRENT_TASKS', '10'))

# Generate or load client name
CLIENT_NAME_FILE = "./client_name.txt"
if os.path.exists(CLIENT_NAME_FILE):
    with open(CLIENT_NAME_FILE, 'r') as file:
        client_name = file.read().strip()
else:
    client_name = "-".join(coolname.generate())
    with open(CLIENT_NAME_FILE, 'w') as file:
        file.write(client_name)

# Deque to store timestamps of processed URLs
processed_urls_timestamps = deque()

# Suppress asyncio warnings about pending tasks being destroyed
warnings.filterwarnings("ignore", category=RuntimeWarning, message="coroutine .* was never awaited")

# Set asyncio logging level to suppress warnings about destroyed tasks
logging.getLogger('asyncio').setLevel(logging.DEBUG)

# Inicializamos el semáforo con el número máximo de tareas concurrentes
logger.critical(f"Initializing Semaphore with {CONCURRENT_TASKS} concurrent tasks...")
        
async def track_scraping_count():
    while True:
        # Calcular el número de URLs procesadas en los últimos 60 segundos
        current_time = time.time()
        while processed_urls_timestamps and (current_time - processed_urls_timestamps[0]) > 60:
            processed_urls_timestamps.popleft()
        logger.critical(f"URLs scraped in the last 60 seconds: {len(processed_urls_timestamps)}")
        await asyncio.sleep(60)
        
async def worker(consumer, producer, semaphore):
    while True:
        logging.debug("Awaiting for Pulsar messages...")

        # Receive the message first without blocking
        msg = consumer.receive()
        domain = msg.data().decode('utf-8')
        logging.info(f"Received {domain} from broker.")
        
        async with semaphore:
            try:
                # Process the domain
                logger.info(f"Initializing scraping process for {domain}...")
                scraped_data = await scrape_website_async(domain)
                result = await from_scraper_to_parsed_data(scraped_data)

                if result:
                    logger.info(f"Sending scraped result to Pulsar...")
                    result['processor'] = client_name
                    producer.send(str(result).encode('utf-8'))
                    processed_urls_timestamps.append(time.time())

                # Acknowledge the message after the domain is processed
                consumer.acknowledge(msg)
            except Exception as e:
                logger.error(f"Error processing domain {domain}: {e}")


async def consume_and_process():
    logger.info(f"Initializing Pulsar client {PULSAR_URL}, subscription {DOMAIN_TOPIC} and producer {RESULT_TOPIC}...")
    
    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        f"persistent://public/default/{DOMAIN_TOPIC}",
        subscription_name='scrapers-subscription',
        consumer_type=pulsar.ConsumerType.Shared
    )
    producer = client.create_producer(f"persistent://public/default/{RESULT_TOPIC}")
    
    semaphore = asyncio.Semaphore(CONCURRENT_TASKS)  # Semaphore to control concurrency

    try:
        
        # Track the number of scrapes in the background
        asyncio.create_task(track_scraping_count())

        # Start the worker tasks
        tasks = [
            asyncio.create_task(worker(consumer, producer, semaphore)) 
            for _ in range(CONCURRENT_TASKS)
        ]

        # Wait for all tasks to complete (they won't, but this keeps the program running)
        await asyncio.gather(*tasks)

    except Exception as e:
        logger.error(f"An error occurred during message consumption: {e}")
    finally:
        # Cancel all tasks safely
        all_tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

        client.close()
    

if __name__ == "__main__":
    try:
        logger.info("starting...")
        asyncio.run(consume_and_process())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)
