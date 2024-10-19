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

# Load Pulsar configurations from .env
PULSAR_IP = os.getenv('PULSAR_IP', 'localhost')
PULSAR_PORT = os.getenv('PULSAR_PORT', '6650')
DOMAIN_TOPIC = os.getenv('DOMAIN_TOPIC', 'domains-to-scrape')
RESULT_TOPIC = os.getenv('RESULT_TOPIC', 'scraped-results')

# Pulsar client setup
PULSAR_URL = f'pulsar://{PULSAR_IP}:{PULSAR_PORT}'

# Load the number of concurrent tasks from .env
concurrent_tasks = int(os.getenv('CONCURRENT_TASKS', '10'))

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

async def process_scrape_task(domain):
    try:
        
        scrape_start = time.time()
        # Continuar la ejecución
        scraped_data = await scrape_website_async(domain)
        scrape_time = (time.time() - scrape_start) * 1000
        logger.info(f"DNS check finished in {scrape_time:.2f} ms")

        html_content = scraped_data.get("html_content")
        logger.info(f"Final html with length {len(html_content)} content gathered...")

        if not html_content:
            return None

        analyzed_data = await from_scraper_to_parsed_data(scraped_data)
        return analyzed_data
    except Exception as e:
        logger.error(f"Error processing scrape task for domain {domain}: {e}")
        return None

async def consume_and_process():
    logger.info(f"Initializing Pulsar client {PULSAR_URL}, subscription {DOMAIN_TOPIC} and producer {RESULT_TOPIC}...")
    
    # Inicializamos el cliente de Pulsar
    client = pulsar.Client(PULSAR_URL)
    
    # Suscribirse al tópico
    consumer = client.subscribe(
        f"persistent://public/default/{DOMAIN_TOPIC}",
        subscription_name='scrapers-subscription',
        consumer_type=pulsar.ConsumerType.Shared  # Uso del enum para el tipo de consumidor
    )
        
    # Crear el productor para los resultados
    producer = client.create_producer(f"persistent://public/default/{RESULT_TOPIC}")

    # Inicializamos el semáforo con el número máximo de tareas concurrentes
    logger.info(f"Initializing Semaphore with {concurrent_tasks} concurrent tasks...")
    semaphore = asyncio.Semaphore(concurrent_tasks)

    async def process_domain(domain):
        try:
            logger.info(f"Initializing scraping process for {domain}...")
            result = await process_scrape_task(domain)
            if result:
                logger.info(f"Sending scraped result to Pulsar...")
                result['processor'] = client_name
                producer.send(str(result).encode('utf-8'))
                # Registrar la marca de tiempo cuando el dominio ha sido procesado
                processed_urls_timestamps.append(time.time())
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error processing domain {domain}: {e}")

    async def track_scraping_count():
        while True:
            # Calcular el número de URLs procesadas en los últimos 60 segundos
            current_time = time.time()
            while processed_urls_timestamps and (current_time - processed_urls_timestamps[0]) > 60:
                processed_urls_timestamps.popleft()
            logger.info(f"URLs scraped in the last 60 seconds: {len(processed_urls_timestamps)}")
            await asyncio.sleep(60)

    try:
        # Iniciar la tarea para rastrear el número de scraping en segundo plano
        asyncio.create_task(track_scraping_count())

        while True:
            logging.debug("Awaiting for Pulsar messages...")

            # Limitar la recepción de mensajes con el semáforo
            async with semaphore:
                msg = consumer.receive()
                domain = msg.data().decode('utf-8')
                logging.info(f"Received {domain} from broker.")
                
                # Procesar el dominio
                logging.debug(f"URL received and sent for processing: {domain}")
                await asyncio.create_task(process_domain(domain))
                consumer.acknowledge(msg)

    except Exception as e:
        logger.error(f"An error occurred during message consumption: {e}")
    finally:
        # Cancelar todas las tareas pendientes de manera segura
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
