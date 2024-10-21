import os
import asyncio
import logging
from app.scraper.scraper_service import scrape_website_async
from app.processing.data_builder import from_scraper_to_parsed_data
from app.pulsar_manager import PulsarManager
from app.task_processing_manager import TaskProcessingManager
from dotenv import load_dotenv

load_dotenv()

# Load Pulsar configurations from .env
PULSAR_IP = os.getenv('PULSAR_IP', 'localhost')
PULSAR_PORT = os.getenv('PULSAR_PORT', '6650')
CONSUMER_TOPIC = os.getenv('CONSUMER_TOPIC', 'domains-to-scrape')
PRODUCER_TOPIC = os.getenv('PRODUCER_TOPIC', 'scraped-results')
PULSAR_URL = f"pulsar://{PULSAR_IP}:{PULSAR_PORT}"
LOG_LEVEL = os.getenv('LOG_LEVEL', 'ERROR').upper()

# Convert the string level to the corresponding logging level
numeric_level = logging._nameToLevel.get(LOG_LEVEL, logging.ERROR)

# Configure logging
logging.basicConfig(
    level=numeric_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("ScraperService")

# Example task processor action
async def task_processor_action(task):
    """An example task processor action that processes tasks."""
    logger.debug(f"Processing {task}...")
    scraped_data = await scrape_website_async(task)
    result = await from_scraper_to_parsed_data(scraped_data)
    
    # Return the result so it can be used in the acknowledgment function
    return result

# Acknowledgment function for Pulsar messages
async def pulsar_acknowledgment_function(pulsar_manager, msg, task, result):
    """
    Produces a new message with the processed result and acknowledges Pulsar messages after processing.
    
    :param pulsar_manager: The PulsarManager instance for producing the message.
    :param msg: The Pulsar message object.
    :param task: The task that was processed.
    :param result: The result from the task processor that will be sent in the new Pulsar message.
    """
    consumer, message = msg  # Extract the consumer and message tuple

    # Produce a new message to the producer topic
    await pulsar_manager.produce_message(result)
    logger.debug(f"Produced message for task: {task} with result: {result}")

    # Acknowledge the message via the consumer
    consumer.acknowledge(message)
    logger.debug(f"Message acknowledged: {message.message_id()}")

async def main():
    # Instantiate the PulsarManager with the appropriate topics and subscription
    pulsar_manager = PulsarManager(
        pulsar_url=PULSAR_URL,
        producer_topic=PRODUCER_TOPIC,
        consumer_topic=CONSUMER_TOPIC,
        subscription_name='scrapers-subscription'
    )

    # Use pulsar_task_fetcher from PulsarManager as the task fetcher
    task_processing_manager = TaskProcessingManager(
        task_fetcher=pulsar_manager.pulsar_task_fetcher,
        task_processor_action=task_processor_action,
        processors_number=120,
        queue_maxsize=1,
        semaphore_value=250,
        # Pass the pulsar_manager along with the other arguments to the acknowledgment function
        acknowledgment_function=lambda msg, task, result: pulsar_acknowledgment_function(pulsar_manager, msg, task, result),
        log_interval=60  # Log and reset every 60 seconds
    )

    # Start the task processing system
    await task_processing_manager.start()

    # Clean up resources after execution
    pulsar_manager.close()

# Run the example
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted.")
