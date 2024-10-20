import asyncio
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ScraperService")


class TaskProcessingManager:
    def __init__(self, 
                 task_fetcher, 
                 task_processor_action, 
                 processors_number=5, 
                 queue_maxsize=10, 
                 semaphore_value=2, 
                 acknowledgment_function=None, 
                 log_interval=60):
        """
        Initializes the TaskProcessingManager.
        """
        self.task_fetcher = task_fetcher
        self.task_processor_action = task_processor_action
        self.processors_number = processors_number
        self.queue_maxsize = queue_maxsize
        self.semaphore_value = semaphore_value
        self.acknowledgment_function = acknowledgment_function

        # Task queue for fetcher and processors to share
        self.queue = asyncio.Queue(maxsize=self.queue_maxsize)
        # Semaphore to limit concurrent processing
        self.semaphore = asyncio.Semaphore(self.semaphore_value)
        # Stop event to gracefully shutdown
        self.stop_event = asyncio.Event()

        # Counters for successful and failed tasks
        self.success_count = 0
        self.error_count = 0
        self.active_workers = 0  # Track active workers
        self.total_processed = 0  # Track total processed tasks
        self.processing_times = []  # Store processing times
        # Lock to ensure thread-safe updates to counters
        self.lock = asyncio.Lock()

        # Time interval for logging status and resetting counts
        self.log_interval = log_interval

    async def fetch_tasks(self):
        """The task fetcher coroutine that retrieves tasks and puts them in the queue."""
        logger.debug("TaskFetcher started!")
        try:
            async for task, msg in self.task_fetcher():
                # Wait for space in the queue if it's full
                await self.queue.put((task, msg))
                logger.debug(f"Task added to queue: {task}")
        except Exception as e:
            logger.error(f"TaskFetcher error: {e}")
        finally:
            logger.info("TaskFetcher stopped fetching tasks.")
            # Send poison pills to processors to indicate completion
            for _ in range(self.processors_number):
                await self.queue.put((None, None))

    async def process_tasks(self, processor_name):
        """The task processor coroutine that processes tasks from the queue."""
        logger.debug(f"{processor_name} started!")
        while True:
            task, msg = await self.queue.get()
            if task is None:
                # Poison pill received, stop the processor
                break

            logger.debug(f"{processor_name} took task from queue: {task}")

            # Control concurrency using a semaphore
            async with self.semaphore:
                # Increment active workers count
                async with self.lock:
                    self.active_workers += 1

                start_time = time.time()  # Start task processing timer
                try:
                    result = await self.task_processor_action(task)
                    logger.debug(f"{processor_name} finished processing task: {task}")

                    # Acknowledge the message if an acknowledgment function is provided
                    if self.acknowledgment_function:
                        await self.acknowledgment_function(msg, task, result)

                    # Task was processed successfully
                    await self.increment_success_count()
                except Exception as e:
                    logger.error(f"Error processing task {task}: {e}")
                    # Task produced an error
                    await self.increment_error_count()
                finally:
                    # Decrement active workers count
                    async with self.lock:
                        self.active_workers -= 1
                    # Record the processing time
                    async with self.lock:
                        self.processing_times.append(time.time() - start_time)

        logger.debug(f"{processor_name} finished working!")

    async def increment_success_count(self):
        """Increment the success count safely."""
        async with self.lock:
            self.success_count += 1
            self.total_processed += 1

    async def increment_error_count(self):
        """Increment the error count safely."""
        async with self.lock:
            self.error_count += 1
            self.total_processed += 1

    async def log_and_reset_counts(self):
        """Periodically logs the number of successes, errors, active workers, and queue size, and resets the counts."""
        while not self.stop_event.is_set():
            await asyncio.sleep(self.log_interval)

            async with self.lock:
                queue_size = self.queue.qsize()  # Get the current queue size
                avg_processing_time = (sum(self.processing_times) / len(self.processing_times)) if self.processing_times else 0
                throughput = self.total_processed / self.log_interval if self.log_interval > 0 else 0
                error_rate = (self.error_count / self.total_processed) * 100 if self.total_processed > 0 else 0

                logger.info(f"{self.success_count} messages processed successfully in the last {self.log_interval} seconds.")
                logger.info(f"{self.error_count} messages produced an error in the last {self.log_interval} seconds.")
                logger.info(f"{queue_size} items currently in the queue.")
                logger.info(f"{self.active_workers} active workers out of {self.processors_number} total workers.")
                logger.info(f"Average task processing time: {avg_processing_time:.2f} seconds.")
                logger.info(f"Throughput: {throughput:.2f} tasks per second.")
                logger.info(f"Error rate: {error_rate:.2f}%.")

                # Reset the counters
                self.success_count = 0
                self.error_count = 0
                self.total_processed = 0
                self.processing_times.clear()

    async def start(self):
        """Starts the task fetcher, processors, and periodic logging asynchronously."""
        fetcher_task = asyncio.create_task(self.fetch_tasks())

        # Create multiple processors
        processor_tasks = [
            asyncio.create_task(self.process_tasks(f"Processor#{i+1}"))
            for i in range(self.processors_number)
        ]

        # Start the logging task
        logging_task = asyncio.create_task(self.log_and_reset_counts())

        # Wait for all tasks to complete
        await asyncio.gather(fetcher_task, *processor_tasks)

        # Stop the logging
        self.stop_event.set()
        await logging_task

    async def stop(self):
        """Gracefully stops the system by setting the stop event and waiting for processors to finish."""
        self.stop_event.set()
        logger.info("Task processing system stopping...")
