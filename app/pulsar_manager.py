import pulsar
import logging
import msgpack


# Configure logging
logger = logging.getLogger("PulsarManager")

class PulsarManager:
    def __init__(self, pulsar_url, producer_topic, consumer_topic, subscription_name):
        """
        Initializes the PulsarManager to manage producing and consuming messages.
        
        :param pulsar_url: URL of the Pulsar service (e.g., pulsar://localhost:6650).
        :param producer_topic: The Pulsar topic to which the producer will send messages.
        :param consumer_topic: The Pulsar topic from which the consumer will consume messages.
        :param subscription_name: The subscription name for the consumer.
        """
        self.pulsar_url = pulsar_url
        self.producer_topic = producer_topic
        self.consumer_topic = consumer_topic
        self.subscription_name = subscription_name

        # Create a shared Pulsar client
        self.client = pulsar.Client(self.pulsar_url)

        # Create a producer instance to send messages to the producer_topic
        self.producer = self.client.create_producer(self.producer_topic)

        # Create a consumer instance to consume messages from the consumer_topic
        self.consumer = self.client.subscribe(
            self.consumer_topic,
            subscription_name=self.subscription_name,
            consumer_type=pulsar.ConsumerType.Shared  # Shared subscription type
        )

        logger.info(f"PulsarManager connected to Pulsar at {self.pulsar_url}")

    async def produce_message(self, message):
        """
        Sends a message to the producer topic.
        :param message: The message content to be sent (str or bytes).
        """
        self.producer.send(msgpack.packb(message))  # Encode message to bytes
        logger.debug(f"Produced message to {self.producer_topic}: {message}")

    async def pulsar_task_fetcher(self):
        """
        Consumes messages from the Pulsar topic and yields them for processing.
        This is used as the task fetcher for the TaskProcessingManager.
        """
        logger.debug(f"Subscribing to {self.consumer_topic} for task consumption.")

        try:
            while True:
                msg = self.consumer.receive()  # Blocking call to receive a message
                task = msg.data().decode('utf-8')  # Convert message data to a string
                logger.debug(f"Received message from Pulsar: {task}")
                yield task, (self.consumer, msg)  # Yield the task and the (consumer, message) tuple
        except Exception as e:
            logger.debug(f"Error in Pulsar consumer: {e}")
        finally:
            self.consumer.close()
            self.client.close()
            logger.info("Pulsar consumer stopped.")

    def close(self):
        """
        Closes the Pulsar client and other resources.
        """
        self.consumer.close()
        self.producer.close()
        self.client.close()
        logger.info("PulsarManager closed all resources.")
