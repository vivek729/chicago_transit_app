"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "schema.registry.url": "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(self.broker_properties)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        client = AdminClient({"bootstrap.servers": "localhost:9092"})
        topic_spec = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
            #config={
            #   "cleanup.policy": "delete",
            #    "compression.type": "lz4"
            #}
        )
        topic_future_dict = client.create_topics([topic_spec])

        for topic, future in topic_future_dict.items():
            try:
                future.result()
                logger.info(f"Topic {self.topic_name} created")
            except Exception as e:
                logger.exception(e)
                logger.error(f"failed to create topic {self.topic_name}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        queue_len = self.producer.flush(3)  # 3 seconds timeout

        # Messages still in producer queue
        if queue_len > 0:
            logger.warning("producer close incomplete - skipping")
        else:
            logger.info("Successfully flushed producer queue")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
