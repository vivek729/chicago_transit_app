"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def create_topic(topic_name, num_partitions, num_replicas):
    """Creates the topic if it does not already exist"""
    client = AdminClient({"bootstrap.servers": "localhost:9092"})
    topic_spec = NewTopic(
        topic_name,
        num_partitions=num_partitions,
        replication_factor=num_replicas
    )
    topic_future_dict = client.create_topics([topic_spec])

    for topic, future in topic_future_dict.items():
        try:
            future.result()
            logger.info(f"Topic {topic_name} created")
        except Exception as e:
            logger.exception(e)
            logger.error(f"failed to create topic {topic_name}")

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    create_topic(topic_name="cta_stations", num_partitions=1, num_replicas=1)
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.info("connector already created skipping recreation")
        return


    # Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.

    # logger.info("connector code not completed skipping connector creation")
    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://localhost:5432/cta",
               "connection.user": "cta_admin",
               "connection.password": "chicago",
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "cta_",
               "poll.interval.ms": "100000",
           }
       }),
    )

    ## Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except Exception as e:
        logger.exception(e)
        logger.error("Unable to configure JDBC connector")

    logger.info("connector created successfully")


if __name__ == "__main__":
    configure_connector()
