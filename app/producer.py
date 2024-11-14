import os
import json
import glob
import logging
import pathlib
import sys
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import time

# Configure logging to display information and error messages
logging.basicConfig(level=logging.INFO)

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Streams JSON data from a text file into a Kafka topic using kafka-python.")
parser.add_argument("-f", "--file", default="./data/data.txt", help="Path to the text file containing JSON lines.")
parser.add_argument("-t", "--topic", default="Twitter", help="Kafka topic to publish the messages.")
parser.add_argument("-b", "--brokers", default="0.0.0.0:19092,0.0.0.0:29092,0.0.0.0:39092", help="Comma-separated list of brokers.")
parser.add_argument("-d", "--date", help="Date field to manipulate (if present in the JSON).")
parser.add_argument("-r", "--reverse", action="store_true", help="Reverse the order of data before sending.")
parser.add_argument("-l", "--loop", action="store_true", help="Loop through data continuously.")
args = parser.parse_args()

# Initialize Kafka producer with serialization settings for keys and values
producer = KafkaProducer(
    bootstrap_servers=args.brokers.split(','),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert value to JSON string before sending
)

# Callback function to log message delivery success
def on_send_success(record_metadata):
    logging.info(f"Message delivered to {record_metadata.topic} [{record_metadata.partition}] offset {record_metadata.offset}")

# Callback function to log message delivery error
def on_send_error(excp):
    logging.error('Message delivery failed: %s', excp)

# Initialize Kafka admin client to manage topics
admin_client = KafkaAdminClient(bootstrap_servers=args.brokers.split(','))

# Check if the topic already exists
existing_topics = admin_client.list_topics()
if args.topic not in existing_topics:
  topic_list = [NewTopic(name=args.topic, num_partitions=1, replication_factor=3)]
  try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    logging.info(f"Topic '{args.topic}' created successfully.")
  except Exception as e:
    logging.error("Failed to create topic: %s", e)
else:
  logging.info(f"Topic '{args.topic}' already exists.")

# Main loop to process text file
try:
    while True:
        logging.info("Processing file: %s", args.file)
        with open(args.file, 'r', encoding='utf-8') as jsonfile:
            lines = jsonfile.readlines()
            if args.reverse:
                lines.reverse()  # Reverse the order of data if requested

            # Main loop to process each line of text file
            for line in lines:
                try:
                    row = json.loads(line.strip())  # Parse each line as JSON

                    # Send the JSON object as a message to Kafka/Redpanda
                    producer.send(args.topic, value=row).add_callback(on_send_success).add_errback(on_send_error)
                    time.sleep(2)

                except json.JSONDecodeError:
                    logging.error("Failed to decode line as JSON: %s", line)

            producer.flush()  # Ensure all messages are sent before continuing

        if not args.loop:
            break  # Exit the loop if not set to continuous mode
except KeyboardInterrupt:
    logging.info("Terminating the producer.")

producer.close()  # Close the producer connection gracefully
logging.info("Producer has been closed.")