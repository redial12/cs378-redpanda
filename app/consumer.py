from kafka import KafkaConsumer

# Initialize the Kafka consumer
consumer = KafkaConsumer(
  'Twitter',  # Replace with your topic name
  bootstrap_servers=['0.0.0.0:19092','0.0.0.0:29092','0.0.0.0:39092'],  # Replace with your Kafka server address
  auto_offset_reset='earliest',
  enable_auto_commit=True,
)

# Actively listen for events and print them out
try:
  for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
  print("Terminating the consumer...")
finally:
  consumer.close()