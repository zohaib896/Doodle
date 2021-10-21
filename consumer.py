# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
# Import sys module
import sys
# Import json module to serialize data
import json

# Initialize consumer variable and set property for JSON decode
consumer = KafkaConsumer ('my_topic',bootstrap_servers = ['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Read data from kafka
for message in consumer:
    print("Consumer records:\n")
    print(message)
    print("\nReading from my_topic\n")
    print("time:", message[6]['@time'])
    print("currency:",message[6]['@currency'])
    print("rate:",message[6]['@rate'])
# Terminate t