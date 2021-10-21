# Import KafkaProducer from Kafka library
from kafka import KafkaProducer

import configparser
# Import JSON module to serialize data
import json
config = configparser.RawConfigParser()
config.read('ConfigFile.properties')
jsonFilePath = config.get('FileInfo', 'json.path')

# Initialize producer variable and set parameter for JSON encode
producer = KafkaProducer(bootstrap_servers=
                         ['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
with open(jsonFilePath) as f:
    data = json.load(f)
# extract data from json and insert into exchange rates
for dic in data:
    time = dic['@time']

    for subdic in dic['Cube']:
        # Send data in JSON format
        subdic['@time'] = time
        producer.send('my_topic', subdic )

# Print message
print("Message Sent to my_topic")