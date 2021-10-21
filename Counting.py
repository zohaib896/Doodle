from confluent_kafka import Consumer

topic = "my_topic"
broker = "localhost:9092"

def get_count():
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest',
    })

    consumer.subscribe([topic])

    total_message_count = 0
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            print("No more messages")
            break
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        total_message_count = total_message_count + 1
        print('Received message {}: {}'.format(total_message_count, msg.decode('utf-8')))

    consumer.close()

    print(total_message_count)