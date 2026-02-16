from confluent_kafka import Consumer
import json

########################################################################
# This Kafka consumer is for testing purposes only,
# later it will be made redundant after implementaiton of 
# Spark Structured Streaming
########################################################################

KAFKA_TOPIC = "rides_raw"
KAFKA_SERVER = "localhost:9092"

consumer_config = {
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'nyc-test-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

print("Listening for messages...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f" Error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode("utf-8"))
        print("Received -->: ", data)

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
