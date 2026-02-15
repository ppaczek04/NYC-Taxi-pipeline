from confluent_kafka import Producer
import uuid
import json

def deilvery_report(err, msg):
    if err:
        print(f" Delivery failed: {err}")
    else:
        print(f" Delivered: {msg.value().decode('utf-8')}")
        print(f" Delivered to \"{msg.topic()}\" : partition {msg.partition()} : at offset {msg.offset()} ")


producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

order = {
    "order_id": str(uuid.uuid4()),
    "user": "Taxi driver 1",
    "item": "20min ride",
    "quantity": 4
}
                # to string     ## and then to binary
binary_value = json.dumps(order).encode("utf-8")
# if message/event send for topic that does not exist yet, it will be automatically created by Kafka
producer.produce("orders",
                 binary_value,
                 callback=deilvery_report)
producer.flush()