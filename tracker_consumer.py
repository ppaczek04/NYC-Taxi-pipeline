import json

from confluent_kafka import Consumer

consumer_conifg = {
    'bootstrap.servers': 'localhost:9092',
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_conifg)
consumer.subscribe(['orders'])
print("Consumer is running and subscribed to topic -> orders")


try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print("Error :", message.error())
            continue

        #we receive message in the binary value so we transform it back to string
        string_message = message.value().decode('utf-8')
        order = json.loads(string_message) #and further to json format (in this case pyhton dictionary)
        print(f"Received order: {order['quantity']} x {order['item']} from {order['user']}")

except KeyboardInterrupt:
    print("\nStopping consumer.")

finally:
    consumer.close()

