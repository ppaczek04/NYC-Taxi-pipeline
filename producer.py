from confluent_kafka import Producer
import pandas as pd
import json
import time

KAFKA_TOPIC = "rides_stream"
KAFKA_SERVER = "kafka:9092"
FILE_PATH = "data/yellow_tripdata_2025-11.parquet"


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    # else:   # THIS SLOWED DOWN SENDING DATA TOO MUCH
    #     print(f"Delivered to {msg.topic()} "
    #           f"[{msg.partition()}] at offset {msg.offset()}")


producer_config = {
    'bootstrap.servers': KAFKA_SERVER,
    'client.id': 'nyc-producer'
}

producer = Producer(producer_config)

print("Loading parquet...")
df = pd.read_parquet(FILE_PATH) # there we simulate the data getting into the system

print("Starting stream simulation...")

batch_size = 10000 # reduce the dataframe to smaller size to iterate over
for i in range(0, len(df), batch_size):
    batch = df.iloc[i : i + batch_size]
    for _, row in batch.iterrows():
        value = json.dumps(row.to_dict(), default=str).encode("utf-8")

        producer.produce(
            topic=KAFKA_TOPIC,
            value=value,
            callback=delivery_report
        )

        producer.poll(0)  
        time.sleep(0.0001)  # we simulate live data here by custom 0.0001s sleeptime between each record of data
        
producer.flush()
print("Live data streaming finished.")