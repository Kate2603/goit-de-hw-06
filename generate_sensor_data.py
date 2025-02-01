from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

while True:
    sensor_data = {
        "sensor_id": random.randint(1, 100),
        "temperature": random.uniform(20.0, 40.0),
        "humidity": random.uniform(30.0, 70.0),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.produce('building_sensors', value=json.dumps(sensor_data).encode('utf-8'), callback=delivery_report)
    producer.poll(0)
    print(f"Sent: {sensor_data}")
    time.sleep(1)

producer.flush()