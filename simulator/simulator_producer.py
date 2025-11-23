# simulator_producer.py
import random
import time
import string
import json
from datetime import datetime
from kafka import KafkaProducer

# -------------------------
# Vehicle number generator
# -------------------------
def generate_vehicle_number():
    states = ["KA", "DL", "MH", "TN", "UP", "RJ", "PB", "HR", "GJ", "MP"]
    state = random.choice(states)
    district = random.randint(1, 99)
    series = ''.join(random.choice(string.ascii_uppercase) for _ in range(2))
    number = random.randint(1000, 9999)
    return f"{state}{district:02d}{series}{number}"

def generate_event(car_id):
    event = {
        "car_id": car_id,
        "lat": round(random.uniform(12.90, 12.95), 6),
        "lon": round(random.uniform(77.55, 77.65), 6),
        "speed": random.randint(0, 120),
        "timestamp": datetime.utcnow().isoformat()
    }
    return event

# -------------------------
# Kafka producer setup
# -------------------------
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=5,        # small batching delay (tunable)
    retries=3
)

TOPIC = "vehicle_tracking"   # change to test-topic if you prefer

if __name__ == "__main__":
    car_ids = [generate_vehicle_number() for _ in range(10)]
    print("Generated vehicle IDs:", car_ids)
    print("Starting real-time simulation and sending to Kafka...\n")

    try:
        while True:
            for car_id in car_ids:
                evt = generate_event(car_id)
                # send asynchronously
                producer.send(TOPIC, value=evt)
                print("Sent:", evt)
            # ensure current batch is sent every loop iteration
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping simulator...")
    finally:
        producer.flush()
        producer.close()
