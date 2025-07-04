import json
import random
import time
from datetime import datetime
from google.cloud import pubsub_v1

# GCP settings
PROJECT_ID = "healthmonitoringsystem-464616"
TOPIC_ID = "healthmono"  # Make sure this topic exists

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def generate_patient_record(patient_id):
    return {
        "Patient_ID": str(patient_id),
        # Simulated real-time vitals
        "Temperature_C": round(random.uniform(36.5, 39.0), 1),
        "Blood_Pressure": f"{random.randint(100, 140)}/{random.randint(70, 90)}",
        "Heart_Rate": random.randint(60, 100),
        # Timestamp for streaming
        "event_timestamp": datetime.utcnow().isoformat()
    }

def publish_messages(interval_sec=1.0):
    for i in range(1,1000):
        record = generate_patient_record(i)
        message_json = json.dumps(record)
        message_bytes = message_json.encode("utf-8")
        future = publisher.publish(topic_path, data=message_bytes)
        print(f"Published patient {i}: {record}")
        time.sleep(interval_sec)

if __name__ == "__main__":
    print(f"Publishing to Pub/Sub topic: {topic_path}")
    publish_messages(interval_sec=2)  # Send one message every 2 seconds
