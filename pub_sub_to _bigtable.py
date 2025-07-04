import json
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import bigtable
from google.cloud.bigtable import row

# Config - replace with your values
PROJECT_ID = 'healthmonitoringsystem-464616'
SUBSCRIPTION_ID = 'healthmonot'  # your Pub/Sub subscription name
BIGTABLE_INSTANCE_ID = 'healthmono'
BIGTABLE_TABLE_ID = 'patient_iot_stream'

def write_to_bigtable(bigtable_table, patient_id, data_dict):
    row_key = patient_id.encode('utf-8')
    bt_row = bigtable_table.direct_row(row_key)

    for key, value in data_dict.items():
        # Store all data under 'cf1' column family, columns are attribute names
        bt_row.set_cell('cf1', key, str(value).encode('utf-8'))

    bt_row.commit()

def callback(message):
    print(f"Received message: {message.data}")
    try:
        data = json.loads(message.data.decode('utf-8'))
        patient_id = data.get('Patient_ID') or data.get('patient_id')  # Adjust key as per your message

        if not patient_id:
            print("Patient_ID missing in message, skipping")
            message.ack()
            return

        write_to_bigtable(bt_table, patient_id, data)
        print(f"Written to Bigtable: Patient_ID={patient_id}")

        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")
        # Optionally: message.nack() to retry

if __name__ == '__main__':
    # Setup Bigtable client
    bt_client = bigtable.Client(project=PROJECT_ID, admin=False)
    instance = bt_client.instance(BIGTABLE_INSTANCE_ID)
    bt_table = instance.table(BIGTABLE_TABLE_ID)

    # Setup Pub/Sub subscriber client
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Stopped listening to Pub/Sub.")
