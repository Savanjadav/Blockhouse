import json
import time
import pandas as pd
from kafka import KafkaProducer

# Constants
KAFKA_TOPIC = 'mock_l1_stream'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Adjust if needed
CSV_FILE = 'l1_day.csv'

# Time window for simulation
START_TIME = '13:36:32'
END_TIME = '13:45:14'


def main():
    # Set up Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Read CSV in chunks to handle large files
    chunk_iter = pd.read_csv(CSV_FILE, chunksize=100_000)
    prev_ts = None
    sent = 0
    for chunk in chunk_iter:
        # Parse timestamp column
        chunk['ts_event'] = pd.to_datetime(chunk['ts_event'])
        # Filter for the required time window (assume date is present or irrelevant)
        mask = chunk['ts_event'].dt.strftime('%H:%M:%S').between(START_TIME, END_TIME)
        filtered = chunk[mask]
        # Sort by timestamp to ensure correct pacing
        filtered = filtered.sort_values('ts_event')
        for _, row in filtered.iterrows():
            # Prepare snapshot
            snapshot = {
                'publisher_id': row['publisher_id'],
                'ask_px_00': row['ask_px_00'],
                'ask_sz_00': row['ask_sz_00'],
                'ts_event': row['ts_event'].isoformat()
            }
            # Simulate real-time pacing
            if prev_ts is not None:
                delta = (row['ts_event'] - prev_ts).total_seconds()
                if delta > 0:
                    time.sleep(min(delta, 1.0))  # Cap sleep to 1s for speed
            prev_ts = row['ts_event']
            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=snapshot)
            sent += 1
            if sent % 1000 == 0:
                print(f"Sent {sent} messages...")
    print(f"Done. Total messages sent: {sent}")

if __name__ == '__main__':
    main() 
