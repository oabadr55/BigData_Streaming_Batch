import pandas as pd
import json
import time
from datetime import datetime

# Kafka import
from kafka import KafkaProducer
from kafka.errors import KafkaError

class CSVToKafkaStreamer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic_name=None):
        self.topic_name = topic_name
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=5
            )
            print(f"[INFO] Connected to Kafka at {bootstrap_servers}")
        except Exception as e:
            raise Exception(f"[ERROR] Could not connect to Kafka: {e}")

    def stream_csv(self, csv_file_path, chunk_size=1, delay=0.1, max_rows=None):
        df = pd.read_csv(csv_file_path, nrows=max_rows)
        print(f"[INFO] Streaming {len(df)} rows to topic '{self.topic_name}'...")

        records = df.to_dict('records')
        sent_count = 0

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            message = {
                'timestamp': datetime.now().isoformat(),
                'data': chunk if chunk_size > 1 else chunk[0],
                'chunk_id': i // chunk_size
            }

            try:
                future = self.producer.send(self.topic_name, value=message)
                future.get(timeout=10)  # wait for ack
                sent_count += len(chunk)
                print(f"[SENT] Chunk {i // chunk_size} ({len(chunk)} rows). Total sent: {sent_count}/{len(df)}")
            except KafkaError as e:
                print(f"[ERROR] Failed to send chunk {i // chunk_size}: {e}")
            time.sleep(delay)

        print(f"[INFO] Completed streaming {sent_count} rows to topic '{self.topic_name}'")

    def close(self):
        self.producer.flush()
        self.producer.close()
        print("[INFO] Kafka producer closed")

# --- USAGE EXAMPLE ---
if __name__ == "__main__":
    streamer = CSVToKafkaStreamer(bootstrap_servers=['localhost:9092'], topic_name='nyc_taxi_trips')
    try:
        streamer.stream_csv(
            csv_file_path='NYC_taxi_with_coordinates_weather.csv',  # replace with your path
            chunk_size=1,
            delay=0.8,
            max_rows=300
        )
    finally:
        streamer.close()
