"""
Script to load YOLO detection results from CSV into PostgreSQL.
"""

import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "medical_warehouse")

# Paths
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_PATH, "data", "yolo_results.csv")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def create_table(conn):
    with conn.cursor() as cur:
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                message_id INTEGER,
                channel_name VARCHAR(255),
                detected_class VARCHAR(255),
                confidence_score FLOAT,
                image_category VARCHAR(255),
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (message_id, channel_name)
            );
        """)
        conn.commit()

def load_csv(conn):
    if not os.path.exists(CSV_PATH):
        print(f"✗ CSV file not found: {CSV_PATH}")
        return

    df = pd.read_csv(CSV_PATH)
    
    # Ensure message_id is integer
    df['message_id'] = df['message_id'].astype(int)
    
    records = df.values.tolist()
    
    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO raw.yolo_detections 
            (message_id, channel_name, detected_class, confidence_score, image_category)
            VALUES %s
            ON CONFLICT (message_id, channel_name) DO UPDATE SET
                detected_class = EXCLUDED.detected_class,
                confidence_score = EXCLUDED.confidence_score,
                image_category = EXCLUDED.image_category,
                loaded_at = CURRENT_TIMESTAMP;
        """
        execute_values(cur, insert_query, records)
        conn.commit()
        print(f"✓ Loaded {len(records)} records into raw.yolo_detections")

def main():
    try:
        conn = get_connection()
        create_table(conn)
        load_csv(conn)
        conn.close()
    except Exception as e:
        print(f"✗ Error: {e}")

if __name__ == "__main__":
    main()
