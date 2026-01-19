"""
Script to load raw Telegram messages from JSON files into PostgreSQL.
Creates the raw schema and raw.telegram_messages table, then loads all JSON data.
"""

import os
import json
import glob
from datetime import datetime
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
DATA_PATH = os.path.join(BASE_PATH, "data", "raw", "telegram_messages")


def get_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


def create_schema_and_table(conn):
    """Create the raw schema and telegram_messages table if they don't exist."""
    with conn.cursor() as cur:
        # Create schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                message_id INTEGER,
                channel_name VARCHAR(255),
                message_date TIMESTAMP,
                message_text TEXT,
                has_media BOOLEAN,
                image_path TEXT,
                views INTEGER,
                forwards INTEGER,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (message_id, channel_name)
            );
        """)
        
        conn.commit()
        print("✓ Schema and table created successfully")


def load_json_files(conn):
    """Load all JSON files from the data lake into PostgreSQL."""
    # Find all JSON files (excluding manifest files)
    pattern = os.path.join(DATA_PATH, "**", "*.json")
    json_files = [f for f in glob.glob(pattern, recursive=True) 
                  if not os.path.basename(f).startswith("_")]
    
    if not json_files:
        print(f"⚠ No JSON files found in {DATA_PATH}")
        return
    
    print(f"Found {len(json_files)} JSON file(s) to load")
    
    total_records = 0
    
    with conn.cursor() as cur:
        for json_file in json_files:
            print(f"Loading: {os.path.basename(json_file)}")
            
            with open(json_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            if not messages:
                print(f"  ⚠ No messages in {os.path.basename(json_file)}")
                continue
            
            # Prepare data for insertion
            records = []
            for msg in messages:
                records.append((
                    msg.get('message_id'),
                    msg.get('channel_name'),
                    msg.get('message_date'),
                    msg.get('message_text'),
                    msg.get('has_media'),
                    msg.get('image_path'),
                    msg.get('views'),
                    msg.get('forwards')
                ))
            
            # Insert data using execute_values for better performance
            insert_query = """
                INSERT INTO raw.telegram_messages 
                (message_id, channel_name, message_date, message_text, 
                 has_media, image_path, views, forwards)
                VALUES %s
                ON CONFLICT (message_id, channel_name) DO NOTHING;
            """
            
            execute_values(cur, insert_query, records)
            conn.commit()
            
            print(f"  ✓ Loaded {len(records)} records")
            total_records += len(records)
    
    print(f"\n✓ Total records loaded: {total_records}")


def verify_load(conn):
    """Verify the data was loaded correctly."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
        count = cur.fetchone()[0]
        print(f"\n✓ Verification: {count} total records in raw.telegram_messages")
        
        cur.execute("""
            SELECT channel_name, COUNT(*) as count 
            FROM raw.telegram_messages 
            GROUP BY channel_name 
            ORDER BY count DESC;
        """)
        
        print("\nRecords per channel:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} messages")


def main():
    """Main execution function."""
    print("=" * 60)
    print("Loading Raw Telegram Data to PostgreSQL")
    print("=" * 60)
    
    try:
        # Connect to database
        print(f"\nConnecting to database: {DB_NAME}@{DB_HOST}:{DB_PORT}")
        conn = get_connection()
        print("✓ Connected successfully")
        
        # Create schema and table
        print("\nCreating schema and table...")
        create_schema_and_table(conn)
        
        # Load JSON files
        print("\nLoading JSON files...")
        load_json_files(conn)
        
        # Verify
        print("\nVerifying data...")
        verify_load(conn)
        
        conn.close()
        print("\n" + "=" * 60)
        print("✓ Data loading completed successfully!")
        print("=" * 60)
        
    except psycopg2.OperationalError as e:
        print(f"\n✗ Database connection error: {e}")
        print("\nPlease ensure:")
        print("  1. PostgreSQL is running")
        print("  2. Database credentials in .env are correct")
        print("  3. Database 'medical_warehouse' exists")
        print("\nTo create the database, run:")
        print(f"  psql -U {DB_USER} -c 'CREATE DATABASE {DB_NAME};'")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()
