import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def extract_insights():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "1234"),
            dbname=os.getenv("DB_NAME", "medical_warehouse")
        )
        
        print("--- 1. Posting Patterns (Hour of Day) ---")
        df_hour = pd.read_sql("""
            SELECT EXTRACT(HOUR FROM message_datetime) as hour, count(*) as post_count, AVG(view_count) as avg_views
            FROM public_marts.fct_messages
            GROUP BY hour
            ORDER BY hour
        """, conn)
        print(df_hour)
        
        print("\n--- 2. Posting Patterns (Day of Week) ---")
        df_dow = pd.read_sql("""
            SELECT EXTRACT(DOW FROM message_datetime) as dow, count(*) as post_count, AVG(view_count) as avg_views
            FROM public_marts.fct_messages
            GROUP BY dow
            ORDER BY dow
        """, conn)
        # 0 is Sunday
        print(df_dow)
        
        print("\n--- 3. Price Mentions ---")
        df_prices = pd.read_sql("""
            SELECT channel_name, message_text
            FROM public_marts.fct_messages
            WHERE message_text ~* '[0-9]+.*(ETB|ብር|birr)'
            LIMIT 10
        """, conn)
        print(f"Found {len(df_prices)} messages with potential prices.")
        for i, row in df_prices.iterrows():
            # Print with 'replace' to avoid encoding errors in terminal
            safe_text = row['message_text'].replace('\n', ' ')[:100]
            print(f"{row['channel_name']}: {safe_text.encode('ascii', 'replace').decode()}")
            
        print("\n--- 4. Availability Mentions ---")
        df_availability = pd.read_sql("""
            SELECT channel_name, 
                   COUNT(*) FILTER (WHERE message_text ~* '(available|instock|stock|አለ)') as in_stock,
                   COUNT(*) FILTER (WHERE message_text ~* '(out of stock|sold out|የለም)') as out_of_stock
            FROM public_marts.fct_messages
            GROUP BY channel_name
        """, conn)
        print(df_availability)

        print("\n--- 5. Top Products vs Views ---")
        df_products = pd.read_sql("""
            SELECT d.detected_class, COUNT(*) as detections, AVG(m.view_count) as avg_views
            FROM public_marts.fct_image_detections d
            JOIN public_marts.fct_messages m ON d.message_key = m.message_key
            GROUP BY d.detected_class
            ORDER BY detections DESC
            LIMIT 10
        """, conn)
        print(df_products)
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    extract_insights()
