
import asyncio
import logging
import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import MessageMediaPhoto

import datalake

# Load environment variables
load_dotenv()

# Configuration
API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
PHONE = os.getenv("TG_PHONE")
CHANNELS = [
    "https://t.me/CheMed123",
    "https://t.me/lobelia4cosmetics",
    "https://t.me/tikvahpharma"
]
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_PATH, "logs")

# Setup logging
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "scraper.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def scrape_channel(client, channel_url, date_str):
    logger.info(f"Scraping channel: {channel_url}")
    try:
        entity = await client.get_entity(channel_url)
        channel_name = entity.username or channel_url.split("/")[-1]
        
        messages_data = []
        image_count = 0
        
        # Ensure image directory exists for this channel
        img_dir = datalake.channel_images_dir(BASE_PATH, channel_name)

        async for message in client.iter_messages(entity, limit=100):
            if not message:
                continue
            
            msg_dict = {
                "message_id": message.id,
                "channel_name": channel_name,
                "message_date": message.date.isoformat() if message.date else None,
                "message_text": message.text,
                "has_media": bool(message.media),
                "image_path": None,
                "views": message.views,
                "forwards": message.forwards,
            }

            # Download image if present
            if message.media and isinstance(message.media, MessageMediaPhoto):
                # Naming convention: {message_id}.jpg
                # Telethon download_media handles extension usually, but strict requirement: {message_id}.jpg
                # We'll use download_media with file path
                file_path = os.path.join(img_dir, f"{message.id}.jpg")
                if not os.path.exists(file_path):
                    await client.download_media(message.media, file=file_path)
                    image_count += 1
                msg_dict["image_path"] = str(file_path) # Store absolute path or relative? Req says path.
            
            messages_data.append(msg_dict)
        
        # Save to data lake
        if messages_data:
            datalake.write_channel_messages_json(
                base_path=BASE_PATH,
                date_str=date_str,
                channel_name=channel_name,
                messages=messages_data
            )
            logger.info(f"Saved {len(messages_data)} messages for {channel_name} to data lake.")
        else:
            logger.info(f"No messages found for {channel_name}")

        return len(messages_data)

    except Exception as e:
        logger.error(f"Error scraping {channel_url}: {e}", exc_info=True)
        return 0

async def main():
    if not API_ID or not API_HASH:
        logger.error("TG_API_ID or TG_API_HASH not set in .env")
        return

    client = TelegramClient('anon', int(API_ID), API_HASH)
    
    logger.info("Connecting to Telegram...")
    await client.start(phone=PHONE)
    
    # Ensure authorized
    if not await client.is_user_authorized():
        logger.info("Client not authorized. Interactive setup needed.")
        await client.send_code_request(PHONE)
        # This part might be tricky in non-interactive background, but 'start' usually handles interactive login 
        # if run in a proper terminal. Since we are automating, we rely on 'start's interactive capability 
        # or the user having a session file. 
        # For this environment, we might need to rely on the user running this once manually 
        # or us providing a way to input code. 
        # However, 'client.start()' is interactive by default. 
    
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_messages = 0
    channel_counts = {}

    for channel in CHANNELS:
        count = await scrape_channel(client, channel, date_str)
        channel_counts[channel] = count
        total_messages += count

    # Write manifest
    datalake.write_manifest(
        base_path=BASE_PATH,
        date_str=date_str,
        channel_message_counts=channel_counts
    )
    
    logger.info("Scraping completed.")
    await client.disconnect()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
