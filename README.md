# Medical Telegram Data Warehouse

A robust data scraping pipeline designed to extract messages and images from specific medical and pharmaceutical Telegram channels and store them in a structured raw data lake.

## Project Structure

```text
medical-telegram-warehouse/
├── .github/
│   └── workflows/
│       └── unittest.yml        # CI/CD pipeline for automated testing
├── .vscode/
│   └── settings.json           # VS Code project configurations
├── data/                       # Local data lake (gitignored)
│   └── raw/
│       ├── images/             # Downloaded channel images
│       │   └── {channel_name}/
│       │       └── {message_id}.jpg
│       └── telegram_messages/  # Scraped JSON messages
│           └── {YYYY-MM-DD}/
│               ├── {channel_name}.json
│               └── _manifest.json
├── logs/                       # Scraper execution logs
│   └── scraper.log
├── notebooks/                  # Experimental notebooks
├── src/                        # Source code
│   ├── datalake.py             # Data storage and partitioning logic
│   └── scraper.py              # Telegram scraping and extraction logic
├── tests/                      # Unit tests
│   └── test_datalake.py        # Datalake logic verification
├── .env                        # Environment variables (credentials)
├── .gitignore                  # Git exclusion rules
├── README.md                   # Project documentation
└── requirements.txt            # Python dependencies
```

## Setup Instructions

### 1. Prerequisites
- Python 3.8+
- Telegram API Credentials (API ID and API Hash from [my.telegram.org](https://my.telegram.org))

### 2. Installation
Clone the repository and install the required dependencies:
```bash
python -m venv .venv
# On Windows
.venv\Scripts\activate
# On Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Configuration
Create/edit the `.env` file in the root directory and add your Telegram credentials:
```env
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
TG_PHONE=+your_phone_number
```

## Usage

### Running the Scraper
To start the scraping process for the configured channels:
```bash
python src/scraper.py
```
**Note:** On the first execution, you will be prompted to enter a verification code sent to your Telegram account.

### Targeted Channels
The script currently scrapes:
1. `CheMed123`
2. `lobelia4cosmetics`
3. `tikvahpharma`

### Data Extraction
For each message, the pipeline extracts:
- `message_id`
- `channel_name`
- `message_date`
- `message_text`
- `has_media`
- `image_path`
- `views`
- `forwards`

## Testing
Run the automated test suite using:
```bash
python -m unittest discover tests
```

## Features
- **Partitioned Data Lake**: Scalable storage structure organized by date and channel.
- **Automated Image Handling**: Detects and downloads images mentioned in messages.
- **Robust Logging**: Comprehensive tracking of scraped channels and error handling.
- **CI/CD Integrated**: Automated unit testing on every push.
