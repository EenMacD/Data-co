# Data Scraping UI

A simple web interface for configuring and running Companies House data scraping workflows.

## Features

- Configure search criteria (locality, company status, SIC codes, limits)
- Set technical parameters (API rate limits, financial data settings)
- Start/stop scraping workflows
- View real-time logs
- Monitor progress and statistics

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Make sure your database is set up and `.env` file is configured with:
```
STAGING_DB_HOST=localhost
STAGING_DB_PORT=5432
STAGING_DB_NAME=your_database
STAGING_DB_USER=your_username
STAGING_DB_PASSWORD=your_password
COMPANIES_HOUSE_API_KEY=your_api_key
```

## Usage

1. Start the UI server:
```bash
python app.py
```

2. Open your browser to:
```
http://localhost:5000
```

3. Configure your search parameters in the UI

4. Click "Save Configuration" to save to `config/filters.yaml`

5. Click "Start Scraping" to begin the data collection process

## How It Works

- The UI saves your configuration to `config/filters.yaml`
- When you start scraping, it runs the `api-main-db.py` workflow
- Logs are streamed in real-time to the browser
- Status and statistics are updated automatically
- All data goes into your PostgreSQL staging database

## Notes

- The UI runs on port 5000 by default
- Scraping runs in the background
- You can monitor progress in real-time
- Database statistics update every 5 seconds
