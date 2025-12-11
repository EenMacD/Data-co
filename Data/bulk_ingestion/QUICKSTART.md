# Quick Start

## Run It (One Command)

```bash
cd Data/bulk_ingestion
./run.sh
```

That's it! The script automatically:
- ✅ Creates Python venv (first time only)
- ✅ Installs dependencies
- ✅ Starts database
- ✅ Applies migrations
- ✅ Launches UI at http://localhost:{DATA_UI_PORT}

## Download Files

1. Open http://localhost:{DATA_UI_PORT}
2. Select date range → Click "Discover Files"
3. Check files you want → Click "Add to List"
4. Click "Start Ingestion"
5. Watch the progress bar

## Stop

Press `Ctrl+C`

## Troubleshooting

**If .env missing:**
```bash
cd ../../..  # Go to project root
cp .env.example .env
nano .env    # Edit with your DB credentials
```

**If Docker not running:**
```bash
# Start Docker Desktop, then run script again
```

That's all you need!
