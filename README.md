# HFT Order-Book Imbalance Scalping Bot

MEXC scalping bot using order-book flow signals.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ccxt loguru pydantic pydantic-settings python-dotenv aiosqlite numpy websockets
cp .env.example .env  # Edit with your API keys
sqlite3 signals.db < schema.sql
python src/main.py
```

## Signals
- Weighted imbalance (50 levels)
- Cumulative delta (σ bands)
- Imbalance speed
- Volume spike
- Microprice momentum
