
# HFT Order-Book Imbalance Scalping Bot

MEXC scalping bot using order-book flow signals.

## Quick Start (Local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ccxt loguru pydantic pydantic-settings python-dotenv aiosqlite numpy websockets
cp .env.example .env  # Edit with your API keys
sqlite3 signals.db < schema.sql
python src/main.py
```

## VPS Deployment

```bash
# Clone and setup
git clone https://github.com/armcoincrypto/hft_imbalance_bot.git
cd hft_imbalance_bot
python3 -m venv .venv
source .venv/bin/activate
pip install ccxt loguru pydantic pydantic-settings python-dotenv aiosqlite numpy websockets
cp .env.example .env && nano .env  # Add API keys
sqlite3 signals.db < schema.sql

# Systemd service
cat > /etc/systemd/system/hft_bot.service << 'EOF'
[Unit]
Description=HFT Imbalance Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/hft_imbalance_bot
ExecStart=/root/hft_imbalance_bot/.venv/bin/python src/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable hft_bot
systemctl start hft_bot
```

## Monitoring

```bash
# Live metrics
curl -s http://localhost:8080/debug | python3 -m json.tool

# Service logs
journalctl -u hft_bot -f

# Trade history
sqlite3 signals.db "SELECT * FROM signals ORDER BY id DESC LIMIT 10;"

# Performance summary
sqlite3 signals.db "
SELECT COUNT(*) as trades,
       SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins,
       ROUND(SUM(pnl_usd), 2) as total_pnl
FROM signals WHERE exit_price IS NOT NULL;"
```

## Signal Thresholds

| Parameter | Default | Description |
|-----------|---------|-------------|
| IMBALANCE_BUY_THRESHOLD | 1.75 | Buy when imbalance >= this |
| IMBALANCE_SELL_THRESHOLD | 0.57 | Sell when imbalance <= this |
| DELTA_SIGMA_THRESHOLD | 3.8 | Delta deviation threshold |
| SPEED_MULTIPLIER | 4.2 | Imbalance speed ratio |
| VOLUME_SPIKE_RATIO | 2.7 | Volume vs 20min SMA |
| MICROPRICE_BUY_PCT | 0.04 | Microprice momentum % |

## Signals

- Weighted imbalance (50 levels)
- Cumulative delta (σ bands)
- Imbalance speed
- Volume spike
- Microprice momentum

