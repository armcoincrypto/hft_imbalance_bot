# HFT Order-Book Imbalance Scalping Bot - Commands Reference
# =============================================================================

## Quick Start (Fresh Server)

```bash
# 1. Upload files to server (from local machine)
scp -r hft_imbalance_bot root@your-server:/root/

# 2. SSH into server
ssh root@your-server

# 3. Run setup
chmod +x /root/hft_imbalance_bot/setup.sh
/root/hft_imbalance_bot/setup.sh

# 4. Edit credentials
nano /root/hft_imbalance_bot/.env
# Set MEXC_API_KEY and MEXC_API_SECRET

# 5. Syntax check
cd /root/hft_imbalance_bot
python3 -c "import py_compile; py_compile.compile('src/main.py', doraise=True); print('OK')"

# 6. Test foreground run (Ctrl+C to stop)
.venv/bin/python src/main.py

# 7. Start as service
systemctl start hft_imbalance_bot
systemctl enable hft_imbalance_bot
```

## Service Management

```bash
# Start/Stop/Restart
systemctl start hft_imbalance_bot
systemctl stop hft_imbalance_bot
systemctl restart hft_imbalance_bot

# Check status
systemctl status hft_imbalance_bot

# View logs (real-time)
journalctl -u hft_imbalance_bot -f

# View logs (last 100 lines)
journalctl -u hft_imbalance_bot -n 100

# View application log file
tail -f /root/hft_imbalance_bot/bot.log
```

## Health Check & Debug

```bash
# Health check
curl http://localhost:8080/health

# Debug endpoint (non-secret config, metrics, balances)
curl http://localhost:8080/debug | jq .

# Example debug output:
# {
#   "version": "1.0.0",
#   "mode": "DRY_RUN",
#   "symbols": {
#     "BTC/USDT": {
#       "orderbook": { "best_bid": 97500.0, "best_ask": 97501.0 },
#       "metrics": { "weighted_imbalance": 1.23, "delta_sigma": 2.1, ... },
#       "positions": 0
#     }
#   }
# }
```

## Database Queries

```bash
cd /root/hft_imbalance_bot

# Open database
sqlite3 signals.db

# Today's trades
SELECT symbol, side, entry_price, exit_price, pnl_usd, exit_reason 
FROM signals 
WHERE date(timestamp) = date('now') 
ORDER BY timestamp DESC 
LIMIT 20;

# Daily summary
SELECT 
    date(timestamp) as day,
    COUNT(*) as trades,
    SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins,
    ROUND(SUM(pnl_usd), 2) as total_pnl,
    ROUND(AVG(pnl_pct), 4) as avg_pnl_pct
FROM signals
WHERE exit_price IS NOT NULL
GROUP BY date(timestamp)
ORDER BY day DESC;

# Signal quality analysis
SELECT 
    CASE WHEN pnl_usd > 0 THEN 'WIN' ELSE 'LOSS' END as result,
    ROUND(AVG(imbalance), 2) as avg_imbalance,
    ROUND(AVG(delta_sigma), 2) as avg_delta_sigma,
    ROUND(AVG(volume_ratio), 2) as avg_volume_ratio
FROM signals
WHERE exit_price IS NOT NULL
GROUP BY result;

# Exit
.quit
```

## Switching from DRY_RUN to LIVE

⚠️ **CRITICAL: Only do this when confident in bot performance**

```bash
# 1. Stop the bot
systemctl stop hft_imbalance_bot

# 2. Review dry-run performance
sqlite3 /root/hft_imbalance_bot/signals.db \
  "SELECT COUNT(*), SUM(pnl_usd), AVG(pnl_pct) FROM signals WHERE dry_run=1;"

# 3. Verify MEXC account has funds
# - Log into MEXC, check USDT balance
# - Ensure API key has trading permissions

# 4. Switch to LIVE mode
sed -i 's/DRY_RUN=true/DRY_RUN=false/' /root/hft_imbalance_bot/.env

# 5. Verify change
grep DRY_RUN /root/hft_imbalance_bot/.env
# Should show: DRY_RUN=false

# 6. Start with reduced risk (optional)
# sed -i 's/RISK_PER_TRADE_PCT=0.5/RISK_PER_TRADE_PCT=0.1/' /root/hft_imbalance_bot/.env

# 7. Start the bot
systemctl start hft_imbalance_bot

# 8. Monitor closely for first hour
journalctl -u hft_imbalance_bot -f
```

## Rollback Instructions

```bash
# 1. Emergency stop
systemctl stop hft_imbalance_bot

# 2. Switch back to DRY_RUN
sed -i 's/DRY_RUN=false/DRY_RUN=true/' /root/hft_imbalance_bot/.env

# 3. Verify
grep DRY_RUN /root/hft_imbalance_bot/.env

# 4. Check for open positions on MEXC
# - Log into MEXC web interface
# - Close any open positions manually if needed

# 5. Restart in dry-run mode
systemctl start hft_imbalance_bot
```

## Complete Removal

```bash
# Stop and disable service
systemctl stop hft_imbalance_bot
systemctl disable hft_imbalance_bot

# Remove service file
rm /etc/systemd/system/hft_imbalance_bot.service
systemctl daemon-reload

# Remove bot directory (backup database first!)
cp /root/hft_imbalance_bot/signals.db ~/signals_backup_$(date +%Y%m%d).db
rm -rf /root/hft_imbalance_bot
```

## Troubleshooting

### No signals generated
```bash
# Check order book data is flowing
curl http://localhost:8080/debug | jq '.symbols["BTC/USDT"].orderbook'

# Check metrics are being calculated
curl http://localhost:8080/debug | jq '.symbols["BTC/USDT"].metrics'

# If orderbook shows zeros, WebSocket may be disconnected
journalctl -u hft_imbalance_bot | grep -i "websocket\|reconnect"
```

### Signals generated but no trades
```bash
# Check position capacity
curl http://localhost:8080/debug | jq '.symbols["BTC/USDT"].positions'

# Check if spoofing filter is active
curl http://localhost:8080/debug | jq '.symbols["BTC/USDT"].spoof_blocked_for'

# Review signal thresholds in config
curl http://localhost:8080/debug | jq '.config'
```

### Exchange errors (LIVE mode)
```bash
# Check balance
journalctl -u hft_imbalance_bot | grep -i "balance"

# Check for API errors
journalctl -u hft_imbalance_bot | grep -i "error\|failed"

# Verify API permissions on MEXC
# - Spot trading must be enabled
# - IP whitelist (if enabled) must include server IP
```

## Parameter Tuning

Edit `/root/hft_imbalance_bot/.env` and restart:

```bash
# Make signals more conservative (fewer trades)
IMBALANCE_BUY_THRESHOLD=2.0      # was 1.75
IMBALANCE_SELL_THRESHOLD=0.5     # was 0.57
DELTA_SIGMA_THRESHOLD=4.0        # was 3.8
VOLUME_SPIKE_RATIO=3.0           # was 2.7

# Make signals more aggressive (more trades)
IMBALANCE_BUY_THRESHOLD=1.5
IMBALANCE_SELL_THRESHOLD=0.65
DELTA_SIGMA_THRESHOLD=3.5
VOLUME_SPIKE_RATIO=2.5

# Restart to apply
systemctl restart hft_imbalance_bot
```

## macOS Local Development (zsh)

```zsh
# Clone/create project
mkdir -p ~/hft_imbalance_bot/src
cd ~/hft_imbalance_bot

# Create venv
python3.11 -m venv .venv
source .venv/bin/activate

# Install deps
pip install ccxt loguru pydantic pydantic-settings python-dotenv aiosqlite numpy websockets

# Create .env
cat > .env << 'EOF'
DRY_RUN=true
MEXC_API_KEY=
MEXC_API_SECRET=
SYMBOLS=BTC/USDT,ETH/USDT
LOG_LEVEL=DEBUG
EOF

# Syntax check
python -c "import py_compile; py_compile.compile('src/main.py', doraise=True); print('OK')"

# Initialize DB
python -c "
import sqlite3
conn = sqlite3.connect('signals.db')
conn.execute('''CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY, timestamp TEXT, symbol TEXT, side TEXT,
    entry_price REAL, exit_price REAL, quantity REAL, tp_price REAL,
    sl_price REAL, exit_reason TEXT, pnl_usd REAL, pnl_pct REAL,
    duration_ms INTEGER, imbalance REAL, delta_sigma REAL,
    speed_ratio REAL, volume_ratio REAL, microprice_pct REAL,
    dry_run INTEGER DEFAULT 1, created_at TEXT DEFAULT CURRENT_TIMESTAMP
)''')
conn.commit()
print('DB created')
"

# Run
python src/main.py
```
