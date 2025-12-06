#!/bin/bash
# =============================================================================
# HFT Order-Book Imbalance Scalping Bot - Setup Script
# Target: Ubuntu/Debian fresh server
# =============================================================================
set -euo pipefail

BOT_DIR="/root/hft_imbalance_bot"
VENV_DIR="$BOT_DIR/.venv"
SERVICE_NAME="hft_imbalance_bot"

echo "═══════════════════════════════════════════════════════════════"
echo " HFT Imbalance Bot Setup"
echo "═══════════════════════════════════════════════════════════════"

# Create directory structure
echo "[1/6] Creating directory structure..."
mkdir -p "$BOT_DIR/src"
cd "$BOT_DIR"

# Create virtual environment
echo "[2/6] Creating Python virtual environment..."
python3.11 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

# Install dependencies
echo "[3/6] Installing dependencies..."
pip install --upgrade pip wheel
pip install \
    ccxt==4.4.54 \
    loguru==0.7.3 \
    pydantic==2.10.4 \
    pydantic-settings==2.7.1 \
    python-dotenv==1.0.1 \
    aiosqlite==0.20.0 \
    numpy==2.2.1 \
    websockets==14.1

# Create .env template
echo "[4/6] Creating .env configuration..."
cat > "$BOT_DIR/.env" << 'ENVFILE'
# =============================================================================
# HFT Imbalance Bot Configuration
# =============================================================================

# ─── SAFETY MODE ─────────────────────────────────────────────────────────────
# CRITICAL: Set to false ONLY when ready for live trading
DRY_RUN=true

# ─── EXCHANGE CREDENTIALS ────────────────────────────────────────────────────
MEXC_API_KEY=your_api_key_here
MEXC_API_SECRET=your_api_secret_here

# ─── TRADING PAIRS ───────────────────────────────────────────────────────────
SYMBOLS=BTC/USDT,ETH/USDT

# ─── SIGNAL THRESHOLDS ───────────────────────────────────────────────────────
# Weighted imbalance (top 50 levels)
IMBALANCE_BUY_THRESHOLD=1.75
IMBALANCE_SELL_THRESHOLD=0.57

# Cumulative delta (sigma multiplier)
DELTA_SIGMA_THRESHOLD=3.8

# Imbalance speed multiplier
SPEED_MULTIPLIER=4.2

# Volume spike ratio (current / SMA20)
VOLUME_SPIKE_RATIO=2.7

# Microprice momentum (percentage)
MICROPRICE_BUY_PCT=0.04
MICROPRICE_SELL_PCT=-0.04

# ─── SPOOFING FILTER ─────────────────────────────────────────────────────────
SPOOF_BTC_SIZE=50.0
SPOOF_ETH_SIZE=1000.0
SPOOF_WINDOW_MS=800
SPOOF_BLOCK_SECONDS=15

# ─── EXECUTION PARAMETERS ────────────────────────────────────────────────────
# Take-profit percentages
TP_BTC_PCT=0.08
TP_ETH_PCT=0.12

# Stop-loss percentage (universal)
SL_PCT=0.04

# Max position lifetime in seconds
MAX_POSITION_LIFETIME=4.0

# Risk per trade (% of USDT balance)
RISK_PER_TRADE_PCT=0.5

# Max concurrent positions per pair
MAX_POSITIONS_PER_PAIR=3

# ─── TIMING PARAMETERS ───────────────────────────────────────────────────────
DELTA_WINDOW_MS=300
MICROPRICE_WINDOW_MS=200
IMBALANCE_SPEED_WINDOW_SEC=5
SPEED_AVG_WINDOW_SEC=300

# ─── NETWORK ─────────────────────────────────────────────────────────────────
WS_PING_INTERVAL=30
WS_RECONNECT_MAX_DELAY=60
REQUEST_TIMEOUT=10
MAX_RETRIES=3

# ─── LOGGING ─────────────────────────────────────────────────────────────────
LOG_LEVEL=INFO
LOG_ROTATION=100 MB
LOG_RETENTION=7 days

# ─── VERSION ─────────────────────────────────────────────────────────────────
BOT_VERSION=1.0.0
ENVFILE

# Create SQLite database
echo "[5/6] Creating SQLite database..."
cat > "$BOT_DIR/init_db.py" << 'PYDB'
import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), "signals.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    quantity REAL NOT NULL,
    tp_price REAL NOT NULL,
    sl_price REAL NOT NULL,
    exit_reason TEXT,
    pnl_usd REAL,
    pnl_pct REAL,
    duration_ms INTEGER,
    imbalance REAL,
    delta_sigma REAL,
    speed_ratio REAL,
    volume_ratio REAL,
    microprice_pct REAL,
    dry_run INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
""")

cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
""")
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
""")

conn.commit()
conn.close()
print(f"Database initialized: {db_path}")
PYDB

python3 "$BOT_DIR/init_db.py"
rm "$BOT_DIR/init_db.py"

# Create systemd service
echo "[6/6] Creating systemd service..."
cat > "/etc/systemd/system/${SERVICE_NAME}.service" << SVCFILE
[Unit]
Description=HFT Order-Book Imbalance Scalping Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=$BOT_DIR
Environment="PATH=$VENV_DIR/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=$VENV_DIR/bin/python $BOT_DIR/src/main.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=$BOT_DIR

[Install]
WantedBy=multi-user.target
SVCFILE

systemctl daemon-reload

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " Setup Complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo " Next steps:"
echo "   1. Edit .env with your MEXC API credentials"
echo "   2. Copy src/main.py to $BOT_DIR/src/"
echo "   3. Syntax check:  python -c \"import py_compile; py_compile.compile('$BOT_DIR/src/main.py', doraise=True); print('OK')\""
echo "   4. Test run:      cd $BOT_DIR && .venv/bin/python src/main.py"
echo "   5. Start service: systemctl start $SERVICE_NAME"
echo "   6. Enable boot:   systemctl enable $SERVICE_NAME"
echo "   7. View logs:     journalctl -u $SERVICE_NAME -f"
echo ""
echo " ⚠️  DRY_RUN=true by default. Only set to false when ready!"
echo ""
