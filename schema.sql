CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY,
    timestamp TEXT,
    symbol TEXT,
    side TEXT,
    entry_price REAL,
    exit_price REAL,
    quantity REAL,
    tp_price REAL,
    sl_price REAL,
    exit_reason TEXT,
    pnl_usd REAL,
    pnl_pct REAL,
    duration_ms INTEGER,
    imbalance REAL,
    delta_sigma REAL,
    speed_ratio REAL,
    volume_ratio REAL,
    microprice_pct REAL,
    dry_run INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
