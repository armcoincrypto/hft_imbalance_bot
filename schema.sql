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
    -- Signal metrics at entry
    imbalance REAL,
    delta_sigma REAL,
    speed_ratio REAL,
    volume_ratio REAL,
    microprice_pct REAL,
    -- Market conditions at entry (NEW)
    spread_at_entry REAL,
    bid_depth REAL,
    ask_depth REAL,
    mid_price REAL,
    -- Metadata
    dry_run INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_side ON signals(side);
