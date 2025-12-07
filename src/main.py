#!/usr/bin/env python3
"""
=============================================================================
HFT Order-Book Imbalance Scalping Bot for MEXC
Version: 2.0.0 - Professional Regime-Aware Strategy
=============================================================================
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import aiosqlite
import ccxt.async_support as ccxt
import numpy as np
from loguru import logger
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# CONFIGURATION
# =============================================================================

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    dry_run: bool = Field(default=True)
    mexc_api_key: str = Field(default="")
    mexc_api_secret: str = Field(default="")
    symbols: str = Field(default="BTC/USDT,ETH/USDT")

    # Signal thresholds - MEXC 2025 Pro Config
    imbalance_buy_threshold: float = Field(default=0.80)  # Tighter lower bound
    imbalance_buy_max: float = Field(default=1.40)  # Tighter upper bound
    imbalance_sell_threshold: float = Field(default=0.0)  # Disabled
    disable_sell: bool = Field(default=True)  # Long-only strategy
    delta_sigma_threshold: float = Field(default=2.5)
    speed_multiplier: float = Field(default=3.0)
    volume_spike_ratio: float = Field(default=2.0)
    microprice_buy_pct: float = Field(default=0.0004)
    microprice_sell_pct: float = Field(default=-0.0004)

    # PRO FILTERS - New in v2.0
    rate_limit_seconds: float = Field(default=60.0)  # Min seconds between trades per symbol
    cooldown_losses: int = Field(default=3)  # Consecutive losses before cooldown
    cooldown_minutes: float = Field(default=10.0)  # Cooldown duration after losses
    max_spread_pct: float = Field(default=0.05)  # Max spread % for entry (volatility filter)
    trading_hours_start: int = Field(default=13)  # UTC hour to start trading
    trading_hours_end: int = Field(default=20)  # UTC hour to stop trading
    enable_time_filter: bool = Field(default=True)  # Enable/disable time filter
    min_confidence_score: float = Field(default=60.0)  # Min confidence score (0-100)

    # Spoofing filter - Raised thresholds for quality signals
    spoof_btc_size: float = Field(default=200.0)  # Doubled from 100
    spoof_eth_size: float = Field(default=4000.0)  # Doubled from 2000
    spoof_window_ms: int = Field(default=800)
    spoof_block_seconds: int = Field(default=15)

    # Execution
    tp_btc_pct: float = Field(default=0.04)
    tp_eth_pct: float = Field(default=0.06)
    sl_pct: float = Field(default=0.03)  # Tighter stop
    max_position_lifetime: float = Field(default=12.0)  # Longer for trends
    risk_per_trade_pct: float = Field(default=0.3)  # Reduced risk
    max_positions_per_pair: int = Field(default=1)  # Only 1 position at a time
    use_trailing_stop: bool = Field(default=True)  # Enable trailing stops
    trailing_stop_pct: float = Field(default=0.02)  # Trail at 0.02%

    # Timing
    delta_window_ms: int = Field(default=300)
    microprice_window_ms: int = Field(default=2000)
    imbalance_speed_window_sec: int = Field(default=5)
    speed_avg_window_sec: int = Field(default=300)

    # Network
    ws_ping_interval: int = Field(default=30)
    ws_reconnect_max_delay: int = Field(default=60)
    request_timeout: int = Field(default=10)
    max_retries: int = Field(default=3)

    # Logging
    log_level: str = Field(default="INFO")
    log_rotation: str = Field(default="100 MB")
    log_retention: str = Field(default="7 days")
    bot_version: str = Field(default="2.0.0")

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip() for s in self.symbols.split(",") if s.strip()]

    def get_tp_pct(self, symbol: str) -> float:
        return self.tp_btc_pct if "BTC" in symbol else self.tp_eth_pct

    def get_spoof_size(self, symbol: str) -> float:
        return self.spoof_btc_size if "BTC" in symbol else self.spoof_eth_size


# =============================================================================
# DATA STRUCTURES
# =============================================================================

class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class OrderBook:
    symbol: str
    bids: list[tuple[float, float]] = field(default_factory=list)
    asks: list[tuple[float, float]] = field(default_factory=list)
    timestamp: float = 0.0
    
    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0
    
    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0
    
    @property
    def mid_price(self) -> float:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return 0.0
    
    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid if self.best_bid and self.best_ask else 0.0


@dataclass
class Position:
    id: str
    symbol: str
    side: Side
    entry_price: float
    quantity: float
    tp_price: float
    sl_price: float
    entry_time: float
    order_id: Optional[str] = None
    # Signal metrics at entry
    imbalance: float = 0.0
    delta_sigma: float = 0.0
    speed_ratio: float = 0.0
    volume_ratio: float = 0.0
    microprice_pct: float = 0.0
    confidence_score: float = 0.0
    # Market conditions at entry
    spread_at_entry: float = 0.0
    bid_depth: float = 0.0
    ask_depth: float = 0.0
    mid_price: float = 0.0
    # Trailing stop tracking
    highest_price: float = 0.0  # Track highest price since entry (for BUY)
    trailing_sl: float = 0.0  # Current trailing stop level


@dataclass
class SignalMetrics:
    timestamp: float
    symbol: str
    weighted_imbalance: float = 0.0
    cumulative_delta: float = 0.0
    delta_sigma: float = 0.0
    imbalance_speed: float = 0.0
    speed_ratio: float = 0.0
    volume_ratio: float = 0.0
    microprice_change_pct: float = 0.0
    spread_pct: float = 0.0  # Current spread as % of mid price
    buy_signal: bool = False
    sell_signal: bool = False
    spoof_blocked: bool = False
    confidence_score: float = 0.0  # 0-100 weighted signal strength

    buy_confirmations: int = 0
    sell_confirmations: int = 0

    # Filter states
    rate_limited: bool = False
    in_cooldown: bool = False
    outside_hours: bool = False
    spread_too_wide: bool = False

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "weighted_imbalance": round(self.weighted_imbalance, 4),
            "cumulative_delta": round(self.cumulative_delta, 4),
            "delta_sigma": round(self.delta_sigma, 2),
            "imbalance_speed": round(self.imbalance_speed, 6),
            "speed_ratio": round(self.speed_ratio, 2),
            "volume_ratio": round(self.volume_ratio, 2),
            "microprice_change_pct": round(self.microprice_change_pct, 4),
            "spread_pct": round(self.spread_pct, 4),
            "buy_signal": self.buy_signal,
            "sell_signal": self.sell_signal,
            "spoof_blocked": self.spoof_blocked,
            "confidence_score": round(self.confidence_score, 1),
            "buy_confirmations": self.buy_confirmations,
            "sell_confirmations": self.sell_confirmations,
            "rate_limited": self.rate_limited,
            "in_cooldown": self.in_cooldown,
            "outside_hours": self.outside_hours,
            "spread_too_wide": self.spread_too_wide,
        }


@dataclass
class TimestampedValue:
    timestamp: float
    value: float


@dataclass
class LargeOrder:
    timestamp: float
    side: str
    price: float
    size: float


# =============================================================================
# TIME SERIES BUFFER
# =============================================================================

class TimeSeriesBuffer:
    def __init__(self, window_seconds: float, max_size: int = 10000):
        self.window_seconds = window_seconds
        self.max_size = max_size
        self._data: deque[TimestampedValue] = deque(maxlen=max_size)
    
    def add(self, value: float, timestamp: float | None = None) -> None:
        ts = timestamp if timestamp is not None else time.time()
        self._data.append(TimestampedValue(timestamp=ts, value=value))
        self._prune(ts)
    
    def _prune(self, current_time: float) -> None:
        cutoff = current_time - self.window_seconds
        while self._data and self._data[0].timestamp < cutoff:
            self._data.popleft()
    
    def get_values(self, current_time: float | None = None) -> list[float]:
        ts = current_time if current_time is not None else time.time()
        self._prune(ts)
        return [v.value for v in self._data]
    
    def get_with_timestamps(self, current_time: float | None = None) -> list[tuple[float, float]]:
        ts = current_time if current_time is not None else time.time()
        self._prune(ts)
        return [(v.timestamp, v.value) for v in self._data]
    
    def sum(self, current_time: float | None = None) -> float:
        return sum(self.get_values(current_time))
    
    def mean(self, current_time: float | None = None) -> float:
        values = self.get_values(current_time)
        return np.mean(values) if values else 0.0
    
    def std(self, current_time: float | None = None) -> float:
        values = self.get_values(current_time)
        return float(np.std(values)) if len(values) > 1 else 0.0
    
    def __len__(self) -> int:
        return len(self._data)


# =============================================================================
# SYMBOL STATE
# =============================================================================

class SymbolState:
    def __init__(self, symbol: str, settings: Settings):
        self.symbol = symbol
        self.settings = settings
        self.orderbook = OrderBook(symbol=symbol)

        # Buffers
        self.delta_buffer = TimeSeriesBuffer(settings.delta_window_ms / 1000.0)
        self.delta_30sec_buffer = TimeSeriesBuffer(30.0)
        self.imbalance_buffer = TimeSeriesBuffer(settings.imbalance_speed_window_sec)
        self.speed_buffer = TimeSeriesBuffer(settings.speed_avg_window_sec)
        self.microprice_buffer = TimeSeriesBuffer(settings.microprice_window_ms / 1000.0)
        self.volume_1m_buffer = TimeSeriesBuffer(60.0)
        self.volume_sma_buffer = TimeSeriesBuffer(20 * 60.0)

        # Spoofing - now used as CONFIRMATION, not blocking
        self.large_orders: dict[str, LargeOrder] = {}
        self.spoof_confirmed_until: float = 0.0  # Spoof = positive signal window

        # Previous state
        self._prev_bid_volume: float = 0.0
        self._prev_ask_volume: float = 0.0

        # Market info
        self.tick_size: float = 0.01
        self.step_size: float = 0.0001
        self.min_notional: float = 5.0

        # Positions
        self.positions: dict[str, Position] = {}

        # Full orderbook for incremental updates
        self._full_bids: dict[float, float] = {}
        self._full_asks: dict[float, float] = {}

        # PRO FILTERS - Rate limiting and cooldown
        self.last_trade_time: float = 0.0  # Last trade timestamp
        self.consecutive_losses: int = 0  # Track consecutive losses
        self.cooldown_until: float = 0.0  # Cooldown end time
        self.total_trades: int = 0
        self.winning_trades: int = 0
    
    def update_orderbook_full(self, bids: list, asks: list, timestamp: float) -> None:
        """Full orderbook snapshot."""
        self._full_bids = {float(p): float(s) for p, s, _ in bids if float(s) > 0}
        self._full_asks = {float(p): float(s) for p, s, _ in asks if float(s) > 0}
        self._rebuild_orderbook(timestamp)
    
    def update_orderbook_delta(self, bids: list, asks: list, timestamp: float) -> None:
        """Incremental orderbook update."""
        for item in bids:
            price, size = float(item[0]), float(item[1])
            if size == 0:
                self._full_bids.pop(price, None)
            else:
                self._full_bids[price] = size
        
        for item in asks:
            price, size = float(item[0]), float(item[1])
            if size == 0:
                self._full_asks.pop(price, None)
            else:
                self._full_asks[price] = size
        
        self._rebuild_orderbook(timestamp)
    
    def _rebuild_orderbook(self, timestamp: float) -> None:
        """Rebuild sorted orderbook from full data."""
        now = timestamp
        
        # Sort and take top 50 levels
        sorted_bids = sorted(self._full_bids.items(), key=lambda x: -x[0])[:50]
        sorted_asks = sorted(self._full_asks.items(), key=lambda x: x[0])[:50]
        
        self.orderbook.bids = sorted_bids
        self.orderbook.asks = sorted_asks
        self.orderbook.timestamp = now
        
        # Update metrics
        self._update_delta(now)
        self._update_imbalance(now)
        self._update_microprice(now)
        self._check_spoofing(now)
    
    def update_volume(self, quote_volume: float, timestamp: float) -> None:
        self.volume_1m_buffer.add(quote_volume, timestamp)
        self.volume_sma_buffer.add(quote_volume, timestamp)
    
    def _update_delta(self, now: float) -> None:
        bid_vol = sum(size for _, size in self.orderbook.bids)
        ask_vol = sum(size for _, size in self.orderbook.asks)
        
        if self._prev_bid_volume > 0:
            delta = (bid_vol - self._prev_bid_volume) - (ask_vol - self._prev_ask_volume)
            self.delta_buffer.add(delta, now)
            self.delta_30sec_buffer.add(delta, now)
        
        self._prev_bid_volume = bid_vol
        self._prev_ask_volume = ask_vol
    
    def _update_imbalance(self, now: float) -> None:
        imbalance = self.compute_weighted_imbalance()
        self.imbalance_buffer.add(imbalance, now)
        
        speed = self.compute_imbalance_speed(now)
        if speed is not None:
            self.speed_buffer.add(abs(speed), now)
    
    def _update_microprice(self, now: float) -> None:
        if self.orderbook.mid_price > 0:
            self.microprice_buffer.add(self.orderbook.mid_price, now)
    
    def _check_spoofing(self, now: float) -> None:
        """
        MEXC 2025 strategy: Spoof = CONFIRMATION, not blocking.
        When whales spoof (large order appears then vanishes),
        the real move follows 10-15 seconds later.
        We WANT to trade when spoof is detected.
        """
        spoof_size = self.settings.get_spoof_size(self.symbol)
        window_sec = self.settings.spoof_window_ms / 1000.0

        current_large: set[str] = set()

        for price, size in self.orderbook.bids:
            if size >= spoof_size:
                key = f"bid_{price}"
                current_large.add(key)
                if key not in self.large_orders:
                    self.large_orders[key] = LargeOrder(now, "bid", price, size)

        for price, size in self.orderbook.asks:
            if size >= spoof_size:
                key = f"ask_{price}"
                current_large.add(key)
                if key not in self.large_orders:
                    self.large_orders[key] = LargeOrder(now, "ask", price, size)

        to_remove = []
        for key, order in self.large_orders.items():
            if key not in current_large:
                if now - order.timestamp <= window_sec:
                    # Spoof detected! This is a POSITIVE signal for trading
                    self.spoof_confirmed_until = now + self.settings.spoof_block_seconds
                    logger.info(f"[{self.symbol}] SPOOF CONFIRMED - trade window open for {self.settings.spoof_block_seconds}s")
                to_remove.append(key)
            elif now - order.timestamp > window_sec:
                to_remove.append(key)

        for key in to_remove:
            del self.large_orders[key]
    
    def compute_weighted_imbalance(self) -> float:
        bid_weighted = sum(p * s for p, s in self.orderbook.bids)
        ask_weighted = sum(p * s for p, s in self.orderbook.asks)
        if ask_weighted == 0:
            return 999.0 if bid_weighted > 0 else 1.0
        return bid_weighted / ask_weighted
    
    def compute_cumulative_delta_sigma(self, now: float) -> tuple[float, float]:
        cum_delta = self.delta_buffer.sum(now)
        std_dev = self.delta_30sec_buffer.std(now)
        if std_dev == 0:
            return cum_delta, 0.0
        return cum_delta, cum_delta / std_dev
    
    def compute_imbalance_speed(self, now: float) -> float | None:
        data = self.imbalance_buffer.get_with_timestamps(now)
        if len(data) < 2:
            return None
        current_imbalance = data[-1][1]
        target_time = now - self.settings.imbalance_speed_window_sec
        oldest_in_window = None
        for ts, val in data:
            if ts <= target_time:
                oldest_in_window = (ts, val)
            else:
                break
        if oldest_in_window is None and data:
            oldest_in_window = data[0]
        if oldest_in_window is None:
            return None
        time_diff = now - oldest_in_window[0]
        if time_diff == 0:
            return 0.0
        return (current_imbalance - oldest_in_window[1]) / time_diff
    
    def compute_speed_ratio(self, now: float) -> float | None:
        current_speed = self.compute_imbalance_speed(now)
        if current_speed is None:
            return None
        avg_speed = self.speed_buffer.mean(now)
        if avg_speed == 0:
            return 0.0
        return current_speed / avg_speed
    
    def compute_volume_ratio(self, now: float) -> float:
        current_vol = self.volume_1m_buffer.sum(now)
        all_volumes = self.volume_sma_buffer.get_values(now)
        if len(all_volumes) < 20:
            return 0.0
        sma20 = np.mean(all_volumes[-20:])
        if sma20 == 0:
            return 0.0
        return current_vol / sma20
    
    def compute_microprice_momentum(self, now: float) -> float | None:
        data = self.microprice_buffer.get_with_timestamps(now)
        if len(data) < 2:
            return None
        current_mid = data[-1][1]
        oldest_mid = data[0][1]
        if oldest_mid == 0:
            return None
        return ((current_mid - oldest_mid) / oldest_mid) * 100
    
    def compute_signal(self, now: float) -> SignalMetrics:
        metrics = SignalMetrics(timestamp=now, symbol=self.symbol)

        # Basic metrics
        metrics.weighted_imbalance = self.compute_weighted_imbalance()
        _, metrics.delta_sigma = self.compute_cumulative_delta_sigma(now)
        metrics.cumulative_delta = self.delta_buffer.sum(now)

        speed = self.compute_imbalance_speed(now)
        speed_ratio = self.compute_speed_ratio(now)
        metrics.imbalance_speed = speed if speed is not None else 0.0
        metrics.speed_ratio = speed_ratio if speed_ratio is not None else 0.0
        metrics.volume_ratio = self.compute_volume_ratio(now)

        microprice = self.compute_microprice_momentum(now)
        metrics.microprice_change_pct = microprice if microprice is not None else 0.0

        # Spread as percentage of mid price
        if self.orderbook.mid_price > 0:
            metrics.spread_pct = (self.orderbook.spread / self.orderbook.mid_price) * 100
        else:
            metrics.spread_pct = 999.0

        # Spoof confirmation
        metrics.spoof_blocked = now < self.spoof_confirmed_until

        # =====================================================================
        # PRO FILTERS - v2.0
        # =====================================================================

        # 1. RATE LIMITING - Min time between trades
        time_since_last = now - self.last_trade_time
        metrics.rate_limited = time_since_last < self.settings.rate_limit_seconds

        # 2. COOLDOWN - After consecutive losses
        metrics.in_cooldown = now < self.cooldown_until

        # 3. TIME FILTER - Only trade during high-liquidity hours
        if self.settings.enable_time_filter:
            current_hour = datetime.now(timezone.utc).hour
            metrics.outside_hours = not (
                self.settings.trading_hours_start <= current_hour < self.settings.trading_hours_end
            )
        else:
            metrics.outside_hours = False

        # 4. VOLATILITY FILTER - Spread too wide = choppy market
        metrics.spread_too_wide = metrics.spread_pct > self.settings.max_spread_pct

        # =====================================================================
        # CONFIDENCE SCORING (0-100)
        # Weight signals to create a single quality score
        # =====================================================================
        confidence = 0.0

        # Imbalance in sweet spot (0.8-1.4) = 40 points
        imb = metrics.weighted_imbalance
        if self.settings.imbalance_buy_threshold <= imb <= self.settings.imbalance_buy_max:
            # Peak confidence at 1.0-1.1 (neutral with slight bullish)
            if 0.95 <= imb <= 1.15:
                confidence += 40
            elif 0.85 <= imb <= 1.25:
                confidence += 30
            else:
                confidence += 20

        # Delta sigma strength = 20 points
        if metrics.delta_sigma >= self.settings.delta_sigma_threshold:
            confidence += 20
        elif metrics.delta_sigma >= self.settings.delta_sigma_threshold * 0.5:
            confidence += 10

        # Speed ratio = 15 points
        if metrics.speed_ratio >= self.settings.speed_multiplier:
            confidence += 15
        elif metrics.speed_ratio >= self.settings.speed_multiplier * 0.5:
            confidence += 7

        # Volume spike = 15 points
        if metrics.volume_ratio >= self.settings.volume_spike_ratio:
            confidence += 15
        elif metrics.volume_ratio >= self.settings.volume_spike_ratio * 0.5:
            confidence += 7

        # Microprice momentum = 10 points
        if metrics.microprice_change_pct >= self.settings.microprice_buy_pct:
            confidence += 10
        elif metrics.microprice_change_pct >= self.settings.microprice_buy_pct * 0.5:
            confidence += 5

        metrics.confidence_score = confidence

        # Count confirmations for debugging
        buy_confirmations = sum([
            metrics.delta_sigma >= self.settings.delta_sigma_threshold,
            metrics.speed_ratio >= self.settings.speed_multiplier,
            metrics.volume_ratio >= self.settings.volume_spike_ratio,
            metrics.microprice_change_pct >= self.settings.microprice_buy_pct,
        ])
        metrics.buy_confirmations = buy_confirmations
        metrics.sell_confirmations = 0

        # =====================================================================
        # FINAL BUY SIGNAL - All filters must pass
        # =====================================================================
        buy_in_range = (
            metrics.weighted_imbalance >= self.settings.imbalance_buy_threshold and
            metrics.weighted_imbalance <= self.settings.imbalance_buy_max
        )

        buy_conditions = [
            metrics.spoof_blocked,  # Spoof confirmed
            buy_in_range,  # In winning imbalance range
            metrics.confidence_score >= self.settings.min_confidence_score,  # Min confidence
            not metrics.rate_limited,  # Not rate limited
            not metrics.in_cooldown,  # Not in cooldown
            not metrics.outside_hours,  # Within trading hours
            not metrics.spread_too_wide,  # Spread acceptable
            len(self.positions) < self.settings.max_positions_per_pair,  # Position limit
        ]

        # SELL disabled
        sell_conditions = [False]

        metrics.buy_signal = all(buy_conditions)
        metrics.sell_signal = False if self.settings.disable_sell else all(sell_conditions)

        return metrics


# =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: aiosqlite.Connection | None = None
    
    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        logger.info(f"Database connected: {self.db_path}")
    
    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            logger.info("Database connection closed")
    
    async def log_signal(
        self,
        position: Position,
        exit_price: float | None,
        exit_reason: str | None,
        pnl_usd: float | None,
        pnl_pct: float | None,
        duration_ms: int | None,
        dry_run: bool,
    ) -> None:
        if not self._conn:
            return
        await self._conn.execute(
            """INSERT INTO signals (
                timestamp, symbol, side, entry_price, exit_price, quantity,
                tp_price, sl_price, exit_reason, pnl_usd, pnl_pct, duration_ms,
                imbalance, delta_sigma, speed_ratio, volume_ratio, microprice_pct,
                spread_at_entry, bid_depth, ask_depth, mid_price, dry_run
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                position.symbol, position.side.value, position.entry_price,
                exit_price, position.quantity, position.tp_price, position.sl_price,
                exit_reason, pnl_usd, pnl_pct, duration_ms,
                position.imbalance, position.delta_sigma, position.speed_ratio,
                position.volume_ratio, position.microprice_pct,
                position.spread_at_entry, position.bid_depth, position.ask_depth,
                position.mid_price, 1 if dry_run else 0,
            ),
        )
        await self._conn.commit()


# =============================================================================
# EXCHANGE CLIENT
# =============================================================================

class ExchangeClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.exchange: ccxt.mexc | None = None
        self._markets_loaded = False
    
    async def connect(self) -> None:
        self.exchange = ccxt.mexc({
            "apiKey": self.settings.mexc_api_key,
            "secret": self.settings.mexc_api_secret,
            "enableRateLimit": True,
            "timeout": self.settings.request_timeout * 1000,
            "options": {"defaultType": "spot"},
        })
        await self.exchange.load_markets()
        self._markets_loaded = True
        logger.info("Exchange connected")
    
    async def close(self) -> None:
        if self.exchange:
            await self.exchange.close()
    
    def get_market_info(self, symbol: str) -> dict:
        if not self.exchange or not self._markets_loaded:
            return {}
        market = self.exchange.markets.get(symbol, {})
        return {
            "tick_size": market.get("precision", {}).get("price", 0.01),
            "step_size": market.get("precision", {}).get("amount", 0.0001),
            "min_notional": market.get("limits", {}).get("cost", {}).get("min", 5.0),
        }
    
    def quantize_price(self, symbol: str, price: float) -> float:
        info = self.get_market_info(symbol)
        tick = info.get("tick_size", 0.01)
        if isinstance(tick, int):
            return round(price, tick)
        return round(price / tick) * tick
    
    def quantize_amount(self, symbol: str, amount: float) -> float:
        info = self.get_market_info(symbol)
        step = info.get("step_size", 0.0001)
        if isinstance(step, int):
            return round(amount, step)
        return round(amount / step) * step
    
    async def get_usdt_balance(self) -> float:
        if not self.exchange:
            return 0.0
        try:
            balance = await self.exchange.fetch_balance()
            return float(balance.get("USDT", {}).get("free", 0))
        except Exception as e:
            logger.error(f"Balance error: {e}")
            return 0.0
    
    async def place_limit_order(self, symbol: str, side: Side, amount: float, price: float) -> dict | None:
        if not self.exchange:
            return None
        q_price = self.quantize_price(symbol, price)
        q_amount = self.quantize_amount(symbol, amount)
        try:
            order = await self.exchange.create_limit_order(symbol, side.value.lower(), q_amount, q_price)
            logger.info(f"[{symbol}] Order: {side.value} {q_amount} @ {q_price}")
            return order
        except Exception as e:
            logger.error(f"Order error: {e}")
            return None
    
    async def place_market_order(self, symbol: str, side: Side, amount: float) -> dict | None:
        if not self.exchange:
            return None
        q_amount = self.quantize_amount(symbol, amount)
        try:
            order = await self.exchange.create_market_order(symbol, side.value.lower(), q_amount)
            return order
        except Exception as e:
            logger.error(f"Market order error: {e}")
            return None


# =============================================================================
# WEBSOCKET MANAGER - MEXC CONTRACT/FUTURES
# =============================================================================

class WebSocketManager:
    """Uses MEXC Contract WebSocket (works globally)."""
    
    WS_URL = "wss://contract.mexc.com/edge"
    
    def __init__(self, settings: Settings, states: dict[str, SymbolState]):
        self.settings = settings
        self.states = states
        self._ws = None
        self._running = False
        self._reconnect_delay = 1
    
    def _to_contract_symbol(self, symbol: str) -> str:
        """BTC/USDT -> BTC_USDT"""
        return symbol.replace("/", "_")
    
    def _from_contract_symbol(self, ws_symbol: str) -> str | None:
        """BTC_USDT -> BTC/USDT"""
        for sym in self.states.keys():
            if self._to_contract_symbol(sym) == ws_symbol:
                return sym
        return None
    
    async def connect(self) -> None:
        import websockets
        self._running = True
        
        while self._running:
            try:
                logger.info(f"Connecting to WebSocket: {self.WS_URL}")
                
                async with websockets.connect(self.WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    self._ws = ws
                    self._reconnect_delay = 1
                    
                    await self._subscribe()
                    ping_task = asyncio.create_task(self._ping_loop())
                    
                    try:
                        await self._message_loop()
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
            
            except Exception as e:
                if not self._running:
                    break
                logger.error(f"WebSocket error: {e}")
                await self._reconnect()
    
    async def close(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
    
    async def _subscribe(self) -> None:
        for symbol in self.states.keys():
            ws_symbol = self._to_contract_symbol(symbol)
            
            # Subscribe to depth (orderbook)
            depth_sub = {"method": "sub.depth", "param": {"symbol": ws_symbol}}
            await self._ws.send(json.dumps(depth_sub))
            
            # Subscribe to ticker for volume
            ticker_sub = {"method": "sub.ticker", "param": {"symbol": ws_symbol}}
            await self._ws.send(json.dumps(ticker_sub))
            
            logger.info(f"Subscribed to {symbol} (contract: {ws_symbol})")
        
        await asyncio.sleep(0.5)
    
    async def _message_loop(self) -> None:
        async for message in self._ws:
            try:
                data = json.loads(message)
                await self._handle_message(data)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.error(f"Message error: {e}")
    
    async def _handle_message(self, data: dict) -> None:
        channel = data.get("channel", "")
        ws_symbol = data.get("symbol", "")
        
        symbol = self._from_contract_symbol(ws_symbol)
        if not symbol:
            return
        
        state = self.states.get(symbol)
        if not state:
            return
        
        now = time.time()
        
        if channel == "push.depth.full":
            # Full orderbook snapshot
            d = data.get("data", {})
            bids = d.get("bids", [])
            asks = d.get("asks", [])
            state.update_orderbook_full(bids, asks, now)
            
        elif channel == "push.depth":
            # Incremental update
            d = data.get("data", {})
            bids = d.get("bids", [])
            asks = d.get("asks", [])
            state.update_orderbook_delta(bids, asks, now)
            
        elif channel == "push.ticker":
            # Ticker with volume
            d = data.get("data", {})
            volume = float(d.get("volume24", 0) or d.get("amount24", 0))
            if volume > 0:
                state.update_volume(volume, now)
    
    async def _ping_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.settings.ws_ping_interval)
                if self._ws:
                    await self._ws.send(json.dumps({"method": "ping"}))
            except Exception:
                break
    
    async def _reconnect(self) -> None:
        delay = min(self._reconnect_delay, self.settings.ws_reconnect_max_delay)
        logger.info(f"Reconnecting in {delay}s...")
        await asyncio.sleep(delay)
        self._reconnect_delay = min(self._reconnect_delay * 2, self.settings.ws_reconnect_max_delay)


# =============================================================================
# TRADING ENGINE
# =============================================================================

class TradingEngine:
    def __init__(self, settings: Settings, states: dict[str, SymbolState], 
                 exchange: ExchangeClient, db: DatabaseManager):
        self.settings = settings
        self.states = states
        self.exchange = exchange
        self.db = db
        self._running = False
        self._position_counter = 0
    
    async def start(self) -> None:
        self._running = True
        signal_task = asyncio.create_task(self._signal_loop())
        position_task = asyncio.create_task(self._position_loop())
        try:
            await asyncio.gather(signal_task, position_task)
        except asyncio.CancelledError:
            pass
    
    async def stop(self) -> None:
        self._running = False
    
    async def _signal_loop(self) -> None:
        while self._running:
            try:
                now = time.time()
                for symbol, state in self.states.items():
                    metrics = state.compute_signal(now)
                    if metrics.buy_signal:
                        await self._open_position(state, Side.BUY, metrics)
                    elif metrics.sell_signal:
                        await self._open_position(state, Side.SELL, metrics)
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Signal loop error: {e}")
                await asyncio.sleep(1)
    
    async def _position_loop(self) -> None:
        while self._running:
            try:
                now = time.time()
                for symbol, state in self.states.items():
                    to_close = []
                    for pos_id, pos in state.positions.items():
                        reason = self._check_exit(state, pos, now)
                        if reason:
                            to_close.append((pos_id, reason))
                    for pos_id, reason in to_close:
                        await self._close_position(state, pos_id, reason)
                await asyncio.sleep(0.02)
            except Exception as e:
                logger.error(f"Position loop error: {e}")
                await asyncio.sleep(1)
    
    def _check_exit(self, state: SymbolState, pos: Position, now: float) -> str | None:
        price = state.orderbook.mid_price
        if price == 0:
            return None

        elapsed = now - pos.entry_time
        if elapsed >= self.settings.max_position_lifetime:
            return "TIMEOUT"

        # TRAILING STOP LOGIC
        if self.settings.use_trailing_stop and pos.side == Side.BUY:
            # Update highest price
            if price > pos.highest_price:
                pos.highest_price = price
                # Update trailing stop
                trail_pct = self.settings.trailing_stop_pct / 100
                pos.trailing_sl = price * (1 - trail_pct)
                # Only update if trailing SL is higher than original SL
                if pos.trailing_sl > pos.sl_price:
                    pos.sl_price = pos.trailing_sl

        if pos.side == Side.BUY:
            if price >= pos.tp_price:
                return "TAKE_PROFIT"
            if price <= pos.sl_price:
                return "STOP_LOSS" if pos.trailing_sl == 0 else "TRAILING_STOP"
        else:
            if price <= pos.tp_price:
                return "TAKE_PROFIT"
            if price >= pos.sl_price:
                return "STOP_LOSS"

        return None
    
    async def _open_position(self, state: SymbolState, side: Side, metrics: SignalMetrics) -> None:
        symbol = state.symbol
        now = time.time()
        ob = state.orderbook

        if ob.best_bid == 0 or ob.best_ask == 0:
            return

        tick = state.tick_size
        entry_price = ob.best_ask + tick if side == Side.BUY else ob.best_bid - tick

        balance = 10000.0 if self.settings.dry_run else await self.exchange.get_usdt_balance()
        risk_usd = balance * (self.settings.risk_per_trade_pct / 100)
        quantity = self.exchange.quantize_amount(symbol, risk_usd / entry_price)

        tp_pct = self.settings.get_tp_pct(symbol) / 100
        sl_pct = self.settings.sl_pct / 100

        if side == Side.BUY:
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else:
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        tp_price = self.exchange.quantize_price(symbol, tp_price)
        sl_price = self.exchange.quantize_price(symbol, sl_price)

        self._position_counter += 1
        pos_id = f"{symbol}_{side.value}_{self._position_counter}"

        # Capture market conditions at entry
        bid_depth = sum(size for _, size in ob.bids)
        ask_depth = sum(size for _, size in ob.asks)

        position = Position(
            id=pos_id, symbol=symbol, side=side, entry_price=entry_price,
            quantity=quantity, tp_price=tp_price, sl_price=sl_price,
            entry_time=now, imbalance=metrics.weighted_imbalance,
            delta_sigma=metrics.delta_sigma, speed_ratio=metrics.speed_ratio,
            volume_ratio=metrics.volume_ratio, microprice_pct=metrics.microprice_change_pct,
            confidence_score=metrics.confidence_score,
            spread_at_entry=ob.spread, bid_depth=bid_depth, ask_depth=ask_depth,
            mid_price=ob.mid_price,
            highest_price=entry_price,  # Initialize for trailing stop
        )

        # UPDATE RATE LIMIT TIMESTAMP
        state.last_trade_time = now
        state.total_trades += 1

        if self.settings.dry_run:
            logger.info(
                f"[DRY_RUN] [{symbol}] OPEN {side.value} {quantity:.6f} @ {entry_price:.2f} "
                f"| TP: {tp_price:.2f} | SL: {sl_price:.2f} | Imb: {metrics.weighted_imbalance:.2f} "
                f"| Conf: {metrics.confidence_score:.0f}"
            )
        else:
            order = await self.exchange.place_limit_order(symbol, side, quantity, entry_price)
            if order:
                position.order_id = order.get("id")
            else:
                return

        state.positions[pos_id] = position
        await self.db.log_signal(position, None, None, None, None, None, self.settings.dry_run)
    
    async def _close_position(self, state: SymbolState, pos_id: str, reason: str) -> None:
        if pos_id not in state.positions:
            return

        position = state.positions[pos_id]
        now = time.time()
        exit_price = state.orderbook.mid_price

        if position.side == Side.BUY:
            pnl_pct = ((exit_price - position.entry_price) / position.entry_price) * 100
        else:
            pnl_pct = ((position.entry_price - exit_price) / position.entry_price) * 100

        pnl_usd = position.quantity * position.entry_price * (pnl_pct / 100)
        duration_ms = int((now - position.entry_time) * 1000)

        # TRACK CONSECUTIVE LOSSES FOR COOLDOWN
        if pnl_usd > 0:
            state.consecutive_losses = 0  # Reset on win
            state.winning_trades += 1
        else:
            state.consecutive_losses += 1
            # Trigger cooldown after X consecutive losses
            if state.consecutive_losses >= self.settings.cooldown_losses:
                state.cooldown_until = now + (self.settings.cooldown_minutes * 60)
                logger.warning(
                    f"[{position.symbol}] COOLDOWN TRIGGERED - {state.consecutive_losses} losses | "
                    f"Pausing for {self.settings.cooldown_minutes} min"
                )
                state.consecutive_losses = 0  # Reset after triggering

        if self.settings.dry_run:
            logger.info(
                f"[DRY_RUN] [{position.symbol}] CLOSE {position.side.value} @ {exit_price:.2f} "
                f"| {reason} | PnL: ${pnl_usd:+.2f} ({pnl_pct:+.3f}%) | {duration_ms}ms"
            )
        else:
            close_side = Side.SELL if position.side == Side.BUY else Side.BUY
            await self.exchange.place_market_order(position.symbol, close_side, position.quantity)

        await self.db.log_signal(position, exit_price, reason, pnl_usd, pnl_pct, duration_ms, self.settings.dry_run)
        del state.positions[pos_id]


# =============================================================================
# HEALTH SERVER
# =============================================================================

class HealthServer:
    def __init__(self, settings: Settings, states: dict[str, SymbolState], port: int = 8080):
        self.settings = settings
        self.states = states
        self.port = port
        self._start_time = time.time()
    
    async def start(self) -> None:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import threading
        
        parent = self
        
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            
            def do_GET(self):
                if self.path == "/health":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status":"ok"}')
                elif self.path == "/debug":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(parent._debug_info(), indent=2).encode())
                else:
                    self.send_response(404)
                    self.end_headers()
        
        server = HTTPServer(("0.0.0.0", self.port), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"Health server on port {self.port}")
    
    def _debug_info(self) -> dict:
        now = time.time()
        current_hour = datetime.now(timezone.utc).hour
        symbols = {}
        for symbol, state in self.states.items():
            ob = state.orderbook
            metrics = state.compute_signal(now)
            symbols[symbol] = {
                "orderbook": {
                    "best_bid": ob.best_bid,
                    "best_ask": ob.best_ask,
                    "spread": round(ob.spread, 4),
                    "spread_pct": round(metrics.spread_pct, 4),
                    "mid_price": round(ob.mid_price, 2),
                    "levels": len(ob.bids),
                },
                "metrics": metrics.to_dict(),
                "positions": len(state.positions),
                "pro_filters": {
                    "rate_limited": metrics.rate_limited,
                    "in_cooldown": metrics.in_cooldown,
                    "outside_hours": metrics.outside_hours,
                    "spread_too_wide": metrics.spread_too_wide,
                    "consecutive_losses": state.consecutive_losses,
                    "last_trade_ago": round(now - state.last_trade_time, 1) if state.last_trade_time > 0 else None,
                    "total_trades": state.total_trades,
                    "winning_trades": state.winning_trades,
                },
            }
        return {
            "version": self.settings.bot_version,
            "mode": "DRY_RUN" if self.settings.dry_run else "LIVE",
            "uptime": round(now - self._start_time, 1),
            "current_hour_utc": current_hour,
            "trading_hours": f"{self.settings.trading_hours_start}:00-{self.settings.trading_hours_end}:00 UTC",
            "symbols": symbols,
        }


# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:
    bot_dir = Path(__file__).parent.parent
    os.chdir(bot_dir)
    
    settings = Settings()
    
    # Logging
    logger.remove()
    logger.add(sys.stdout, format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>", level=settings.log_level)
    logger.add(bot_dir / "bot.log", rotation=settings.log_rotation, retention=settings.log_retention, level=settings.log_level)
    
    mode = "DRY_RUN" if settings.dry_run else "LIVE"
    logger.info("=" * 50)
    logger.info(f"HFT Imbalance Bot v{settings.bot_version} [{mode}]")
    logger.info(f"Symbols: {settings.symbol_list}")
    logger.info("=" * 50)
    
    db = DatabaseManager(str(bot_dir / "signals.db"))
    await db.connect()
    
    exchange = ExchangeClient(settings)
    if not settings.dry_run:
        await exchange.connect()
        logger.info(f"Balance: ${await exchange.get_usdt_balance():.2f}")
    else:
        exchange.exchange = ccxt.mexc()
        await exchange.exchange.load_markets()
    
    states: dict[str, SymbolState] = {}
    for symbol in settings.symbol_list:
        state = SymbolState(symbol, settings)
        info = exchange.get_market_info(symbol)
        state.tick_size = info.get("tick_size", 0.01)
        state.step_size = info.get("step_size", 0.0001)
        states[symbol] = state
        logger.info(f"[{symbol}] tick={state.tick_size}, step={state.step_size}")
    
    engine = TradingEngine(settings, states, exchange, db)
    ws = WebSocketManager(settings, states)
    health = HealthServer(settings, states)
    await health.start()
    
    stop = asyncio.Event()
    
    def handle_sig(sig, frame):
        logger.info("Shutting down...")
        stop.set()
    
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)
    
    ws_task = asyncio.create_task(ws.connect())
    engine_task = asyncio.create_task(engine.start())
    
    await stop.wait()
    
    await engine.stop()
    await ws.close()
    await exchange.close()
    await db.close()
    
    ws_task.cancel()
    engine_task.cancel()
    
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
