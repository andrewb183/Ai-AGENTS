from datetime import datetime, timezone, timedelta
import fcntl
import json
import math
import os
import sqlite3
import tempfile
import threading
import time
import traceback
from collections import defaultdict
import argparse
# --- Load .env variables early ---
try:
    from dotenv import load_dotenv
    # Keep explicit process environment values (for safe test toggles) over .env defaults.
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'), override=False)
    print("[startup] .env loaded via python-dotenv.")
except Exception as e:
    print(f"[startup][WARNING] Could not load .env: {e}")

print("[startup] trader.py script loaded and starting execution...")
# --- Constants and File Paths ---
def _env_float(name, default):
    """Auto-generated docstring."""
    try:
        raw = os.environ.get(name)
        if raw is None:
            raw = default
        return float(raw)
    except Exception:
        return float(default)

STATE_DIR = os.path.dirname(__file__)
POSITIONS_FILE = os.path.join(STATE_DIR, "positions.json")
TRADES_FILE = os.path.join(STATE_DIR, "trades.json")
QUEUE_FILE = os.path.join(STATE_DIR, "trading_queue.json")
STATUS_FILE = os.path.join(STATE_DIR, "trader_status.json")
LOCK_FILE = os.path.join(STATE_DIR, "trader.lock")
AIRDROP_TRIGGER_THRESHOLD_USD = 1.0
AIRDROP_TRIGGER_POLL_MIN = 60
MIN_TRADE_USD = max(0.0001, min(_env_float("MIN_TRADE_USD", "0.01"), 0.01))
MAX_TRADE_USD = float(os.environ.get("MAX_TRADE_USD", "12"))
KRAKEN_MIN_TRADE_USD = max(0.0001, min(_env_float("KRAKEN_MIN_TRADE_USD", "0.01"), 0.01))
COINBASE_MIN_TRADE_USD = max(0.0001, min(_env_float("COINBASE_MIN_TRADE_USD", "0.01"), 0.01))
TRADE_PAIRS = tuple(
    p.strip().upper() for p in os.environ.get("TRADE_PAIRS", "").split(",") if p.strip()
)
PAIR_QUOTES = tuple(
    q.strip().upper() for q in os.environ.get("PAIR_QUOTES", "USDT,USD,USDC").split(",") if q.strip()
)
DISCOVER_ALL_PAIRS = os.environ.get("DISCOVER_ALL_PAIRS", "true").lower() in ("1", "true", "yes", "on")
MAX_PAIRS_PER_EXCHANGE = int(os.environ.get("MAX_PAIRS_PER_EXCHANGE", "75"))
MAX_TRADES_PER_EXCHANGE_CYCLE = int(os.environ.get("MAX_TRADES_PER_EXCHANGE_CYCLE", "25"))
COIN_SELL_RESERVE_PCT = float(os.environ.get("COIN_SELL_RESERVE_PCT", "0.75"))
SELL_NOTIONAL_PCT = float(os.environ.get("SELL_NOTIONAL_PCT", "0.10"))
CYCLE_SLEEP_SECONDS = int(os.environ.get("CYCLE_SLEEP_SECONDS", "30"))
DAILY_LOSS_CAP_USD = float(os.environ.get("DAILY_LOSS_CAP_USD", "50"))
DAILY_PROFIT_TARGET_USD = float(os.environ.get("DAILY_PROFIT_TARGET_USD", "500"))
DAILY_TRADE_TARGET = int(os.environ.get("DAILY_TRADE_TARGET", "250"))
SELL_PROFIT_PCT = float(os.environ.get("SELL_PROFIT_PCT", "0.50"))
SELL_STOP_LOSS_PCT = float(os.environ.get("SELL_STOP_LOSS_PCT", "-1.50"))
RISK_CAP_EMAIL_ALERT = os.environ.get("RISK_CAP_EMAIL_ALERT", "1") != "0"
RISK_TRACKING_FILE = os.path.join(STATE_DIR, "daily_risk_tracking.json")
ANALYTICS_DB_FILE = os.path.join(STATE_DIR, "trader_analytics.db")
VOLATILITY_LOOKBACK = int(os.environ.get("VOLATILITY_LOOKBACK", "24"))
VOLATILITY_SPIKE_MULTIPLIER = float(os.environ.get("VOLATILITY_SPIKE_MULTIPLIER", "1.05"))
VOLATILITY_MIN_PCT = float(os.environ.get("VOLATILITY_MIN_PCT", "0.05"))
ADAPTIVE_VOLATILITY_RELAX = os.environ.get("ADAPTIVE_VOLATILITY_RELAX", "1").lower() in ("1", "true", "yes", "on")
RELAX_AFTER_ZERO_TRADE_STREAK = int(os.environ.get("RELAX_AFTER_ZERO_TRADE_STREAK", "1"))
RELAX_MAX_STEPS = int(os.environ.get("RELAX_MAX_STEPS", "4"))
RELAX_STEP_PCT = float(os.environ.get("RELAX_STEP_PCT", "0.15"))
ALLOW_MOMENTUM_BUY_FALLBACK = os.environ.get("ALLOW_MOMENTUM_BUY_FALLBACK", "1").lower() in ("1", "true", "yes", "on")
MOMENTUM_BUY_CHANGE_PCT = float(os.environ.get("MOMENTUM_BUY_CHANGE_PCT", "0.10"))
ENABLE_DCA_ENTRY_FALLBACK = os.environ.get("ENABLE_DCA_ENTRY_FALLBACK", "1").lower() in ("1", "true", "yes", "on")
DCA_AFTER_ZERO_TRADE_STREAK = int(os.environ.get("DCA_AFTER_ZERO_TRADE_STREAK", "1"))
DCA_MAX_BUYS_PER_EXCHANGE_CYCLE = int(os.environ.get("DCA_MAX_BUYS_PER_EXCHANGE_CYCLE", "5"))
DCA_MIN_CHANGE_PCT = float(os.environ.get("DCA_MIN_CHANGE_PCT", "-1.00"))
SELL_ALL_AFTER_ZERO_TRADE_STREAK = int(os.environ.get("SELL_ALL_AFTER_ZERO_TRADE_STREAK", "1"))
SELL_ALL_RESERVE_PCT = float(os.environ.get("SELL_ALL_RESERVE_PCT", "0.00"))
SPREAD_ARB_THRESHOLD_PCT = float(os.environ.get("SPREAD_ARB_THRESHOLD_PCT", "0.80"))
SPREAD_ARB_MAX_PAIRS = int(os.environ.get("SPREAD_ARB_MAX_PAIRS", "20"))
MONTHLY_DRAWDOWN_ALERT_PCT = float(os.environ.get("MONTHLY_DRAWDOWN_ALERT_PCT", "10.0"))
EXCHANGE_FAILURE_ALERT_COOLDOWN_SECONDS = int(os.environ.get("EXCHANGE_FAILURE_ALERT_COOLDOWN_SECONDS", "1800"))
TRADER_TIMEFRAME = os.environ.get("TRADER_TIMEFRAME", "5m")
MICRO_TRADE_MODE = os.environ.get("MICRO_TRADE_MODE", "1").lower() in ("1", "true", "yes", "on")
MICRO_KRAKEN_MAX_MIN_COST_USD = float(os.environ.get("MICRO_KRAKEN_MAX_MIN_COST_USD", "0.50"))
MICRO_COINBASE_MIN_QUOTE_USD = max(0.0001, min(_env_float("MICRO_COINBASE_MIN_QUOTE_USD", "0.01"), 0.01))
TREND_MODEL_METADATA_FILE = os.path.join(STATE_DIR, "trend_model_metadata.json")
WEEKLY_TREND_LOOKBACK_DAYS = int(os.environ.get("WEEKLY_TREND_LOOKBACK_DAYS", "14"))
WEEKLY_TREND_MIN_TOTAL_TRADES = int(os.environ.get("WEEKLY_TREND_MIN_TOTAL_TRADES", "5"))
WEEKLY_TREND_MIN_TRADES_PER_PAIR = int(os.environ.get("WEEKLY_TREND_MIN_TRADES_PER_PAIR", "2"))
TREND_FRESH_MAX_DAYS = int(os.environ.get("TREND_FRESH_MAX_DAYS", "8"))
TREND_SCAN_PRIORITY_MIN_STRENGTH = float(os.environ.get("TREND_SCAN_PRIORITY_MIN_STRENGTH", "0.45"))
TREND_ENABLE_AGGRESSIVE_ENTRY = os.environ.get("TREND_ENABLE_AGGRESSIVE_ENTRY", "1").lower() in ("1", "true", "yes", "on")
TREND_STRONG_BUY_STRENGTH = float(os.environ.get("TREND_STRONG_BUY_STRENGTH", "0.50"))
TREND_FALLBACK_MIN_CHANGE_PCT = float(os.environ.get("TREND_FALLBACK_MIN_CHANGE_PCT", "-0.60"))
TREND_UP_AVG_PNL_PCT = float(os.environ.get("TREND_UP_AVG_PNL_PCT", "0.15"))
TREND_UP_WIN_RATE = float(os.environ.get("TREND_UP_WIN_RATE", "0.58"))
TREND_DOWN_AVG_PNL_PCT = float(os.environ.get("TREND_DOWN_AVG_PNL_PCT", "-0.15"))
TREND_DOWN_WIN_RATE = float(os.environ.get("TREND_DOWN_WIN_RATE", "0.45"))
TREND_SKIP_VOLATILITY_FOR_BUY_SIGNALS = os.environ.get("TREND_SKIP_VOLATILITY_FOR_BUY_SIGNALS", "1").lower() in ("1", "true", "yes", "on")
TREND_BACKFILL_TARGET_PAIRS_PER_RUN = int(os.environ.get("TREND_BACKFILL_TARGET_PAIRS_PER_RUN", "50"))
TREND_OHLCV_TIMEFRAME = os.environ.get("TREND_OHLCV_TIMEFRAME", "1d")
TREND_OHLCV_LIMIT = int(os.environ.get("TREND_OHLCV_LIMIT", "365"))
NO_TRADE_ALERT_AFTER_CYCLES = int(os.environ.get("NO_TRADE_ALERT_AFTER_CYCLES", "5"))

# --- Margin Trading Configuration ---
ENABLE_MARGIN_TRADING = os.environ.get("ENABLE_MARGIN_TRADING", "1").lower() in ("1", "true", "yes", "on")
MARGIN_EXCHANGES = tuple(
    ex.strip().lower() for ex in os.environ.get("MARGIN_EXCHANGES", "kraken").split(",") if ex.strip()
)
MARGIN_MODE = os.environ.get("MARGIN_MODE", "cross").lower()
KRAKEN_MARGIN_MAX_LEVERAGE = int(os.environ.get("KRAKEN_MARGIN_MAX_LEVERAGE", "4"))
KRAKEN_MARGIN_DEFAULT_LEVERAGE = int(os.environ.get("KRAKEN_MARGIN_DEFAULT_LEVERAGE", "2"))
MARGIN_MIN_EDGE_SCORE = float(os.environ.get("MARGIN_MIN_EDGE_SCORE", "0.55"))
MARGIN_MAX_NOTIONAL_USD = float(os.environ.get("MARGIN_MAX_NOTIONAL_USD", "100"))
MARGIN_REQUIREMENT_BUFFER_PCT = float(os.environ.get("MARGIN_REQUIREMENT_BUFFER_PCT", "2.0"))
MARGIN_LIQUIDATION_DISTANCE_MIN_PCT = float(os.environ.get("MARGIN_LIQUIDATION_DISTANCE_MIN_PCT", "5.0"))
MAX_MARGIN_TRADES_PER_CYCLE = int(os.environ.get("MAX_MARGIN_TRADES_PER_CYCLE", "3"))
MARGIN_ALLOWED_PAIRS = tuple(
    pair.strip().upper() for pair in os.environ.get("MARGIN_ALLOWED_PAIRS", "").split(",") if pair.strip()
) if os.environ.get("MARGIN_ALLOWED_PAIRS") else None

_ALERT_CACHE = {}
_INSTANCE_LOCK_FH = None

# --- Helper Functions ---
def _load_json(path, default):
    """Auto-generated docstring."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default

def _write_json(path, data):
    """Auto-generated docstring."""
    with open(path, "w") as fh:
        if isinstance(data, (dict, list)):
            json.dump(data, fh, indent=2)
        else:
            fh.write(str(data))

def _write_atomic_json(path, data):
    """Auto-generated docstring."""
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=parent, delete=False) as tmp:
            json.dump(data, tmp, indent=2)
            tmp.flush()
            os.fsync(tmp.fileno())
            temp_path = tmp.name
        os.replace(temp_path, path)
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception:
                pass

def _safe_float(value, default=0.0):
    """Auto-generated docstring."""
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)

def _is_dry_run():
    """Auto-generated docstring."""
    return os.environ.get("DRY_RUN", "true").lower() in ("1", "true", "yes", "on")

def _quote_assets():
    """Auto-generated docstring."""
    return ("USDT", "USD", "USDC", "DAI", "FDUSD")

def _load_exchange(name):
    """Auto-generated docstring."""
    import ccxt
    if name == "kraken":
        return ccxt.kraken({
            "apiKey": os.environ.get("KRAKEN_API_KEY", "").strip(),
            "secret": os.environ.get("KRAKEN_SECRET", "").strip(),
            "enableRateLimit": True,
        })
    if name == "coinbase":
        return ccxt.coinbase({
            "apiKey": os.environ.get("COINBASE_API_KEY", "").strip(),
            "secret": os.environ.get("COINBASE_SECRET", "").strip(),
            "enableRateLimit": True,
        })
    raise ValueError(f"Unsupported exchange: {name}")

def _exchange_floor_usd(exchange_name):
    """Auto-generated docstring."""
    if exchange_name == "kraken":
        return max(0.01, KRAKEN_MIN_TRADE_USD)
    if exchange_name == "coinbase":
        return max(0.01, COINBASE_MIN_TRADE_USD)
    return max(0.01, MIN_TRADE_USD)

def _market_min_cost(ex, pair):
    """Auto-generated docstring."""
    try:
        market = ex.market(pair)
        return _safe_float((market.get("limits", {}) or {}).get("cost", {}).get("min"), 0.0)
    except Exception:
        return 0.0

def _market_min_amount(ex, pair):
    """Auto-generated docstring."""
    try:
        market = ex.market(pair)
        return _safe_float((market.get("limits", {}) or {}).get("amount", {}).get("min"), 0.0)
    except Exception:
        return 0.0

def _adaptive_trade_notional(ex, pair, quote_balance, exchange_name, price=None):
    """Auto-generated docstring."""
    floor_usd = _exchange_floor_usd(exchange_name)
    min_cost = _market_min_cost(ex, pair)
    min_amount = _market_min_amount(ex, pair)
    min_notional_from_amount = 0.0
    ref_price = _safe_float(price, 0.0)
    if min_amount > 0 and ref_price > 0:
        min_notional_from_amount = min_amount * ref_price

    # Use the smallest exchange-valid notional instead of forcing a larger percentage-based size.
    target = max(floor_usd, min_cost, min_notional_from_amount)
    target = min(MAX_TRADE_USD, target)
    return min(target, _safe_float(quote_balance, 0.0)), min_cost

def _prioritize_pairs_with_quote_balance(pairs, balance, exchange_name):
    """Auto-generated docstring."""
    totals = (balance or {}).get("total", {}) or {}
    floor_usd = _exchange_floor_usd(exchange_name)

    def _rank(pair):
        """Auto-generated docstring."""
        try:
            _, quote_asset = pair.split("/")
        except ValueError:
            return (2, pair)
        quote_balance = _safe_float(totals.get(quote_asset, 0.0), 0.0)
        if quote_balance >= floor_usd:
            return (0, pair)
        if quote_balance > 0:
            return (1, pair)
        return (2, pair)

    return sorted(pairs, key=_rank)

def _quote_liquidity_usd(balance):
    """Auto-generated docstring."""
    totals = (balance or {}).get("total", {}) or {}
    return (
        _safe_float(totals.get("USD"), 0.0)
        + _safe_float(totals.get("USDT"), 0.0)
        + _safe_float(totals.get("USDC"), 0.0)
    )

def _filter_micro_pairs_for_kraken(ex, pairs):
    """Auto-generated docstring."""
    filtered = []
    for pair in pairs:
        min_cost = _market_min_cost(ex, pair)
        if min_cost <= 0 or min_cost <= MICRO_KRAKEN_MAX_MIN_COST_USD:
            filtered.append(pair)
    return filtered

def _utc_now_iso():
    """Auto-generated docstring."""
    return datetime.now(timezone.utc).isoformat()

def _parse_iso_utc(value):
    """Auto-generated docstring."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

def _utc_week_key(dt_obj):
    """Auto-generated docstring."""
    iso = dt_obj.astimezone(timezone.utc).isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

def _load_trend_model_metadata():
    """Auto-generated docstring."""
    metadata = _load_json(TREND_MODEL_METADATA_FILE, {})
    if not isinstance(metadata, dict):
        metadata = {}
    metadata.setdefault("version", "1.0")
    metadata.setdefault("trends_by_pair", {})
    metadata.setdefault("training_stats", {})
    metadata.setdefault("backfill_complete", False)
    metadata.setdefault("backfill_pairs_processed", [])
    metadata.setdefault("backfill_sundays_done", 0)
    metadata.setdefault("cumulative_pair_stats", {})
    metadata.setdefault("last_trained_at", "")
    return metadata

def _save_trend_model_metadata(metadata):
    """Auto-generated docstring."""
    try:
        _write_atomic_json(TREND_MODEL_METADATA_FILE, metadata)
    except Exception as exc:
        print(f"[trend][WARN] Could not persist trend metadata: {exc}")

def _is_trend_model_fresh(metadata, now_utc=None):
    """Auto-generated docstring."""
    now_utc = now_utc or datetime.now(timezone.utc)
    if not isinstance(metadata, dict):
        return False
    trained_at = _parse_iso_utc(metadata.get("trained_at"))
    if not trained_at:
        return False
    age = now_utc - trained_at
    return timedelta(0) <= age <= timedelta(days=max(1, TREND_FRESH_MAX_DAYS))

def _should_run_weekly_trend_training(now_utc, metadata):
    """Auto-generated docstring."""
    if now_utc.weekday() != 6:
        return False
    current_week = _utc_week_key(now_utc)
    last_week = ""
    if isinstance(metadata, dict):
        last_week = str(metadata.get("last_trained_week_key", "") or "")
    return last_week != current_week

def _prioritize_pairs_with_trend(pairs, metadata):
    """Auto-generated docstring."""
    if not pairs:
        return pairs
    trends = (metadata or {}).get("trends_by_pair", {})
    if not isinstance(trends, dict) or not trends:
        return pairs

    def _rank(pair):
        """Auto-generated docstring."""
        trend_info = trends.get(pair) or {}
        direction = str(trend_info.get("trend_direction", "neutral")).lower()
        signal = str(trend_info.get("momentum_signal", "neutral")).lower()
        strength = _safe_float(trend_info.get("trend_strength"), 0.0)
        if direction == "up" and strength >= TREND_SCAN_PRIORITY_MIN_STRENGTH:
            return (0, -strength, pair)
        if signal in ("strong_buy", "buy"):
            return (1, -strength, pair)
        if direction == "neutral":
            return (2, -strength, pair)
        if direction == "down":
            return (3, strength, pair)
        return (2, -strength, pair)

    return sorted(pairs, key=_rank)

def _recompute_trends_from_stats(pair_stats, now_utc):
    """Compute trends_by_pair from a cumulative pair_stats dict.
    pair_stats: {pair: {samples, sum_pnl, wins, losses, buy_count, sell_count}}
    Returns trends_by_pair dict suitable for metadata storage."""
    trends_by_pair = {}
    for pair_text, stats in pair_stats.items():
        samples = int(_safe_float(stats.get("samples"), 0))
        if samples < max(1, WEEKLY_TREND_MIN_TRADES_PER_PAIR):
            continue
        avg_pnl = _safe_float(stats.get("sum_pnl"), 0.0) / max(1, samples)
        win_rate = _safe_float(stats.get("wins"), 0.0) / max(1, samples)

        if avg_pnl >= TREND_UP_AVG_PNL_PCT or win_rate >= TREND_UP_WIN_RATE:
            direction = "up"
        elif avg_pnl <= TREND_DOWN_AVG_PNL_PCT and win_rate <= TREND_DOWN_WIN_RATE:
            direction = "down"
        else:
            direction = "neutral"

        sample_component = min(samples / 20.0, 1.0) * 0.35
        pnl_component = min(abs(avg_pnl) / 3.0, 1.0) * 0.35
        win_component = min(abs(win_rate - 0.5) / 0.5, 1.0) * 0.30
        strength = max(0.0, min(1.0, sample_component + pnl_component + win_component))

        if direction == "up" and strength >= TREND_STRONG_BUY_STRENGTH:
            momentum_signal = "strong_buy"
        elif direction == "up":
            momentum_signal = "buy"
        elif direction == "down" and strength >= TREND_STRONG_BUY_STRENGTH:
            momentum_signal = "strong_sell"
        elif direction == "down":
            momentum_signal = "sell"
        else:
            momentum_signal = "neutral"

        trends_by_pair[pair_text] = {
            "trend_direction": direction,
            "trend_strength": round(strength, 4),
            "avg_pnl_pct": round(avg_pnl, 4),
            "win_rate": round(win_rate, 4),
            "samples": samples,
            "buy_count": int(_safe_float(stats.get("buy_count"), 0)),
            "sell_count": int(_safe_float(stats.get("sell_count"), 0)),
            "momentum_signal": momentum_signal,
            "trained_at": now_utc.isoformat(),
        }
    return trends_by_pair

def _fetch_ohlcv_trend_for_pair(ex, pair, timeframe, limit):
    """Fetch OHLCV from exchange and compute a stat bucket compatible with cumulative_pair_stats.
    Returns dict {samples, sum_pnl, wins, losses, buy_count, sell_count} or None on failure."""
    try:
        candles = ex.fetch_ohlcv(pair, timeframe=timeframe, limit=limit)
    except Exception as exc:
        print(f"[trend][OHLCV][WARN] {pair}: fetch failed: {exc}")
        return None
    if not candles or len(candles) < 5:
        return None

    samples = 0
    sum_pnl = 0.0
    wins = 0
    losses = 0
    buy_count = 0  # candles with close > open (up-day)
    sell_count = 0  # candles with close <= open (down-day)

    for candle in candles:
        if len(candle) < 5:
            continue
        open_price = _safe_float(candle[1], 0.0)
        close_price = _safe_float(candle[4], 0.0)
        if open_price <= 0:
            continue
        daily_return_pct = ((close_price - open_price) / open_price) * 100.0
        samples += 1
        sum_pnl += daily_return_pct
        if daily_return_pct >= 0:
            wins += 1
            buy_count += 1
        else:
            losses += 1
            sell_count += 1

    if samples == 0:
        return None

    return {
        "samples": samples,
        "sum_pnl": sum_pnl,
        "wins": wins,
        "losses": losses,
        "buy_count": buy_count,
        "sell_count": sell_count,
    }

def _train_weekly_trend_model(now_utc, prior_metadata=None):
    """Incrementally train the trend model from exchange OHLCV data.

    Two modes:
      Path A (backfill_complete=False): each Sunday fetches OHLCV for the next
        TREND_BACKFILL_TARGET_PAIRS_PER_RUN pairs not yet processed, merging stats
        into cumulative_pair_stats.  Repeats each Sunday until all known pairs are done.
      Path B (backfill_complete=True): keeps up to date by fetching OHLCV for all
        known pairs (up to TREND_BACKFILL_TARGET_PAIRS_PER_RUN per run) each Sunday
        and merging fresh data into the cumulative stats.
    """
    training_note = "trained"
    metadata = prior_metadata if isinstance(prior_metadata, dict) else _load_trend_model_metadata()

    # --- Discover pairs from exchange clients ---
    all_known_pairs_set = set()
    primary_ex = None
    for ex_name in ("coinbase", "kraken"):
        try:
            ex_tmp = _load_exchange(ex_name)
            ex_tmp.load_markets()
            discovered = _discover_trade_pairs(ex_tmp)
            all_known_pairs_set.update(discovered)
            if primary_ex is None:
                primary_ex = ex_tmp
                print(f"[trend][INFO] Using {ex_name} OHLCV for trend training ({len(discovered)} pairs discovered)")
        except Exception as exc:
            print(f"[trend][WARN] Could not load {ex_name} for OHLCV discovery: {exc}")

    if primary_ex is None or not all_known_pairs_set:
        print("[trend][WARN] No exchange available for OHLCV training, falling back to SQLite only")
        return _train_weekly_trend_model_sqlite_fallback(now_utc, metadata), "sqlite_fallback"

    all_known_pairs = sorted(all_known_pairs_set)
    backfill_complete = bool(metadata.get("backfill_complete", False))
    backfill_pairs_processed = list(metadata.get("backfill_pairs_processed") or [])
    cumulative_pair_stats = dict(metadata.get("cumulative_pair_stats") or {})
    backfill_sundays_done = int(_safe_float(metadata.get("backfill_sundays_done"), 0))

    if not backfill_complete:
        # Path A: process next unseen chunk
        remaining = [p for p in all_known_pairs if p not in set(backfill_pairs_processed)]
        chunk = remaining[:max(1, TREND_BACKFILL_TARGET_PAIRS_PER_RUN)]
        print(
            f"[trend][BACKFILL] Sunday {backfill_sundays_done + 1}: "
            f"fetching OHLCV for {len(chunk)} pairs "
            f"({len(remaining)} remaining, {len(backfill_pairs_processed)} done)"
        )
        for pair in chunk:
            bucket = _fetch_ohlcv_trend_for_pair(primary_ex, pair, TREND_OHLCV_TIMEFRAME, TREND_OHLCV_LIMIT)
            if bucket is None:
                continue
            if pair in cumulative_pair_stats:
                existing = cumulative_pair_stats[pair]
                existing["samples"] = int(_safe_float(existing.get("samples"), 0)) + bucket["samples"]
                existing["sum_pnl"] = _safe_float(existing.get("sum_pnl"), 0.0) + bucket["sum_pnl"]
                existing["wins"] = int(_safe_float(existing.get("wins"), 0)) + bucket["wins"]
                existing["losses"] = int(_safe_float(existing.get("losses"), 0)) + bucket["losses"]
                existing["buy_count"] = int(_safe_float(existing.get("buy_count"), 0)) + bucket["buy_count"]
                existing["sell_count"] = int(_safe_float(existing.get("sell_count"), 0)) + bucket["sell_count"]
            else:
                cumulative_pair_stats[pair] = bucket

        backfill_pairs_processed.extend(chunk)
        backfill_sundays_done += 1

        still_remaining = [p for p in all_known_pairs if p not in set(backfill_pairs_processed)]
        if not still_remaining:
            backfill_complete = True
            print(
                f"[trend][BACKFILL] Complete! All {len(backfill_pairs_processed)} pairs processed "
                f"across {backfill_sundays_done} Sundays. Switching to keep-up-to-date mode."
            )
        else:
            print(
                f"[trend][BACKFILL] Progress: {len(backfill_pairs_processed)}/{len(all_known_pairs)} pairs done. "
                f"{len(still_remaining)} remaining ({len(still_remaining) // max(1, TREND_BACKFILL_TARGET_PAIRS_PER_RUN) + 1} Sundays left)"
            )

        training_note = f"backfill_sunday_{backfill_sundays_done}_pairs_{len(chunk)}"

    else:
        # Path B: keep up to date — fetch fresh OHLCV for known pairs and merge
        update_chunk = all_known_pairs[:max(1, TREND_BACKFILL_TARGET_PAIRS_PER_RUN)]
        print(f"[trend][UPDATE] Keep-up-to-date: refreshing OHLCV for {len(update_chunk)} pairs")
        for pair in update_chunk:
            bucket = _fetch_ohlcv_trend_for_pair(primary_ex, pair, TREND_OHLCV_TIMEFRAME, TREND_OHLCV_LIMIT)
            if bucket is None:
                continue
            if pair in cumulative_pair_stats:
                existing = cumulative_pair_stats[pair]
                existing["samples"] = int(_safe_float(existing.get("samples"), 0)) + bucket["samples"]
                existing["sum_pnl"] = _safe_float(existing.get("sum_pnl"), 0.0) + bucket["sum_pnl"]
                existing["wins"] = int(_safe_float(existing.get("wins"), 0)) + bucket["wins"]
                existing["losses"] = int(_safe_float(existing.get("losses"), 0)) + bucket["losses"]
                existing["buy_count"] = int(_safe_float(existing.get("buy_count"), 0)) + bucket["buy_count"]
                existing["sell_count"] = int(_safe_float(existing.get("sell_count"), 0)) + bucket["sell_count"]
            else:
                cumulative_pair_stats[pair] = bucket
        backfill_sundays_done += 1
        training_note = f"keep_up_to_date_sunday_{backfill_sundays_done}"

    # Recompute trends from accumulated stats
    trends_by_pair = _recompute_trends_from_stats(cumulative_pair_stats, now_utc)

    metadata.update({
        "version": "1.0",
        "trained_at": now_utc.isoformat(),
        "last_trained_at": now_utc.isoformat(),
        "last_trained_week_key": _utc_week_key(now_utc),
        "backfill_complete": backfill_complete,
        "backfill_pairs_processed": backfill_pairs_processed,
        "backfill_sundays_done": backfill_sundays_done,
        "cumulative_pair_stats": cumulative_pair_stats,
        "trends_by_pair": trends_by_pair,
        "training_stats": {
            "pairs_in_cumulative": len(cumulative_pair_stats),
            "pairs_with_trend": len(trends_by_pair),
            "total_known_pairs": len(all_known_pairs),
            "backfill_pairs_processed": len(backfill_pairs_processed),
        },
    })
    _save_trend_model_metadata(metadata)
    print(
        f"[trend][INFO] Training done: pairs_with_trend={len(trends_by_pair)} "
        f"cumulative_pairs={len(cumulative_pair_stats)} backfill_complete={backfill_complete} "
        f"note={training_note}"
    )
    return metadata, training_note

def _train_weekly_trend_model_sqlite_fallback(now_utc, metadata):
    """Fallback trainer using SQLite trade history when exchange OHLCV is unavailable."""
    cutoff = now_utc - timedelta(days=max(1, WEEKLY_TREND_LOOKBACK_DAYS))
    pair_stats = {}
    try:
        conn = _db_connection()
        trade_rows = conn.execute(
            "SELECT recorded_at, pair, pnl_pct, side, status FROM trade_events ORDER BY id DESC LIMIT 5000"
        ).fetchall()
        conn.close()
    except Exception as exc:
        print(f"[trend][WARN] SQLite fallback query failed: {exc}")
        return metadata

    for recorded_at, pair, pnl_pct, side, status in trade_rows:
        ts = _parse_iso_utc(recorded_at)
        if not ts or ts < cutoff:
            continue
        if str(status or "").lower() in ("rejected", "canceled", "cancelled", "error"):
            continue
        pair_text = str(pair or "").upper().strip()
        if not pair_text:
            continue
        bucket = pair_stats.setdefault(pair_text, {"samples": 0, "sum_pnl": 0.0, "wins": 0, "losses": 0, "buy_count": 0, "sell_count": 0})
        pnl_value = _safe_float(pnl_pct, 0.0)
        bucket["samples"] += 1
        bucket["sum_pnl"] += pnl_value
        if pnl_value >= 0:
            bucket["wins"] += 1
        else:
            bucket["losses"] += 1
        if str(side or "").lower() == "buy":
            bucket["buy_count"] += 1
        elif str(side or "").lower() == "sell":
            bucket["sell_count"] += 1

    cumulative = dict(metadata.get("cumulative_pair_stats") or {})
    for pair_text, bucket in pair_stats.items():
        if pair_text in cumulative:
            for k in ("samples", "wins", "losses", "buy_count", "sell_count"):
                cumulative[pair_text][k] = int(_safe_float(cumulative[pair_text].get(k), 0)) + bucket[k]
            cumulative[pair_text]["sum_pnl"] = _safe_float(cumulative[pair_text].get("sum_pnl"), 0.0) + bucket["sum_pnl"]
        else:
            cumulative[pair_text] = bucket

    trends_by_pair = _recompute_trends_from_stats(cumulative, now_utc)
    metadata.update({
        "trained_at": now_utc.isoformat(),
        "last_trained_at": now_utc.isoformat(),
        "last_trained_week_key": _utc_week_key(now_utc),
        "cumulative_pair_stats": cumulative,
        "trends_by_pair": trends_by_pair,
    })
    _save_trend_model_metadata(metadata)
    return metadata

def _margin_exchange_supports_pair(exchange_name, pair):
    """Check if exchange supports margin trading for a given pair.
    
    Uses MARGIN_ALLOWED_PAIRS env var if set, otherwise falls back to hardcoded list.
    Env var format: 'BTC/USD,BTC/USDT,ETH/USD,ETH/USDT' (comma-separated, case-insensitive)
    """
    if not ENABLE_MARGIN_TRADING:
        return False
    if exchange_name.lower() not in MARGIN_EXCHANGES:
        return False
    if exchange_name.lower() != "kraken":
        return False
    
    # Use env-configured pairs if provided, otherwise fall back to hardcoded list
    if MARGIN_ALLOWED_PAIRS is not None:
        return pair.upper() in MARGIN_ALLOWED_PAIRS
    
    # Fallback: hardcoded conservative list
    kraken_margin_pairs_default = {
        "BTC/USD", "BTC/USDT", "ETH/USD", "ETH/USDT",
        "ADA/USD", "ADA/USDT", "XRP/USD", "XRP/USDT",
        "SOL/USD", "SOL/USDT", "DOGE/USD", "DOGE/USDT",
    }
    return pair in kraken_margin_pairs_default

def _get_margin_leverage_for_exchange(exchange_name):
    """Return max leverage for exchange."""
    if exchange_name.lower() == "kraken":
        return KRAKEN_MARGIN_MAX_LEVERAGE
    return 1

def _compute_edge_score(trend_direction, trend_strength, trend_signal, volatility_ok, change_pct, fallback_type):
    """Compute a 0-1 edge score for routing to margin vs spot."""
    score = 0.0
    if trend_direction == "up" and trend_signal in ("strong_buy", "buy"):
        trend_component = 0.4 + (min(trend_strength, 1.0) * 0.1)
    elif trend_direction == "neutral":
        trend_component = 0.15
    else:
        trend_component = 0.0
    score += trend_component
    if volatility_ok:
        score += 0.15
    elif fallback_type in ("trend", "momentum"):
        score += 0.05
    if change_pct >= 0.5:
        score += 0.30
    elif change_pct >= 0.0:
        score += 0.15
    elif change_pct >= -0.5:
        score += 0.05
    return min(1.0, max(0.0, score))

def _should_use_margin_for_buy(edge_score, free_margin_usd, pair, exchange_name, notional_usd, leverage):
    """Determine if margin should be used for this BUY candidate."""
    if not ENABLE_MARGIN_TRADING:
        return False, "margin_disabled_globally"
    if edge_score < MARGIN_MIN_EDGE_SCORE:
        return False, f"edge_score={edge_score:.2f}<{MARGIN_MIN_EDGE_SCORE}"
    if not _margin_exchange_supports_pair(exchange_name, pair):
        return False, f"margin_not_supported_{exchange_name}_{pair}"
    if notional_usd > MARGIN_MAX_NOTIONAL_USD:
        return False, f"notional_${notional_usd:.2f}>{MARGIN_MAX_NOTIONAL_USD}"
    margin_required = (notional_usd / leverage) * (1.0 + MARGIN_REQUIREMENT_BUFFER_PCT / 100.0)
    if free_margin_usd < margin_required:
        return False, f"insufficient_margin:${free_margin_usd:.2f}<${margin_required:.2f}"
    leveraged_distance_pct = (1.0 - 1.0 / leverage) * 100.0
    if leveraged_distance_pct < MARGIN_LIQUIDATION_DISTANCE_MIN_PCT:
        return False, f"liquidation_too_close:distance={leveraged_distance_pct:.1f}%"
    return True, "margin_eligible"

def _should_send_alert(key, cooldown_seconds):
    """Auto-generated docstring."""
    now_ts = time.time()
    last_ts = _ALERT_CACHE.get(key, 0)
    if now_ts - last_ts < cooldown_seconds:
        return False
    _ALERT_CACHE[key] = now_ts
    return True

def _maybe_alert_no_trades(cycle_executed, zero_streak, volatility_gate_hits, volatility_factor, balances_by_exchange, exchanges):
    """Send an email explaining why no trades were made if streak >= NO_TRADE_ALERT_AFTER_CYCLES.
    Has a 6-hour cooldown to avoid spam."""
    if cycle_executed > 0:
        return
    if zero_streak < NO_TRADE_ALERT_AFTER_CYCLES:
        return
    if not _should_send_alert("no_trade_streak", 6 * 60 * 60):
        return

    balance_lines = []
    seen_exchanges = set()
    for ex_name, ex in (exchanges or []):
        seen_exchanges.add(ex_name)
        try:
            snapshot = _build_wallet_snapshot(ex)
            totals = _safe_float(snapshot.get("estimated_total_usd"), 0.0)
            entries = snapshot.get("balances", []) or []
            quote_liquidity = sum(
                _safe_float(item.get("estimated_usd"), 0.0)
                for item in entries
                if str(item.get("asset", "")).upper() in _quote_assets()
            )
            non_quote_assets = [
                item for item in entries
                if str(item.get("asset", "")).upper() not in _quote_assets() and _safe_float(item.get("amount"), 0.0) > 0
            ]
            non_quote_assets.sort(key=lambda i: _safe_float(i.get("estimated_usd"), 0.0), reverse=True)

            balance_lines.append(
                f"  {ex_name}: total_estimated_usd=${totals:.4f}, quote_liquidity=${quote_liquidity:.4f}"
            )
            for item in non_quote_assets[:8]:
                asset = str(item.get("asset", ""))
                amount = _safe_float(item.get("amount"), 0.0)
                est = item.get("estimated_usd")
                est_text = f"${_safe_float(est, 0.0):.4f}" if est is not None else "unpriced"
                balance_lines.append(f"    {asset}: amount={amount:.8f}, est_usd={est_text}")
        except Exception as exc:
            balance_lines.append(f"  {ex_name}: wallet snapshot unavailable ({exc})")

    # Fallback to already-fetched balances for any exchange we could not snapshot.
    for ex_name, bal in (balances_by_exchange or {}).items():
        if ex_name in seen_exchanges:
            continue
        totals = (bal or {}).get("total", {}) or {}
        quote_total = (
            _safe_float(totals.get("USD"), 0.0)
            + _safe_float(totals.get("USDT"), 0.0)
            + _safe_float(totals.get("USDC"), 0.0)
        )
        balance_lines.append(f"  {ex_name}: quote_liquidity=${quote_total:.4f} (fallback totals)")

    balance_text = "\n".join(balance_lines) if balance_lines else "  (no wallet balances detected)"

    reasons = []
    if volatility_gate_hits == 0:
        reasons.append(
            f"- Volatility gate: ALL scanned pairs failed the volatility check "
            f"(VOLATILITY_MIN_PCT={VOLATILITY_MIN_PCT:.2f}%, current relaxation factor={volatility_factor:.2f}x). "
            f"The market may be unusually quiet. Consider lowering VOLATILITY_MIN_PCT or VOLATILITY_SPIKE_MULTIPLIER."
        )
    else:
        reasons.append(
            f"- Volatility gate: {volatility_gate_hits} pairs passed volatility but none converted to trades. "
            f"Check min-cost/min-amount limits or balance levels."
        )
    if zero_streak >= RELAX_AFTER_ZERO_TRADE_STREAK:
        reasons.append(
            f"- Adaptive relaxation is active (streak={zero_streak}). "
            f"Threshold factor already at {volatility_factor:.2f}x (floor 0.25x)."
        )
    reasons.append(
        f"- Current thresholds: trend_strong_buy={TREND_STRONG_BUY_STRENGTH}, "
        f"trend_fallback_change_floor={TREND_FALLBACK_MIN_CHANGE_PCT}%, "
        f"momentum_buy_change={MOMENTUM_BUY_CHANGE_PCT}%, dca_min_change={DCA_MIN_CHANGE_PCT}%"
    )
    reasons.append(
        f"- Wallet conversion fallback: after streak >= {SELL_ALL_AFTER_ZERO_TRADE_STREAK}, "
        f"bot may sell non-quote holdings with reserve {SELL_ALL_RESERVE_PCT * 100:.1f}% when exchange min limits allow."
    )
    reasons.append(
        "- To raise frequency: lower TREND_STRONG_BUY_STRENGTH (<0.50), "
        "lower TREND_FALLBACK_MIN_CHANGE_PCT (e.g., -0.80), "
        "or lower MOMENTUM_BUY_CHANGE_PCT (e.g., 0.20) in .env"
    )

    body = (
        f"CryptoTrader has completed {zero_streak} consecutive cycles with 0 trades.\n\n"
        f"Wallet balances (USD-estimated, includes non-quote coins):\n{balance_text}\n\n"
        f"Likely reasons no trades were placed:\n" + "\n".join(reasons) + "\n\n"
        f"The bot continues running and will trade as soon as conditions are met."
    )
    send_email(
        f"[CryptoBot] No trades for {zero_streak} cycles — here's why",
        body,
    )
    print(f"[no-trade-alert] Sent zero-trade explanation email (streak={zero_streak})")

def _acquire_single_instance_lock():
    """Auto-generated docstring."""
    global _INSTANCE_LOCK_FH
    try:
        lock_fh = open(LOCK_FILE, "w")
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fh.write(f"pid={os.getpid()} started_at={_utc_now_iso()}\n")
        lock_fh.flush()
        _INSTANCE_LOCK_FH = lock_fh
        return True
    except BlockingIOError:
        print("[startup][ERROR] Another trader instance is already running. Exiting this instance.")
        return False
    except Exception as exc:
        print(f"[startup][ERROR] Could not acquire instance lock: {exc}")
        return False

def _ensure_trade_events_schema():
    """Migrate trade_events schema to include margin fields (idempotent)."""
    try:
        conn = sqlite3.connect(ANALYTICS_DB_FILE)
        cursor = conn.cursor()
        pragma_result = cursor.execute("PRAGMA table_info(trade_events)").fetchall()
        existing_cols = {row[1] for row in pragma_result}
        if "trade_mode" not in existing_cols:
            conn.execute("ALTER TABLE trade_events ADD COLUMN trade_mode TEXT DEFAULT 'spot'")
        if "leverage" not in existing_cols:
            conn.execute("ALTER TABLE trade_events ADD COLUMN leverage INTEGER DEFAULT 1")
        if "edge_score" not in existing_cols:
            conn.execute("ALTER TABLE trade_events ADD COLUMN edge_score REAL DEFAULT 0.0")
        if "margin_fallback_reason" not in existing_cols:
            conn.execute("ALTER TABLE trade_events ADD COLUMN margin_fallback_reason TEXT")
        conn.commit()
        conn.close()
    except Exception as exc:
        print(f"[analytics][WARN] Schema migration failed: {exc}")

def _db_connection():
    """Auto-generated docstring."""
    conn = sqlite3.connect(ANALYTICS_DB_FILE)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cycle_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            mode TEXT NOT NULL,
            pairs_scanned INTEGER NOT NULL,
            trades_executed INTEGER NOT NULL,
            errors INTEGER NOT NULL,
            daily_realized_pnl REAL NOT NULL,
            daily_open_pnl REAL NOT NULL,
            total_equity_usd REAL NOT NULL,
            volatility_gate_hits INTEGER NOT NULL,
            spread_opportunities INTEGER NOT NULL,
            active_exchanges TEXT NOT NULL,
            cycle_notes TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TEXT NOT NULL,
            exchange_name TEXT NOT NULL,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            strategy TEXT NOT NULL,
            size_usd REAL NOT NULL,
            base_amount REAL NOT NULL,
            price REAL NOT NULL,
            pnl_pct REAL,
            order_id TEXT,
            status TEXT NOT NULL,
            trade_mode TEXT DEFAULT 'spot',
            leverage INTEGER DEFAULT 1,
            edge_score REAL DEFAULT 0.0,
            margin_fallback_reason TEXT
        )
        """
    )
    conn.commit()
    return conn

def _record_trade_event(record, strategy, trade_mode="spot", leverage=1, edge_score=0.0, margin_fallback_reason=None):
    """Auto-generated docstring."""
    try:
        conn = _db_connection()
        conn.execute(
            """
            INSERT INTO trade_events (
                recorded_at, exchange_name, pair, side, strategy,
                size_usd, base_amount, price, pnl_pct, order_id, status,
                trade_mode, leverage, edge_score, margin_fallback_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.get("opened_at", _utc_now_iso()),
                record.get("exchange", ""),
                record.get("pair", ""),
                record.get("side", ""),
                strategy,
                _safe_float(record.get("size_usd"), 0.0),
                _safe_float(record.get("base_amount"), 0.0),
                _safe_float(record.get("price"), 0.0),
                _safe_float(record.get("change_pct"), 0.0),
                record.get("order_id", ""),
                record.get("status", "submitted"),
                trade_mode,
                leverage,
                edge_score,
                margin_fallback_reason,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        print(f"[analytics][WARN] Could not write trade event: {exc}")

def _record_cycle_summary(summary):
    """Auto-generated docstring."""
    try:
        conn = _db_connection()
        conn.execute(
            """
            INSERT INTO cycle_summaries (
                recorded_at, mode, pairs_scanned, trades_executed, errors,
                daily_realized_pnl, daily_open_pnl, total_equity_usd,
                volatility_gate_hits, spread_opportunities, active_exchanges, cycle_notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                summary.get("recorded_at", _utc_now_iso()),
                summary.get("mode", "LIVE"),
                int(summary.get("pairs_scanned", 0)),
                int(summary.get("trades_executed", 0)),
                int(summary.get("errors", 0)),
                _safe_float(summary.get("daily_realized_pnl"), 0.0),
                _safe_float(summary.get("daily_open_pnl"), 0.0),
                _safe_float(summary.get("total_equity_usd"), 0.0),
                int(summary.get("volatility_gate_hits", 0)),
                int(summary.get("spread_opportunities", 0)),
                ",".join(summary.get("active_exchanges", [])),
                summary.get("cycle_notes", ""),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        print(f"[analytics][WARN] Could not write cycle summary: {exc}")

def _monthly_drawdown_snapshot(current_equity_usd):
    """Auto-generated docstring."""
    month_prefix = datetime.now(timezone.utc).strftime("%Y-%m")
    try:
        conn = _db_connection()
        cursor = conn.execute(
            "SELECT MAX(total_equity_usd) FROM cycle_summaries WHERE recorded_at LIKE ?",
            (f"{month_prefix}%",),
        )
        peak_equity = _safe_float((cursor.fetchone() or [current_equity_usd])[0], current_equity_usd)
        conn.close()
    except Exception as exc:
        print(f"[analytics][WARN] Could not query monthly drawdown: {exc}")
        peak_equity = current_equity_usd

    peak_equity = max(peak_equity, current_equity_usd, 0.01)
    drawdown_pct = ((peak_equity - current_equity_usd) / peak_equity) * 100.0
    return peak_equity, drawdown_pct

def _alert_monthly_drawdown(current_equity_usd):
    """Auto-generated docstring."""
    peak_equity, drawdown_pct = _monthly_drawdown_snapshot(current_equity_usd)
    if drawdown_pct < MONTHLY_DRAWDOWN_ALERT_PCT:
        return drawdown_pct
    if not _should_send_alert("monthly_drawdown", 6 * 60 * 60):
        return drawdown_pct
    send_email(
        "[CryptoBot ALERT] Monthly Drawdown",
        (
            f"Monthly drawdown threshold breached.\n"
            f"Current equity: ${current_equity_usd:.2f}\n"
            f"Peak month equity: ${peak_equity:.2f}\n"
            f"Drawdown: {drawdown_pct:.2f}%\n"
            f"Threshold: {MONTHLY_DRAWDOWN_ALERT_PCT:.2f}%"
        ),
    )
    return drawdown_pct

def _recent_zero_trade_streak(limit=12):
    """Auto-generated docstring."""
    try:
        conn = _db_connection()
        rows = conn.execute(
            "SELECT trades_executed FROM cycle_summaries ORDER BY id DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
        conn.close()
    except Exception:
        return 0

    streak = 0
    for row in rows:
        executed = int((row or [0])[0])
        if executed == 0:
            streak += 1
        else:
            break
    return streak

def _volatility_relax_factor():
    """Auto-generated docstring."""
    if not ADAPTIVE_VOLATILITY_RELAX:
        return 1.0, 0
    zero_streak = _recent_zero_trade_streak()
    if zero_streak < RELAX_AFTER_ZERO_TRADE_STREAK:
        return 1.0, zero_streak

    steps = min(RELAX_MAX_STEPS, zero_streak - RELAX_AFTER_ZERO_TRADE_STREAK + 1)
    relax_factor = max(0.25, 1.0 - (steps * RELAX_STEP_PCT))
    return relax_factor, zero_streak

def _volatility_signal(ex, pair, threshold_factor=1.0):
    """Auto-generated docstring."""
    try:
        candles = ex.fetch_ohlcv(pair, timeframe=TRADER_TIMEFRAME, limit=max(VOLATILITY_LOOKBACK, 8))
    except Exception as exc:
        print(f"[volatility][WARN] {pair}: could not fetch candles: {exc}")
        return False, 0.0, 0.0

    closes = [_safe_float(candle[4], 0.0) for candle in candles if len(candle) >= 5]
    closes = [close for close in closes if close > 0]
    if len(closes) < 6:
        return False, 0.0, 0.0

    returns = []
    for index in range(1, len(closes)):
        prior = closes[index - 1]
        current = closes[index]
        if prior > 0:
            returns.append((current - prior) / prior)
    if len(returns) < 5:
        return False, 0.0, 0.0

    recent_slice = returns[-5:]
    baseline_slice = returns[:-5] or returns
    recent_vol = math.sqrt(sum(r * r for r in recent_slice) / len(recent_slice)) * 100.0
    baseline_vol = math.sqrt(sum(r * r for r in baseline_slice) / len(baseline_slice)) * 100.0
    factor = max(_safe_float(threshold_factor, 1.0), 0.1)
    threshold = max(VOLATILITY_MIN_PCT * factor, baseline_vol * VOLATILITY_SPIKE_MULTIPLIER * factor)
    return recent_vol >= threshold, recent_vol, threshold

def _build_exchange_clients():
    """Auto-generated docstring."""
    exchanges = []
    for exchange_name in ("kraken", "coinbase"):
        try:
            exchange = _load_exchange(exchange_name)
            exchange.load_markets()
            exchanges.append((exchange_name, exchange))
        except Exception as exc:
            print(f"[failover][WARN] {exchange_name} unavailable: {exc}")
            if _should_send_alert(f"failover:{exchange_name}", EXCHANGE_FAILURE_ALERT_COOLDOWN_SECONDS):
                send_email(
                    f"[CryptoBot ALERT] Exchange Failover: {exchange_name}",
                    f"{exchange_name} became unavailable and trading will fail over to remaining exchanges.\nError: {exc}",
                )
    return exchanges

def _execute_spread_trade(pair, exchange_map, balances_by_exchange):
    """Auto-generated docstring."""
    available_names = sorted(exchange_map.keys())
    if len(available_names) < 2:
        return False

    prices = {}
    for exchange_name in available_names:
        try:
            ticker = exchange_map[exchange_name].fetch_ticker(pair)
            prices[exchange_name] = _safe_float(ticker.get("last"), 0.0)
        except Exception as exc:
            print(f"[spread][WARN] {exchange_name} {pair}: ticker unavailable: {exc}")
            return False

    sorted_prices = sorted(prices.items(), key=lambda item: item[1])
    cheapest_name = sorted_prices[0][0]
    richest_name = sorted_prices[-1][0]
    cheap_price = prices[cheapest_name]
    rich_price = prices[richest_name]
    if cheap_price <= 0 or rich_price <= 0 or cheapest_name == richest_name:
        return False

    spread_pct = ((rich_price - cheap_price) / cheap_price) * 100.0
    if spread_pct < SPREAD_ARB_THRESHOLD_PCT:
        return False

    base_asset, quote_asset = pair.split("/")
    cheap_balance = balances_by_exchange.get(cheapest_name, {})
    rich_balance = balances_by_exchange.get(richest_name, {})
    cheap_quote_balance = _safe_float((cheap_balance.get("total", {}) or {}).get(quote_asset), 0.0)
    rich_base_balance = _safe_float((rich_balance.get("total", {}) or {}).get(base_asset), 0.0)
    trade_usd, min_cost = _adaptive_trade_notional(
        exchange_map[cheapest_name], pair, cheap_quote_balance, cheapest_name, cheap_price
    )
    buy_amount, buy_min_amount = _market_trade_amount(exchange_map[cheapest_name], pair, trade_usd, cheap_price)
    sell_amount = min(rich_base_balance * (1.0 - COIN_SELL_RESERVE_PCT), buy_amount)
    sell_amount = _quantize_amount(exchange_map[richest_name], pair, sell_amount)
    if buy_amount <= 0 or buy_amount < buy_min_amount or sell_amount <= 0:
        return False
    if min_cost > 0 and trade_usd < min_cost:
        return False
    if _safe_float(sell_amount * rich_price, 0.0) < _market_min_cost(exchange_map[richest_name], pair):
        return False

    print(
        f"[spread][OPPORTUNITY] {pair}: buy on {cheapest_name} @ {cheap_price:.8f}, "
        f"sell on {richest_name} @ {rich_price:.8f}, spread={spread_pct:.2f}%"
    )
    try:
        cheap_exchange = exchange_map[cheapest_name]
        rich_exchange = exchange_map[richest_name]
        if cheapest_name == "coinbase":
            buy_order = cheap_exchange.create_market_buy_order(
                pair,
                trade_usd,
                params={"createMarketBuyOrderRequiresPrice": False},
            )
        else:
            buy_order = cheap_exchange.create_market_buy_order(pair, buy_amount)
        sell_order = rich_exchange.create_market_sell_order(pair, sell_amount)
        now_iso = _utc_now_iso()
        _record_trade_event(
            {
                "opened_at": now_iso,
                "exchange": cheapest_name,
                "pair": pair,
                "side": "buy",
                "size_usd": trade_usd,
                "base_amount": buy_amount,
                "price": cheap_price,
                "change_pct": spread_pct,
                "order_id": buy_order.get("id", ""),
                "status": buy_order.get("status", "submitted"),
            },
            "cross_exchange_spread",
        )
        _record_trade_event(
            {
                "opened_at": now_iso,
                "exchange": richest_name,
                "pair": pair,
                "side": "sell",
                "size_usd": sell_amount * rich_price,
                "base_amount": sell_amount,
                "price": rich_price,
                "change_pct": spread_pct,
                "order_id": sell_order.get("id", ""),
                "status": sell_order.get("status", "submitted"),
            },
            "cross_exchange_spread",
        )
        return True
    except Exception as exc:
        print(f"[spread][ERROR] Failed to execute spread trade for {pair}: {exc}")
        return False

def _build_wallet_snapshot(ex):
    """
    Return a full balance snapshot that includes coin holdings and rough USD valuation.
    This is intentionally conservative: if we cannot price an asset reliably, we keep
    the raw amount and mark the valuation as unknown instead of guessing.
    """
    balance = ex.fetch_balance()
    totals = balance.get("total", {}) or {}
    snapshot = []
    estimated_total = 0.0
    missing_prices = []

    try:
        ex.load_markets()
    except Exception:
        pass

    for asset, raw_amount in sorted(totals.items()):
        amount = _safe_float(raw_amount, 0.0)
        if amount <= 0:
            continue

        estimated_usd = None
        if asset in _quote_assets():
            estimated_usd = amount
        else:
            for quote in ("USDT", "USD", "USDC"):
                direct_symbol = f"{asset}/{quote}"
                inverse_symbol = f"{quote}/{asset}"
                try:
                    if direct_symbol in getattr(ex, "markets", {}):
                        ticker = ex.fetch_ticker(direct_symbol)
                        last = _safe_float(ticker.get("last"), 0.0)
                        if last > 0:
                            estimated_usd = amount * last if quote == "USD" else amount * last
                            break
                    if inverse_symbol in getattr(ex, "markets", {}):
                        ticker = ex.fetch_ticker(inverse_symbol)
                        last = _safe_float(ticker.get("last"), 0.0)
                        if last > 0:
                            estimated_usd = amount / last
                            break
                except Exception:
                    continue

        if estimated_usd is None:
            missing_prices.append(asset)
        else:
            estimated_total += estimated_usd

        snapshot.append({
            "asset": asset,
            "amount": amount,
            "estimated_usd": estimated_usd,
        })

    return {
        "balances": snapshot,
        "estimated_total_usd": estimated_total,
        "missing_prices": missing_prices,
    }

def _format_wallet_snapshot(exchange_name, snapshot):
    """Auto-generated docstring."""
    lines = [f"[wallet] {exchange_name} estimated total: ${snapshot['estimated_total_usd']:.2f}"]
    if snapshot["missing_prices"]:
        lines.append(f"[wallet] {exchange_name} unpriced assets: {', '.join(snapshot['missing_prices'])}")
    for entry in snapshot["balances"]:
        usd = entry["estimated_usd"]
        usd_text = f"${usd:.2f}" if usd is not None else "unpriced"
        lines.append(f"[wallet]   {entry['asset']}: {entry['amount']:.8f} ({usd_text})")
    return "\n".join(lines)

def _market_trade_amount(ex, pair, trade_usd, price):
    """Auto-generated docstring."""
    price = _safe_float(price, 0.0)
    if price <= 0:
        return 0.0, 0.0
    base_amount = _safe_float(trade_usd, 0.0) / price
    try:
        ex.load_markets()
        market = ex.market(pair)
        min_amount = _safe_float((market.get("limits", {}) or {}).get("amount", {}).get("min"), 0.0)
        precision = market.get("precision", {}) or {}
        amount_precision = precision.get("amount")
        if amount_precision is not None:
            base_amount = round(base_amount, int(amount_precision))
        if min_amount > 0 and base_amount < min_amount:
            return 0.0, min_amount
    except Exception:
        min_amount = 0.0
    return base_amount, min_amount

def _quantize_amount(ex, pair, amount):
    """Auto-generated docstring."""
    quantized = max(_safe_float(amount, 0.0), 0.0)
    try:
        market = ex.market(pair)
        precision = (market.get("precision", {}) or {}).get("amount")
        if precision is not None:
            quantized = round(quantized, int(precision))
    except Exception:
        pass
    return quantized

def _discover_trade_pairs(ex):
    """Auto-generated docstring."""
    discovered = []
    try:
        markets = ex.load_markets() or {}
    except Exception as exc:
        print(f"[pairs][WARN] Could not load markets: {exc}")
        return discovered

    for symbol, market in markets.items():
        if not isinstance(market, dict):
            continue
        if market.get("spot") is False:
            continue
        if market.get("active") is False:
            continue
        if ":" in symbol:
            continue
        base = (market.get("base") or "").upper()
        quote = (market.get("quote") or "").upper()
        if not base or not quote:
            continue
        if quote not in PAIR_QUOTES:
            continue
        discovered.append(f"{base}/{quote}")

    discovered = sorted(set(discovered))
    if MAX_PAIRS_PER_EXCHANGE > 0:
        discovered = discovered[:MAX_PAIRS_PER_EXCHANGE]
    return discovered

def _pairs_for_exchange(ex):
    """Auto-generated docstring."""
    if TRADE_PAIRS and not DISCOVER_ALL_PAIRS:
        return list(TRADE_PAIRS)

    discovered = _discover_trade_pairs(ex)
    if TRADE_PAIRS and DISCOVER_ALL_PAIRS:
        merged = sorted(set(discovered).union(set(TRADE_PAIRS)))
        return merged

    if discovered:
        return discovered

    return list(TRADE_PAIRS) if TRADE_PAIRS else ["BTC/USDT", "ETH/USDT"]

def _update_status(data):
    """Auto-generated docstring."""
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if isinstance(data, dict):
        payload.update(data)
    try:
        _write_json(STATUS_FILE, payload)
    except Exception as exc:
        print(f"[status][WARN] Failed to write status file: {exc}")

def _append_trade_record(record, trade_mode="spot", leverage=1, edge_score=0.0, margin_fallback_reason=None):
    """Auto-generated docstring."""
    trades = _load_json(TRADES_FILE, [])
    if not isinstance(trades, list):
        trades = []
    record["trade_mode"] = trade_mode
    record["leverage"] = leverage
    record["edge_score"] = edge_score
    record["margin_fallback_reason"] = margin_fallback_reason
    trades.append(record)
    _write_json(TRADES_FILE, trades)
    _record_trade_event(record, record.get("strategy", "spot_cycle"), trade_mode, leverage, edge_score, margin_fallback_reason)

def _get_daily_pnl():
    """Auto-generated docstring."""
    trades = _load_json(TRADES_FILE, [])
    today = datetime.now(timezone.utc).date().isoformat()
    today_trades = [t for t in trades if t.get('opened_at', '').startswith(today)]
    realized_pnl = sum(t.get('pnl_realized', 0.0) for t in today_trades if t.get('status') in ('closed', 'filled'))
    open_pnl = sum(t.get('pnl_unrealized', 0.0) for t in today_trades if t.get('status') == 'open')
    return realized_pnl, open_pnl, len(today_trades)

def _get_daily_trade_count():
    """Auto-generated docstring."""
    trades = _load_json(TRADES_FILE, [])
    today = datetime.now(timezone.utc).date().isoformat()
    return len([t for t in trades if t.get('opened_at', '').startswith(today)])

def _check_profit_stop_condition(price, entry_price, change_pct, pair):
    """Auto-generated docstring."""
    if entry_price <= 0:
        return None, change_pct
    pnl_pct = ((price - entry_price) / entry_price) * 100.0
    if pnl_pct >= SELL_PROFIT_PCT:
        return 'profit_target', pnl_pct
    if pnl_pct <= SELL_STOP_LOSS_PCT:
        return 'stop_loss', pnl_pct
    return None, pnl_pct

def _check_daily_risk_cap(realized_pnl):
    """Auto-generated docstring."""
    if realized_pnl <= -DAILY_LOSS_CAP_USD:
        return True, 'Daily loss cap hit'
    if realized_pnl >= DAILY_PROFIT_TARGET_USD:
        return True, 'Daily profit target reached'
    return False, None

def _alert_risk_cap_breach(summary):
    """Auto-generated docstring."""
    if not RISK_CAP_EMAIL_ALERT:
        return
    send_email(
        '[CryptoBot ALERT] Risk Cap Breach',
        f'Daily risk management alert:\n{summary}\n\nBot will continue monitoring but stop placing new trades.'
    )

def send_email(subject, body):
    """Auto-generated docstring."""
    import smtplib
    from email.mime.text import MIMEText
    smtp_user = os.environ.get("GMAIL_USER") or os.environ.get("SMTP_USER")
    smtp_password = (
        os.environ.get("GMAIL_APP_PASSWORD")
        or os.environ.get("SMTP_PASSWORD")
        or os.environ.get("SMTP_PASS")
    )
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_use_tls = os.environ.get("SMTP_USE_TLS", "1") != "0"
    from_addr = os.environ.get("SMTP_FROM", smtp_user or "skynetv1@localhost")

    # Ensure to_addr is always a valid string (not None)
    to_addr = os.environ.get("EMAIL_RECIPIENT") or smtp_user or ""
    if not to_addr:
        print("[EMAIL][ERROR] No recipient address configured. Email not sent.")
        return False
    if not smtp_user or not smtp_password:
        print("[EMAIL][ERROR] SMTP credentials are not configured. Email not sent.")
        return False

    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = to_addr
        # sendmail expects a list of valid strings, not None
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            if smtp_use_tls:
                server.starttls()
            # Ensure credentials are strings, not None
            login_user = smtp_user or ""
            login_pass = smtp_password or ""
            server.login(str(login_user), str(login_pass))
            server.sendmail(msg["From"], [str(to_addr)], msg.as_string())
        print(f"[EMAIL] Sent to {to_addr}: {subject}")
        return True
    except Exception as exc:
        print(f"[EMAIL][ERROR] Failed to send: {exc}\nSubject: {subject}\n{body}")
        return False

def _ensure_tradable_funds(min_usd=4.0):
    """Auto-generated docstring."""
    print(f"[startup] Ensuring at least ${min_usd} tradable funds available (stub).")

# --- Stubs for missing command functions ---
def cmd_scan():
    """Auto-generated docstring."""
    print("[stub] cmd_scan called.")

def cmd_selftest():
    """Auto-generated docstring."""
    print("[selftest] Running startup diagnostics...")
    print(f"[selftest] STATE_DIR={STATE_DIR}")
    print(f"[selftest] POSITIONS_FILE exists={os.path.exists(POSITIONS_FILE)}")
    print(f"[selftest] TRADES_FILE exists={os.path.exists(TRADES_FILE)}")
    print(f"[selftest] QUEUE_FILE exists={os.path.exists(QUEUE_FILE)}")
    print(f"[selftest] DRY_RUN={_is_dry_run()}")
    print(f"[selftest] TRADE_PAIRS={TRADE_PAIRS}")
    print(f"[selftest] Floors: global={MIN_TRADE_USD}, kraken={KRAKEN_MIN_TRADE_USD}, coinbase={COINBASE_MIN_TRADE_USD}")
    print(
        f"[selftest] Micro mode={MICRO_TRADE_MODE}, kraken_max_min_cost={MICRO_KRAKEN_MAX_MIN_COST_USD}, "
        f"coinbase_skip_below={MICRO_COINBASE_MIN_QUOTE_USD}"
    )
    print(f"[selftest] ANALYTICS_DB_FILE={ANALYTICS_DB_FILE}")
    print(f"[selftest] Volatility gate: lookback={VOLATILITY_LOOKBACK}, multiplier={VOLATILITY_SPIKE_MULTIPLIER}, min_pct={VOLATILITY_MIN_PCT}")
    print(f"[selftest] Spread threshold={SPREAD_ARB_THRESHOLD_PCT}% | Monthly drawdown alert={MONTHLY_DRAWDOWN_ALERT_PCT}%")

    for ex_name in ("kraken", "coinbase"):
        try:
            ex = _load_exchange(ex_name)
            markets = ex.load_markets()
            print(f"[selftest] {ex_name}: markets loaded ({len(markets) if hasattr(markets, '__len__') else 'unknown'} symbols)")
            try:
                balance = ex.fetch_balance()
                totals = balance.get("total", {}) or {}
                tracked = {k: totals.get(k) for k in ("USD", "USDT", "USDC", "BTC", "ETH")}
                print(f"[selftest] {ex_name}: balance snapshot={tracked}")
            except Exception as balance_exc:
                print(f"[selftest][WARN] {ex_name}: could not fetch balance: {balance_exc}")
        except Exception as exc:
            print(f"[selftest][WARN] {ex_name}: unavailable or misconfigured: {exc}")

    print("[selftest] Diagnostics complete.")
def cmd_evaluate_exchanges():
    """Auto-generated docstring."""
    print("[stub] cmd_evaluate_exchanges called.")

def cmd_execute(trade_id):
    """Auto-generated docstring."""
    print(f"[stub] cmd_execute called for trade_id={trade_id}")

def cmd_monitor():
    """Auto-generated docstring."""
    print("[stub] cmd_monitor called.")

def run_backtest():
    """Auto-generated docstring."""
    print("[stub] run_backtest called.")

# --- Additional Command Implementations ---
def cmd_paper_summary():
    """Auto-generated docstring."""
    print("[debug] Entering cmd_paper_summary() - generating 2-hour challenge report...")
    trades = _load_json(TRADES_FILE, [])
    if not isinstance(trades, list):
        trades = []
    positions = _load_json(POSITIONS_FILE, [])
    if not isinstance(positions, list):
        positions = []
    now = datetime.now(timezone.utc)
    window_end = now
    window_start = now - timedelta(hours=2)

    def _trade_ts(trade):
        for key in ("closed_at", "opened_at", "recorded_at"):
            ts = _parse_iso_utc(trade.get(key))
            if ts:
                return ts
        return None

    window_trade_items = []
    for trade in trades:
        ts = _trade_ts(trade)
        if ts and window_start <= ts <= window_end:
            window_trade_items.append((ts, trade))

    open_positions = [p for p in positions if not p.get("closed_at")]
    new_trades_json = len(window_trade_items)
    closed_trades_json = len(
        [
            t
            for _, t in window_trade_items
            if str(t.get("status", "")).lower() in ("closed", "filled") or t.get("closed_at")
        ]
    )
    realized_pnl_json = sum(
        _safe_float(t.get("pnl_realized", t.get("pnl", 0.0)), 0.0)
        for _, t in window_trade_items
        if str(t.get("status", "")).lower() in ("closed", "filled") or t.get("closed_at")
    )

    db_window_rows = []
    db_pairs_scanned = 0
    db_volatility_hits = 0
    db_spread_hits = 0
    db_margin_trades = 0
    db_spot_trades = 0
    db_avg_leverage = 0.0
    try:
        conn = _db_connection()
        db_window_rows = conn.execute(
            """
            SELECT recorded_at, pair, side, strategy, size_usd, pnl_pct, status, trade_mode, leverage, edge_score
            FROM trade_events
            WHERE recorded_at >= ?
            ORDER BY id DESC
            LIMIT 200
            """,
            (window_start.isoformat(),),
        ).fetchall()
        
        # Query margin vs spot breakdown
        margin_spot_rows = conn.execute(
            """
            SELECT trade_mode, COUNT(*) as count, AVG(leverage) as avg_leverage
            FROM trade_events
            WHERE recorded_at >= ? AND side = 'buy'
            GROUP BY trade_mode
            """,
            (window_start.isoformat(),),
        ).fetchall()
        for row in margin_spot_rows:
            mode, count, avg_lev = row
            if mode == "margin":
                db_margin_trades = count
                db_avg_leverage = _safe_float(avg_lev, 1.0)
            elif mode == "spot":
                db_spot_trades = count
        
        cycle_rollup = conn.execute(
            """
            SELECT
                COALESCE(SUM(pairs_scanned), 0),
                COALESCE(SUM(volatility_gate_hits), 0),
                COALESCE(SUM(spread_opportunities), 0)
            FROM cycle_summaries
            WHERE recorded_at >= ?
            """,
            (window_start.isoformat(),),
        ).fetchone()
        conn.close()
        if cycle_rollup:
            db_pairs_scanned = int(_safe_float(cycle_rollup[0], 0))
            db_volatility_hits = int(_safe_float(cycle_rollup[1], 0))
            db_spread_hits = int(_safe_float(cycle_rollup[2], 0))
    except Exception as exc:
        print(f"[summary][WARN] Could not query analytics DB for 2-hour report: {exc}")

    db_new_trades = len(db_window_rows)
    db_closed_trades = len(
        [
            row
            for row in db_window_rows
            if str((row[6] or "")).lower() in ("closed", "filled")
        ]
    )

    new_trades = max(new_trades_json, db_new_trades)
    closed_trades = max(closed_trades_json, db_closed_trades)
    realized_pnl = realized_pnl_json

    strategy_counts = defaultdict(int)
    for row in db_window_rows:
        strategy_counts[str(row[3] or "unknown")] += 1

    # Aggregate wallet values for both exchanges
    wallet_values = {}
    for ex_name in ["kraken", "coinbase"]:
        try:
            ex = _load_exchange(ex_name)
            wallet_values[ex_name] = _build_wallet_snapshot(ex)
        except Exception as e:
            wallet_values[ex_name] = f"Error: {e}"

    pair_pnl = defaultdict(float)
    for _, t in window_trade_items:
        pair_key = t.get("pair", "")
        pair_pnl[pair_key] += _safe_float(t.get("pnl_realized", t.get("pnl", 0.0)), 0.0)

    top_gains = sorted(pair_pnl.items(), key=lambda x: -x[1])[:2]
    top_losses = sorted(pair_pnl.items(), key=lambda x: x[1])[:2]

    trend_meta = _load_trend_model_metadata()
    trends = (trend_meta.get("trends_by_pair", {}) or {}) if isinstance(trend_meta, dict) else {}
    fresh = _is_trend_model_fresh(trend_meta, now)
    up_count = 0
    neutral_count = 0
    down_count = 0
    for info in trends.values():
        direction = str((info or {}).get("trend_direction", "neutral")).lower()
        if direction == "up":
            up_count += 1
        elif direction == "down":
            down_count += 1
        else:
            neutral_count += 1

    ranked_trends = []
    for pair, info in trends.items():
        ranked_trends.append((
            pair,
            _safe_float((info or {}).get("avg_pnl_pct"), 0.0),
            int(_safe_float((info or {}).get("samples"), 0)),
            str((info or {}).get("trend_direction", "neutral")),
        ))
    ranked_trends = [x for x in ranked_trends if x[2] >= max(1, WEEKLY_TREND_MIN_TRADES_PER_PAIR)]
    best_learned = sorted(ranked_trends, key=lambda x: x[1], reverse=True)[:3]
    worst_learned = sorted(ranked_trends, key=lambda x: x[1])[:3]

    summary = f"""
2-Hour Challenge Report
============================================

Window start: {window_start.isoformat()}
Window end:   {window_end.isoformat()}
Status:       running

What was done:
  New trades executed:        {new_trades}
  Trades closed this period:  {closed_trades}
  Open positions now:         {len(open_positions)}
    Pairs scanned (2h):         {db_pairs_scanned}
    Volatility passes (2h):     {db_volatility_hits}
    Spread opportunities (2h):  {db_spread_hits}

Margin vs Spot:
  Margin trades (2h):         {db_margin_trades}
  Spot trades (2h):           {db_spot_trades}
  Avg leverage (margin):      {db_avg_leverage:.2f}x

Wallet values:
  Kraken:   {wallet_values['kraken']['estimated_total_usd'] if isinstance(wallet_values.get('kraken'), dict) else wallet_values['kraken']}
  Coinbase: {wallet_values['coinbase']['estimated_total_usd'] if isinstance(wallet_values.get('coinbase'), dict) else wallet_values['coinbase']}

Made / Loss:
  Realized P&L (period):      ${realized_pnl:+.2f}

Recent Trades (last 5):
"""
    if db_window_rows:
        for row in db_window_rows[:5]:
            recorded_at, pair, side, strategy, size_usd, pnl_pct, status, trade_mode, leverage, edge_score = row + (None,) * max(0, 10 - len(row))
            size_usd = _safe_float(size_usd, 0.0)
            leverage = int(_safe_float(leverage, 1))
            edge_score = _safe_float(edge_score, 0.0)
            tag = "SMALL" if size_usd <= 5 else "BIG"
            trade_mode_str = f"[{trade_mode}" + (f"_{leverage}x]" if trade_mode == "margin" else "]")
            summary += (
                f"  {pair}: {str(side or '').upper()} {tag} ${size_usd:.2f} | "
                f"strat={strategy or 'unknown'} | mode={trade_mode_str} | edge={edge_score:.2f} | "
                f"status={status or 'submitted'} | pnl_pct={_safe_float(pnl_pct, 0.0):+.2f}% | {str(recorded_at or '')[:19]}\n"
            )
    else:
        for _, t in sorted(window_trade_items, key=lambda item: item[0], reverse=True)[:5]:
            size_usd = _safe_float(t.get("size_usd", 0.0), 0.0)
            trade_mode = t.get("trade_mode", "spot")
            leverage = int(_safe_float(t.get("leverage", 1), 1))
            edge_score = _safe_float(t.get("edge_score", 0.0), 0.0)
            tag = "SMALL" if size_usd <= 5 else "BIG"
            trade_mode_str = f"[{trade_mode}" + (f"_{leverage}x]" if trade_mode == "margin" else "]")
            summary += (
                f"  {t.get('pair','')}: {str(t.get('side','')).upper()} {tag} ${size_usd:.2f} | "
                f"strat={t.get('strategy','unknown')} | mode={trade_mode_str} | edge={edge_score:.2f} | "
                f"status={t.get('status','submitted')} | pnl_pct={_safe_float(t.get('change_pct', 0.0), 0.0):+.2f}%\n"
            )

    summary += "\nLearning summary:\n"
    summary += (
        f"Trend model: {'fresh' if fresh else 'stale'} | "
        f"last_trained={trend_meta.get('last_trained_at', trend_meta.get('trained_at', 'never'))} | "
        f"pairs_profiled={len(trends)} (up={up_count}, neutral={neutral_count}, down={down_count})\n"
    )

    if strategy_counts:
        summary += "Strategies used this 2h window:\n"
        for strategy_name, count in sorted(strategy_counts.items(), key=lambda item: -item[1])[:5]:
            summary += f"  {strategy_name}: {count}\n"

    summary += "\nTop pair gains (realized in-window):\n"
    for pair, pnl in top_gains:
        summary += f"  {pair}: ${pnl:+.2f}\n"
    summary += "\nTop pair losses (realized in-window):\n"
    for pair, pnl in top_losses:
        summary += f"  {pair}: ${pnl:+.2f}\n"

    summary += "\nBest learned pairs (from trend model):\n"
    for pair, avg_pnl_pct, samples, direction in best_learned:
        summary += f"  {pair}: avg_pnl={avg_pnl_pct:+.2f}% | samples={samples} | dir={direction}\n"

    summary += "\nWorst learned pairs (from trend model):\n"
    for pair, avg_pnl_pct, samples, direction in worst_learned:
        summary += f"  {pair}: avg_pnl={avg_pnl_pct:+.2f}% | samples={samples} | dir={direction}\n"

    if new_trades == 0:
        summary += (
            "\nWhy no executions likely happened:\n"
            f"  - pairs_scanned={db_pairs_scanned}, volatility_passes={db_volatility_hits}, spread_hits={db_spread_hits}\n"
            "  - likely blocked by min-cost/min-amount exchange limits, insufficient quote balance, or gated strategy thresholds\n"
        )
    print("[debug] Sending 2-hour challenge report email...")
    send_email("[CryptoBot] 2-hour challenge report", summary)
    print("[debug] 2-hour challenge report content:")
    print(summary)
    print("[debug] Exiting cmd_paper_summary()")

def cmd_wallet_snapshot():
    """Auto-generated docstring."""
    print("[wallet] Collecting a full wallet snapshot...")
    for ex_name in ("kraken", "coinbase"):
        try:
            ex = _load_exchange(ex_name)
            snapshot = _build_wallet_snapshot(ex)
            print(_format_wallet_snapshot(ex_name, snapshot))
        except Exception as exc:
            print(f"[wallet][ERROR] {ex_name}: {exc}")

def cmd_check_trigger():
    """Auto-generated docstring."""
    print("[check-trigger] Checking Kraken balance against threshold...")
    try:
        key = os.environ.get("KRAKEN_API_KEY", "").strip()
        secret = os.environ.get("KRAKEN_SECRET", "").strip()
        ex = _load_exchange("kraken")
        balance = ex.fetch_balance()
        usd_amt = float(balance.get("total", {}).get("USD", 0.0))
        print(f"[check-trigger] Kraken USD balance: ${usd_amt:.2f}")
        if usd_amt >= AIRDROP_TRIGGER_THRESHOLD_USD:
            print(f"[check-trigger] Threshold met! Scanning and emailing...")
            cmd_scan()
            send_email("Airdrop Trigger Met", f"Kraken USD balance: ${usd_amt:.2f}")
        else:
            print(f"[check-trigger] Threshold not met.")
    except Exception as e:
        print(f"[check-trigger] Error: {e}")

def cmd_watch_trigger():
    """Auto-generated docstring."""
    print(f"[watch-trigger] Polling Kraken balance every {AIRDROP_TRIGGER_POLL_MIN} seconds...")
    while True:
        cmd_check_trigger()
        time.sleep(AIRDROP_TRIGGER_POLL_MIN)

def cmd_check_wallet_alert():
    """Auto-generated docstring."""
    print("[check-wallet-alert] Checking BTC_WATCH_ADDRESS for balance increase...")
    addr = os.environ.get("BTC_WATCH_ADDRESS", "")
    # Placeholder: In real use, query a blockchain API for balance
    print(f"[check-wallet-alert] (Simulated) Checked address: {addr}")
    send_email("Wallet Alert", f"Checked BTC address: {addr}")

def cmd_start_7d_challenge():
    """Auto-generated docstring."""
    print("[start-7d-challenge] Starting 7-day $5->$10 challenge...")
    # Placeholder: In real use, initialize challenge state
    send_email("7D Challenge Started", "Challenge initialized.")

def cmd_watch_7d_challenge():
    """Auto-generated docstring."""
    print("[watch-7d-challenge] Running 7-day challenge loop...")
    # Placeholder: In real use, loop and monitor challenge progress
    for i in range(7):
        print(f"[watch-7d-challenge] Day {i+1}: Monitoring...")
        time.sleep(1)
    send_email("7D Challenge Complete", "Challenge monitoring finished.")

def cmd_run_transfer_checklist():
    """Auto-generated docstring."""
    print("[run-transfer-checklist] Running transfer checklist...")
    # Placeholder: In real use, automate BTC transfer and challenge start
    send_email("Transfer Checklist", "Transfer and challenge started.")

def main():
    """Auto-generated docstring."""
    print("[startup] Entered main() function.")
    if not _acquire_single_instance_lock():
        return False

    parser = argparse.ArgumentParser(description="CryptoTrader engine")
    parser.add_argument("--selftest", action="store_true", help="Run environment self-test")
    parser.add_argument("--evaluate-exchanges", action="store_true", help="Compare exchanges")
    parser.add_argument("--scan", action="store_true", help="Scan for trading signals")
    parser.add_argument("--execute", metavar="TRADE_ID", help="Execute one queued trade by ID")
    parser.add_argument("--monitor", action="store_true", help="Monitor and close positions if SL/TP hit")
    parser.add_argument("--paper-summary", action="store_true", help="Email paper trading summary")
    parser.add_argument("--wallet-snapshot", action="store_true", help="Print a full wallet snapshot with coin holdings")
    parser.add_argument("--check-trigger", action="store_true", help="Check Kraken balance vs $35 threshold; scan+email if met")
    parser.add_argument("--watch-trigger", action="store_true", help="Continuously poll threshold and alert when met")
    parser.add_argument("--check-wallet-alert", action="store_true", help="Alert when BTC_WATCH_ADDRESS balance increases")
    parser.add_argument("--start-7d-challenge", action="store_true", help="Start one-time 7-day $5->$10 challenge")
    parser.add_argument("--watch-7d-challenge", action="store_true", help="Run the 7-day challenge loop")
    parser.add_argument("--run-transfer-checklist", action="store_true", help="One-command transfer flow: send BTC to Kraken, confirm, then start challenge")
    parser.add_argument("--force-email", action="store_true", help="Immediately send the 2-hour summary email now.")
    parser.add_argument("--test-periodic-email", action="store_true", help="Test periodic_email logic immediately (single call, no wait)")
    parser.add_argument("--backtest", action="store_true", help="Run backtest engine on simulated historical data")

    args = parser.parse_args()

    # --- Ensure state files exist without wiping trading history ---
    for f in [POSITIONS_FILE, TRADES_FILE, QUEUE_FILE]:
        try:
            if not os.path.exists(f):
                _write_json(f, [])
        except Exception as e:
            print(f"[startup] Failed to initialize {f}: {e}")

    # --- Ensure tradable funds ---
    _ensure_tradable_funds(min_usd=4.0)

    if args.test_periodic_email:
        print("[test] Forcing a single periodic_email cycle (no wait)...")
        try:
            print("[test] Calling cmd_paper_summary() from periodic_email test...")
            cmd_paper_summary()
        except Exception as e:
            print(f"[test][ERROR] Exception in test-periodic-email: {e}")
        print("[test] Done with test-periodic-email.")
        return True
    elif args.selftest:
        cmd_selftest()
    elif args.evaluate_exchanges:
        cmd_evaluate_exchanges()
    elif args.scan:
        cmd_scan()
    elif args.execute:
        cmd_execute(args.execute)
    elif args.monitor:
        cmd_monitor()
    elif args.paper_summary:
        cmd_paper_summary()
    elif args.wallet_snapshot:
        cmd_wallet_snapshot()
    elif args.force_email:
        cmd_paper_summary()
    elif args.check_trigger:
        cmd_check_trigger()
    elif args.watch_trigger:
        cmd_watch_trigger()
    elif args.check_wallet_alert:
        cmd_check_wallet_alert()
    elif args.start_7d_challenge:
        cmd_start_7d_challenge()
    elif args.watch_7d_challenge:
        cmd_watch_7d_challenge()
    elif args.run_transfer_checklist:
        cmd_run_transfer_checklist()
    elif args.backtest:
        run_backtest()
    else:
        # If no arguments, start the live trading loop
        run_live_loop()
        return True

    return True

def periodic_email():
    """Auto-generated docstring."""
    print("[PERIODIC_EMAIL] periodic_email() thread started and running.")
    while True:
        print("[PERIODIC_EMAIL] Triggering cmd_paper_summary() from periodic_email loop.")
        try:
            cmd_paper_summary()
        except Exception as e:
            print(f"[PERIODIC_EMAIL] Exception in cmd_paper_summary: {e}")
            traceback.print_exc()
        print("[PERIODIC_EMAIL] Sleeping for 2 hours...")
        time.sleep(2 * 60 * 60)  # 2 hours

def run_live_loop():
    """Auto-generated docstring."""
    mode = "DRY-RUN" if _is_dry_run() else "LIVE"
    try:
        _db_connection().close()
        _ensure_trade_events_schema()
    except Exception as exc:
        print(f"[analytics][WARN] Could not initialize analytics database: {exc}")
    # Print API key/secret status (masked)
    kraken_key = os.environ.get("KRAKEN_API_KEY", "")
    kraken_secret = os.environ.get("KRAKEN_SECRET", "")
    coinbase_key = os.environ.get("COINBASE_API_KEY", "")
    coinbase_secret = os.environ.get("COINBASE_SECRET", "")
    def mask(s):
        """Auto-generated docstring."""
        return s[:4] + "..." + s[-4:] if len(s) > 8 else "(empty)"
    print(f"[startup] CryptoTrader is running in {mode} mode.")
    print(f"[startup] Kraken key: {mask(kraken_key)}, secret: {mask(kraken_secret)}")
    print(f"[startup] Coinbase key: {mask(coinbase_key)}, secret: {mask(coinbase_secret)}")
    print(f"[startup] Trade mode: {mode}")
    print(f"[startup] Trade floors USD: global={MIN_TRADE_USD:.2f}, kraken={KRAKEN_MIN_TRADE_USD:.2f}, coinbase={COINBASE_MIN_TRADE_USD:.2f}")
    print(
        f"[startup] Micro mode: {MICRO_TRADE_MODE} | Kraken max min_cost={MICRO_KRAKEN_MAX_MIN_COST_USD:.2f} "
        f"| Coinbase skip below={MICRO_COINBASE_MIN_QUOTE_USD:.2f}"
    )
    print(f"[startup] Daily loss cap: ${DAILY_LOSS_CAP_USD:.2f} | Profit target: ${DAILY_PROFIT_TARGET_USD:.2f}")
    print(f"[startup] Sell profit threshold: {SELL_PROFIT_PCT}% | Stop loss: {SELL_STOP_LOSS_PCT}%")
    print(f"[startup] Volatility gate: {VOLATILITY_MIN_PCT:.2f}% min, spike x{VOLATILITY_SPIKE_MULTIPLIER:.2f}")
    print(f"[startup] Spread arbitrage threshold: {SPREAD_ARB_THRESHOLD_PCT:.2f}% | Monthly drawdown alert: {MONTHLY_DRAWDOWN_ALERT_PCT:.2f}%")
    print("[debug] Sending startup email...")
    startup_email_ok = send_email(
        "CryptoTrader LIVE Startup",
        f"Bot starting LIVE trading. Loss cap: ${DAILY_LOSS_CAP_USD:.2f}, Profit target: ${DAILY_PROFIT_TARGET_USD:.2f}."
    )
    print(f"[startup] Startup email status: {'sent' if startup_email_ok else 'failed'}")
    _update_status({
        "mode": mode,
        "startup_email_ok": startup_email_ok,
        "state": "starting",
    })
    print("[startup] Launching periodic_email thread...")
    t = threading.Thread(target=periodic_email, daemon=True)
    t.start()
    print("[live] Starting continuous trading: scan -> execute -> monitor loop")
    print("[debug] Entering run_live_loop() - trading loop started.")

    def _place_order_with_mode(ex, ex_name, pair, action, amount_usd, amount_base, trade_mode="spot", leverage=1):
        """Unified order placement for spot vs margin with error handling and fallback."""
        order = None
        fallback_reason = None
        try:
            if action == "buy":
                if trade_mode == "margin" and ex_name.lower() == "kraken":
                    try:
                        params = {
                            "margintrading": "margin",
                            "leverage": leverage,
                        }
                        order = ex.create_margin_buy_order(pair, amount_base, params) if hasattr(ex, 'create_margin_buy_order') else None
                        if order is None:
                            raise Exception("create_margin_buy_order not available")
                        print(f"[live][MARGIN-BUY] {ex_name} {pair}: margin order placed with {leverage}x leverage, notional ${amount_usd:.2f}")
                    except Exception as margin_exc:
                        print(f"[live][MARGIN-FALLBACK] {ex_name} {pair}: margin order failed: {margin_exc}. Falling back to spot.")
                        fallback_reason = str(margin_exc)[:50]
                        trade_mode = "spot"
                        order = None
                if order is None and trade_mode == "spot":
                    if ex_name == "coinbase":
                        params = {"createMarketBuyOrderRequiresPrice": False}
                        order = ex.create_market_buy_order(pair, amount_usd, params=params)
                    else:
                        order = ex.create_market_buy_order(pair, amount_base)
                    print(f"[live][BUY] {ex_name} {pair}: spot order submitted, notional ${amount_usd:.2f}")
            elif action == "sell":
                order = ex.create_market_sell_order(pair, amount_base)
                print(f"[live][SELL] {ex_name} {pair}: order submitted, amount {amount_base:.8f}")
            return order, trade_mode, leverage, fallback_reason
        except Exception as exc:
            print(f"[live][ERROR] Failed to place {action} order on {ex_name} {pair}: {exc}")
            return None, trade_mode, leverage, str(exc)[:50]

    def spot_trading_and_airdrop_loop():
        """Auto-generated docstring."""
        print("[live] Entering spot_trading_and_airdrop_loop: will inspect wallets, then trade LIVE.")

        realized_pnl, open_pnl, today_trades = _get_daily_pnl()
        risk_breached, risk_msg = _check_daily_risk_cap(realized_pnl)
        if risk_breached:
            breached_summary = f"Realized PnL: ${realized_pnl:+.2f} | Today's trades: {today_trades}"
            print(f"[risk][ALERT] {risk_msg}: {breached_summary}")
            _alert_risk_cap_breach(breached_summary)
            # Continue monitoring but halt new trades for today
            print("[risk] Daily risk cap reached. Continuing to monitor but stopping new trades for today.")
            time.sleep(CYCLE_SLEEP_SECONDS)
            return

        exchanges = _build_exchange_clients()
        if not exchanges:
            print("[failover][ERROR] No exchanges available. Sleeping before retry.")
            time.sleep(CYCLE_SLEEP_SECONDS)
            return

        cycle_scanned = 0
        cycle_executed = 0
        cycle_errors = 0
        volatility_gate_hits = 0
        momentum_fallback_hits = 0
        dca_fallback_hits = 0
        trend_fallback_hits = 0
        spread_opportunities = 0
        margin_trades_this_cycle = 0
        margin_decisions = []
        total_equity_usd = 0.0
        active_exchange_names = [exchange_name for exchange_name, _ in exchanges]
        balances_by_exchange = {}
        exchange_map = {exchange_name: exchange for exchange_name, exchange in exchanges}
        now_utc = datetime.now(timezone.utc)
        trend_metadata = _load_trend_model_metadata()
        weekly_train_status = "weekday_no_train"
        if _should_run_weekly_trend_training(now_utc, trend_metadata):
            trend_metadata, weekly_train_status = _train_weekly_trend_model(now_utc, trend_metadata)
        elif now_utc.weekday() == 6:
            weekly_train_status = "already_trained_this_week"
        trend_model_fresh = _is_trend_model_fresh(trend_metadata, now_utc)
        trend_pairs_total = len((trend_metadata.get("trends_by_pair", {}) or {})) if isinstance(trend_metadata, dict) else 0
        if trend_model_fresh and trend_pairs_total > 0:
            print(f"[trend][INFO] Using fresh trend metadata: {trend_pairs_total} pair profiles")

        volatility_factor, zero_trade_streak = _volatility_relax_factor()
        if volatility_factor < 1.0:
            print(
                f"[adaptive] zero-trade streak={zero_trade_streak}; "
                f"volatility threshold factor relaxed to x{volatility_factor:.2f}"
            )

        # --- Spot Trading ---
        for ex_name, ex in exchanges:
            dca_buys_for_exchange = 0
            try:
                snapshot = _build_wallet_snapshot(ex)
                print(_format_wallet_snapshot(ex_name, snapshot))
                total_equity_usd += _safe_float(snapshot.get("estimated_total_usd"), 0.0)
            except Exception as exc:
                print(f"[wallet][ERROR] {ex_name}: {exc}")
                cycle_errors += 1

            pairs = _pairs_for_exchange(ex)
            try:
                balance = ex.fetch_balance()
                balances_by_exchange[ex_name] = balance
            except Exception as exc:
                print(f"[live][ERROR] {ex_name}: could not fetch balance before scan: {exc}")
                cycle_errors += 1
                continue

            if MICRO_TRADE_MODE and ex_name == "coinbase":
                coinbase_quote_usd = _quote_liquidity_usd(balance)
                if coinbase_quote_usd < MICRO_COINBASE_MIN_QUOTE_USD:
                    print(
                        f"[micro][SKIP] coinbase: quote liquidity ${coinbase_quote_usd:.2f} "
                        f"below ${MICRO_COINBASE_MIN_QUOTE_USD:.2f}, routing micro trades to kraken"
                    )
                    continue

            pairs = _prioritize_pairs_with_quote_balance(pairs, balance, ex_name)
            if trend_model_fresh and trend_pairs_total > 0:
                pairs = _prioritize_pairs_with_trend(pairs, trend_metadata)
            if MICRO_TRADE_MODE and ex_name == "kraken":
                before_count = len(pairs)
                pairs = _filter_micro_pairs_for_kraken(ex, pairs)
                print(
                    f"[micro] kraken pair filter: {before_count} -> {len(pairs)} "
                    f"(min_cost <= ${MICRO_KRAKEN_MAX_MIN_COST_USD:.2f})"
                )
            print(f"[pairs] {ex_name}: scanning {len(pairs)} spot pairs quoted in {PAIR_QUOTES} (quote-balance prioritized)")

            for pair in pairs:
                try:
                    cycle_scanned += 1
                    print(f"[live] [TRADE] Fetching ticker for {pair} on {ex_name}")
                    ticker = ex.fetch_ticker(pair)
                    price = _safe_float(ticker.get("last"), 0.0)
                    if price <= 0:
                        print(f"[limits][SKIP] {ex_name} {pair}: invalid price {price}")
                        continue

                    print(f"[live] [TRADE] {ex_name} {pair} price: {price}")
                    base_asset, quote_asset = pair.split("/")
                    totals = balance.get("total", {}) or {}
                    quote_balance = _safe_float(balance.get("total", {}).get(quote_asset, 0.0), 0.0)
                    base_balance = _safe_float(totals.get(base_asset, 0.0), 0.0)
                    change_pct = _safe_float(ticker.get("percentage"), 0.0)
                    base_notional_usd = base_balance * price
                    trade_usd, buy_min_cost = _adaptive_trade_notional(ex, pair, quote_balance, ex_name, price)

                    # Determine buy path (quote balance) and sell path (coin inventory).
                    buy_amount, buy_min_amount = _market_trade_amount(ex, pair, trade_usd, price)
                    can_buy = (
                        quote_balance >= _exchange_floor_usd(ex_name)
                        and trade_usd > 0
                        and (buy_min_cost <= 0 or trade_usd >= buy_min_cost)
                        and buy_amount >= buy_min_amount
                        and buy_amount > 0
                    )

                    reserve_base = base_balance * COIN_SELL_RESERVE_PCT
                    if zero_trade_streak >= SELL_ALL_AFTER_ZERO_TRADE_STREAK:
                        reserve_base = base_balance * min(COIN_SELL_RESERVE_PCT, max(0.0, SELL_ALL_RESERVE_PCT))
                    sellable_base = max(0.0, base_balance - reserve_base)
                    sell_target_usd = min(MAX_TRADE_USD, max(MIN_TRADE_USD, base_notional_usd * SELL_NOTIONAL_PCT))
                    sell_amount, sell_min_amount = _market_trade_amount(ex, pair, sell_target_usd, price)
                    sell_amount = min(sell_amount, sellable_base)
                    sell_amount = _quantize_amount(ex, pair, sell_amount)
                    sell_min_cost = _market_min_cost(ex, pair)
                    can_sell = (
                        sell_amount >= sell_min_amount
                        and sell_amount > 0
                        and (sell_min_cost <= 0 or (sell_amount * price) >= sell_min_cost)
                    )

                    # If normal sell sizing is too small, attempt to liquidate most/all wallet holdings.
                    if not can_sell and base_balance > 0 and zero_trade_streak >= SELL_ALL_AFTER_ZERO_TRADE_STREAK:
                        liquidation_amount = _quantize_amount(
                            ex,
                            pair,
                            max(0.0, base_balance * (1.0 - max(0.0, SELL_ALL_RESERVE_PCT))),
                        )
                        liquidation_min_amount = _market_min_amount(ex, pair)
                        liquidation_min_cost = _market_min_cost(ex, pair)
                        liquidation_notional = _safe_float(liquidation_amount * price, 0.0)
                        if (
                            liquidation_amount >= liquidation_min_amount
                            and liquidation_amount > 0
                            and (liquidation_min_cost <= 0 or liquidation_notional >= liquidation_min_cost)
                        ):
                            sell_amount = liquidation_amount
                            sell_min_amount = liquidation_min_amount
                            sell_min_cost = liquidation_min_cost
                            can_sell = True
                            print(
                                f"[sell][FALLBACK] {ex_name} {pair}: liquidation enabled after streak={zero_trade_streak}; "
                                f"selling {sell_amount:.8f} {base_asset} (~${liquidation_notional:.2f})"
                            )

                    print(
                        f"[debug][BALANCES] {ex_name} {pair}: {quote_asset}={quote_balance:.8f}, "
                        f"{base_asset}={base_balance:.8f}, buy_notional={trade_usd:.2f}, "
                        f"buy_min_cost={buy_min_cost:.2f}, base_notional={base_notional_usd:.2f}"
                    )

                    if cycle_executed >= MAX_TRADES_PER_EXCHANGE_CYCLE:
                        print(
                            f"[limits][SKIP] {ex_name}: reached cycle trade cap "
                            f"({MAX_TRADES_PER_EXCHANGE_CYCLE}), continuing scan without execution"
                        )
                        continue

                    # Extract trend info first (metadata only, no API call) so we can
                    # skip the expensive OHLCV volatility fetch for confirmed buy signals.
                    trend_info = {}
                    trend_direction = "neutral"
                    trend_strength = 0.0
                    trend_signal = "neutral"
                    if trend_model_fresh:
                        trend_info = (trend_metadata.get("trends_by_pair", {}) or {}).get(pair, {})
                        trend_direction = str(trend_info.get("trend_direction", "neutral")).lower()
                        trend_strength = _safe_float(trend_info.get("trend_strength"), 0.0)
                        trend_signal = str(trend_info.get("momentum_signal", "neutral")).lower()

                    # Skip slow OHLCV fetch when the trend model already confirms a strong buy
                    if (
                        TREND_SKIP_VOLATILITY_FOR_BUY_SIGNALS
                        and trend_model_fresh
                        and trend_signal in ("strong_buy", "buy")
                        and can_buy
                    ):
                        volatility_ok, recent_vol, volatility_threshold = True, 0.0, 0.0
                    else:
                        volatility_ok, recent_vol, volatility_threshold = _volatility_signal(
                            ex, pair, threshold_factor=volatility_factor
                        )

                    trend_override = False
                    momentum_override = False
                    dca_override = False
                    if not volatility_ok:
                        if (
                            TREND_ENABLE_AGGRESSIVE_ENTRY
                            and can_buy
                            and trend_direction == "up"
                            and trend_strength >= TREND_STRONG_BUY_STRENGTH
                            and change_pct >= TREND_FALLBACK_MIN_CHANGE_PCT
                            and trend_signal in ("strong_buy", "buy")
                        ):
                            trend_override = True
                            trend_fallback_hits += 1
                            print(
                                f"[trend][FALLBACK] {ex_name} {pair}: direction=up strength={trend_strength:.2f} "
                                f"signal={trend_signal} change_pct={change_pct:+.2f}%"
                            )
                        elif ALLOW_MOMENTUM_BUY_FALLBACK and can_buy and change_pct >= MOMENTUM_BUY_CHANGE_PCT:
                            momentum_override = True
                            momentum_fallback_hits += 1
                            print(
                                f"[momentum][FALLBACK] {ex_name} {pair}: "
                                f"change_pct={change_pct:+.2f}% >= {MOMENTUM_BUY_CHANGE_PCT:.2f}% with can_buy=True"
                            )
                        elif (
                            ENABLE_DCA_ENTRY_FALLBACK
                            and can_buy
                            and zero_trade_streak >= DCA_AFTER_ZERO_TRADE_STREAK
                            and dca_buys_for_exchange < DCA_MAX_BUYS_PER_EXCHANGE_CYCLE
                            and change_pct >= DCA_MIN_CHANGE_PCT
                        ):
                            dca_override = True
                            dca_fallback_hits += 1
                            dca_buys_for_exchange += 1
                            print(
                                f"[dca][FALLBACK] {ex_name} {pair}: zero_streak={zero_trade_streak}, "
                                f"change_pct={change_pct:+.2f}% >= {DCA_MIN_CHANGE_PCT:+.2f}%"
                            )
                        else:
                            print(
                                f"[volatility][SKIP] {ex_name} {pair}: recent_vol={recent_vol:.2f}% "
                                f"threshold={volatility_threshold:.2f}%"
                            )
                            continue
                    else:
                        volatility_gate_hits += 1

                    action = None
                    if trend_override:
                        action = "buy"
                    elif momentum_override:
                        action = "buy"
                    elif dca_override:
                        action = "buy"
                    elif can_buy and can_sell:
                        if zero_trade_streak >= SELL_ALL_AFTER_ZERO_TRADE_STREAK and base_notional_usd > trade_usd:
                            action = "sell"
                        else:
                            action = "buy" if change_pct >= 0 else "sell"
                    elif can_buy:
                        action = "buy"
                    elif can_sell:
                        action = "sell"

                    if not action:
                        print(
                            f"[limits][SKIP] {ex_name} {pair}: no valid action "
                            f"(can_buy={can_buy}, can_sell={can_sell}, change_pct={change_pct:+.2f})"
                        )
                        continue

                    # For sell actions, check profit/stop conditions
                    sell_reason = None
                    if action == "sell" and price > 0:
                        entry_price = _safe_float(ticker.get("open"), price)
                        sell_reason, pnl_pct = _check_profit_stop_condition(price, entry_price, change_pct, pair)
                        if sell_reason:
                            print(
                                f"[profit-stop] {ex_name} {pair}: {sell_reason} triggered at {pnl_pct:+.2f}% PnL. "
                                f"Selling {sell_amount:.8f} {base_asset}"
                            )

                    order = None
                    trade_mode = "spot"
                    leverage = 1
                    edge_score = 0.0
                    margin_fallback_reason = None
                    
                    # For BUY actions, determine if margin should be used
                    if action == "buy":
                        trend_signal = (trend_info or {}).get("signal", "hold") if trend_override else "hold"
                        trend_strength = _safe_float((trend_info or {}).get("trend_strength", 0.0), 0.0) if trend_override else 0.0
                        fallback_type = "trend" if trend_override else ("momentum" if momentum_override else ("dca" if dca_override else "manual"))
                        edge_score = _compute_edge_score(
                            trend_direction, trend_strength, trend_signal,
                            volatility_ok, change_pct, fallback_type
                        )
                        
                        should_margin, margin_reason = _should_use_margin_for_buy(
                            edge_score, _safe_float(quote_balance, 0.0), pair, ex_name,
                            trade_usd, KRAKEN_MARGIN_DEFAULT_LEVERAGE
                        )
                        if should_margin:
                            trade_mode = "margin"
                            leverage = KRAKEN_MARGIN_DEFAULT_LEVERAGE
                            margin_decisions.append({
                                "pair": pair, "edge_score": edge_score, "reason": margin_reason
                            })
                        else:
                            margin_fallback_reason = margin_reason
                    
                    # LIVE TRADING - no dry-run mode
                    try:
                        if action == "buy":
                            order, trade_mode, leverage, fallback_reason = _place_order_with_mode(
                                ex, ex_name, pair, action, trade_usd, buy_amount, trade_mode, leverage
                            )
                            if fallback_reason:
                                margin_fallback_reason = fallback_reason
                        else:
                            order = ex.create_market_sell_order(pair, sell_amount)
                            print(
                                f"[live][SELL] {ex_name} {pair}: order submitted, amount {sell_amount:.8f} {base_asset} "
                                f"(reason: {sell_reason or 'rebalance'})"
                            )
                        print(f"[live] [TRADE] Order details: {order.get('id', 'no-id') if order else 'no-order'} | Status: {order.get('status', 'unknown') if order else 'failed'}")
                    except Exception as order_exc:
                        print(f"[live][ERROR] Failed to place {action} order on {ex_name} {pair}: {order_exc}")
                        cycle_errors += 1
                        continue

                    if order:
                        if trade_mode == "margin":
                            margin_trades_this_cycle += 1
                        cycle_executed += 1
                        try:
                            trade_record = {
                                "exchange": ex_name,
                                "pair": pair,
                                "side": action,
                                "mode": mode,
                                "opened_at": datetime.now(timezone.utc).isoformat(),
                                "order_id": order.get('id', ''),
                                "size_usd": trade_usd if action == "buy" else sell_amount * price,
                                "base_amount": buy_amount if action == "buy" else sell_amount,
                                "price": price,
                                "change_pct": change_pct,
                                "profit_stop_reason": sell_reason,
                                "strategy": (
                                    "trend_fallback"
                                    if trend_override
                                    else (
                                        "momentum_fallback"
                                        if momentum_override
                                        else ("dca_fallback" if dca_override else "volatility_spot_cycle")
                                    )
                                ),
                                "status": "submitted",
                            }
                            _append_trade_record(trade_record, trade_mode, leverage, edge_score, margin_fallback_reason)
                        except Exception as exc:
                            print(f"[live][WARN] Could not persist trade record for {pair}: {exc}")

                        # Refresh balances after real orders to avoid stale available funds.
                        try:
                            balance = ex.fetch_balance()
                            balances_by_exchange[ex_name] = balance
                        except Exception as exc:
                            print(f"[live][WARN] {ex_name}: balance refresh failed after order: {exc}")
                except Exception as e:
                    print(f"[live] Error trading {pair} on {ex_name}: {e}")
                    traceback.print_exc()
                    cycle_errors += 1

        common_pairs = None
        for ex_name, ex in exchanges:
            pair_set = set(_pairs_for_exchange(ex))
            common_pairs = pair_set if common_pairs is None else common_pairs.intersection(pair_set)
        for pair in sorted(common_pairs or [])[:SPREAD_ARB_MAX_PAIRS]:
            if cycle_executed >= MAX_TRADES_PER_EXCHANGE_CYCLE:
                break
            if _execute_spread_trade(pair, exchange_map, balances_by_exchange):
                spread_opportunities += 1
                cycle_executed += 1

        # --- Airdrop Trigger Check ---
        try:
            print("[airdrop][DEBUG] Checking Kraken USD balance for airdrop trigger...")
            ex = _load_exchange("kraken")
            balance = ex.fetch_balance()
            usd_amt = float(balance.get("total", {}).get("USD", 0.0))
            print(f"[airdrop][DEBUG] Kraken USD balance: ${usd_amt:.2f} (threshold: ${AIRDROP_TRIGGER_THRESHOLD_USD})")
            if usd_amt >= AIRDROP_TRIGGER_THRESHOLD_USD:
                print(f"[airdrop][DEBUG] Threshold met! Will scan and send email...")
                cmd_scan()
                email_result = send_email("Airdrop Trigger Met (Auto)", f"Kraken USD balance: ${usd_amt:.2f}\nAirdrop scan triggered automatically by trading loop.")
                print(f"[airdrop][DEBUG] Email send result: {email_result}")
            else:
                print(f"[airdrop][DEBUG] Threshold not met. No email sent.")
        except Exception as e:
            print(f"[airdrop][ERROR] Exception during airdrop check: {e}")
            cycle_errors += 1

        _maybe_alert_no_trades(
            cycle_executed, zero_trade_streak, volatility_gate_hits,
            volatility_factor, balances_by_exchange, exchanges,
        )
        drawdown_pct = _alert_monthly_drawdown(total_equity_usd)

        realized_pnl_cycle, open_pnl_cycle, _ = _get_daily_pnl()
        cycle_summary = {
            "recorded_at": _utc_now_iso(),
            "mode": mode,
            "state": "running",
            "pairs_scanned": cycle_scanned,
            "trades_executed": cycle_executed,
            "errors": cycle_errors,
            "daily_realized_pnl": realized_pnl_cycle,
            "daily_open_pnl": open_pnl_cycle,
            "total_equity_usd": total_equity_usd,
            "volatility_gate_hits": volatility_gate_hits,
            "spread_opportunities": spread_opportunities,
            "active_exchanges": active_exchange_names,
            "cycle_notes": (
                f"drawdown_pct={drawdown_pct:.2f}; "
                f"volatility_factor={volatility_factor:.2f}; "
                f"zero_trade_streak={zero_trade_streak}; "
                f"weekly_train_status={weekly_train_status}; "
                f"trend_model_fresh={trend_model_fresh}; "
                f"trend_pairs_total={trend_pairs_total}; "
                f"trend_fallback_hits={trend_fallback_hits}; "
                f"momentum_fallback_hits={momentum_fallback_hits}; "
                f"dca_fallback_hits={dca_fallback_hits}; "
                f"backfill_complete={trend_metadata.get('backfill_complete', False) if isinstance(trend_metadata, dict) else False}; "
                f"backfill_sundays_done={trend_metadata.get('backfill_sundays_done', 0) if isinstance(trend_metadata, dict) else 0}; "
                f"backfill_pairs_done={len(trend_metadata.get('backfill_pairs_processed') or []) if isinstance(trend_metadata, dict) else 0}"
            ),
            "loss_cap_usd": DAILY_LOSS_CAP_USD,
            "profit_target_usd": DAILY_PROFIT_TARGET_USD,
            "sleep_seconds": CYCLE_SLEEP_SECONDS,
        }
        print(f"[live][SUMMARY] {cycle_summary}")
        _update_status(cycle_summary)
        _record_cycle_summary(cycle_summary)

        print(f"[live] spot_trading_and_airdrop_loop completed one cycle. Sleeping {CYCLE_SLEEP_SECONDS}s.")
        # Watchdog print to confirm 24/7 operation
        print(f"[watchdog] {datetime.now(timezone.utc).isoformat()} - Bot is alive and running 24/7.")
        time.sleep(CYCLE_SLEEP_SECONDS)

    while True:
        try:
            spot_trading_and_airdrop_loop()
        except Exception as exc:
            print(f"[live][FATAL-RECOVERABLE] Loop exception: {exc}")
            traceback.print_exc()
            _update_status({
                "mode": mode,
                "state": "recovering",
                "last_exception": str(exc),
            })
            time.sleep(10)
    print("[debug] Exiting run_live_loop() - trading loop ended.")

# --- Standard Python entry point ---
if __name__ == "__main__":
    main()
