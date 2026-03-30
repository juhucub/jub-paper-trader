from statistics import mean
from typing import Any

def summarize_symbol_decision(symbol: str, bars: list[dict], quote: dict) -> dict[str, Any]:
    closes = [float(bar["c"]) for bar in bars if bar.get("c") is not None]
    volumes = [float(bar.get("v", 0.0)) for bar in bars]

    first_close = closes[0] if closes else 0.0
    last_close = closes[-1] if closes else 0.0
    min_close = min(closes) if closes else 0.0
    max_close = max(closes) if closes else 0.0
    avg_close = mean(closes) if closes else 0.0

    ap = quote.get("ap")
    bp = quote.get("bp")
    mid = ((ap + bp) / 2.0) if ap and bp else None
    spread = (ap - bp) if ap and bp else None
    spread_pct = ((ap - bp) / bp * 100.0) if ap and bp and bp > 0 else None

    return {
        "symbol": symbol,
        "bar_count": len(bars),
        "first_close": first_close,
        "last_close": last_close,
        "min_close": min_close,
        "max_close": max_close,
        "avg_close": avg_close,
        "avg_volume": mean(volumes) if volumes else 0.0,
        "ask": ap,
        "bid": bp,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
        "quote_time": quote.get("t"),

        # decision fields
        "signal": None,
        "target_weight": None,
        "candidate_order_side": None,
        "candidate_order_qty": None,
        "decision_status": "NO_FEATURE_DECISION_YET",
        "decision_reason": None,
        "blocked_reason": None,
    }

def print_symbol_summary(summary: dict) -> None:
    print("\n=== BOT DECISION SUMMARY ===")
    print(f"Symbol:       {summary['symbol']}")
    print(f"Bars:         {summary['bar_count']}")
    print(f"First close:  {summary['first_close']:.2f}")
    print(f"Last close:   {summary['last_close']:.2f}")
    print(f"Range:        {summary['min_close']:.2f} - {summary['max_close']:.2f}")
    print(f"Avg close:    {summary['avg_close']:.2f}")
    print(f"Avg volume:   {summary['avg_volume']:.2f}")
    print(f"Bid / Ask:    {summary['bid']} / {summary['ask']}")
    print(f"Mid price:    {summary['mid']}")
    print(f"Spread:       {summary['spread']}")
    print(f"Spread %:     {summary['spread_pct']}")
    print(f"Quote time:   {summary['quote_time']}")
    print(f"Signal:       {summary['signal']}")
    print(f"Target wt:    {summary['target_weight']}")
    print(f"Order side:   {summary['candidate_order_side']}")
    print(f"Order qty:    {summary['candidate_order_qty']}")
    print(f"Status:       {summary['decision_status']}")
    print(f"Reason:       {summary['decision_reason']}")
    print(f"Blocked by:   {summary['blocked_reason']}")