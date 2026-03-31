from statistics import mean
from typing import Any

def _fmt_float(value, decimals=2):
    if value is None:
        return "n/a"
    if isinstance(value, dict):
        return str(value)
    return f"{float(value):,.{decimals}f}"

def _fmt_optional(value: Any) -> str:
    return "n/a" if value is None else str(value)


def _normalize_reason(value: Any) -> str:
    if value is None:
        return "n/a"
    return str(value).replace("_", " ")


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
    mid = ((ap + bp) / 2.0) if ap is not None and bp is not None else None
    spread = (ap - bp) if ap is not None and bp is not None else None
    spread_pct = ((ap - bp) / bp * 100.0) if ap is not None and bp is not None and bp > 0 else None

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
        "policy_action": None,
        "policy_reason": None,
        "portfolio_constraints_triggered": [],
    }

def print_symbol_summary(summary: dict) -> None:
    spread_pct = summary.get("spread_pct")
    spread_pct_display = f"{_fmt_float(spread_pct, 3)}%" if spread_pct is not None else "n/a"
    signal = summary.get("signal")

    if isinstance(signal, dict):
        signal_display = (
            f"action={signal.get('action')} "
            f"score={_fmt_float(signal.get('score'), 6)} "
            f"confidence={_fmt_float(signal.get('confidence'), 3)}"
        )
    else:
        signal_display = _fmt_float(signal, 6) if signal is not None else "n/a"

    target_weight = summary.get("target_weight")
    target_weight_display = _fmt_float(target_weight, 4) if target_weight is not None else "n/a"

    blocked_by = summary.get("blocked_reason")
    decision_reason = summary.get("decision_reason")
    policy_constraints = summary.get("portfolio_constraints_triggered") or []
    print("\n=== BOT DECISION SUMMARY ===")
    print(f"Symbol:       {summary['symbol']}")
    print(f"Bars:         {summary['bar_count']}")
    print(f"First close:  {_fmt_float(summary['first_close'])}")
    print(f"Last close:   {_fmt_float(summary['last_close'])}")
    print(f"Range:        {_fmt_float(summary['min_close'])} - {_fmt_float(summary['max_close'])}")
    print(f"Avg close:    {_fmt_float(summary['avg_close'])}")
    print(f"Avg volume:   {_fmt_float(summary['avg_volume'])}")
    print(f"Bid / Ask:    {_fmt_float(summary['bid'])} / {_fmt_float(summary['ask'])}")
    print(f"Mid price:    {_fmt_float(summary['mid'])}")
    print(f"Spread:       {_fmt_float(summary['spread'])}")
    print(f"Spread %:     {spread_pct_display}")
    print(f"Quote time:   {_fmt_optional(summary['quote_time'])}")
    print(f"Signal:       {signal_display}")
    print(f"Target wt:    {target_weight_display}")
    print(f"Order side:   {_fmt_optional(summary['candidate_order_side'])}")
    print(f"Order qty:    {_fmt_float(summary['candidate_order_qty'], 4)}")
    print(f"Status:       {_fmt_optional(summary['decision_status'])}")
    print(f"Reason:       {_normalize_reason(decision_reason)}")
    print(f"Policy:       {_fmt_optional(summary.get('policy_action'))}")
    print(f"Policy why:   {_normalize_reason(summary.get('policy_reason'))}")
    print(f"Constraints:  {', '.join(policy_constraints) if policy_constraints else 'none'}")
    print(f"Blocked by:   {_normalize_reason(blocked_by)}")