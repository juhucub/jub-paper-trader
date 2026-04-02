"""Signal normalization and ranking utilities."""

from __future__ import annotations

from statistics import mean, pstdev


def normalize_and_rank_signals(
    signals: dict[str, dict[str, float | str]],
    top_n: int = 1,
    bottom_n: int = 1,
    debug: bool = False,
) -> dict[str, dict[str, float | str]]:
    """Add z-score, rank, and bucket labels to signal payloads."""

    if not signals:
        return {}

    raw_scores = {symbol: float(signal.get("score", 0.0)) for symbol, signal in signals.items()}
    score_values = list(raw_scores.values())
    avg_score = mean(score_values)
    std_score = pstdev(score_values) if len(score_values) > 1 else 0.0

    ranked_symbols = sorted(raw_scores, key=raw_scores.get, reverse=True)
    buy_cutoff = min(top_n, len(ranked_symbols))
    sell_cutoff = min(bottom_n, len(ranked_symbols))

    for rank, symbol in enumerate(ranked_symbols, start=1):
        signal = signals[symbol]
        raw_score = raw_scores[symbol]
        z_score = (raw_score - avg_score) / std_score if std_score > 0 else 0.0
        clipped_z = max(-2.5, min(2.5, z_score))
        normalized_score = clipped_z / 2.5
        if debug:
            print(f"Symbol: {symbol}, Raw Score: {raw_score:.4f}, Z-Score: {z_score:.4f}, Rank: {rank}")
        if rank <= buy_cutoff:
            rank_bucket = "BUY"
        elif rank > len(ranked_symbols) - sell_cutoff:
            rank_bucket = "SELL"
        else:
            rank_bucket = "HOLD"

        signal["z_score"] = z_score
        signal["normalized_score"] = normalized_score
        signal["rank"] = rank
        signal["rank_bucket"] = rank_bucket
        signal["universe_size"] = len(ranked_symbols)
        
    return signals
