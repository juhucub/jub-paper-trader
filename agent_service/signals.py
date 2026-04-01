"""Signal generation from feature vectors."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class SignalModelConfig:
    """Weights for converting features into a raw alpha signal."""

    momentum_weight: float = 0.28
    mean_reversion_weight: float = 0.22
    returns_weight: float = 0.16
    sentiment_weight: float = 0.14
    volatility_penalty: float = 0.10
    spread_penalty: float = 0.06
    illiquidity_penalty: float = 0.02
    volume_trend_weight: float = 0.10


class SignalGenerator:
    """Generate per-symbol raw and structured signal payloads."""

    def __init__(self, config: SignalModelConfig | None = None) -> None:
        self.config = config or SignalModelConfig()

    #signals_i = f(features_i) for each stock i
    def generate(self, features: dict[str, dict[str, float]]) -> dict[str, dict[str, float | str]]:
        liquidity_values = [float(row.get("liquidity", 0.0)) for row in features.values()]
        max_liquidity = max(liquidity_values) if liquidity_values else 0.0

        signals: dict[str, dict[str, float | str]] = {}
        #for each stock i, generate signals_i = f(features_i) 
        for symbol, row in features.items():
            raw_score = self._raw_score(row, max_liquidity=max_liquidity)
            expected_return = raw_score
            probability_up = max(0.0, min(1.0, 0.5 + (raw_score * 2.0)))

            if raw_score > 0:
                direction = "long"
                action = "buy"
            elif raw_score < 0:
                direction = "short"
                action = "sell"
            else:
                direction = "flat"
                action = "hold"

            strength = abs(raw_score)
            confidence = min(1.0, max(0.0, strength * 20.0))
            expected_horizon = "15m" if strength >= 0.03 else "30m"

            signals[symbol] = {
                "score": raw_score,
                "action": action,
                "expected_return": expected_return,
                "probability_up": probability_up,
                "direction": direction,
                "strength": strength,
                "confidence": confidence,
                "expected_horizon": expected_horizon,
            }

        #signals[symbol] = signals_i
        return signals

    def _raw_score(self, feature_row: dict[str, float], max_liquidity: float) -> float:
        cfg = self.config

        liquidity = float(feature_row.get("liquidity", 0.0))
        liquidity_penalty = 0.0
        if max_liquidity > 0:
            liquidity_penalty = 1.0 - min(1.0, liquidity / max_liquidity)

        #
        score = (
            cfg.momentum_weight * float(feature_row.get("momentum", 0.0))
            + cfg.mean_reversion_weight * float(feature_row.get("mean_reversion", 0.0))
            + cfg.returns_weight * float(feature_row.get("returns", 0.0))
            + cfg.sentiment_weight * float(feature_row.get("sentiment_score", 0.0))
            + cfg.volume_trend_weight * float(feature_row.get("volume_trend", 0.0))
            - cfg.volatility_penalty * float(feature_row.get("volatility", 0.0))
            - cfg.spread_penalty * float(feature_row.get("bid_ask_spread", 0.0))
            - cfg.illiquidity_penalty * liquidity_penalty
        )

        return max(-1.0, min(1.0, score))
