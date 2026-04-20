"""Signal generation from feature vectors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _clamp(value: float, floor: float, ceiling: float) -> float:
    return max(floor, min(ceiling, value))


@dataclass(slots=True, frozen=True)
class SignalModelConfig:
    """Weights for converting features into a raw alpha signal."""

    momentum_weight: float = 0.28
    mean_reversion_weight: float = 0.22
    short_return_weight: float = 0.16
    sentiment_weight: float = 0.14
    volatility_penalty: float = 0.10
    spread_penalty: float = 0.06
    illiquidity_penalty: float = 0.02
    volume_trend_weight: float = 0.10


class SignalGenerator:
    """Generate per-symbol raw and structured signal payloads."""

    def __init__(self, config: SignalModelConfig | None = None) -> None:
        self.config = config or SignalModelConfig()

    def generate(self, features: dict[str, dict[str, float]]) -> dict[str, dict[str, Any]]:
        """Build proposal-oriented statistical signal payloads with auditable diagnostics."""

        liquidity_values = [float(row.get("liquidity", 0.0) or 0.0) for row in features.values()]
        max_liquidity = max(liquidity_values) if liquidity_values else 0.0

        signals: dict[str, dict[str, Any]] = {}
        for symbol, row in features.items():
            diagnostics = self._build_diagnostics(row, max_liquidity=max_liquidity)
            raw_score = float(diagnostics["score"])
            expected_return = float(diagnostics["expected_return"])
            probability_up = _clamp(0.5 + (expected_return * 3.0), 0.0, 1.0)

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
            confidence = float(diagnostics["confidence"])
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
                "diagnostics": diagnostics,
            }

        return signals

    def _build_diagnostics(self, feature_row: dict[str, float], max_liquidity: float) -> dict[str, Any]:
        cfg = self.config

        momentum = float(feature_row.get("momentum", 0.0) or 0.0)
        mean_reversion = float(feature_row.get("mean_reversion", 0.0) or 0.0)
        short_return = float(feature_row.get("returns", 0.0) or 0.0)
        sentiment = float(feature_row.get("sentiment_score", 0.0) or 0.0)
        volume_trend = float(feature_row.get("volume_trend", 0.0) or 0.0)
        volatility = float(feature_row.get("volatility", 0.0) or 0.0)
        spread = float(feature_row.get("bid_ask_spread", 0.0) or 0.0)
        liquidity = float(feature_row.get("liquidity", 0.0) or 0.0)

        liquidity_penalty_raw = 0.0
        if max_liquidity > 0.0:
            liquidity_penalty_raw = 1.0 - min(1.0, liquidity / max_liquidity)

        expected_return_components = {
            "momentum": cfg.momentum_weight * momentum,
            "mean_reversion": cfg.mean_reversion_weight * mean_reversion,
            "short_return": cfg.short_return_weight * short_return,
            "sentiment": cfg.sentiment_weight * sentiment,
            "volume_trend": cfg.volume_trend_weight * volume_trend,
        }
        execution_penalties = {
            "volatility": cfg.volatility_penalty * volatility,
            "spread": cfg.spread_penalty * spread,
            "illiquidity": cfg.illiquidity_penalty * liquidity_penalty_raw,
        }

        raw_score = sum(expected_return_components.values()) - sum(execution_penalties.values())
        raw_score = _clamp(raw_score, -1.0, 1.0)

        liquidity_risk = _clamp(liquidity_penalty_raw, 0.0, 1.0)
        execution_risk = _clamp((spread * 12.0) + (volatility * 4.0) + (liquidity_risk * 0.8), 0.0, 1.0)
        uncertainty_score = _clamp((volatility * 4.5) + (liquidity_risk * 0.6), 0.0, 1.0)

        anomaly_flags: list[str] = []
        if volatility >= 0.08:
            anomaly_flags.append("elevated_volatility")
        if spread >= 0.01:
            anomaly_flags.append("wide_spread")
        if liquidity <= 250_000.0:
            anomaly_flags.append("thin_liquidity")

        volatility_regime = "high" if volatility >= 0.08 else "normal" if volatility >= 0.03 else "low"
        confidence = _clamp(
            (abs(raw_score) * 10.0)
            + max(0.0, 0.15 - (execution_risk * 0.10))
            + max(0.0, 0.10 - (uncertainty_score * 0.05)),
            0.0,
            1.0,
        )
        expected_return = raw_score * _clamp(1.0 - (execution_risk * 0.35), 0.45, 1.0)

        return {
            "score": raw_score,
            "expected_return": expected_return,
            "confidence": confidence,
            "expected_return_decomposition": {
                "positive_components": expected_return_components,
                "penalties": execution_penalties,
                "net_expected_return": expected_return,
            },
            "regime_evidence": {
                "volatility_regime": volatility_regime,
                "volatility": volatility,
                "volume_trend": volume_trend,
                "momentum": momentum,
                "mean_reversion": mean_reversion,
            },
            "execution_risk": {
                "score": execution_risk,
                "liquidity_risk": liquidity_risk,
                "spread": spread,
                "avg_dollar_volume": float(feature_row.get("avg_dollar_volume", liquidity) or liquidity),
            },
            "uncertainty": {
                "score": uncertainty_score,
                "confidence_inputs": {
                    "raw_score_abs": abs(raw_score),
                    "execution_risk": execution_risk,
                    "liquidity_risk": liquidity_risk,
                    "volatility": volatility,
                },
                "anomaly_flags": anomaly_flags,
            },
            "anomaly_flags": anomaly_flags,
        }

