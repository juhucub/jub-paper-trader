#Lightweight optimizer layer used by the orchestration cycle.

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class OptimizerQPO:
    """
    Deterministic stand-in for NVIDIA QP optimizer.

    v1 behavior:
    - Convert signal strengths into long-only target weights.
    - Reserve a configurable cash buffer.
    - Add benchmark tracking metadata for future v3 extensions.
    """

    max_symbol_weight: float = 0.20
    cash_buffer: float = 0.05

    def optimize_target_weights(
        self,
        scored_signals: dict[str, float],
        benchmark_symbol: str = "SPY",
    ) -> dict[str, float]:
        positive = {symbol: max(score, 0.0) for symbol, score in scored_signals.items()}
        total_signal = sum(positive.values())
        if total_signal <= 0:
            return {benchmark_symbol: 0.0}

        investable = max(0.0, 1.0 - self.cash_buffer)
        target_weights: dict[str, float] = {}
        for symbol, score in positive.items():
            raw_weight = investable * (score / total_signal)
            target_weights[symbol] = min(raw_weight, self.max_symbol_weight)

        allocated = sum(target_weights.values())
        if allocated > 0 and allocated != investable:
            scale = investable / allocated
            for symbol in list(target_weights):
                target_weights[symbol] *= scale

        target_weights[benchmark_symbol] = 0.0
        return target_weights
