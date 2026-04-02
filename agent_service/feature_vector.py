from __future__ import annotations

from statistics import mean, pstdev
""" 3) Feature Engineering - feature_vector.py
-Cleaned, raw data is transformed into signals (features) 

For each stock i:

    *Compute features_i:
        - Momentum (Trend Strength)
        - Mean Reversion (Overbought / Oversold)
        - Volatility (Risk / Uncertainty)
        - Liquidity (Can we actually trade it?)
        - Bid-Ask Spread 
        - Volume Trend (Participation)
        - Returns
        - Sentiment Scores

```bash
    features_i = [
        momentum_i,
        mean_reversion_i,
        volatility_i,
        liquidity_i,
        ...
    ]
"""

"""Feature engineering utilities for transforming market data into model-ready signals."""
class FeatureVector:
    """Build per-symbol feature vectors from raw bars/quotes/sentiment."""

    @classmethod
    def build(
        cls,
        bars: list[dict],
        quote: dict,
        sentiment_score: float = 0.0,
    ) -> dict[str, float]:
        closes = [float(bar.get("c", 0.0)) for bar in bars if bar.get("c") is not None]
        volumes = [float(bar.get("v", 0.0)) for bar in bars]
        #"ap" = ask price, "bp" = bid price. If neither is available, fall back to last close.
        last_price = float(quote.get("ap") or quote.get("bp") or (closes[-1] if closes else 0.0))

        return {
            "last_price": last_price,
            "momentum": cls.momentum(closes),
            "mean_reversion": cls.mean_reversion(closes),
            "volatility": cls.volatility(closes),
            "liquidity": cls.liquidity(bars),
            "avg_dollar_volume": cls.liquidity(bars),
            "bid_ask_spread": cls.bid_ask_spread(quote),
            "volume_trend": cls.volume_trend(volumes),
            "returns": cls.returns(closes),
            "sentiment_score": float(sentiment_score),
        }

    def _endpoint_return(closes: list[float], lookback_bars: int) -> float:
        if len(closes) < 2:
            return 0.0
        
        if lookback_bars <= 1:
            return 0.0

        window = closes[-lookback_bars:] if len(closes) >= lookback_bars else closes
        start = window[0]
        end = window[-1]
        if start <= 0:
            return 0.0
        return (end - start) / start
    
    @staticmethod
    def momentum(closes: list[float]) -> float:
        # Medium-horizon trend proxy (distinct from short-term endpoint return).
        return FeatureVector._endpoint_return(closes, lookback_bars=20)

    @staticmethod
    def mean_reversion(closes: list[float]) -> float:
        if len(closes) < 10:
            return 0.0
        window = closes[-10:]
        baseline = mean(window)
        if baseline <= 0:
            return 0.0
        return (baseline - closes[-1]) / baseline

    @staticmethod
    def volatility(closes: list[float]) -> float:
        if len(closes) < 2:
            return 0.0
        one_bar_returns = [
            (closes[idx] - closes[idx - 1]) / closes[idx - 1]
            for idx in range(1, len(closes))
            if closes[idx - 1] > 0
        ]
        if len(one_bar_returns) < 2:
            return 0.0
        return pstdev(one_bar_returns)

    @staticmethod
    def liquidity(bars: list[dict]) -> float:
        if not bars:
            return 0.0
        dollar_volumes = [float(bar.get("v", 0.0)) * float(bar.get("c", 0.0)) for bar in bars]
        return mean(dollar_volumes) if dollar_volumes else 0.0

    @staticmethod
    def bid_ask_spread(quote: dict) -> float:
        ask = float(quote.get("ap") or 0.0)
        bid = float(quote.get("bp") or 0.0)
        
        if ask <= 0.0 or bid <= 0.0:
            return 0.0
        if ask < bid:
            return 0.0

        mid = (ask + bid) / 2.0
        if mid <= 0.0:
            return 0.0

    
        return (ask - bid) / mid

    @staticmethod
    def volume_trend(volumes: list[float]) -> float:
        if len(volumes) < 10:
            return 0.0
        recent = mean(volumes[-5:])
        prior = mean(volumes[-10:-5])
        if prior <= 0:
            return 0.0
        return (recent - prior) / prior

    @staticmethod
    def returns(closes: list[float]) -> float:
        # Short-horizon endpoint return over the most recent bars.
        return FeatureVector._endpoint_return(closes, lookback_bars=5)
