# jub-paper-trader

Paper trading bot with FastAPI backend, scheduler + orchestrator loop, and Alpaca integrations.

## 1) environment config .env 

```env
ALPACA_API_KEY=...
ALPACA_API_SECRET=...
ALPACA_BASE_URL="https://paper-api.alpaca.markets/v2"
ALPACA_DATA_URL="https://data.alpaca.markets"
DATABASE_URL=sqlite:///./jub_paper_trader.db
```

## 2) Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

# 3) Run the bot against Alpaca paper account

Single validation cycle:

```bash
python -m scheduler.runner --symbols AAPL,MSFT,NVDA --once
```

Continous min-by-min loop:

```bash
python -m scheduler.runner --symbols AAPL,MSFT,NVDA --interval-seconds 60
```

# 4) Run API

```bash
uvicorn backend.map:app --reload
```

# 5) Visualization Code Flow GOAL

1) Market Data - alpaca_data.py
    -Instantiate Symbol With Raw Market Data

    Inputs:
1) Market Data - alpaca_data.py
    -Analyze each independent symbol

    ```bash
        signal_i = f(features_i)
    ```

    ```bash
        features_i = [
            momentum_i,
            mean_reversion_i,
            volatility_i,
            liquidity_i,
            ...
        ]
    ```

2) Signal Engine - 
    -Normalize across all stocks by ranking:

    ```bash
        z_score_i = (signal_i - mean(signals)) / std(signals)
    ```

    Top 3 Highest = BUY
    Middle n = HOLD 
    Bottom 3 Lowest = SELL

3) Convert Signal -> Position Size
    -Size our position:

    ```bash
        target_weight = confidence * risk_budget
    ```

    Example:
        +5% Portfolio STRONG
        +0.5% Porfolio WEAK

    
Market Data -> Signal Engine -> Decision Engine -> Portfolio Manager -> Risk Guardrails -> Alpaca 