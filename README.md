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

## 3) Run the bot against Alpaca paper account

Single validation cycle:

```bash
python -m scheduler.runner --symbols AAPL,MSFT,NVDA,AMZN,GOOG,META,TSLA,JPM,GS,V,CAT,BA,XOM,CVX,WMT,COST,SBUX,KR,UNH,JNJ,LLY,DIS,NFLX,ADP,SFM --once
```

Continous min-by-min loop:

```bash
python -m scheduler.runner --symbols AAPL,MSFT,NVDA --interval-seconds 60
```

## 4) Run API

```bash
uvicorn backend.map:app --reload
```

## 4.1) Codex Repo Guidance

This repo now includes repo-local Codex scaffolding:

- `AGENTS.md` defines the two-intelligence model and six-layer architecture
- `.codex/skills/` contains repo-specific skills for architecture, signal/scenario work, allocation-risk handoffs, and execution-monitoring work
- `.codex/subagents/` contains focused role files for cross-layer planning, statistical research, deterministic safety, and monitoring validation
- `agent_service/interfaces/contracts.py` defines typed handoff objects for:
  - `SignalBundle`
  - `ScenarioBundle`
  - `AllocationProposal`
  - `RiskAdjustedAllocation`
  - `OrderProposal`
  - `MonitoringDecision`

Use these typed contracts when extending the orchestration flow so model proposals, deterministic vetoes, and monitoring outputs stay auditable.

## 5) Visualization Code Flow GOAL

### 1) Raw Market Data - alpaca_data.py
-Instantiate Symbol With Raw Market Data

Inputs:

    * Price bars (Open, High, Low, Close, Volume)
    * Tick data (every trade)
    * VWAP (Volume-Weighted Average Price)
    * Quotes (bid/ask)
    * Trade Stamps (Price, Size, Timestamp)
    * Fundamentals (optional)
    * News/alt data (sentiment)

These help define our feature vector by calculating:
    returns, volatility, momentum

    features_i = [
        ...
    ]
    
### 2) Clean Market Data - data_processor.py
-Analyze each independent symbol

    *Clean data
    *Align timestamps
    *Normalize units
    *Ensure low latency and completeness of symbols Market Data 

### 3) Feature Engineering - feature_vector.py
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
```
### 4) Signal Generation (alpha model) - signals.py
-Model takes feature vectors and outputs signals

Input:

    Feature Vector

Output: 

    signal_i = [
        expected return, 
        probability of price increase, 
        ranking scores,
        ...
    ] => float(0,1) e.g. 0.07

Types of models: Rule-based -> Statistical -> ML models

### 5) Signal Normalization & Ranking - normalize.py
-Convert raw symbol signals into relative signals:

    z_score_i = (signal_i - mean(signals)) / std(signals)


    Top 3 Highest = BUY
    Middle n = HOLD 
    Bottom 3 Lowest = SELL

### 6) Portfolio Construction (NVIDIA QPO-based Optimizer)
-Size our position by:

    A) Allocate capital 
        -Strong signals -> larger weights
        -Weak signals -> smaller weights
    B) Balance risk
        -Avoid concentration
        -Control volatility
    C) Enforce constraints
        -Max position %
        -Max leverage
        -Sector limits

Input:

    * Signals (ranked)
    * Risk model
    * Constraints

Output:

```bash
target_weights = {
    AAPL: +5%,
    NVDA: +8%,
    MSFT: -3%,
}
```

Market Data -> Signal Engine -> Portfolio Manager
