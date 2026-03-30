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