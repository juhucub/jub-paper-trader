# Production System Analysis: `jub-paper-trader`

## 1) SYSTEM PIPELINE (STRICT ORDER)

### 1. Market Data Ingestion
- **Files / functions**
  - `scheduler/runner.py::main/run_once` enters the cycle and invokes scheduler.  
  - `scheduler/cycle.py::BotScheduler.run_minute` calls `BotCycleService.run_cycle`.  
  - `agent_service/bot_cycle.py::_pull_features` calls Alpaca data methods for each symbol.  
  - `services/alpaca_data.py::{get_historical_bars,get_latest_quote}` performs HTTP requests.
- **Inputs**
  - CLI symbol list (`--symbols`), interval settings.  
  - Alpaca historical bars and latest quote APIs.
- **Transformations**
  - For each symbol, fetches 30 x 1-minute bars and a latest quote, with a hard-coded lookback window (`end=now-20m`, `start=end-5d`).
- **Outputs**
  - Raw bar list + quote dict per symbol handed to feature builder.
- **Data flow**
  - Output feeds directly into `_pull_features` → `FeatureVector.build` in the same cycle.

### 2. Data Cleaning / Processing
- **Files / functions**
  - There is no standalone `data_processor.py` in the repo.
  - Minimal validation occurs in `agent_service/bot_cycle.py::_pull_features` (checks: bars exist, closes exist, last_price > 0).
- **Inputs**
  - Raw bars and quotes from Alpaca.
- **Transformations**
  - Primitive filtering only: missing bars/closes/price guard clauses.
  - No timestamp alignment, outlier handling, missing-value imputation, or stale-data checks.
- **Outputs**
  - Either skips symbol (`NO_TRADE`) or passes raw data through for feature computation.
- **Data flow**
  - Valid symbols continue directly to feature engineering.

### 3. Feature Engineering
- **Files / functions**
  - `agent_service/feature_vector.py::FeatureVector.build` and helper methods.
- **Inputs**
  - Bars (`c`, `v`), quote (`ap`,`bp`), optional news sentiment.
- **Transformations**
  - Computes momentum, mean reversion, volatility, liquidity, bid/ask spread, volume trend, returns, sentiment, and last price.
- **Outputs**
  - Per-symbol feature dict.
- **Data flow**
  - Feature map passed into `SignalGenerator.generate`.

### 4. Signal Generation (Alpha Model)
- **Files / functions**
  - `agent_service/signals.py::SignalGenerator.generate/_raw_score`.
- **Inputs**
  - Feature map from stage 3.
- **Transformations**
  - Weighted linear score with penalties for volatility/spread/illiquidity.
  - Derives direction/action, strength/confidence, expected horizon.
- **Outputs**
  - Structured signal payload per symbol: `score`, `action`, `direction`, `strength`, `confidence`, etc.
- **Data flow**
  - Signal payload is normalized/ranked, then passed to decision policy.

### 5. Signal Normalization / Ranking
- **Files / functions**
  - `agent_service/normalize.py::normalize_and_rank_signals`.
- **Inputs**
  - Raw structured signals.
- **Transformations**
  - Computes z-score and ordinal rank; assigns BUY/HOLD/SELL rank bucket using `top_n` and `bottom_n`.
- **Outputs**
  - Same signal dict augmented with `z_score`, `rank`, `rank_bucket`.
- **Data flow**
  - Passed to `DecisionPolicy.evaluate` and then into optimizer path.

### 6. Portfolio Construction / Optimization
- **Files / functions**
  - `agent_service/bot_cycle.py::_plan_targets_and_deltas` (policy + exit + target logic).
  - `agent_service/decision_policy.py::DecisionPolicy.evaluate` (candidate filtering).
  - `agent_service/exit_policy.py::ExitPolicy.evaluate_positions` (position lifecycle actions).
  - `agent_service/optimizer_qpo.py::optimize_target_weights` (target weights).
  - `services/position_sizer.py::size_targets` (risk/notional sizing).
  - `services/execution_router.py::to_rebalance_deltas` (convert targets to trade deltas).
- **Inputs**
  - Signals, account equity/cash, positions, open orders, latest prices, previous cycle payload.
- **Transformations**
  - Policy filters candidates; exit policy mutates approved signals/weights; optimizer creates long-only weights with cash buffer; position sizer caps by risk/sector/leverage; router generates buy/sell deltas.
- **Outputs**
  - Target weights, sized targets, rebalance deltas.
- **Data flow**
  - Deltas passed to execution function `_execute_deltas`.

### 7. Execution / Order Handling
- **Files / functions**
  - `agent_service/bot_cycle.py::_execute_deltas`.
  - `services/risk_guardrails.py::validate_order`.
  - `services/alpaca_client.py::submit_order`.
  - `agent_service/bot_cycle.py::_reconcile_portfolio` + `services/portfolio_engine.py::sync_account_state`.
- **Inputs**
  - Deltas, current positions, open-sell reservations, market features.
- **Transformations**
  - Sell availability checks, pre-trade risk validation, order type selection (market vs limit for extended hours), order submission, then post-trade broker sync.
- **Outputs**
  - `submitted_orders`, `blocked_orders`, reconciliation snapshot persisted to DB.
- **Data flow**
  - Snapshot persisted and becomes state input for next cycle (`_latest_snapshot_payload`).

---

## 2) SIGNAL GENERATION ANALYSIS

### A. Rule-based weighted composite alpha
- **Where**: `SignalGenerator._raw_score`.
- **Data used**: momentum, mean reversion, returns, sentiment, volume trend, volatility, spread, relative liquidity.
- **Output**: bounded score [-1,1], converted to direction/action/strength/confidence.
- **Strengths**: deterministic, interpretable, includes transaction-cost proxies (spread, illiquidity).
- **Weaknesses**:
  - Momentum and returns are mathematically redundant here (both from same window endpoints).
  - Confidence is mechanically tied to |score| (`strength*20` clamp), not calibrated from historical hit rates.
  - No regime conditioning by market state beyond simple volatility veto later.

### B. Statistical normalization layer
- **Where**: `normalize_and_rank_signals`.
- **Data used**: cross-sectional score distribution in current symbol set.
- **Output**: z-score/rank/rank bucket.
- **Strengths**: cross-sectional comparability.
- **Weaknesses**:
  - z-score/rank are not used to allocate capital in optimizer (optimizer uses direction/strength/confidence).
  - `top_n=1` / `bottom_n=1` in cycle hard-codes sparse bucketing and can be unstable for small universes.

### C. No ML model present
- **Evidence**: all signal logic is deterministic and hand-weighted; no training/inference pipeline exists.
- **Impact**: no adaptive learning loop; parameters static.

### Redundancy/conflict summary
- **Redundant**: momentum + returns duplication from same inputs.
- **Conflicting path**: system emits short signals (`direction='short'`), but optimizer forces long-only allocation by zeroing non-long directions.

---

## 3) PORTFOLIO LOGIC & POSITION SIZING

### How signals become weights
1. Signals generated + normalized.
2. `DecisionPolicy` filters buy/sell/hold based on confidence, cash, concentration, liquidity, volatility.
3. `OptimizerQPO` maps approved signals to long-only positive weights and keeps a cash buffer.
4. `ExitPolicy` can override/reduce/add exposure for currently held positions.
5. `PositionSizer` converts weights to risk-adjusted notionals and quantities.
6. `ExecutionRouter` computes deltas vs current holdings.

### Capital allocation and risk controls
- **Allocation**: proportional to `strength * confidence`, then capped per symbol, rescaled to investable capital.
- **Sizer controls**: per-symbol cap, leverage cap, sector cap, min/max notional, confidence multiplier, volatility-proxy risk budget.
- **Pre-trade controls**: max daily loss, max order size % equity, max open positions, price floor, liquidity floor, cooldown.

### Whether existing portfolio is considered
- **Yes, partially**:
  - Current positions and prices used for concentration, delta computation, and per-position adjustments.
  - Previous snapshot used by exit policy and score-deterioration checks.
- **Critical disconnects**:
  - `portfolio_state['daily_realized_pnl']` passed to guardrails is hardcoded `0.0` in `_execute_deltas`, so daily-loss protection is not connected to actual portfolio PnL state.
  - Short signals approved by policy do not map to short target weights due to long-only optimizer.
  - `latest_prices` only for symbols with computed features; symbols from held positions with missing fresh features can be skipped in sizing/rebalance logic.

---

## 4) DATA → DECISION TRACE (AAPL EXAMPLE)

1. **Raw data**: `_pull_features('AAPL')` fetches bars (`timeframe='1Min', limit=30`) and quote from Alpaca.
2. **Features**: `FeatureVector.build` computes `momentum`, `mean_reversion`, `volatility`, `liquidity`, `spread`, `volume_trend`, `returns`, `sentiment_score`, `last_price`.
3. **Signal**: `SignalGenerator.generate` calculates raw score; if score > 0 => `direction='long'`, `action='buy'`, with derived strength/confidence.
4. **Normalization**: `normalize_and_rank_signals` assigns z-score/rank/rank_bucket.
5. **Weight**: in `_plan_targets_and_deltas`, policy approves/rejects; optimizer assigns a target weight (long-only); exit policy may adjust/zero this; position sizer converts to target qty/notional.
6. **Order decision**: router computes delta quantity; `_execute_deltas` validates via `RiskGuardrails`; if allowed submits Alpaca order.
7. **State feedback**: cycle writes snapshot and reconciles broker state into local DB for next cycle reference.

---

## 5) CONSISTENT PORTFOLIO GROWTH — EVALUATION

### Where growth is enforced
- **Some risk containment exists**: concentration, max position %, leverage/sector caps, stop/reduce/exit bands, and drawdown metrics in portfolio DB.
- **Rebalancing loop exists**: cycle recalculates targets each run and executes deltas.

### Where growth is NOT enforced
- **No objective tied to compounded return**: optimizer is deterministic proportional allocator, not return-risk optimizer (no covariance, no forecast error model, no turnover penalty).
- **No performance feedback loop into alpha weights**: signal weights never retrained or adapted.
- **Daily loss guardrail is disconnected in execution path** due to hardcoded zero PnL input.
- **No transaction-cost/slippage model in portfolio objective** (only a minimum notional and spread penalty in alpha).
- **No benchmark-relative performance loop** despite benchmark symbol field.

### Realistic assessment
- Current system can execute and rebalance positions, but **cannot realistically guarantee consistent growth** under production conditions because key controls are heuristic and disconnected from realized performance and risk-adjusted optimization.

---

## 6) GAPS, RISKS, FAILURE POINTS

1. **Missing data-cleaning stage** despite documentation claiming `data_processor.py`.
2. **Duplicate method definition** in `BotCycleService` (`_plan_targets_and_deltas` appears twice; first is dead/overwritten), signaling maintainability risk.
3. **Short-signal dead-end**: signal engine and policy produce sell/short intents, optimizer discards non-long allocations.
4. **Risk guardrail wiring gap**: daily realized PnL not passed from portfolio state to pre-trade checks.
5. **Potential stale or missing price coverage**: positions merged into symbol list, but if feature fetch fails there is no fallback pricing for sizing.
6. **No explicit order fill management in cycle** beyond snapshot reconciliation; no order aging/cancel-replace behavior.
7. **Normalization output underutilized**: z-score/rank not used in optimizer/sizer decisions.
8. **No regime-aware adaptation**: static thresholds may fail across volatility regimes.

---

## 7) ACTIONABLE IMPROVEMENTS (PRIORITIZED)

### Priority 1 — Portfolio performance (highest impact)
1. **Refactor optimizer to true risk-adjusted objective**:
   - Add expected return vector from alpha, covariance estimate, turnover and transaction-cost penalties.
   - Allow long/short or explicitly enforce long-only upstream by suppressing short signals early.
2. **Remove feature redundancy**:
   - Keep either momentum or returns definition, or differentiate horizons (e.g., short-term returns + medium-term momentum).
3. **Use normalized cross-sectional signals in allocation**:
   - Map z-score/rank to target gross exposure rather than ignoring them.

### Priority 2 — Risk control
4. **Fix daily loss guardrail plumbing**:
   - Pass actual `PortfolioAccountState.daily_realized_pnl` into `_execute_deltas` portfolio_state.
5. **Add order lifecycle risk controls**:
   - Cancel stale open orders, enforce max slippage/price bands, retry policy with bounded attempts.
6. **Add drawdown-based dynamic de-risking**:
   - When current/max drawdown breaches thresholds, scale down gross leverage and max position caps.

### Priority 3 — Reliability / correctness
7. **Implement explicit data quality layer** (`data_processor.py` or equivalent):
   - Freshness checks, missing-bar fill policy, outlier filtering, and timestamp alignment.
8. **Eliminate duplicate `_plan_targets_and_deltas` definition** and add lint/test guard against duplicate methods.
9. **Add deterministic end-to-end trace test**:
   - Single-symbol fixture asserting full chain: bars→features→signal→weight→delta→order side/qty.
10. **Expand observability**:
   - Persist structured reason codes for each blocked/approved step and aggregate metrics per cycle.

