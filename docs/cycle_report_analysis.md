# Cycle Report Analysis (30-second rebalance bot)

Analysis window: 10 most recent cycles from `bot_cycle_snapshots` on **2026-04-06**.

## What the cycle report shows

- 28 submitted orders in 10 cycles (~2.8 orders/cycle).
- 9 immediate side flips (symbol traded opposite direction in the next cycle), including:
  - `KR` buy at 13:47:28 then sell at 13:48:05.
  - `ADP` and `BA` buy at 13:51:30 then sell at 13:52:06.
  - `META` and `TSLA` buy at 13:53:19 then sell at 13:53:55.
- Almost every cycle had `HAS_DELTAS`, so the system is constantly generating trades rather than waiting for meaningful divergence.
- Sell pressure is often triggered by score deterioration in one snapshot-to-snapshot step and by aggressive reduce/exit bands in the same short horizon.

## Root cause

The over-rotation is structural and comes from three interacting behaviors:

1. **Rebalance is too sensitive to tiny changes.**
   - The execution router was only guarded by a dollar notional floor, so even small target-weight drift can repeatedly trigger orders on a 30-second cadence.

2. **Portfolio action logic can flatten positions too quickly.**
   - Position review can close when score falls slightly negative vs prior snapshot, which is noisy on 30-second intervals.

3. **Exit policy had no minimum hold gate for non-risk actions.**
   - Take-profit reductions/exits and signal-deterioration/add actions can fire immediately on very young positions, reinforcing churn.

## Optimal fix

### 1) Rebalance logic (reduce mechanical churn)

- Add a **weight-delta deadband / tolerance**, e.g. skip rebalances unless `abs(target_weight - current_weight) >= 2%`.
- Keep hard exits (target weight to zero) exempt from the deadband so risk exits still execute.

### 2) Exit logic (prioritize risk, de-emphasize micro-noise)

- Keep stop-loss / MAE risk exits active at all times.
- Gate profit-band and signal-based reduce/add actions behind a minimum hold requirement.

### 3) Minimum hold duration (force signal persistence)

- Introduce `min_holding_minutes` and enforce it in both:
  - exit policy non-risk actions,
  - portfolio-level score-based close/reduce transitions.
- Practical starting point for a 30-second cycle: **15 minutes** minimum hold (30 cycles), then tune with walk-forward testing.

## Expected effect

- Fewer one-cycle reversals.
- Lower turnover and spread/fee drag.
- Improved chance for signal edge to manifest before an exit decision.

