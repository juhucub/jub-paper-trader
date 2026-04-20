from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone

from agent_service.interfaces import (
    AllocationLine,
    AllocationProposal,
    CycleReport,
    DecisionPolicyContext,
    DecisionPolicyDecision,
    DecisionPolicyOutput,
    ExecutionAttempt,
    ExecutionResult,
    ExitPolicyDirective,
    ExitPolicyOutput,
    MonitoringAlert,
    MonitoringDecision,
    OptimizerConstraintSet,
    OptimizerDiagnostics,
    OptimizerInput,
    OrderProposal,
    PolicyConstraint,
    PortfolioActionAnalysis,
    ReconciliationAnomaly,
    ReconciliationResult,
    ReplayEvaluation,
    RiskAllocationDetail,
    RiskAdjustedAllocation,
    Scenario,
    ScenarioBundle,
    SignalBundle,
    SignalIntent,
)


def _json_ready(value):
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def test_trading_contracts_are_constructible() -> None:
    now = datetime.now(timezone.utc)
    signal_bundle = SignalBundle(
        as_of=now,
        benchmark_symbol="SPY",
        intents=[
            SignalIntent(
                symbol="AAPL",
                direction="long",
                action="buy",
                score=0.25,
                confidence=0.8,
                expected_return=0.03,
                normalized_score=0.7,
                rank=1,
                rationale="momentum and liquidity aligned",
                diagnostics={"regime_evidence": {"volatility": 0.04}},
            )
        ],
    )
    scenario_bundle = ScenarioBundle(
        as_of=now,
        forecast_horizon="1d",
        regime_label="risk_on",
        scenarios=[Scenario(name="base_case", regime="risk_on", probability=1.0)],
    )
    decision_context = DecisionPolicyContext(
        as_of=now,
        portfolio_cash=10_000.0,
        portfolio_equity=12_000.0,
    )
    decision_output = DecisionPolicyOutput(
        as_of=now,
        approved_signal_bundle=signal_bundle,
        source_signal_bundle_at=signal_bundle.as_of,
        source_scenario_bundle_at=scenario_bundle.as_of,
        decisions=[
            DecisionPolicyDecision(
                symbol="AAPL",
                requested_intent=signal_bundle.intents[0],
                approved_intent=signal_bundle.intents[0],
                policy_action="buy",
                reason="policy_approved",
            )
        ],
    )
    exit_output = ExitPolicyOutput(
        as_of=now,
        adjusted_signal_bundle=signal_bundle,
        directives=[
            ExitPolicyDirective(
                symbol="AAPL",
                action="HOLD",
                trigger="none",
                trigger_type="none",
                current_qty=10.0,
                requested_intent=signal_bundle.intents[0],
                adjusted_intent=signal_bundle.intents[0],
            )
        ],
    )
    optimizer_constraints = OptimizerConstraintSet(max_symbol_weight=0.20, cash_buffer=0.05)
    optimizer_input = OptimizerInput(
        as_of=now,
        benchmark_symbol="SPY",
        expected_returns={"AAPL": 0.03},
        scenario_returns={"base_case": {"AAPL": 0.03}},
        constraints=optimizer_constraints,
        source_signal_bundle_at=signal_bundle.as_of,
        source_scenario_bundle_at=scenario_bundle.as_of,
    )
    optimizer_diagnostics = OptimizerDiagnostics(as_of=now, backend_name="scenario_mean_cvar_proxy")
    proposal = AllocationProposal(
        as_of=now,
        source_signal_bundle_at=signal_bundle.as_of,
        source_scenario_bundle_at=scenario_bundle.as_of,
        source_decision_policy_at=decision_output.as_of,
        source_exit_policy_at=exit_output.as_of,
        target_gross_exposure=0.95,
        cash_buffer=0.05,
        lines=[AllocationLine(symbol="AAPL", target_weight=0.12, confidence=0.8)],
    )
    adjusted = RiskAdjustedAllocation(
        as_of=now,
        approved_lines=proposal.lines,
        symbol_details=[
            RiskAllocationDetail(
                symbol="AAPL",
                status="approved",
                requested_weight=0.12,
                approved_weight=0.12,
                clip_amount=0.0,
                reasons=[PolicyConstraint(code="ok", message="Allocation passed deterministic risk checks.")],
            )
        ],
        cash_buffer_applied=0.05,
    )
    portfolio_action = PortfolioActionAnalysis(
        cycle_id="cycle-1",
        as_of=now,
        symbol="AAPL",
        action="hold",
        reason="position_healthy",
        current_weight=0.1,
        base_target_weight=0.12,
        adjusted_target_weight=0.12,
        current_score=0.03,
        previous_score=0.02,
        score_delta=0.01,
        holding_minutes=30.0,
        minimum_hold_satisfied=True,
    )
    order = OrderProposal(
        cycle_id="cycle-1",
        symbol="AAPL",
        side="buy",
        qty=10.0,
        order_type="limit",
        rationale="rebalance into approved target",
        reference_price=190.0,
        limit_price=190.25,
    )
    execution_attempt = ExecutionAttempt(
        cycle_id="cycle-1",
        as_of=now,
        symbol="AAPL",
        status="submitted",
        stage="primary_execution",
        reason="order_submitted",
        side="buy",
        qty=10.0,
        order_type="limit",
        source_order_proposal=order,
    )
    execution_result = ExecutionResult(
        cycle_id="cycle-1",
        as_of=now,
        attempts=[execution_attempt],
        summary="submitted=1 blocked=0",
        submitted_count=1,
        acted=True,
    )
    reconciliation_result = ReconciliationResult(
        cycle_id="cycle-1",
        as_of=now,
        status="ok",
        account_state={"cash": 10_000.0},
        anomalies=[
            ReconciliationAnomaly(
                cycle_id="cycle-1",
                as_of=now,
                code="none",
                message="no anomalies",
                severity="info",
            )
        ],
    )
    monitoring = MonitoringDecision(
        as_of=now,
        status="healthy",
        summary="cycle completed",
        next_action="continue",
        alerts=[MonitoringAlert(severity="info", message="all checks passed", symbol="AAPL")],
    )
    cycle_report = CycleReport(
        cycle_id="cycle-1",
        as_of=now,
        status="healthy",
        symbols=["AAPL"],
        summary="cycle completed",
        submitted_order_count=1,
        blocked_order_count=0,
        acted=True,
    )
    replay = ReplayEvaluation(
        as_of=now,
        strategy_name="test",
        benchmark_symbol="SPY",
        cycle_ids=["cycle-1"],
        summary="ok",
        total_return=0.01,
        benchmark_return=0.005,
        excess_return=0.005,
        max_drawdown=0.01,
        turnover=0.12,
        slippage_drag=0.001,
        spread_drag=0.001,
    )

    assert decision_context.portfolio_cash == 10_000.0
    assert optimizer_input.constraints == optimizer_constraints
    assert optimizer_diagnostics.backend_name == "scenario_mean_cvar_proxy"
    assert adjusted.approved_lines[0].target_weight == 0.12
    assert portfolio_action.action == "hold"
    assert execution_result.acted is True
    assert reconciliation_result.status == "ok"
    assert monitoring.kill_switch_engaged is False
    assert cycle_report.submitted_order_count == 1
    assert replay.excess_return == 0.005


def test_trading_contracts_round_trip_into_snapshot_ready_payloads() -> None:
    now = datetime(2026, 4, 16, 15, 30, tzinfo=timezone.utc)
    signal_bundle = SignalBundle(
        as_of=now,
        benchmark_symbol="SPY",
        intents=[
            SignalIntent(
                symbol="AAPL",
                direction="long",
                action="buy",
                score=0.41,
                confidence=0.9,
                expected_return=0.03,
                normalized_score=0.8,
                rank=1,
                diagnostics={
                    "regime_evidence": {"volatility": 0.04},
                    "execution_risk": {"liquidity_risk": 0.1},
                    "uncertainty": {"score": 0.2},
                },
            )
        ],
        feature_snapshot={"AAPL": {"last_price": 190.5, "avg_dollar_volume": 1_500_000.0}},
    )
    scenario_bundle = ScenarioBundle(
        as_of=now,
        forecast_horizon="1d",
        regime_label="risk_on",
        scenarios=[
            Scenario(
                name="base_case",
                regime="risk_on",
                probability=0.40,
                shock_map={"SPY": -0.01},
                symbol_impacts={"AAPL": {"shocked_return": 0.05, "liquidity_stress": 0.02}},
            )
        ],
        diagnostics={"scenario_matrix": {"base_case": {"AAPL": 0.05}}},
    )
    optimizer_input = OptimizerInput(
        as_of=now,
        benchmark_symbol="SPY",
        expected_returns={"AAPL": 0.03},
        scenario_returns={"base_case": {"AAPL": 0.05}},
        constraints=OptimizerConstraintSet(max_symbol_weight=0.4, cash_buffer=0.05),
    )
    execution_attempt = ExecutionAttempt(
        cycle_id="cycle-typed-1",
        as_of=now,
        symbol="AAPL",
        status="submitted",
        stage="primary_execution",
        reason="order_submitted",
        side="buy",
        qty=10.5,
        order_type="limit",
        request_payload={"symbol": "AAPL"},
        response_payload={"id": "ord-1"},
    )
    reconciliation_result = ReconciliationResult(
        cycle_id="cycle-typed-1",
        as_of=now,
        status="warning",
        account_state={"cash": 9_000.0},
        order_deltas=[{"symbol": "AAPL", "status": "new"}],
        position_deltas=[{"symbol": "AAPL", "quantity": 10.5}],
        anomalies=[
            ReconciliationAnomaly(
                cycle_id="cycle-typed-1",
                as_of=now,
                code="filled_quantity_exceeds_order_quantity",
                message="warning",
                symbol="AAPL",
            )
        ],
    )
    cycle_report = CycleReport(
        cycle_id="cycle-typed-1",
        as_of=now,
        status="degraded",
        symbols=["AAPL"],
        summary="submitted=1 blocked=0 reconciliation_anomalies=1",
        submitted_order_count=1,
        blocked_order_count=0,
        acted=True,
    )

    payload = _json_ready(
        {
            "signal_bundle": signal_bundle,
            "scenario_bundle": scenario_bundle,
            "optimizer_input": optimizer_input,
            "execution_attempt": execution_attempt,
            "reconciliation_result": reconciliation_result,
            "cycle_report": cycle_report,
        }
    )

    assert payload["signal_bundle"]["as_of"] == "2026-04-16T15:30:00+00:00"
    assert payload["signal_bundle"]["intents"][0]["diagnostics"]["execution_risk"]["liquidity_risk"] == 0.1
    assert payload["scenario_bundle"]["diagnostics"]["scenario_matrix"]["base_case"]["AAPL"] == 0.05
    assert payload["optimizer_input"]["constraints"]["max_symbol_weight"] == 0.4
    assert payload["execution_attempt"]["request_payload"]["symbol"] == "AAPL"
    assert payload["reconciliation_result"]["anomalies"][0]["code"] == "filled_quantity_exceeds_order_quantity"
    assert payload["cycle_report"]["status"] == "degraded"

