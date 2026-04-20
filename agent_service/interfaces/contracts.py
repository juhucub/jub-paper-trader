"""Typed handoff objects for the six-layer trading workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


Direction = Literal["long", "short", "flat"]
Action = Literal["buy", "sell", "hold"]
OrderType = Literal["market", "limit"]
MonitoringStatus = Literal["healthy", "degraded", "halted"]
PolicyAction = Literal["buy", "sell", "hold", "skip"]
ExitAction = Literal["HOLD", "REDUCE", "EXIT", "ADD"]
ApprovalStatus = Literal["approved", "clipped", "blocked", "unchanged"]
PortfolioActionKind = Literal["hold", "reduce", "close", "add"]
ExecutionStatus = Literal["submitted", "blocked", "cancelled", "replaced", "failed", "skipped"]
ReconciliationStatus = Literal["ok", "warning", "error"]


@dataclass(slots=True, frozen=True)
class PolicyConstraint:
    """Normalized policy/risk condition used in audit logs and snapshots."""

    code: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class SignalIntent:
    """Symbol-level statistical proposal and supporting diagnostics."""

    symbol: str
    direction: Direction
    action: Action
    score: float
    confidence: float
    expected_return: float = 0.0
    normalized_score: float = 0.0
    rank: int = 0
    rationale: str = ""
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class SignalBundle:
    """Statistical signal package handed from feature/signal layers into policy."""

    as_of: datetime
    benchmark_symbol: str
    intents: list[SignalIntent]
    feature_snapshot: dict[str, dict[str, float]] = field(default_factory=dict)
    model_name: str = "signal_generator_v1"
    notes: list[str] = field(default_factory=list)
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class Scenario:
    """Scenario row used to stress statistical proposals before optimization."""

    name: str
    regime: str
    probability: float
    confidence: float = 0.0
    expected_volatility: float = 0.0
    expected_drawdown: float = 0.0
    liquidity_stress: float = 0.0
    rationale: str = ""
    shock_map: dict[str, float] = field(default_factory=dict)
    symbol_impacts: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ScenarioBundle:
    """Scenario package handed from the statistical layer into optimization/risk."""

    as_of: datetime
    forecast_horizon: str
    regime_label: str
    scenarios: list[Scenario]
    regime_confidence: float = 0.0
    regime_probabilities: dict[str, float] = field(default_factory=dict)
    anomaly_flags: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class DecisionPolicyContext:
    """Typed deterministic inputs used to approve, skip, or convert a signal intent."""

    as_of: datetime
    portfolio_cash: float
    portfolio_equity: float
    current_positions: dict[str, float] = field(default_factory=dict)
    concentration_by_symbol: dict[str, float] = field(default_factory=dict)
    market_volatility: float = 0.0
    liquidity_by_symbol: dict[str, float] = field(default_factory=dict)
    scenario_regime: str | None = None
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class DecisionPolicyDecision:
    """Per-symbol deterministic decision taken after statistical proposals are generated."""

    symbol: str
    requested_intent: SignalIntent
    approved_intent: SignalIntent | None
    policy_action: PolicyAction
    reason: str
    constraints: list[PolicyConstraint] = field(default_factory=list)
    allow_exit_only: bool = False
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class DecisionPolicyOutput:
    """Deterministic approval/rejection output for the statistical proposal set."""

    as_of: datetime
    approved_signal_bundle: SignalBundle
    decisions: list[DecisionPolicyDecision]
    notes: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ExitPolicyDirective:
    """Typed instruction describing how position lifecycle rules reshaped a proposal."""

    symbol: str
    action: ExitAction
    trigger: str
    trigger_type: str
    current_qty: float
    requested_intent: SignalIntent | None = None
    adjusted_intent: SignalIntent | None = None
    target_weight_multiplier: float = 1.0
    force_target_weight: float | None = None
    rationale: str = ""
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ExitPolicyOutput:
    """Deterministic position-lifecycle output applied before optimization sizing."""

    as_of: datetime
    adjusted_signal_bundle: SignalBundle
    directives: list[ExitPolicyDirective]
    state: dict[str, dict[str, Any]] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class OptimizerConstraintSet:
    """Explicit constraint set consumed by scenario-aware allocators."""

    max_symbol_weight: float
    cash_buffer: float
    max_gross_exposure: float = 0.95
    max_turnover: float = 0.35
    turnover_penalty: float = 0.15
    transaction_cost_bps: float = 10.0
    slippage_bps: float = 5.0
    min_weight: float = 0.0
    long_only: bool = True
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class OptimizerInput:
    """Typed scenario-to-optimizer bridge that isolates portfolio math from infra."""

    as_of: datetime
    benchmark_symbol: str
    expected_returns: dict[str, float]
    scenario_returns: dict[str, dict[str, float]]
    current_weights: dict[str, float] = field(default_factory=dict)
    confidence_by_symbol: dict[str, float] = field(default_factory=dict)
    volatility_by_symbol: dict[str, float] = field(default_factory=dict)
    liquidity_risk_by_symbol: dict[str, float] = field(default_factory=dict)
    uncertainty_by_symbol: dict[str, float] = field(default_factory=dict)
    regime_label: str | None = None
    scenario_probabilities: dict[str, float] = field(default_factory=dict)
    constraints: OptimizerConstraintSet | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class OptimizerDiagnostics:
    """Auditable summary of the allocator objective, penalties, and feasibility."""

    as_of: datetime
    backend_name: str
    objective_summary: dict[str, float] = field(default_factory=dict)
    per_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)
    infeasible: bool = False
    infeasibility_reasons: list[str] = field(default_factory=list)
    turnover_estimate: float = 0.0
    transaction_cost_estimate: float = 0.0
    scenario_tail_loss: float = 0.0
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class AllocationLine:
    """Single-symbol optimizer proposal row."""

    symbol: str
    target_weight: float
    confidence: float
    rationale: str = ""
    target_notional: float | None = None
    target_qty: float | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class AllocationProposal:
    """Statistical optimizer proposal before deterministic risk reshaping."""

    as_of: datetime
    source_signal_bundle_at: datetime
    target_gross_exposure: float
    cash_buffer: float
    lines: list[AllocationLine]
    scenario_regime: str | None = None
    optimizer_name: str = "optimizer_qpo"
    constraints_requested: dict[str, float] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    source_exit_policy_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class BlockedAllocationLine:
    """Requested allocation row that deterministic risk vetoed or clipped to zero."""

    symbol: str
    requested_weight: float
    reason: str
    requested_notional: float | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class RiskAllocationDetail:
    """Human-readable explanation of how deterministic risk handled one symbol."""

    symbol: str
    status: ApprovalStatus
    requested_weight: float
    approved_weight: float
    clip_amount: float
    requested_notional: float | None = None
    approved_notional: float | None = None
    requested_qty: float | None = None
    approved_qty: float | None = None
    reasons: list[PolicyConstraint] = field(default_factory=list)
    risk_reducing: bool = False
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class RiskAdjustedAllocation:
    """Deterministic post-risk allocation artifact consumed by execution."""

    as_of: datetime
    approved_lines: list[AllocationLine]
    blocked_lines: list[BlockedAllocationLine] = field(default_factory=list)
    symbol_details: list[RiskAllocationDetail] = field(default_factory=list)
    gross_exposure_cap: float | None = None
    net_exposure_cap: float | None = None
    cash_buffer_applied: float = 0.0
    guardrail_notes: list[str] = field(default_factory=list)
    source_allocation_at: datetime | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    source_exit_policy_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class PortfolioActionAnalysis:
    """Typed portfolio-state analysis that reshapes optimizer targets before sizing."""

    cycle_id: str
    as_of: datetime
    symbol: str
    action: PortfolioActionKind
    reason: str
    current_weight: float
    base_target_weight: float
    adjusted_target_weight: float
    current_score: float
    previous_score: float
    score_delta: float
    holding_minutes: float
    minimum_hold_satisfied: bool
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    source_exit_policy_at: datetime | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class OrderProposal:
    """Deterministic execution proposal emitted from approved rebalance deltas."""

    cycle_id: str
    symbol: str
    side: Literal["buy", "sell"]
    qty: float
    order_type: OrderType
    rationale: str
    reference_price: float
    time_in_force: str = "day"
    limit_price: float | None = None
    extended_hours: bool = False
    source_layer: str = "execution"
    approved_allocation_at: datetime | None = None
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    source_exit_policy_at: datetime | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ExecutionAttempt:
    """One broker-facing submission, block, cancel, or replacement decision."""

    cycle_id: str
    as_of: datetime
    symbol: str
    status: ExecutionStatus
    stage: str
    reason: str
    side: Literal["buy", "sell"] | None = None
    qty: float = 0.0
    order_type: OrderType | None = None
    reference_price: float | None = None
    limit_price: float | None = None
    broker_order_id: str | None = None
    request_payload: dict[str, Any] = field(default_factory=dict)
    response_payload: dict[str, Any] = field(default_factory=dict)
    source_order_proposal: OrderProposal | None = None
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    source_exit_policy_at: datetime | None = None
    source_allocation_at: datetime | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ExecutionResult:
    """Cycle-level execution artifact spanning stale-order handling and new submissions."""

    cycle_id: str
    as_of: datetime
    attempts: list[ExecutionAttempt]
    summary: str
    submitted_count: int = 0
    blocked_count: int = 0
    acted: bool = False
    sell_first_order: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    source_exit_policy_at: datetime | None = None
    source_allocation_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ReconciliationAnomaly:
    """Serializable anomaly emitted while reconciling broker truth to local state."""

    cycle_id: str
    as_of: datetime
    code: str
    message: str
    severity: Literal["info", "warning", "critical"] = "warning"
    symbol: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ReconciliationResult:
    """Typed broker/local-state reconciliation artifact."""

    cycle_id: str
    as_of: datetime
    status: ReconciliationStatus
    account_state: dict[str, Any]
    order_deltas: list[dict[str, Any]] = field(default_factory=list)
    position_deltas: list[dict[str, Any]] = field(default_factory=list)
    fill_events: list[dict[str, Any]] = field(default_factory=list)
    realized_pnl_delta: float = 0.0
    unrealized_pnl: float = 0.0
    anomalies: list[ReconciliationAnomaly] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_execution_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class MonitoringAlert:
    """User-facing monitoring alert derived from typed cycle artifacts."""

    severity: Literal["info", "warning", "critical"]
    message: str
    symbol: str | None = None
    code: str | None = None


@dataclass(slots=True, frozen=True)
class MonitoringDecision:
    """Top-level monitoring explanation for action, inaction, and anomalies."""

    as_of: datetime
    status: MonitoringStatus
    summary: str
    next_action: str
    alerts: list[MonitoringAlert] = field(default_factory=list)
    kill_switch_engaged: bool = False
    acted: bool = False
    blocked_symbols: list[str] = field(default_factory=list)
    inaction_reasons: list[str] = field(default_factory=list)
    execution_gate_reasons: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    source_exit_policy_at: datetime | None = None
    source_allocation_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class CycleReport:
    """Compact cycle-level report for scheduler, CLI, and replay tooling."""

    cycle_id: str
    as_of: datetime
    status: MonitoringStatus
    symbols: list[str]
    summary: str
    submitted_order_count: int
    blocked_order_count: int
    acted: bool
    blocked_symbols: list[str] = field(default_factory=list)
    inaction_reasons: list[str] = field(default_factory=list)
    next_action: str = "continue"
    diagnostics: dict[str, Any] = field(default_factory=dict)
    source_signal_bundle_at: datetime | None = None
    source_scenario_bundle_at: datetime | None = None
    source_decision_policy_at: datetime | None = None
    source_exit_policy_at: datetime | None = None
    source_allocation_at: datetime | None = None
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ReplayEvaluation:
    """Walk-forward evaluation summary derived from stored cycle artifacts."""

    as_of: datetime
    strategy_name: str
    benchmark_symbol: str
    cycle_ids: list[str]
    summary: str
    total_return: float
    benchmark_return: float
    excess_return: float
    max_drawdown: float
    turnover: float
    slippage_drag: float
    spread_drag: float
    regime_breakdown: dict[str, Any] = field(default_factory=dict)
    scenario_breakdown: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)

