from __future__ import annotations

from agent_service.debug_tools import (
    build_cycle_dashboard_payload,
    build_cycle_dashboard_payload_from_snapshot,
    classify_decision_bucket,
    print_symbol_summary,
    render_cycle_dashboard_html,
    render_cycle_dashboard_text,
    render_table,
    summarize_symbol_row,
)


def _sample_decision_summaries() -> dict[str, dict]:
    return {
        "AAPL": {
            "symbol": "AAPL",
            "decision_status": "SUBMITTED",
            "decision_reason": "order_submitted",
            "candidate_order_side": "buy",
            "candidate_order_qty": 4.5,
            "target_weight": 0.125,
            "spread_pct": 0.12,
            "quote_time": "2026-04-17T14:31:00+00:00",
            "reject_reasons": [],
        },
        "MSFT": {
            "symbol": "MSFT",
            "decision_status": "BLOCKED",
            "decision_reason": "max_position_pct_exceeded",
            "blocked_reason": "max_position_pct_exceeded",
            "candidate_order_side": "buy",
            "candidate_order_qty": 2.0,
            "target_weight": 0.3,
            "spread_pct": 0.21,
            "quote_time": "2026-04-17T14:31:00+00:00",
            "reject_reasons": [],
        },
        "NVDA": {
            "symbol": "NVDA",
            "decision_status": "EXIT_POLICY_TRIGGERED",
            "decision_reason": "exit_policy:take_profit_exit_band",
            "candidate_order_side": "sell",
            "candidate_order_qty": 1.25,
            "target_weight": 0.0,
            "spread_pct": 0.08,
            "quote_time": "2026-04-17T14:31:00+00:00",
            "reject_reasons": [],
        },
        "TSLA": {
            "symbol": "TSLA",
            "decision_status": "NO_TRADE",
            "decision_reason": "quality_issues",
            "quote_time": None,
            "reject_reasons": [{"code": "stale_quote"}, {"code": "missing_bar_window"}],
        },
        "META": {
            "symbol": "META",
            "decision_status": "NO_TRADE",
            "decision_reason": "short_rejected_long_only",
            "reject_reasons": [],
        },
    }


def test_summarize_symbol_row_handles_missing_fields() -> None:
    row = summarize_symbol_row({"symbol": "AAPL", "decision_status": None, "reject_reasons": []})

    assert row == ["AAPL", "n/a", "n/a", "n/a", "n/a", "n/a", "none", "n/a", "n/a"]


def test_classify_decision_bucket_groups_expected_statuses() -> None:
    summaries = _sample_decision_summaries()

    assert classify_decision_bucket(summaries["AAPL"]) == "submitted_orders"
    assert classify_decision_bucket(summaries["MSFT"]) == "blocked_by_risk"
    assert classify_decision_bucket(summaries["NVDA"]) == "exit_policy_triggered"
    assert classify_decision_bucket(summaries["TSLA"]) == "no_trade_quality_issues"
    assert classify_decision_bucket(summaries["META"]) == "other_no_action"


def test_build_cycle_dashboard_payload_produces_overview_buckets_and_rows() -> None:
    dashboard = build_cycle_dashboard_payload(
        cycle_id="cycle-123",
        as_of="2026-04-17T14:31:00+00:00",
        status="degraded",
        symbols=["AAPL", "MSFT", "NVDA", "TSLA", "META"],
        submitted_order_count=1,
        blocked_order_count=1,
        next_action="continue",
        primary_regime="risk_off",
        decision_summaries=_sample_decision_summaries(),
        alerts=[{"message": "quality issues present", "code": "quality_issues"}],
    )

    assert ("cycle id", "cycle-123") in dashboard["overview"]
    assert ("exit trigger count", "1") in dashboard["overview"]
    assert ("no-trade count", "2") in dashboard["overview"]
    assert dashboard["buckets"][0]["label"] == "submitted orders"
    assert dashboard["buckets"][0]["symbols"] == ["AAPL"]
    assert dashboard["buckets"][3]["symbols"] == ["TSLA"]
    assert dashboard["table_rows"][0]["Symbol"] == "AAPL"
    assert "Dominant data issues: stale_quote(1), missing_bar_window(1)" in dashboard["warnings"]


def test_render_table_and_text_output_are_scan_friendly() -> None:
    dashboard = build_cycle_dashboard_payload(
        cycle_id="cycle-123",
        as_of="2026-04-17T14:31:00+00:00",
        status="healthy",
        symbols=["AAPL", "MSFT"],
        submitted_order_count=1,
        blocked_order_count=1,
        next_action="continue",
        primary_regime="neutral",
        decision_summaries={key: _sample_decision_summaries()[key] for key in ("AAPL", "MSFT")},
        alerts=[],
    )

    table = render_table(dashboard["table_rows"])
    text = render_cycle_dashboard_text(dashboard)

    assert "Symbol" in table
    assert "AAPL" in table
    assert "MSFT" in table
    assert "=== BOT CYCLE OVERVIEW ===" in text
    assert "=== DECISION BUCKETS ===" in text
    assert "submitted orders: 1 [AAPL]" in text


def test_render_symbol_summary_keeps_vertical_fallback(capsys) -> None:
    print_symbol_summary(
        {
            "symbol": "AAPL",
            "bar_count": 30,
            "first_close": 100.0,
            "last_close": 104.0,
            "min_close": 99.0,
            "max_close": 105.0,
            "avg_close": 102.0,
            "avg_volume": 150000.0,
            "bid": 103.9,
            "ask": 104.1,
            "mid": 104.0,
            "spread": 0.2,
            "spread_pct": 0.192,
            "quote_time": "2026-04-17T14:31:00+00:00",
            "signal": {
                "direction": "long",
                "strength": 0.45,
                "confidence": 0.88,
                "expected_horizon": "30m",
            },
            "target_weight": 0.1,
            "candidate_order_side": "buy",
            "candidate_order_qty": 2.5,
            "decision_status": "SUBMITTED",
            "decision_reason": "order_submitted",
            "policy_action": "allow",
            "policy_reason": "rank_bucket_buy",
            "portfolio_constraints_triggered": ["max_symbol_weight"],
            "reject_reasons": [],
        }
    )

    output = capsys.readouterr().out
    assert "=== BOT DECISION SUMMARY ===" in output
    assert "Signal:       direction=long" in output
    assert "Constraints:  max_symbol_weight" in output


def test_snapshot_dashboard_payload_and_html_reuse_persisted_data() -> None:
    snapshot_payload = {
        "cycle_id": "cycle-789",
        "started_at": "2026-04-17T14:31:00+00:00",
        "symbols": ["AAPL", "TSLA"],
        "scenario_bundle": {"regime_label": "trend_up"},
        "decision_summaries": {
            "AAPL": _sample_decision_summaries()["AAPL"],
            "TSLA": _sample_decision_summaries()["TSLA"],
        },
        "monitoring_decision": {
            "status": "degraded",
            "next_action": "review",
            "alerts": [{"message": "quality issues present", "code": "quality_issues"}],
            "diagnostics": {"submitted_order_count": 1, "blocked_order_count": 0},
        },
        "cycle_report": {
            "status": "degraded",
            "submitted_order_count": 1,
            "blocked_order_count": 0,
            "next_action": "review",
        },
    }

    dashboard = build_cycle_dashboard_payload_from_snapshot(snapshot_payload)
    html = render_cycle_dashboard_html(dashboard)

    assert ("primary regime", "trend_up") in dashboard["overview"]
    assert dashboard["table_rows"][0]["Symbol"] == "AAPL"
    assert "Latest Persisted Bot Cycle" in html
    assert "submitted orders" in html
    assert "quality issues present" in html
