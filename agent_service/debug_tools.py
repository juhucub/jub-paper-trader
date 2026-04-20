from __future__ import annotations

from collections import Counter
from html import escape
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping, Sequence


TABLE_COLUMNS: tuple[str, ...] = (
    "Symbol",
    "Status",
    "Side",
    "Qty",
    "Target Wt",
    "Reason",
    "Data Issues",
    "Spread %",
    "Quote Time",
)

BUCKET_SUBMITTED = "submitted_orders"
BUCKET_BLOCKED = "blocked_by_risk"
BUCKET_EXIT = "exit_policy_triggered"
BUCKET_QUALITY = "no_trade_quality_issues"
BUCKET_OTHER = "other_no_action"

BUCKET_ORDER: tuple[str, ...] = (
    BUCKET_SUBMITTED,
    BUCKET_BLOCKED,
    BUCKET_EXIT,
    BUCKET_QUALITY,
    BUCKET_OTHER,
)

BUCKET_LABELS: dict[str, str] = {
    BUCKET_SUBMITTED: "submitted orders",
    BUCKET_BLOCKED: "blocked by risk",
    BUCKET_EXIT: "exit policy triggered",
    BUCKET_QUALITY: "no trade due to quality issues",
    BUCKET_OTHER: "other/no action",
}

MAX_COLUMN_WIDTHS: dict[str, int] = {
    "Symbol": 8,
    "Status": 20,
    "Side": 6,
    "Qty": 10,
    "Target Wt": 10,
    "Reason": 28,
    "Data Issues": 24,
    "Spread %": 9,
    "Quote Time": 25,
}

RIGHT_ALIGN_COLUMNS = {"Side", "Qty", "Target Wt", "Spread %"}
COLOR_BY_STATUS = {
    "SUBMITTED": "\033[32m",
    "BLOCKED": "\033[31m",
    "EXIT_POLICY_TRIGGERED": "\033[33m",
    "NO_TRADE": "\033[90m",
}
COLOR_RESET = "\033[0m"


def _fmt_float(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, dict):
        return str(value)
    return f"{float(value):,.{decimals}f}"


def _fmt_optional(value: Any) -> str:
    return "n/a" if value is None or value == "" else str(value)


def _normalize_reason(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    return str(value).replace("_", " ")


def _truncate(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 1:
        return value[:width]
    return f"{value[: width - 1]}…"


def _format_quote_time(value: Any) -> str:
    if value is None:
        return "n/a"
    text = str(value)
    return text.replace("T", " ")


def _format_bucket_symbols(symbols: Sequence[str]) -> str:
    return ", ".join(symbols) if symbols else "none"


def _collect_reject_reason_codes(summary: Mapping[str, Any]) -> list[str]:
    reject_reasons = summary.get("reject_reasons") or []
    codes = [str(reason.get("code")) for reason in reject_reasons if reason.get("code")]
    return sorted(dict.fromkeys(codes))


def _summarize_data_issues(summary: Mapping[str, Any]) -> str:
    codes = _collect_reject_reason_codes(summary)
    if codes:
        return ", ".join(codes)
    blocked_reason = summary.get("blocked_reason")
    if blocked_reason:
        return str(blocked_reason)
    return "none"


def _coerce_alert_message(alert: Any) -> str:
    if isinstance(alert, Mapping):
        message = alert.get("message") or alert.get("code")
    else:
        message = getattr(alert, "message", None) or getattr(alert, "code", None)
    return _fmt_optional(message)


def summarize_symbol_row(summary: Mapping[str, Any]) -> list[str]:
    spread_pct = summary.get("spread_pct")
    spread_pct_display = f"{_fmt_float(spread_pct, 3)}%" if spread_pct is not None else "n/a"

    target_weight = summary.get("target_weight")
    target_weight_display = _fmt_float(target_weight, 4) if target_weight is not None else "n/a"

    return [
        _fmt_optional(summary.get("symbol")),
        _fmt_optional(summary.get("decision_status")),
        _fmt_optional(summary.get("candidate_order_side")),
        _fmt_float(summary.get("candidate_order_qty"), 4),
        target_weight_display,
        _normalize_reason(summary.get("decision_reason")),
        _summarize_data_issues(summary),
        spread_pct_display,
        _format_quote_time(summary.get("quote_time")),
    ]


def summarize_symbol_decision(symbol: str, bars: list[dict], quote: dict) -> dict[str, Any]:
    closes = [float(bar["c"]) for bar in bars if bar.get("c") is not None]
    volumes = [float(bar.get("v", 0.0)) for bar in bars]

    first_close = closes[0] if closes else 0.0
    last_close = closes[-1] if closes else 0.0
    min_close = min(closes) if closes else 0.0
    max_close = max(closes) if closes else 0.0
    avg_close = mean(closes) if closes else 0.0

    ap = quote.get("ap")
    bp = quote.get("bp")
    mid = ((ap + bp) / 2.0) if ap is not None and bp is not None else None
    spread = (ap - bp) if ap is not None and bp is not None else None
    spread_pct = ((ap - bp) / bp * 100.0) if ap is not None and bp is not None and bp > 0 else None

    return {
        "symbol": symbol,
        "bar_count": len(bars),
        "first_close": first_close,
        "last_close": last_close,
        "min_close": min_close,
        "max_close": max_close,
        "avg_close": avg_close,
        "avg_volume": mean(volumes) if volumes else 0.0,
        "ask": ap,
        "bid": bp,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
        "quote_time": quote.get("t"),
        "signal": None,
        "target_weight": None,
        "candidate_order_side": None,
        "candidate_order_qty": None,
        "decision_status": "NO_FEATURE_DECISION_YET",
        "decision_reason": None,
        "blocked_reason": None,
        "policy_action": None,
        "policy_reason": None,
        "portfolio_constraints_triggered": [],
        "reject_reasons": [],
    }


def classify_decision_bucket(summary: Mapping[str, Any]) -> str:
    status = str(summary.get("decision_status") or "").upper()
    reason = str(summary.get("decision_reason") or "").lower()
    reject_reason_codes = _collect_reject_reason_codes(summary)

    if status == "SUBMITTED":
        return BUCKET_SUBMITTED
    if status == "BLOCKED" or summary.get("blocked_reason"):
        return BUCKET_BLOCKED
    if status == "EXIT_POLICY_TRIGGERED" or reason.startswith("exit_policy:"):
        return BUCKET_EXIT
    quality_reasons = {
        "quality_issues",
        "missing_or_non_positive_prices",
        "missing_price_fallback_failed",
    }
    if status == "NO_TRADE" and (reason in quality_reasons or bool(reject_reason_codes)):
        return BUCKET_QUALITY
    return BUCKET_OTHER


def bucket_decision_summaries(
    decision_summaries: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    buckets = {bucket: [] for bucket in BUCKET_ORDER}
    for symbol, raw_summary in sorted(decision_summaries.items(), key=lambda item: item[0]):
        summary = dict(raw_summary)
        summary.setdefault("symbol", symbol)
        buckets[classify_decision_bucket(summary)].append(summary)
    return buckets


def build_symbol_table_rows(
    decision_summaries: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    buckets = bucket_decision_summaries(decision_summaries)
    for bucket in BUCKET_ORDER:
        for summary in buckets[bucket]:
            values = summarize_symbol_row(summary)
            rows.append(dict(zip(TABLE_COLUMNS, values, strict=True)))
    return rows


def render_table(
    rows: Sequence[Mapping[str, Any]],
    *,
    columns: Sequence[str] = TABLE_COLUMNS,
    use_color: bool = True,
) -> str:
    if not rows:
        return "(no symbol rows)"

    display_rows: list[dict[str, str]] = []
    widths: dict[str, int] = {}
    for column in columns:
        max_width = min(MAX_COLUMN_WIDTHS.get(column, len(column)), len(column))
        for row in rows:
            cell = _truncate(
                _fmt_optional(row.get(column)),
                MAX_COLUMN_WIDTHS.get(column, len(column)),
            )
            max_width = max(max_width, len(cell))
        widths[column] = max_width

    for row in rows:
        display_row: dict[str, str] = {}
        for column in columns:
            display_row[column] = _truncate(
                _fmt_optional(row.get(column)),
                MAX_COLUMN_WIDTHS.get(column, widths[column]),
            )
        display_rows.append(display_row)

    def format_cell(column: str, value: str) -> str:
        width = widths[column]
        text = value.rjust(width) if column in RIGHT_ALIGN_COLUMNS else value.ljust(width)
        if use_color and column == "Status":
            color = COLOR_BY_STATUS.get(value)
            if color:
                return f"{color}{text}{COLOR_RESET}"
        return text

    header_line = " | ".join(column.ljust(widths[column]) for column in columns)
    separator = "-+-".join("-" * widths[column] for column in columns)
    lines = [header_line, separator]
    for row in display_rows:
        lines.append(" | ".join(format_cell(column, row[column]) for column in columns))
    return "\n".join(lines)


def build_cycle_overview(
    *,
    cycle_id: str,
    as_of: Any,
    status: str,
    universe_size: int,
    submitted_order_count: int,
    blocked_order_count: int,
    exit_trigger_count: int,
    no_trade_count: int,
    primary_regime: Any,
    next_action: str,
) -> list[tuple[str, str]]:
    return [
        ("cycle id", _fmt_optional(cycle_id)),
        ("as_of", _fmt_optional(as_of)),
        ("status", _fmt_optional(status)),
        ("universe size", str(universe_size)),
        ("submitted order count", str(submitted_order_count)),
        ("blocked order count", str(blocked_order_count)),
        ("exit trigger count", str(exit_trigger_count)),
        ("no-trade count", str(no_trade_count)),
        ("primary regime", _fmt_optional(primary_regime)),
        ("next action", _fmt_optional(next_action)),
    ]


def collect_cycle_warnings(
    *,
    status: str,
    alerts: Sequence[Any] | None,
    decision_summaries: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    if status.lower() == "degraded":
        warnings.append("Cycle status is degraded.")

    issue_counter: Counter[str] = Counter()
    for summary in decision_summaries.values():
        for code in _collect_reject_reason_codes(summary):
            issue_counter[code] += 1
    if issue_counter:
        dominant = ", ".join(f"{code}({count})" for code, count in issue_counter.most_common(3))
        warnings.append(f"Dominant data issues: {dominant}")

    unique_alerts: list[str] = []
    for alert in alerts or []:
        message = _coerce_alert_message(alert)
        if message != "n/a" and message not in unique_alerts:
            unique_alerts.append(message)
    warnings.extend(unique_alerts[:3])
    return warnings


def build_cycle_dashboard_payload(
    *,
    cycle_id: str,
    as_of: Any,
    status: str,
    symbols: Sequence[str],
    submitted_order_count: int,
    blocked_order_count: int,
    next_action: str,
    primary_regime: Any,
    decision_summaries: Mapping[str, Mapping[str, Any]],
    alerts: Sequence[Any] | None = None,
) -> dict[str, Any]:
    normalized_summaries = {
        symbol: dict(summary, symbol=summary.get("symbol", symbol))
        for symbol, summary in decision_summaries.items()
    }
    buckets = bucket_decision_summaries(normalized_summaries)
    exit_trigger_count = len(buckets[BUCKET_EXIT])
    no_trade_count = len(buckets[BUCKET_QUALITY]) + sum(
        1
        for summary in buckets[BUCKET_OTHER]
        if str(summary.get("decision_status") or "").upper() == "NO_TRADE"
    )
    overview = build_cycle_overview(
        cycle_id=cycle_id,
        as_of=as_of,
        status=status,
        universe_size=len(symbols),
        submitted_order_count=submitted_order_count,
        blocked_order_count=blocked_order_count,
        exit_trigger_count=exit_trigger_count,
        no_trade_count=no_trade_count,
        primary_regime=primary_regime,
        next_action=next_action,
    )
    rows = build_symbol_table_rows(normalized_summaries)
    return {
        "overview": overview,
        "warnings": collect_cycle_warnings(
            status=status,
            alerts=alerts,
            decision_summaries=normalized_summaries,
        ),
        "buckets": [
            {
                "key": bucket,
                "label": BUCKET_LABELS[bucket],
                "count": len(buckets[bucket]),
                "symbols": [str(summary.get("symbol")) for summary in buckets[bucket]],
            }
            for bucket in BUCKET_ORDER
        ],
        "table_columns": list(TABLE_COLUMNS),
        "table_rows": rows,
    }


def build_cycle_dashboard_payload_from_snapshot(
    snapshot_payload: Mapping[str, Any],
) -> dict[str, Any]:
    cycle_report = snapshot_payload.get("cycle_report") or {}
    monitoring_decision = snapshot_payload.get("monitoring_decision") or {}
    scenario_bundle = snapshot_payload.get("scenario_bundle") or {}
    return build_cycle_dashboard_payload(
        cycle_id=str(snapshot_payload.get("cycle_id") or cycle_report.get("cycle_id") or "n/a"),
        as_of=snapshot_payload.get("started_at") or cycle_report.get("as_of") or "n/a",
        status=str(cycle_report.get("status") or monitoring_decision.get("status") or "unknown"),
        symbols=list(snapshot_payload.get("symbols") or cycle_report.get("symbols") or []),
        submitted_order_count=int(
            cycle_report.get("submitted_order_count")
            or monitoring_decision.get("diagnostics", {}).get("submitted_order_count")
            or 0
        ),
        blocked_order_count=int(
            cycle_report.get("blocked_order_count")
            or monitoring_decision.get("diagnostics", {}).get("blocked_order_count")
            or 0
        ),
        next_action=str(
            cycle_report.get("next_action")
            or monitoring_decision.get("next_action")
            or "continue"
        ),
        primary_regime=scenario_bundle.get("regime_label"),
        decision_summaries=snapshot_payload.get("decision_summaries") or {},
        alerts=monitoring_decision.get("alerts") or [],
    )


def render_cycle_dashboard_text(
    dashboard: Mapping[str, Any],
    *,
    use_color: bool = True,
) -> str:
    lines = ["=== BOT CYCLE OVERVIEW ==="]
    for label, value in dashboard.get("overview", []):
        lines.append(f"{label:>22}: {value}")

    warnings = list(dashboard.get("warnings") or [])
    if warnings:
        lines.append("")
        lines.append("=== CYCLE HEALTH / WARNINGS ===")
        for warning in warnings:
            lines.append(f"- {warning}")

    lines.append("")
    lines.append("=== DECISION BUCKETS ===")
    for bucket in dashboard.get("buckets", []):
        symbol_list = ", ".join(bucket["symbols"]) if bucket["symbols"] else "none"
        lines.append(f"- {bucket['label']}: {bucket['count']} [{symbol_list}]")

    lines.append("")
    lines.append("=== SYMBOL TABLE ===")
    lines.append(
        render_table(
            dashboard.get("table_rows", []),
            columns=dashboard.get("table_columns", TABLE_COLUMNS),
            use_color=use_color,
        )
    )
    return "\n".join(lines)


def render_cycle_dashboard_html(dashboard: Mapping[str, Any]) -> str:
    overview = dashboard.get("overview", [])
    warnings = dashboard.get("warnings", [])
    buckets = dashboard.get("buckets", [])
    columns = dashboard.get("table_columns", TABLE_COLUMNS)
    rows = dashboard.get("table_rows", [])

    overview_cards = "".join(
        (
            "<div class='card'>"
            f"<div class='label'>{escape(str(label))}</div>"
            f"<div class='value'>{escape(str(value))}</div>"
            "</div>"
        )
        for label, value in overview
    )
    warning_items = "".join(f"<li>{escape(str(warning))}</li>" for warning in warnings)
    if not warning_items:
        warning_items = "<li>none</li>"
    bucket_items = "".join(
        (
            "<div class='bucket'>"
            f"<div class='bucket-label'>{escape(str(bucket['label']))}</div>"
            f"<div class='bucket-meta'>{escape(str(bucket['count']))} symbols</div>"
            f"<div class='bucket-symbols'>{escape(_format_bucket_symbols(bucket['symbols']))}</div>"
            "</div>"
        )
        for bucket in buckets
    )
    head_cells = "".join(f"<th>{escape(str(column))}</th>" for column in columns)
    body_rows = "".join(
        "<tr>"
        + "".join(f"<td>{escape(str(row.get(column, 'n/a')))}</td>" for column in columns)
        + "</tr>"
        for row in rows
    )

    return (
        "<!DOCTYPE html>"
        "<html lang='en'>"
        "<head>"
        "<meta charset='utf-8' />"
        "<meta name='viewport' content='width=device-width, initial-scale=1' />"
        "<title>Latest Bot Cycle</title>"
        "<style>"
        ":root{color-scheme:light;"
        "font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,sans-serif;"
        "--bg:#f4f1ea;--panel:#fffdf8;--text:#1e2430;--muted:#6c7483;"
        "--accent:#0b6e4f;--warn:#9a3412;"
        "--line:#d9d0c3;}"
        "body{margin:0;background:linear-gradient(180deg,#f8f5ee 0%,#efe8db 100%);"
        "color:var(--text);}"
        ".page{max-width:1280px;margin:0 auto;padding:24px;}"
        ".cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;"
        "margin-bottom:20px;}"
        ".card,.bucket,.panel{background:var(--panel);border:1px solid var(--line);"
        "border-radius:14px;padding:14px;box-shadow:0 8px 20px rgba(30,36,48,.06);}"
        ".label,.bucket-meta{font-size:12px;text-transform:uppercase;letter-spacing:.08em;"
        "color:var(--muted);}"
        ".value{font-size:20px;font-weight:700;margin-top:6px;}"
        ".section-title{font-size:14px;text-transform:uppercase;letter-spacing:.08em;"
        "color:var(--muted);margin:18px 0 10px;}"
        ".warnings{margin:0;padding-left:18px;color:var(--warn);}"
        ".bucket-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));"
        "gap:12px;}"
        ".bucket-label{font-weight:700;margin-bottom:4px;}"
        ".bucket-symbols{margin-top:8px;line-height:1.4;}"
        ".table-wrap{overflow:auto;}"
        "table{width:100%;border-collapse:collapse;background:var(--panel);}"
        "th,td{padding:10px 12px;border-bottom:1px solid var(--line);text-align:left;"
        "white-space:nowrap;}"
        "th{position:sticky;top:0;background:#f6f0e6;font-size:12px;text-transform:uppercase;"
        "letter-spacing:.08em;}"
        "tr:hover td{background:#faf4ea;}"
        "</style>"
        "</head>"
        "<body>"
        "<div class='page'>"
        "<h1>Latest Persisted Bot Cycle</h1>"
        "<div class='cards'>"
        f"{overview_cards}"
        "</div>"
        "<div class='section-title'>Cycle health / warnings</div>"
        "<div class='panel'><ul class='warnings'>"
        f"{warning_items}"
        "</ul></div>"
        "<div class='section-title'>Decision buckets</div>"
        "<div class='bucket-grid'>"
        f"{bucket_items}"
        "</div>"
        "<div class='section-title'>Symbol table</div>"
        "<div class='panel table-wrap'><table><thead><tr>"
        f"{head_cells}"
        "</tr></thead><tbody>"
        f"{body_rows}"
        "</tbody></table></div>"
        "</div>"
        "</body>"
        "</html>"
    )

def write_cycle_dashboard_html_file(
    *,
    cycle_id: str,
    as_of: Any,
    symbols: Sequence[str],
    execution_result: Any,
    monitoring_decision: Any,
    scenario_bundle: Any,
    decision_summaries: Mapping[str, Mapping[str, Any]],
    output_path: str = "latest_cycle_dashboard.html",
) -> str:
    dashboard = build_runtime_cycle_dashboard(
        cycle_id=cycle_id,
        as_of=as_of,
        symbols=symbols,
        execution_result=execution_result,
        monitoring_decision=monitoring_decision,
        scenario_bundle=scenario_bundle,
        decision_summaries=decision_summaries,
    )

    html = render_cycle_dashboard_html(dashboard)
    path = Path(output_path).resolve()
    path.write_text(html, encoding="utf-8")

    print(f"Cycle dashboard HTML written to: {path}")
    return str(path)

def build_runtime_cycle_dashboard(
    *,
    cycle_id: str,
    as_of: Any,
    symbols: Sequence[str],
    execution_result: Any,
    monitoring_decision: Any,
    scenario_bundle: Any,
    decision_summaries: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    return build_cycle_dashboard_payload(
        cycle_id=cycle_id,
        as_of=as_of,
        status=str(getattr(monitoring_decision, "status", "unknown")),
        symbols=symbols,
        submitted_order_count=int(getattr(execution_result, "submitted_count", 0)),
        blocked_order_count=int(getattr(execution_result, "blocked_count", 0)),
        next_action=str(getattr(monitoring_decision, "next_action", "continue")),
        primary_regime=getattr(scenario_bundle, "regime_label", None),
        decision_summaries=decision_summaries,
        alerts=getattr(monitoring_decision, "alerts", []),
    )


def print_cycle_debug_report(
    *,
    cycle_id: str,
    as_of: Any,
    symbols: Sequence[str],
    execution_result: Any,
    monitoring_decision: Any,
    scenario_bundle: Any,
    decision_summaries: Mapping[str, Mapping[str, Any]],
    use_color: bool = False,
) -> None:
    dashboard = build_runtime_cycle_dashboard(
        cycle_id=cycle_id,
        as_of=as_of,
        symbols=symbols,
        execution_result=execution_result,
        monitoring_decision=monitoring_decision,
        scenario_bundle=scenario_bundle,
        decision_summaries=decision_summaries,
    )
    print(render_cycle_dashboard_text(dashboard, use_color=use_color))


def _format_signal(signal: Any) -> str:
    if isinstance(signal, Mapping):
        if "direction" in signal:
            return (
                f"direction={signal.get('direction')} "
                f"strength={_fmt_float(signal.get('strength'), 6)} "
                f"confidence={_fmt_float(signal.get('confidence'), 3)} "
                f"horizon={signal.get('expected_horizon')}"
            )
        return (
            f"action={signal.get('action')} "
            f"score={_fmt_float(signal.get('score'), 6)} "
            f"confidence={_fmt_float(signal.get('confidence'), 3)}"
        )
    return _fmt_float(signal, 6) if signal is not None else "n/a"


def render_symbol_detail(summary: Mapping[str, Any]) -> str:
    spread_pct = summary.get("spread_pct")
    spread_pct_display = f"{_fmt_float(spread_pct, 3)}%" if spread_pct is not None else "n/a"
    target_weight = summary.get("target_weight")
    target_weight_display = _fmt_float(target_weight, 4) if target_weight is not None else "n/a"
    policy_constraints = summary.get("portfolio_constraints_triggered") or []

    lines = [
        "=== BOT DECISION SUMMARY ===",
        f"Symbol:       {_fmt_optional(summary.get('symbol'))}",
        f"Bars:         {_fmt_optional(summary.get('bar_count'))}",
        f"First close:  {_fmt_float(summary.get('first_close'))}",
        f"Last close:   {_fmt_float(summary.get('last_close'))}",
        "Range:        "
        f"{_fmt_float(summary.get('min_close'))} - {_fmt_float(summary.get('max_close'))}",
        f"Avg close:    {_fmt_float(summary.get('avg_close'))}",
        f"Avg volume:   {_fmt_float(summary.get('avg_volume'))}",
        f"Bid / Ask:    {_fmt_float(summary.get('bid'))} / {_fmt_float(summary.get('ask'))}",
        f"Mid price:    {_fmt_float(summary.get('mid'))}",
        f"Spread:       {_fmt_float(summary.get('spread'))}",
        f"Spread %:     {spread_pct_display}",
        f"Quote time:   {_fmt_optional(summary.get('quote_time'))}",
        f"Signal:       {_format_signal(summary.get('signal'))}",
        f"Target wt:    {target_weight_display}",
        f"Order side:   {_fmt_optional(summary.get('candidate_order_side'))}",
        f"Order qty:    {_fmt_float(summary.get('candidate_order_qty'), 4)}",
        f"Status:       {_fmt_optional(summary.get('decision_status'))}",
        f"Reason:       {_normalize_reason(summary.get('decision_reason'))}",
        f"Policy:       {_fmt_optional(summary.get('policy_action'))}",
        f"Policy why:   {_normalize_reason(summary.get('policy_reason'))}",
        f"Constraints:  {', '.join(policy_constraints) if policy_constraints else 'none'}",
        f"Data issues:  {_summarize_data_issues(summary)}",
        f"Blocked by:   {_normalize_reason(summary.get('blocked_reason'))}",
    ]
    return "\n".join(lines)


def print_symbol_summary(summary: Mapping[str, Any]) -> None:
    print(render_symbol_detail(summary))


def print_symbol_table(
    summaries: Iterable[Mapping[str, Any]],
    *,
    use_color: bool = False,
) -> None:
    rows = [
        dict(zip(TABLE_COLUMNS, summarize_symbol_row(summary), strict=True))
        for summary in summaries
    ]
    print("\n=== BOT DECISION TABLE ===")
    print(render_table(rows, columns=TABLE_COLUMNS, use_color=use_color))
