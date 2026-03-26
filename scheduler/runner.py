#Runtime entrypoint for launching 1min bot cycle on Alpaca paper trading

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence

from backend.dependencies.wiring import AppContainer, build_container


def parse_symbols(raw: str) -> list[str]:
    symbols = [piece.strip().upper() for piece in raw.split(",") if piece.strip()]
    if not symbols:
        raise ValueError("No symbols configured. Pass --symbols (example: AAPL,MSFT,NVDA).")
    return symbols


def run_once(container: AppContainer, symbols: list[str]) -> dict:
    return container.bot_scheduler.run_minute(symbols)


def run_forever(container: AppContainer, symbols: list[str], interval_seconds: int = 60) -> None:
    while True:
        result = run_once(container, symbols)
        print(
            f"cycle={result['cycle_id']} submitted={result['submitted_order_count']} "
            f"blocked={result['blocked_order_count']}"
        )
        time.sleep(interval_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Jub paper-trading bot on Alpaca.")
    parser.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated symbols to trade. Example: AAPL,MSFT,NVDA",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Cycle interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single cycle and exit.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    symbols = parse_symbols(args.symbols)
    container = build_container()

    if args.once:
        result = run_once(container, symbols)
        print(result)
        return 0

    try:
        run_forever(container, symbols, interval_seconds=args.interval_seconds)
    except KeyboardInterrupt:
        print("Stopping bot runner.")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
