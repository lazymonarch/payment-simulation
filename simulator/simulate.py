#!/usr/bin/env python3
"""
Payment Simulation Load Test CLI

Usage:
    python simulator/simulate.py --count 1000 --concurrency 100
    python simulator/simulate.py --count 500 --concurrency 50
    python simulator/simulate.py --count 100 --concurrency 10 --base-url http://localhost:8000
"""

import argparse
import asyncio
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator.payload_generator import generate_batch
from simulator.report import print_report


@dataclass
class RequestResult:
    txn_id: str
    status_code: int
    response_ms: float
    outcome: str  # queued / duplicate / rate_limited / error
    error: Optional[str] = None


@dataclass
class SimulationResults:
    total: int = 0
    queued: int = 0
    duplicates: int = 0
    rate_limited: int = 0
    errors: int = 0
    latencies_ms: list = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)
    end_time: float = 0.0
    results: list = field(default_factory=list)


async def submit_payment(
    client: httpx.AsyncClient,
    payload: dict,
    semaphore: asyncio.Semaphore,
) -> RequestResult:
    """Submit a single payment and return the result."""
    async with semaphore:
        start = time.monotonic()
        try:
            response = await client.post(
                "/payments",
                json=payload,
                timeout=30.0,
            )
            elapsed_ms = (time.monotonic() - start) * 1000

            if response.status_code == 202:
                outcome = "queued"
            elif response.status_code == 200:
                outcome = "duplicate"
            elif response.status_code == 429:
                outcome = "rate_limited"
            else:
                outcome = "error"

            return RequestResult(
                txn_id=payload["txn_id"],
                status_code=response.status_code,
                response_ms=elapsed_ms,
                outcome=outcome,
            )

        except Exception as e:
            elapsed_ms = (time.monotonic() - start) * 1000
            return RequestResult(
                txn_id=payload["txn_id"],
                status_code=0,
                response_ms=elapsed_ms,
                outcome="error",
                error=str(e),
            )


async def run_simulation(
    count: int,
    concurrency: int,
    base_url: str,
    duplicate_rate: float,
    timeout_rate: float,
) -> SimulationResults:
    results = SimulationResults(total=count)
    semaphore = asyncio.Semaphore(concurrency)

    print(f"\n  Payment Simulation")
    print(f"  {'─' * 40}")
    print(f"  Target:         {base_url}")
    print(f"  Transactions:   {count:,}")
    print(f"  Concurrency:    {concurrency}")
    print(f"  Duplicate %:    {duplicate_rate * 100:.0f}%")
    print(f"  Timeout %:      {timeout_rate * 100:.0f}%")
    print(f"  {'─' * 40}\n")

    payloads = generate_batch(
        count=count,
        duplicate_rate=duplicate_rate,
        timeout_rate=timeout_rate,
    )

    async with httpx.AsyncClient(base_url=base_url) as client:
        try:
            health = await client.get("/health/ready", timeout=5.0)
            if health.status_code != 200:
                print(f"  ERROR: System not ready — {health.text}")
                sys.exit(1)
            print(f"  System health: OK\n")
        except Exception as e:
            print(f"  ERROR: Cannot reach {base_url} — {e}")
            sys.exit(1)

        results.start_time = time.monotonic()

        completed = 0
        total = len(payloads)

        def print_progress() -> None:
            pct = (completed / total) * 100
            bar_len = 30
            filled = int(bar_len * completed / total)
            bar = "█" * filled + "░" * (bar_len - filled)
            print(f"\r  [{bar}] {pct:5.1f}%  {completed:>6,}/{total:,}", end="", flush=True)

        tasks = [submit_payment(client, payload, semaphore) for payload in payloads]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            completed += 1

            results.results.append(result)
            results.latencies_ms.append(result.response_ms)

            if result.outcome == "queued":
                results.queued += 1
            elif result.outcome == "duplicate":
                results.duplicates += 1
            elif result.outcome == "rate_limited":
                results.rate_limited += 1
            else:
                results.errors += 1

            if completed % 10 == 0 or completed == total:
                print_progress()

    results.end_time = time.monotonic()
    print("\n")
    return results


def parse_args():
    parser = argparse.ArgumentParser(
        description="Payment simulation load test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Total number of payment requests to send (default: 100)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Maximum concurrent requests in flight (default: 20)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default="http://localhost:8000",
        help="Base URL of the payment API (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.05,
        help="Fraction of requests that are duplicates (default: 0.05)",
    )
    parser.add_argument(
        "--timeout-rate",
        type=float,
        default=0.03,
        help="Fraction of requests that force BANK_TIMEOUT (default: 0.03)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    results = asyncio.run(
        run_simulation(
            count=args.count,
            concurrency=args.concurrency,
            base_url=args.base_url.rstrip("/"),
            duplicate_rate=args.duplicate_rate,
            timeout_rate=args.timeout_rate,
        )
    )

    print_report(results)


if __name__ == "__main__":
    main()
