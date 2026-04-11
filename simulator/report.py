import statistics
from typing import Optional


def print_report(results) -> None:
    duration_s = results.end_time - results.start_time
    throughput = results.total / duration_s if duration_s > 0 else 0

    lat = sorted(results.latencies_ms)
    p50 = _percentile(lat, 50)
    p95 = _percentile(lat, 95)
    p99 = _percentile(lat, 99)
    avg = statistics.mean(lat) if lat else 0
    mn = min(lat) if lat else 0
    mx = max(lat) if lat else 0

    success_rate = (results.queued / results.total * 100) if results.total > 0 else 0

    w = 44

    print(f"\n  {'═' * w}")
    print(f"  {'SIMULATION REPORT':^{w}}")
    print(f"  {'═' * w}")

    print(f"\n  THROUGHPUT")
    print(f"  {'─' * w}")
    _row("Total requests", f"{results.total:,}")
    _row("Duration", f"{duration_s:.2f}s")
    _row("Requests/sec", f"{throughput:.1f}")

    print(f"\n  OUTCOMES")
    print(f"  {'─' * w}")
    _row("Queued (202)", f"{results.queued:,}", pct=results.queued, total=results.total)
    _row("Duplicates (200)", f"{results.duplicates:,}", pct=results.duplicates, total=results.total)
    _row("Rate limited (429)", f"{results.rate_limited:,}", pct=results.rate_limited, total=results.total)
    _row("Errors", f"{results.errors:,}", pct=results.errors, total=results.total)

    print(f"\n  LATENCY  (API response time)")
    print(f"  {'─' * w}")
    _row("Min", f"{mn:.1f}ms")
    _row("Avg", f"{avg:.1f}ms")
    _row("P50", f"{p50:.1f}ms")
    _row("P95", f"{p95:.1f}ms")
    _row("P99", f"{p99:.1f}ms")
    _row("Max", f"{mx:.1f}ms")

    print(f"\n  {'═' * w}")

    if results.errors == 0 and success_rate >= 90:
        print(f"  RESULT  PASSED — {success_rate:.1f}% success rate")
    elif results.errors == 0:
        print(f"  RESULT  PARTIAL — {success_rate:.1f}% success rate")
    else:
        print(f"  RESULT  ERRORS — {results.errors} requests failed")

    print(f"  {'═' * w}\n")


def _row(label: str, value: str, pct: Optional[int] = None, total: Optional[int] = None) -> None:
    if pct is not None and total:
        percent = f"  ({pct / total * 100:5.1f}%)"
    else:
        percent = ""
    print(f"  {label:<26} {value:>10}{percent}")


def _percentile(sorted_data: list, p: int) -> float:
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])
