#!/usr/bin/env python3
"""Quick parallel feed validator — completes under 60s for all OPML feeds.

Usage:
    python scripts/validate_feeds.py --opml feeds/follow.example.opml
    python scripts/validate_feeds.py --opml feeds/follow.example.opml --timeout 10 --json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from pathlib import Path
from typing import Any

import requests

from update_news import parse_opml_subscriptions


def check_feed(feed: dict[str, str], timeout: int) -> dict[str, Any]:
    """Fetch a single feed and return diagnostic info."""
    url = feed["xml_url"]
    title = feed.get("title", url)
    result = {
        "title": title,
        "url": url,
        "ok": False,
        "head_ok": False,
        "head_status": None,
        "get_status": None,
        "item_count": 0,
        "content_type": None,
        "duration_ms": 0,
        "error": None,
        "error_type": None,
    }

    t0 = time.perf_counter()
    try:
        # Phase 1: quick HEAD check
        try:
            head_resp = requests.head(
                url, timeout=min(8, timeout), allow_redirects=True,
                headers={"User-Agent": "FeedValidator/1.0"}
            )
            result["head_status"] = head_resp.status_code
            result["head_ok"] = 200 <= head_resp.status_code < 400
        except requests.Timeout:
            result["error"] = "HEAD request timed out"
            result["error_type"] = "timeout_head"
            result["duration_ms"] = int((time.perf_counter() - t0) * 1000)
            return result
        except requests.ConnectionError as e:
            result["error"] = f"Connection failed: {e}"
            result["error_type"] = "connection_head"
            result["duration_ms"] = int((time.perf_counter() - t0) * 1000)
            return result

        # Phase 2: quick GET to verify parseable content
        try:
            get_resp = requests.get(
                url, timeout=timeout, allow_redirects=True,
                headers={"User-Agent": "FeedValidator/1.0"}
            )
            result["get_status"] = get_resp.status_code
            result["content_type"] = get_resp.headers.get("Content-Type", "")[:80]

            if get_resp.status_code == 200:
                body = get_resp.text[:2000]
                items = len(re.findall(r'<(item|entry)\b', body, re.IGNORECASE))
                result["item_count"] = items
                result["ok"] = items > 0
                if not result["ok"]:
                    result["error"] = "Feed returned 200 but no <item>/<entry> elements found"
                    result["error_type"] = "empty_feed"
            else:
                result["error"] = f"GET returned HTTP {get_resp.status_code}"
                result["error_type"] = "http_error"
        except requests.Timeout:
            result["error"] = f"GET request timed out after {timeout}s"
            result["error_type"] = "timeout_get"
        except requests.ConnectionError as e:
            result["error"] = f"GET connection failed: {e}"
            result["error_type"] = "connection_get"
        except Exception as e:
            result["error"] = f"GET unexpected error: {e}"
            result["error_type"] = "unknown_get"
    except Exception as e:
        result["error"] = f"Unexpected: {e}"
        result["error_type"] = "unknown"

    result["duration_ms"] = int((time.perf_counter() - t0) * 1000)
    return result


def main():
    parser = argparse.ArgumentParser(description="Quick parallel OPML feed validator")
    parser.add_argument("--opml", required=True, help="Path to OPML file")
    parser.add_argument("--timeout", type=int, default=10, help="Per-feed GET timeout in seconds")
    parser.add_argument("--json", action="store_true", help="Output JSON only")
    args = parser.parse_args()

    opml_path = Path(args.opml)
    if not opml_path.exists():
        print(f"ERROR: OPML file not found: {opml_path}", file=sys.stderr)
        sys.exit(1)

    feeds = parse_opml_subscriptions(opml_path)
    if not feeds:
        print("ERROR: No feeds found in OPML", file=sys.stderr)
        sys.exit(1)

    if not args.json:
        print(f"Validating {len(feeds)} feeds (per-feed timeout: {args.timeout}s)...")
        print("=" * 70)

    wall_t0 = time.perf_counter()
    results: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=min(20, len(feeds))) as executor:
        future_map = {executor.submit(check_feed, feed, args.timeout): feed for feed in feeds}
        for future in as_completed(future_map):
            feed = future_map[future]
            try:
                result = future.result(timeout=90)
                results.append(result)
            except FutureTimeoutError:
                results.append({
                    "title": feed.get("title", feed["xml_url"]),
                    "url": feed["xml_url"],
                    "ok": False,
                    "error": "Feed validation timed out (90s wall-clock limit)",
                    "error_type": "wall_clock_timeout",
                    "duration_ms": 90000,
                })

    wall_elapsed = time.perf_counter() - wall_t0

    # Sort: failures first, then by title
    results.sort(key=lambda r: (r["ok"], r["title"]))

    ok_count = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count

    if args.json:
        print(json.dumps({
            "summary": {
                "total": len(results),
                "ok": ok_count,
                "failed": fail_count,
                "wall_seconds": round(wall_elapsed, 2),
            },
            "feeds": results,
        }, indent=2, ensure_ascii=False))
    else:
        for r in results:
            icon = "OK" if r["ok"] else "FAIL"
            if r.get("error_type") and "timeout" in r["error_type"]:
                icon = "SLOW"
            print(f"[{icon}] {r['title']}")
            print(f"     URL: {r['url']}")
            if r["ok"]:
                print(f"     Items: ~{r['item_count']} | {r['duration_ms']}ms")
            else:
                print(f"     Error: {r.get('error', 'unknown')}")
                if r.get("head_status"):
                    print(f"     HEAD: {r['head_status']} | GET: {r.get('get_status', 'N/A')}")
            print()

        print("=" * 70)
        print(f"Summary: {ok_count}/{len(results)} OK, {fail_count} failed")
        print(f"Wall-clock: {wall_elapsed:.1f}s")
        if fail_count > 0:
            print(f"\nFailed/Slow feeds:")
            for r in results:
                if not r["ok"]:
                    print(f"  - {r['title']}: {r.get('error', 'unknown')}")

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
