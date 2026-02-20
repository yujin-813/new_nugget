#!/usr/bin/env python3
import argparse
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from db_manager import DBManager


def main():
    ap = argparse.ArgumentParser(description="Label-based regression snapshot")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--user-id", type=str, default=None)
    args = ap.parse_args()

    snap = DBManager.get_regression_snapshot(
        user_id=args.user_id,
        days=max(1, args.days),
        limit=max(10, args.limit),
    )
    print(json.dumps(snap, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
