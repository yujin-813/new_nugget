#!/usr/bin/env python3
"""Export anonymized learning logs to JSONL for model training.

Example:
  python3 scripts/export_training_data.py --days 60 --limit 20000 --output training_60d.jsonl
"""

import argparse
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db_manager import DBManager


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--limit", type=int, default=5000)
    p.add_argument("--user-id", type=str, default=None)
    p.add_argument("--include-abstained", action="store_true")
    p.add_argument("--label-filter", type=str, default="good", help="good|bad|unknown|unlabeled|all")
    p.add_argument("--include-unlabeled", action="store_true")
    p.add_argument("--output", type=str, default="training_examples.jsonl")
    args = p.parse_args()

    examples = DBManager.export_training_examples(
        user_id=args.user_id,
        days=max(1, args.days),
        limit=max(1, args.limit),
        include_abstained=bool(args.include_abstained),
        label_filter=args.label_filter,
        include_unlabeled=bool(args.include_unlabeled),
    )

    with open(args.output, "w", encoding="utf-8") as f:
        for ex in examples:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")

    print(f"exported={len(examples)} file={args.output}")


if __name__ == "__main__":
    main()
