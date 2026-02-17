#!/usr/bin/env python3
"""Prune old interaction logs.

Example:
  python3 scripts/prune_learning_data.py --retention-days 180
"""

import argparse
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db_manager import DBManager


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--retention-days", type=int, default=180)
    args = p.parse_args()
    deleted = DBManager.prune_old_interactions(retention_days=max(1, args.retention_days))
    print(f"deleted_rows={deleted}")


if __name__ == "__main__":
    main()
