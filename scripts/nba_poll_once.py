#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Lightweight one-minute guard for NBA live results.

Scheduled runs call this file every minute. It asks ESPN first and only invokes
full parsing/posting when at least one completed game has no local marker yet.
This keeps ordinary minute checks fast and avoids scraping Sports.ru when
nothing has finished.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


# GitHub workflow boolean inputs arrive as strings. The legacy bot intentionally
# treats every non-empty string as True, so normalize false-looking values before
# importing it (bool("false") is True in Python).
_FLAG_NAMES = ("SEND_TEST_MESSAGE", "LIST_EVENTS", "DRY_RUN")
_FALSE_VALUES = {"", "0", "false", "no", "off", "none", "null"}
for _name in _FLAG_NAMES:
    _raw = os.getenv(_name)
    if _raw is not None and _raw.strip().lower() in _FALSE_VALUES:
        os.environ[_name] = ""

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import nba_results_live_bot as bot  # noqa: E402


def _manual_or_test_mode() -> bool:
    return any(
        [
            bot.SEND_TEST_MSG,
            bot.LIST_EVENTS,
            bot.DRY_RUN,
            bool(bot.ONLY_EVENT_ID),
            bool(os.getenv("REPORT_DATE_PT", "").strip()),
            bool(bot.ALT_CHAT_ID),
        ]
    )


def main() -> None:
    # Preserve all existing workflow_dispatch / retro-test behaviour.
    if _manual_or_test_mode():
        bot.main()
        return

    d_pt = bot.pick_report_date_pacific_env()
    Path(bot.MARKER_DIR).mkdir(parents=True, exist_ok=True)

    completed = bot.espn_completed_events_for_pt_day(d_pt)
    pending = [e for e in completed if bot.read_marker_state(e["eventId"]) is None]

    if not pending:
        print(f"OK poll date_pt={d_pt} completed={len(completed)} new=0")
        return

    ids = ",".join(e["eventId"] for e in pending)
    print(f"NEW finals={len(pending)} event_ids={ids}; running full publisher")
    bot.main()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR poll:", repr(exc), file=sys.stderr)
        sys.exit(1)
