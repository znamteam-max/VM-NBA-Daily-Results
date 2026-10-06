#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Production hardening wrapper for minute-level NBA results.

Key guarantees:
- all NBA cards are checked on every tick through the lightweight Sports.ru day pages;
- at most ONE never-posted final is published per tick, so recovery/restarts cannot dump a batch;
- already-published `quick` results may be upgraded to `full` silently in the same tick;
- the final score from the Sports.ru day card is authoritative over any score scraped from
  the player-stat page;
- result posts normally show two standout players from each team (four total);
- push smoke tests never send Telegram messages and regression-test a known NBA slate.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# The existing poller normalizes workflow booleans and owns the fast Sports.ru parser.
import scripts.nba_poll_once as poll  # noqa: E402

bot = poll.bot

REGRESSION_DATE_PT = date(2026, 10, 5)


def _force_authoritative_final_score(info: dict, event: dict) -> None:
    """Always map the already-confirmed final score to the parsed team rows by abbreviation."""
    scores = {
        bot.canon_abbr(event["home"]["abbr"]): int(event["home"]["score"]),
        bot.canon_abbr(event["away"]["abbr"]): int(event["away"]["score"]),
    }
    for side in ("teamA", "teamB"):
        abbr = bot.canon_abbr(info[side]["abbr"])
        if abbr in scores:
            info[side]["score"] = scores[abbr]


# The legacy main() calls this after matching the stats page. Override it so the
# day-card final is authoritative even when the stats page happens to contain a
# plausible but unrelated score elsewhere in its HTML.
bot.fix_scores_with_espn = _force_authoritative_final_score


def _pick_two_team_players(abbr: str, rows: list[dict]) -> list[tuple[dict, bool, bool]]:
    """Pick two useful lines per team whenever at least two player rows exist.

    Keep the existing special treatment for Demin/Goldin, but remove the old
    20-point/double-double gate for the second ordinary player. The old gate was
    why a normal result could contain only one player from each team.
    """
    if not rows:
        return []

    rows = sorted(rows, key=bot.score_key, reverse=True)
    special_keys: list[str] = []
    if abbr == "BKN":
        special_keys = ["дёмин", "demin"]
    elif abbr == "MIA":
        special_keys = ["голдин", "goldin"]

    special = None
    for player in rows:
        name = (player.get("name") or "").lower()
        if any(key in name for key in special_keys):
            special = player
            break

    top = rows[0]
    picked: list[tuple[dict, bool, bool]] = []
    picked_names: set[str] = set()

    if special and special.get("name") == top.get("name"):
        picked.append((special, True, True))
        picked_names.add(special.get("name") or "")
    elif special:
        picked.append((top, False, False))
        picked.append((special, True, True))
        picked_names.add(top.get("name") or "")
        picked_names.add(special.get("name") or "")
    else:
        picked.append((top, False, False))
        picked_names.add(top.get("name") or "")

    if len(picked) < 2:
        for player in rows[1:]:
            name = player.get("name") or ""
            if name in picked_names:
                continue
            picked.append((player, False, False))
            break

    return picked[:2]


# Production formatter uses this function inside build_block().
bot.pick_team_players = _pick_two_team_players


def _event_pair(event: dict) -> frozenset[str]:
    return frozenset(
        (
            bot.canon_abbr(event["home"]["abbr"]),
            bot.canon_abbr(event["away"]["abbr"]),
        )
    )


def _info_pair(info: dict) -> frozenset[str]:
    return frozenset(
        (
            bot.canon_abbr(info["teamA"]["abbr"]),
            bot.canon_abbr(info["teamB"]["abbr"]),
        )
    )


def _smoke_render_known_slate() -> None:
    """End-to-end regression without Telegram: detect, parse stats, force score, render."""
    events, pages = poll._sports_completed_events_for_pt_day(REGRESSION_DATE_PT)
    if pages == 0:
        raise RuntimeError("Regression smoke: Sports.ru day pages unavailable")
    if len(events) < 5:
        raise RuntimeError(f"Regression smoke: expected >=5 finals, got {len(events)}")

    ids = [e["eventId"] for e in events]
    if len(ids) != len(set(ids)):
        raise RuntimeError("Regression smoke: duplicate canonical event IDs")

    games = poll._sports_games_for_events(events)
    games_by_pair = {_info_pair(g): g for g in games}

    rendered = 0
    for event in events:
        info = games_by_pair.get(_event_pair(event))
        if info is None:
            # Stats can disappear/change independently; score-only rendering must still work.
            info = bot.synthesize_game_from_espn(event)
        _force_authoritative_final_score(info, event)

        text = bot.build_block(info)
        expected_scores = {
            str(int(event["home"]["score"])),
            str(int(event["away"]["score"])),
        }
        if not all(score in text for score in expected_scores):
            raise RuntimeError(f"Regression smoke: rendered score mismatch for {event['eventId']}")
        if not text.strip():
            raise RuntimeError(f"Regression smoke: empty render for {event['eventId']}")

        A = info["teamA"]
        B = info["teamB"]
        Aname = bot.ABBR_TO_RU.get(A["abbr"], A["name"])
        Bname = bot.ABBR_TO_RU.get(B["abbr"], B["name"])
        rows_a = info["players"].get(Aname, [])
        rows_b = info["players"].get(Bname, [])
        selected_players = len(bot.pick_team_players(A["abbr"], rows_a)) + len(
            bot.pick_team_players(B["abbr"], rows_b)
        )
        if len(rows_a) >= 2 and len(rows_b) >= 2 and selected_players != 4:
            raise RuntimeError(
                f"Regression smoke: expected 4 selected players for {event['eventId']}, "
                f"got {selected_players}"
            )

        rendered += 1
        players = bot.players_count(info)
        preview = text.replace("\n", " | ")[:320]
        print(
            f"SMOKE RENDER {event['eventId']} players={players} "
            f"selected={selected_players} :: {preview}"
        )

    print(
        f"OK regression date_pt={REGRESSION_DATE_PT} pages={pages} "
        f"finals={len(events)} rendered={rendered}"
    )


def _run_production_tick() -> None:
    d_pt = bot.pick_report_date_pacific_env()
    Path(bot.MARKER_DIR).mkdir(parents=True, exist_ok=True)

    sports_events, sports_pages = poll._sports_completed_events_for_pt_day(d_pt)
    completed = sports_events if sports_pages > 0 else poll._completed_espn_events_for_pt_day(d_pt)

    # Keep all quick markers actionable for silent stats upgrades, but queue at most
    # one never-posted final for actual Telegram publication in this minute.
    quick = []
    new = []
    for event in completed:
        state = bot.read_marker_state(event["eventId"])
        if state == "quick":
            quick.append(event)
        elif state is None:
            new.append(event)

    selected_new = new[:1]
    actionable = quick + selected_new

    if not actionable:
        print(
            f"OK poll date_pt={d_pt} completed={len(completed)} quick={len(quick)} "
            f"new={len(new)} publish_now=0"
        )
        return

    # Enrichment is expensive relative to the day-page check, so only do it for
    # the tiny actionable set. Sports.ru stays authoritative for finish + score.
    if sports_pages > 0:
        actionable = poll._enrich_with_espn(actionable, d_pt)

    if len(new) > 1:
        queued = ",".join(e["eventId"] for e in new[1:])
        print(f"BACKLOG queued_for_later_minutes={queued}")

    print(
        "ACTION quick=" + ",".join(e["eventId"] for e in quick) +
        " publish=" + ",".join(e["eventId"] for e in selected_new)
    )

    # Feed only the selected actions to the proven formatter/sender/marker code.
    bot.espn_completed_events_for_pt_day = lambda _d: actionable
    bot.fetch_sports_games_for_pt_day = lambda _d: poll._sports_games_for_events(actionable)
    bot.main()


def main() -> None:
    if poll.SMOKE_TEST:
        # Current-source probe plus a fixed known slate regression.
        poll._run_smoke(bot.pick_report_date_pacific_env())
        _smoke_render_known_slate()
        return

    # Preserve explicit manual/retro modes exactly as before.
    if poll._manual_or_test_mode():
        poll.main()
        return

    _run_production_tick()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR poll v2:", repr(exc), file=sys.stderr)
        sys.exit(1)
