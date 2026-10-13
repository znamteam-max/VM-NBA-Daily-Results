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
- player lines come from the ESPN box score; the Russian name is taken from the
  Sports.ru stat page when that page shows the identical stat line for the player;
- push smoke tests never send Telegram messages and regression-test a known NBA slate.
"""

from __future__ import annotations

import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# The existing poller normalizes workflow booleans and owns the fast Sports.ru parser.
import scripts.nba_poll_once as poll  # noqa: E402

bot = poll.bot

REGRESSION_DATE_PT = date(2026, 10, 5)

# ESPN box score: complete player lines for every finished game, including games whose
# Sports.ru stat page has not been published yet.
ESPN_SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={eid}"
STAT_KEYS = ("pts", "reb", "ast", "stl", "blk")
# If the ESPN box score is still missing this long after tip-off, post the score alone.
PLAYERS_WAIT = timedelta(hours=4)


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


def _espn_box_rows(espn_event_id: str) -> dict[str, list[dict]]:
    """Player lines per ESPN team id; empty while the box score is not there yet."""
    try:
        r = bot.S.get(ESPN_SUMMARY.format(eid=espn_event_id), timeout=bot.HTTP_TIMEOUT)
        if r.status_code != 200:
            return {}
        data = r.json()
    except Exception as exc:
        bot.log(f"[DBG] ESPN box score failed {espn_event_id}: {exc!r}")
        return {}

    out: dict[str, list[dict]] = {}
    for team in (data.get("boxscore") or {}).get("players") or []:
        team_id = str((team.get("team") or {}).get("id") or "")
        rows = []
        for block in team.get("statistics") or []:
            keys = block.get("keys") or []
            for athlete in block.get("athletes") or []:
                if not athlete.get("stats"):
                    continue
                s = dict(zip(keys, athlete["stats"]))
                name = (athlete.get("athlete") or {}).get("displayName") or ""
                row = {
                    "name": name,
                    "pts": bot._intish(s.get("points")),
                    "reb": bot._intish(s.get("rebounds")),
                    "ast": bot._intish(s.get("assists")),
                    "stl": bot._intish(s.get("steals")),
                    "blk": bot._intish(s.get("blocks")),
                }
                if name and any(row[k] for k in STAT_KEYS):
                    rows.append(row)
        if team_id and rows:
            out[team_id] = rows
    return out


def _sportsru_info(event: dict) -> dict | None:
    """Sports.ru stat page for this game; used only to get Russian player names."""
    try:
        games = poll._sports_games_for_events([event])
    except Exception as exc:
        bot.log(f"[DBG] Sports.ru names unavailable {event['eventId']}: {exc!r}")
        return None
    return games[0] if games else None


def _sportsru_rows(sru_info: dict | None, abbr: str) -> list[dict]:
    if not sru_info:
        return []
    for side in ("teamA", "teamB"):
        team = sru_info[side]
        if bot.canon_abbr(team["abbr"]) == abbr:
            return sru_info["players"].get(team["name"], [])
    return []


def _with_russian_names(espn_rows: list[dict], sru_rows: list[dict]) -> list[dict]:
    """Take the Sports.ru name when its stat line is identical to ESPN's for the same player."""
    by_stats: dict[tuple, list[str]] = {}
    for row in sru_rows:
        by_stats.setdefault(tuple(row[k] for k in STAT_KEYS), []).append(row["name"])
    out = []
    for row in espn_rows:
        found = by_stats.get(tuple(row[k] for k in STAT_KEYS), [])
        out.append({**row, "name": found[0] if len(found) == 1 else row["name"]})
    return out


def _ready_info(event: dict, espn_by_pair: dict) -> dict | None:
    """Game info with player lines, or None while no box score is ready.

    ESPN is the main source. If ESPN has no box score for the game, the Sports.ru
    stat page is used as before, when it is already published."""
    espn_ev = espn_by_pair.get(poll._event_pair(event))
    teams = _espn_box_rows(espn_ev["_raw_ev"]["id"]) if espn_ev else {}
    sru = _sportsru_info(event)
    if not teams:
        if sru and any(sru["players"].values()):
            bot.log(f"[DBG] Sports.ru box used (no ESPN box) {event['eventId']}")
            return sru
        return None

    info = bot.synthesize_game_from_espn(event)
    espn_ids = {
        bot.canon_abbr(espn_ev["home"]["abbr"]): espn_ev["home"]["teamId"],
        bot.canon_abbr(espn_ev["away"]["abbr"]): espn_ev["away"]["teamId"],
    }
    players = {}
    for side in ("teamA", "teamB"):
        abbr = info[side]["abbr"]
        espn_rows = teams.get(espn_ids.get(abbr, ""), [])
        ru_name = bot.ABBR_TO_RU.get(abbr, info[side]["name"])
        players[ru_name] = _with_russian_names(espn_rows, _sportsru_rows(sru, abbr))
    info["players"] = players
    ru_names = sum(1 for rows in players.values() for r in rows if re.search(r"[А-Яа-яЁё]", r["name"]))
    bot.log(f"[DBG] ESPN box {event['eventId']} players={bot.players_count(info)} ru_names={ru_names}")
    return info


def _wait_expired(event: dict) -> bool:
    try:
        start = datetime.fromisoformat(event["utcDate"].replace("Z", "+00:00"))
    except (KeyError, ValueError, AttributeError):
        return True
    return datetime.now(timezone.utc) >= start + PLAYERS_WAIT


def _run_production_tick() -> None:
    d_pt = bot.pick_report_date_pacific_env()
    Path(bot.MARKER_DIR).mkdir(parents=True, exist_ok=True)

    sports_events, sports_pages = poll._sports_completed_events_for_pt_day(d_pt)
    completed = sports_events if sports_pages > 0 else poll._completed_espn_events_for_pt_day(d_pt)

    quick = []
    new = []
    for event in completed:
        state = bot.read_marker_state(event["eventId"])
        if state == "quick":
            quick.append(event)
        elif state is None:
            new.append(event)

    if not quick and not new:
        print(f"OK poll date_pt={d_pt} completed={len(completed)} quick=0 new=0 publish_now=0")
        return

    espn_by_pair = {poll._event_pair(e): e for e in poll._completed_espn_events_for_pt_day(d_pt)}

    # Quick markers are upgraded silently once their box score exists.
    infos = []
    for event in quick:
        info = _ready_info(event, espn_by_pair)
        if info is not None:
            infos.append(info)

    # At most one never-posted final per tick. It is posted only with its ESPN box
    # score, or score-only once PLAYERS_WAIT has passed since tip-off.
    publish = None
    for event in new:
        info = _ready_info(event, espn_by_pair)
        if info is not None or _wait_expired(event):
            publish = event
            if info is not None:
                infos.append(info)
            break

    actionable = quick + ([publish] if publish else [])
    if not actionable:
        print(
            f"OK poll date_pt={d_pt} completed={len(completed)} quick={len(quick)} "
            f"new={len(new)} publish_now=0 waiting_for_box_score=1"
        )
        return

    # Enrichment is expensive relative to the day-page check, so only do it for
    # the tiny actionable set. Sports.ru stays authoritative for finish + score.
    if sports_pages > 0:
        actionable = poll._enrich_with_espn(actionable, d_pt)

    print(
        "ACTION quick=" + ",".join(e["eventId"] for e in quick) +
        " publish=" + (publish["eventId"] if publish else "")
    )

    # Feed only the selected actions to the proven formatter/sender/marker code.
    bot.espn_completed_events_for_pt_day = lambda _d: actionable
    bot.fetch_sports_games_for_pt_day = lambda _d: infos
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
