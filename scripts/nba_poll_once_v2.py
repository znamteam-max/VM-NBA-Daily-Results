#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Production hardening wrapper for minute-level NBA results.

Key guarantees:
- all NBA cards are checked on every tick through the lightweight Sports.ru day pages;
  finals that Sports.ru does not list (e.g. preseason games) come from ESPN;
- at most ONE never-posted final is published per tick, so recovery/restarts cannot dump a batch;
- already-published `quick` results may be upgraded to `full` silently in the same tick;
- the final score from the Sports.ru day card is authoritative over any score scraped from
  the player-stat page;
- player lines come from the ESPN box score. Russian names come from the names database
  (ESPN player id -> Russian name from the Sports.ru profile); unknown players are written
  to names_pending_nba.json;
- a post shows two players per team, the second one only when he is standout (or Demin/Goldin),
  and at least three players in total;
- push smoke tests never send Telegram messages and regression-test a known NBA slate.
"""

from __future__ import annotations

import json
import os
import re
import sys
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# The existing poller normalizes workflow booleans and owns the fast Sports.ru parser.
import scripts.nba_poll_once as poll  # noqa: E402

bot = poll.bot

REGRESSION_DATE_PT = date(2026, 10, 5)

# ESPN box score: complete player lines for every finished game.
ESPN_SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={eid}"
STAT_KEYS = ("pts", "reb", "ast", "stl", "blk")
# If the ESPN box score is still missing this long after tip-off, post the score alone.
PLAYERS_WAIT = timedelta(hours=4)
# Demin and Goldin always get their own line when they play.
SPECIAL_KEYS = {"BKN": ["дёмин", "demin"], "MIA": ["голдин", "goldin"]}

# Name files; the workflow commits them together with posted/.
NAMES_DB_PATH = ROOT / "names_ru_nba.json"            # {espn_player_id: {"en": ..., "ru": ...}}
NAMES_PENDING_PATH = ROOT / "names_pending_nba.json"  # {espn_player_id: {"en": ..., "team": ...}}
SRU_PERSON = "https://www.sports.ru/basketball/person/{slug}/"

# Teams that ESPN lists but Sports.ru does not (preseason exhibitions).
bot.ABBR_TO_RU.setdefault("LON", "Лондон Лайонс")

CYRILLIC = re.compile(r"[А-Яа-яЁё]")


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


# ========================= NAMES =========================

_names_db: dict | None = None
_names_pending: dict | None = None
_names_dirty = False


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}


def _names() -> dict:
    global _names_db
    if _names_db is None:
        _names_db = _load_json(NAMES_DB_PATH)
    return _names_db


def _pending() -> dict:
    global _names_pending
    if _names_pending is None:
        _names_pending = _load_json(NAMES_PENDING_PATH)
    return _names_pending


def save_names() -> None:
    """Write the name files only when they changed."""
    global _names_dirty
    if not _names_dirty:
        return
    dump = lambda d: json.dumps(d, ensure_ascii=False, indent=1, sort_keys=True) + "\n"
    NAMES_DB_PATH.write_text(dump(_names()), encoding="utf-8")
    NAMES_PENDING_PATH.write_text(dump(_pending()), encoding="utf-8")
    _names_dirty = False


def _remember(key: str, en: str, ru: str) -> None:
    global _names_dirty
    _names()[key] = {"en": en, "ru": ru}
    _pending().pop(key, None)
    _names_dirty = True


def _queue(key: str, en: str, team: str) -> None:
    global _names_dirty
    if key not in _pending():
        _pending()[key] = {"en": en, "team": team}
        _names_dirty = True


def _slug_candidates(en: str) -> list[str]:
    """Sports.ru profile slugs built from the English name: 'Kevin McCullar Jr.' -> kevin-mccullar-jr."""
    base = unicodedata.normalize("NFKD", en).encode("ascii", "ignore").decode().lower()
    base = base.replace("'", "").replace("’", "").replace(".", "")
    parts = [p for p in re.split(r"[\s\-]+", base) if p]
    if not parts:
        return []
    plain = "-".join(parts)
    if parts[-1] in {"jr", "sr", "ii", "iii", "iv"}:
        return [plain, "-".join(parts[:-1])]
    return [plain, plain + "-jr"]


def _sportsru_name(en: str) -> str | None:
    """Russian name from the Sports.ru profile page whose address is built from the English name."""
    for slug in _slug_candidates(en):
        try:
            r = bot.S.get(SRU_PERSON.format(slug=slug), timeout=bot.HTTP_TIMEOUT)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        h1 = bot.BeautifulSoup(r.text, "html.parser").find("h1")
        name = h1.get_text(" ", strip=True) if h1 else ""
        if CYRILLIC.search(name):
            return name
    return None


def _display_name(row: dict) -> str:
    """Name to print for a player row. Sports.ru rows are already Russian."""
    en = row["name"]
    if CYRILLIC.search(en):
        return en
    key = str(row.get("id") or "")
    if key and key in _names():
        return _names()[key]["ru"]
    ru = row.get("ru_stat")  # same player with the identical stat line on the Sports.ru page
    if not ru or not CYRILLIC.search(ru):
        ru = _sportsru_name(en)
    if ru and CYRILLIC.search(ru):
        if key:
            _remember(key, en, ru)
        return ru
    if key:
        _queue(key, en, row.get("team", ""))
    return en


# ========================= PLAYERS =========================


def _pick_team(abbr: str, rows: list[dict]) -> list[tuple[dict, bool, bool]]:
    """Top player of the team, plus a second one only when he is standout or Demin/Goldin.

    Returns (row, bold, special_format) tuples."""
    rows = sorted(rows, key=bot.score_key, reverse=True)
    if not rows:
        return []
    keys = SPECIAL_KEYS.get(abbr, [])
    special = next((p for p in rows if any(k in (p["name"] or "").lower() for k in keys)), None)
    top = rows[0]
    if special is top:
        return [(special, True, True)]
    if special is not None:
        return [(top, False, False), (special, True, True)]
    picked = [(top, False, False)]
    second = next((p for p in rows[1:] if bot.second_ok(p)), None)
    if second is not None:
        picked.append((second, False, False))
    return picked


def _pick_game(abbr_a, rows_a, abbr_b, rows_b):
    """Players for both teams: up to two per team, at least three in total."""
    picks_a = _pick_team(abbr_a, rows_a)
    picks_b = _pick_team(abbr_b, rows_b)
    if len(picks_a) + len(picks_b) < 3:
        taken = {id(p) for p, _, _ in picks_a + picks_b}
        rest = [(p, picks_a) for p in rows_a if id(p) not in taken]
        rest += [(p, picks_b) for p in rows_b if id(p) not in taken]
        rest.sort(key=lambda item: bot.score_key(item[0]), reverse=True)
        for p, target in rest:
            if len(picks_a) + len(picks_b) >= 3:
                break
            target.append((p, False, False))
    return picks_a, picks_b


def _build_block(info: dict) -> str:
    A, B = info["teamA"], info["teamB"]
    recA = info.get("records", {}).get(A["abbr"], "")
    recB = info.get("records", {}).get(B["abbr"], "")
    a_win = A["score"] > B["score"]
    b_win = B["score"] > A["score"]
    head = (
        f"{bot.format_score_line(bot.ABBR_TO_RU.get(A['abbr'], A['name']), A['abbr'], A['score'], a_win, recA)}\n"
        f"{bot.format_score_line(bot.ABBR_TO_RU.get(B['abbr'], B['name']), B['abbr'], B['score'], b_win, recB)}\n\n"
    )
    Aname = bot.ABBR_TO_RU.get(A["abbr"], A["name"])
    Bname = bot.ABBR_TO_RU.get(B["abbr"], B["name"])
    picks_a, picks_b = _pick_game(
        A["abbr"], info["players"].get(Aname, []), B["abbr"], info["players"].get(Bname, [])
    )

    def line(p: dict, bold: bool, special: bool) -> str:
        shown = {**p, "name": _display_name(p)}
        text = bot.format_player_special(shown) if special else bot.format_player_regular(shown, bold)
        return bot.sp(text)

    al = [line(*item) for item in picks_a]
    bl = [line(*item) for item in picks_b]
    lines = al + ([""] if al and bl else []) + bl
    return head + ("\n".join(lines) if lines else "")


bot.build_block = _build_block


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
        picks_a, picks_b = _pick_game(A["abbr"], rows_a, B["abbr"], rows_b)
        selected_players = len(picks_a) + len(picks_b)
        if len(rows_a) + len(rows_b) >= 3 and selected_players < 3:
            raise RuntimeError(
                f"Regression smoke: expected at least 3 selected players for {event['eventId']}, "
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
        r = bot.S.get(ESPN_SUMMARY.format(eid=espn_event_id), headers=bot.ESPN_HEADERS, timeout=bot.HTTP_TIMEOUT)
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
                who = athlete.get("athlete") or {}
                name = who.get("displayName") or ""
                row = {
                    "id": str(who.get("id") or ""),
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
    """Sports.ru stat page for this game; used only to match Russian names by stat line."""
    try:
        games = poll._sports_games_for_events([event])
    except Exception as exc:
        bot.log(f"[DBG] Sports.ru stat page unavailable {event['eventId']}: {exc!r}")
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


def _match_stat_lines(espn_rows: list[dict], sru_rows: list[dict]) -> list[tuple[dict, str | None]]:
    """Pair each ESPN row with the Sports.ru name when that name's stat line is identical and unique."""
    by_stats: dict[tuple, list[str]] = {}
    for row in sru_rows:
        by_stats.setdefault(tuple(row[k] for k in STAT_KEYS), []).append(row["name"])
    out = []
    for row in espn_rows:
        found = by_stats.get(tuple(row[k] for k in STAT_KEYS), [])
        out.append((row, found[0] if len(found) == 1 else None))
    return out


def _ready_info(event: dict, espn_by_pair: dict) -> dict | None:
    """Game info with player lines, or None while no box score is ready.

    ESPN is the main source. If ESPN has no box score for the game, the Sports.ru
    stat page is used, when it is already published."""
    espn_ev = espn_by_pair.get(_event_pair(event))
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
        players[ru_name] = [
            {**row, "team": abbr, "ru_stat": ru}
            for row, ru in _match_stat_lines(espn_rows, _sportsru_rows(sru, abbr))
        ]
    info["players"] = players
    bot.log(f"[DBG] ESPN box {event['eventId']} players={bot.players_count(info)}")
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
    espn_events = poll._completed_espn_events_for_pt_day(d_pt)
    if sports_pages > 0:
        # Sports.ru is the main finish signal; ESPN adds finals Sports.ru does not list.
        listed = {_event_pair(e) for e in sports_events}
        completed = sports_events + [e for e in espn_events if _event_pair(e) not in listed]
    else:
        completed = espn_events

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

    espn_by_pair = {_event_pair(e): e for e in espn_events}

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
    save_names()


def main() -> None:
    if poll.SMOKE_TEST:
        # Current-source probe plus a fixed known slate regression.
        poll._run_smoke(bot.pick_report_date_pacific_env())
        _smoke_render_known_slate()
        return

    # Single-event and list modes keep the legacy behaviour. Everything else,
    # including manual runs with a date and test-chat runs, uses the production tick.
    if bot.SEND_TEST_MSG or bot.LIST_EVENTS or bot.ONLY_EVENT_ID:
        poll.main()
        return

    _run_production_tick()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR poll v2:", repr(exc), file=sys.stderr)
        sys.exit(1)
