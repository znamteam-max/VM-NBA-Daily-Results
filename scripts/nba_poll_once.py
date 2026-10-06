#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Fast one-minute guard for NBA live results.

The scheduled workflow calls this every minute. A lightweight official source
is used as the finish signal; Sports.ru is queried only for specific newly
finished games, so player-stat enrichment can never turn a minute poll into a
full-day crawl.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import requests


# GitHub boolean inputs are strings. The legacy bot uses bool(env_string), where
# bool("false") is True, so normalize false-looking values before importing it.
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


SMOKE_TEST = os.getenv("SMOKE_TEST", "").strip().lower() in {"1", "true", "yes", "on"}
NBA_SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"


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


def _all_espn_events_for_pt_day(d_pt):
    raw = []
    for d in bot.espn_dates_for_pt_day(d_pt):
        raw.extend(bot.fetch_espn_events_for_day(d))
    out = []
    seen = set()
    for e in raw:
        eid = e.get("eventId")
        if not eid or eid in seen:
            continue
        dt = bot._espn_pt_start_dt(e)
        if dt and dt.date() == d_pt:
            seen.add(eid)
            out.append(e)
    return out


def _fetch_nba_schedule_games_for_day(d_pt) -> list[dict]:
    """Return raw official NBA schedule entries for an NBA calendar date."""
    try:
        r = requests.get(
            NBA_SCHEDULE_URL,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.nba.com/"},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        bot.log(f"[DBG] NBA schedule fetch failed: {exc!r}")
        return []

    target = d_pt.strftime("%m/%d/%Y")
    league = data.get("leagueSchedule") or {}
    out = []
    for block in league.get("gameDates") or []:
        raw_date = str(block.get("gameDate") or "")
        if not raw_date.startswith(target):
            continue
        out.extend(block.get("games") or [])
    return out


def _fast_soup(url: str):
    """Single short Sports.ru request: no retry fan-out, hard 4s timeout."""
    try:
        headers = {
            "User-Agent": bot.S.headers.get("User-Agent", "Mozilla/5.0"),
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        }
        r = requests.get(url, headers=headers, timeout=4)
        if r.status_code != 200:
            return None
        return bot.BeautifulSoup(r.text, "html.parser")
    except Exception as exc:
        bot.log(f"[DBG] fast Sports.ru request failed {url}: {exc!r}")
        return None


def _nearby_text(anchor) -> str:
    # Prefer a small semantic/card container. Never climb to a huge page wrapper,
    # otherwise one link could appear to contain the names of every game.
    for tag in ("tr", "li", "article"):
        node = anchor.find_parent(tag)
        if node is not None:
            txt = node.get_text(" ", strip=True)
            if 0 < len(txt) <= 1800:
                return txt

    node = anchor
    best = anchor.get_text(" ", strip=True)
    for _ in range(4):
        node = getattr(node, "parent", None)
        if node is None or not getattr(node, "get_text", None):
            break
        txt = node.get_text(" ", strip=True)
        if 0 < len(txt) <= 1200:
            best = txt
        elif len(txt) > 1200:
            break
    return best


def _fast_sports_games_for_events(events: list[dict], d_pt) -> list[dict]:
    """Find and parse only Sports.ru pages belonging to the requested events."""
    if not events:
        return []

    targets = {}
    for e in events:
        h = bot.canon_abbr(e["home"]["abbr"])
        a = bot.canon_abbr(e["away"]["abbr"])
        h_ru = bot.ABBR_TO_RU.get(h, h)
        a_ru = bot.ABBR_TO_RU.get(a, a)
        targets[e["eventId"]] = {
            "abbrs": frozenset((h, a)),
            "names": (h_ru.lower(), a_ru.lower()),
            "urls": [],
        }

    seen_urls = set()
    for d_msk in bot.sportsru_dates_for_pt_day(d_pt):
        soup = _fast_soup(bot.day_url(d_msk))
        if not soup:
            continue
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href") or ""
            if "/basketball/match/" not in href:
                continue
            url = bot._normalize_match_url(href)
            if url in seen_urls:
                continue
            text = _nearby_text(anchor).lower()
            matched_any = False
            for target in targets.values():
                n1, n2 = target["names"]
                if n1 in text and n2 in text:
                    if url not in target["urls"]:
                        target["urls"].append(url)
                    matched_any = True
            if matched_any:
                seen_urls.add(url)

    out = []
    parsed_urls = set()
    original_soup = bot._soup
    bot._soup = _fast_soup
    try:
        for eid, target in targets.items():
            found = False
            # Usually exactly one URL. Cap at 3 so markup oddities can never
            # recreate the old 70+ page crawl.
            for url in target["urls"][:3]:
                if url in parsed_urls:
                    continue
                parsed_urls.add(url)
                info = bot.parse_sports_match(url)
                if not info:
                    continue
                pair = frozenset(
                    (
                        bot.canon_abbr(info["teamA"]["abbr"]),
                        bot.canon_abbr(info["teamB"]["abbr"]),
                    )
                )
                if pair == target["abbrs"]:
                    out.append(info)
                    found = True
                    bot.log(f"[DBG] fast Sports.ru matched {eid} -> {url}")
                    break
            if not found:
                bot.log(f"[DBG] fast Sports.ru miss {eid}; score-only quick post will be used")
    finally:
        bot._soup = original_soup

    return out


def _run_smoke(d_pt) -> None:
    espn_events = _all_espn_events_for_pt_day(d_pt)
    espn_completed = [e for e in espn_events if e.get("completed")]

    nba_games = _fetch_nba_schedule_games_for_day(d_pt)
    nba_completed = [g for g in nba_games if int(g.get("gameStatus") or 0) == 3]
    nba_sample = []
    for g in nba_games[:8]:
        away = (g.get("awayTeam") or {}).get("teamTricode") or "?"
        home = (g.get("homeTeam") or {}).get("teamTricode") or "?"
        nba_sample.append(f"{g.get('gameId')}:{away}@{home}:s={g.get('gameStatus')}:{g.get('gameStatusText')}")

    # Probe Sports.ru only against an ESPN event here; never send anything.
    probe = espn_completed[-1:] or espn_events[:1]
    enriched = _fast_sports_games_for_events(probe, d_pt) if probe else []
    print(
        f"OK smoke date_pt={d_pt} espn_events={len(espn_events)} espn_completed={len(espn_completed)} "
        f"nba_games={len(nba_games)} nba_completed={len(nba_completed)} sports_match={len(enriched)}"
    )
    if nba_sample:
        print("NBA sample: " + " | ".join(nba_sample))


def main() -> None:
    d_pt = bot.pick_report_date_pacific_env()

    if SMOKE_TEST:
        _run_smoke(d_pt)
        return

    # Preserve explicit ping/list/retro/debug modes exactly as before.
    if _manual_or_test_mode():
        bot.main()
        return

    Path(bot.MARKER_DIR).mkdir(parents=True, exist_ok=True)
    completed = bot.espn_completed_events_for_pt_day(d_pt)
    pending = [e for e in completed if bot.read_marker_state(e["eventId"]) is None]

    if not pending:
        print(f"OK poll date_pt={d_pt} completed={len(completed)} new=0")
        return

    ids = ",".join(e["eventId"] for e in pending)
    print(f"NEW finals={len(pending)} event_ids={ids}; running targeted publisher")

    # Replace the old full-day Sports.ru crawl only for this production tick.
    # The legacy publisher still owns formatting, Telegram entities and markers.
    bot.fetch_sports_games_for_pt_day = lambda _d: _fast_sports_games_for_events(pending, d_pt)
    bot.main()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR poll:", repr(exc), file=sys.stderr)
        sys.exit(1)
