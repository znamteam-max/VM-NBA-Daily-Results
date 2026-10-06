#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Fast one-minute guard for NBA live results.

The scheduled workflow calls this every minute. Lightweight scoreboard sources
are used only as the finish signal. Sports.ru is queried only for specific newly
finished games, so player-stat enrichment can never turn a minute poll into a
full-day crawl.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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
SEASON_TYPES = ("1", "2", "3")  # preseason, regular season, postseason
SOFA_DAY_URL = "https://www.sofascore.com/api/v1/sport/basketball/scheduled-events/{ymd}"


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


def _fetch_espn_events_for_day_all_types(d) -> list[dict]:
    """Use the existing parser, but explicitly include every NBA season type."""
    base = bot.ESPN_SB
    out = []
    seen = set()
    try:
        for season_type in SEASON_TYPES:
            bot.ESPN_SB = base + f"&seasontype={season_type}"
            for e in bot.fetch_espn_events_for_day(d):
                eid = e.get("eventId")
                if eid and eid not in seen:
                    seen.add(eid)
                    out.append(e)
    finally:
        bot.ESPN_SB = base
    return out


def _all_espn_events_for_pt_day(d_pt):
    raw = []
    for d in bot.espn_dates_for_pt_day(d_pt):
        raw.extend(_fetch_espn_events_for_day_all_types(d))
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


def _completed_espn_events_for_pt_day(d_pt) -> list[dict]:
    events = _all_espn_events_for_pt_day(d_pt)
    completed = [e for e in events if e.get("completed")]
    bot.log(f"[DBG] ESPN all-season-types completed for {d_pt}: {len(completed)} / events={len(events)}")
    return completed


def _fetch_sofa_raw_day(d) -> list[dict]:
    try:
        r = requests.get(
            SOFA_DAY_URL.format(ymd=d.isoformat()),
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=8,
        )
        if r.status_code != 200:
            bot.log(f"[DBG] Sofa HTTP {r.status_code} for {d}")
            return []
        return (r.json() or {}).get("events") or []
    except Exception as exc:
        bot.log(f"[DBG] Sofa fetch failed {d}: {exc!r}")
        return []


def _is_nba_sofa_event(ev: dict) -> bool:
    tournament = ev.get("tournament") or {}
    unique = tournament.get("uniqueTournament") or {}
    name = " ".join(
        str(x or "") for x in (tournament.get("name"), unique.get("name"), unique.get("slug"))
    ).lower()
    return unique.get("id") == 132 or "nba" in name


def _sofa_events_for_pt_day(d_pt) -> list[dict]:
    """Fetch the two UTC dates that can contain starts from one Pacific day."""
    tz_pt = ZoneInfo("America/Los_Angeles")
    tz_utc = ZoneInfo("UTC")
    start = datetime(d_pt.year, d_pt.month, d_pt.day, 0, 0, tzinfo=tz_pt).astimezone(tz_utc)
    end = datetime(d_pt.year, d_pt.month, d_pt.day, 23, 59, tzinfo=tz_pt).astimezone(tz_utc)
    dates = {start.date(), end.date()}

    out = []
    seen = set()
    for d in sorted(dates):
        for ev in _fetch_sofa_raw_day(d):
            if not _is_nba_sofa_event(ev):
                continue
            try:
                ts = int(ev.get("startTimestamp") or 0)
                dt_pt = datetime.fromtimestamp(ts, tz=tz_utc).astimezone(tz_pt)
            except Exception:
                continue
            if dt_pt.date() != d_pt:
                continue
            eid = str(ev.get("id") or "")
            if not eid or eid in seen:
                continue
            seen.add(eid)
            out.append(ev)
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
    espn = _all_espn_events_for_pt_day(d_pt)
    sofa = _sofa_events_for_pt_day(d_pt)
    sofa_finished = [e for e in sofa if (e.get("status") or {}).get("type") == "finished"]

    sofa_sample = []
    for e in sofa[:10]:
        a = e.get("awayTeam") or {}
        h = e.get("homeTeam") or {}
        sofa_sample.append(
            f"{e.get('id')}:{a.get('name')}@{h.get('name')}:"
            f"status={(e.get('status') or {}).get('type')}:"
            f"score={(e.get('awayScore') or {}).get('current')}-{(e.get('homeScore') or {}).get('current')}"
        )

    # Probe at most one ESPN game card so smoke runs stay short and never send.
    espn_done = [e for e in espn if e.get("completed")]
    probe = espn_done[-1:] or espn[:1]
    enriched = _fast_sports_games_for_events(probe, d_pt) if probe else []
    print(
        f"OK smoke date_pt={d_pt} espn_events={len(espn)} espn_completed={len(espn_done)} "
        f"sofa_events={len(sofa)} sofa_finished={len(sofa_finished)} sports_match={len(enriched)}"
    )
    if sofa_sample:
        print("SOFA sample: " + " | ".join(sofa_sample))


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
    completed = _completed_espn_events_for_pt_day(d_pt)
    pending = [e for e in completed if bot.read_marker_state(e["eventId"]) is None]

    if not pending:
        print(f"OK poll date_pt={d_pt} completed={len(completed)} new=0")
        return

    ids = ",".join(e["eventId"] for e in pending)
    print(f"NEW finals={len(pending)} event_ids={ids}; running targeted publisher")

    # Route legacy main() through the already-fetched current finals and only
    # targeted Sports.ru pages. This keeps its Telegram formatting/markers while
    # avoiding a second ESPN fetch and the old full-day crawl.
    bot.espn_completed_events_for_pt_day = lambda _d: completed
    bot.fetch_sports_games_for_pt_day = lambda _d: _fast_sports_games_for_events(pending, d_pt)
    bot.main()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR poll:", repr(exc), file=sys.stderr)
        sys.exit(1)
