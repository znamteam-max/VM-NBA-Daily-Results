#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Minute-level NBA final detector/publisher.

Production path is deliberately cheap:
1) fetch only the 1-2 Sports.ru day pages overlapping the NBA Pacific day;
2) read NBA cards and their `Завершен` + final score directly from the day page;
3) only when a game is newly final, open that specific match page for player stats;
4) hand the normalized event to the existing Telegram formatter/marker logic.

ESPN remains an enrichment/fallback source, but is no longer required for a game
to be detected as finished (important for preseason, where its feed can be empty).
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests


# GitHub workflow boolean inputs are strings. In the legacy bot bool("false")
# is True, so normalize false-looking values before importing it.
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
TZ_MSK = ZoneInfo("Europe/Moscow")
TZ_PT = ZoneInfo("America/Los_Angeles")
TZ_UTC = ZoneInfo("UTC")


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


def _canonical_event_id(d_pt, abbr1: str, abbr2: str) -> str:
    a, b = sorted((bot.canon_abbr(abbr1), bot.canon_abbr(abbr2)))
    return f"nba_{d_pt:%Y%m%d}_{a}_{b}"


def _event_pair(e: dict) -> frozenset[str]:
    return frozenset((bot.canon_abbr(e["home"]["abbr"]), bot.canon_abbr(e["away"]["abbr"])))


def _fast_soup(url: str):
    """One bounded Sports.ru request; no retry fan-out."""
    try:
        headers = {
            "User-Agent": bot.S.headers.get("User-Agent", "Mozilla/5.0"),
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        }
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code != 200:
            bot.log(f"[DBG] Sports HTTP {r.status_code} {url}")
            return None
        return bot.BeautifulSoup(r.text, "html.parser")
    except Exception as exc:
        bot.log(f"[DBG] Sports request failed {url}: {exc!r}")
        return None


def _nearby_text(anchor) -> str:
    """Return a small card/container around a match link, never the whole page."""
    for tag in ("tr", "li", "article"):
        node = anchor.find_parent(tag)
        if node is not None:
            txt = node.get_text(" ", strip=True)
            if 0 < len(txt) <= 1000:
                return txt

    node = anchor
    best = anchor.get_text(" ", strip=True)
    for _ in range(5):
        node = getattr(node, "parent", None)
        if node is None or not getattr(node, "get_text", None):
            break
        txt = node.get_text(" ", strip=True)
        if 0 < len(txt) <= 600:
            best = txt
        elif len(txt) > 600:
            break
    return best


def _team_before(text: str, pos: int) -> str | None:
    low = text[:pos].lower()
    found = []
    for name in bot.TEAM_RU_TO_ABBR:
        idx = low.rfind(name.lower())
        if idx >= 0:
            found.append((idx, name))
    return max(found)[1] if found else None


def _team_after(text: str, pos: int) -> str | None:
    low = text[pos:].lower()
    found = []
    for name in bot.TEAM_RU_TO_ABBR:
        idx = low.find(name.lower())
        if idx >= 0:
            found.append((idx, name))
    return min(found)[1] if found else None


def _sports_card_to_event(card_text: str, d_msk, match_url: str, d_pt) -> dict | None:
    """Parse a compact card such as `02:00 Завершен Атланта 123 : 132 Мемфис`."""
    text = re.sub(r"\s+", " ", card_text or "").strip()
    low = text.lower()
    if not text or len(text) > 600:
        return None
    if not any(word in low for word in ("заверш", "окончен", "закончен")):
        return None

    tm = re.search(r"(?<!\d)([01]?\d|2[0-3]):([0-5]\d)(?!\d)", text)
    if not tm:
        return None
    hh, mm = int(tm.group(1)), int(tm.group(2))
    start_msk = datetime(d_msk.year, d_msk.month, d_msk.day, hh, mm, tzinfo=TZ_MSK)
    start_pt = start_msk.astimezone(TZ_PT)
    if start_pt.date() != d_pt:
        return None

    score_match = None
    for m in re.finditer(r"(?<!\d)(\d{2,3})\s*:\s*(\d{2,3})(?!\d)", text):
        s1, s2 = int(m.group(1)), int(m.group(2))
        if 50 <= s1 <= 199 and 50 <= s2 <= 199:
            score_match = m
            break
    if not score_match:
        return None

    name1 = _team_before(text, score_match.start())
    name2 = _team_after(text, score_match.end())
    if not name1 or not name2 or name1 == name2:
        return None

    abbr1 = bot.TEAM_RU_TO_ABBR.get(name1)
    abbr2 = bot.TEAM_RU_TO_ABBR.get(name2)
    if not abbr1 or not abbr2:
        return None

    s1, s2 = int(score_match.group(1)), int(score_match.group(2))
    event_id = _canonical_event_id(d_pt, abbr1, abbr2)
    utc_iso = start_msk.astimezone(TZ_UTC).isoformat().replace("+00:00", "Z")
    return {
        "eventId": event_id,
        "completed": True,
        "utcDate": utc_iso,
        "utcCompDate": utc_iso,
        # Home/away labels are not relied on for display; the legacy formatter
        # matches by team pair and handles either page order correctly.
        "home": {
            "abbr": abbr1,
            "teamId": "",
            "score": s1,
            "winner": s1 > s2,
            "record": "",
        },
        "away": {
            "abbr": abbr2,
            "teamId": "",
            "score": s2,
            "winner": s2 > s1,
            "record": "",
        },
        "_sports_url": match_url,
        "_sports_card": text,
        "_start_pt": start_pt,
    }


def _sports_completed_events_for_pt_day(d_pt) -> tuple[list[dict], int]:
    """Detect NBA finals from day-page cards only (normally two HTTP GETs)."""
    events: dict[str, dict] = {}
    successful_pages = 0
    seen_urls = set()

    for d_msk in bot.sportsru_dates_for_pt_day(d_pt):
        soup = _fast_soup(bot.day_url(d_msk))
        if not soup:
            continue
        successful_pages += 1

        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            if "/basketball/match/" not in href:
                continue
            url = bot._normalize_match_url(href)
            if url in seen_urls or url.endswith("/basketball/match/live/"):
                continue
            seen_urls.add(url)

            info = _sports_card_to_event(_nearby_text(a), d_msk, url, d_pt)
            if info:
                events[info["eventId"]] = info

    out = sorted(events.values(), key=lambda e: e.get("_start_pt") or datetime.min.replace(tzinfo=TZ_PT))
    bot.log(
        f"[DBG] Sports finals for PT {d_pt}: {len(out)} "
        f"(day_pages={successful_pages}/{len(bot.sportsru_dates_for_pt_day(d_pt))})"
    )
    return out, successful_pages


def _fetch_espn_events_for_day_all_types(d) -> list[dict]:
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


def _completed_espn_events_for_pt_day(d_pt) -> list[dict]:
    raw = []
    for d in bot.espn_dates_for_pt_day(d_pt):
        raw.extend(_fetch_espn_events_for_day_all_types(d))

    out = []
    seen = set()
    for e in raw:
        if not e.get("completed"):
            continue
        dt = bot._espn_pt_start_dt(e)
        if not dt or dt.date() != d_pt:
            continue
        pair = tuple(sorted((bot.canon_abbr(e["home"]["abbr"]), bot.canon_abbr(e["away"]["abbr"]))))
        if pair in seen:
            continue
        seen.add(pair)
        e = dict(e)
        e["eventId"] = _canonical_event_id(d_pt, pair[0], pair[1])
        out.append(e)
    bot.log(f"[DBG] ESPN fallback/enrichment finals for {d_pt}: {len(out)}")
    return out


def _enrich_with_espn(events: list[dict], d_pt) -> list[dict]:
    """Add regular-season records from ESPN when available; Sports stays authoritative for finish."""
    if not events:
        return events
    espn = _completed_espn_events_for_pt_day(d_pt)
    by_pair = {_event_pair(e): e for e in espn}

    for e in events:
        richer = by_pair.get(_event_pair(e))
        if not richer:
            continue
        by_abbr = {
            bot.canon_abbr(richer["home"]["abbr"]): richer["home"],
            bot.canon_abbr(richer["away"]["abbr"]): richer["away"],
        }
        for side in ("home", "away"):
            abbr = bot.canon_abbr(e[side]["abbr"])
            src = by_abbr.get(abbr)
            if src:
                e[side]["record"] = src.get("record", "")
    return events


def _sports_games_for_events(events: list[dict]) -> list[dict]:
    """Open only the exact newly-final match pages to extract player tables."""
    out = []
    seen = set()
    original_soup = bot._soup
    bot._soup = _fast_soup
    try:
        for e in events:
            url = e.get("_sports_url")
            if not url or url in seen:
                continue
            seen.add(url)
            info = bot.parse_sports_match(url)
            if not info:
                bot.log(f"[DBG] Sports stats not ready {e['eventId']} {url}")
                continue
            if frozenset((bot.canon_abbr(info["teamA"]["abbr"]), bot.canon_abbr(info["teamB"]["abbr"]))) != _event_pair(e):
                bot.log(f"[DBG] Sports stats pair mismatch {e['eventId']} {url}")
                continue
            out.append(info)
            bot.log(f"[DBG] Sports stats matched {e['eventId']} players={bot.players_count(info)}")
    finally:
        bot._soup = original_soup
    return out


def _run_smoke(d_pt) -> None:
    sports, pages = _sports_completed_events_for_pt_day(d_pt)
    print(f"OK smoke date_pt={d_pt} sports_pages={pages} finals={len(sports)}")
    for e in sports:
        print(
            f"SMOKE FINAL {e['eventId']} {e['home']['abbr']} {e['home']['score']} - "
            f"{e['away']['score']} {e['away']['abbr']} url={e.get('_sports_url','')}"
        )
    # Probe one exact finished match page to verify player-stat parsing stays fast.
    if sports:
        parsed = _sports_games_for_events(sports[-1:])
        print(f"SMOKE stats_probe={len(parsed)} players={bot.players_count(parsed[0]) if parsed else 0}")


def main() -> None:
    d_pt = bot.pick_report_date_pacific_env()

    if SMOKE_TEST:
        _run_smoke(d_pt)
        return

    # Preserve explicit manual/retro debug modes exactly as before.
    if _manual_or_test_mode():
        bot.main()
        return

    Path(bot.MARKER_DIR).mkdir(parents=True, exist_ok=True)

    sports_events, sports_pages = _sports_completed_events_for_pt_day(d_pt)

    # If Sports.ru itself is unreachable, use ESPN as a fail-safe rather than
    # treating an empty response as "no games finished".
    if sports_pages == 0:
        completed = _completed_espn_events_for_pt_day(d_pt)
    else:
        completed = sports_events

    # Process never-posted games immediately; keep quick markers eligible so a
    # later minute can upgrade quick->full silently when player stats appear.
    pending = [e for e in completed if bot.read_marker_state(e["eventId"]) != "full"]
    if not pending:
        print(f"OK poll date_pt={d_pt} completed={len(completed)} actionable=0")
        return

    # ESPN is now enrichment only. This request happens only when there is an
    # actionable final, not every minute.
    if sports_pages > 0:
        pending = _enrich_with_espn(pending, d_pt)

    print("ACTION finals=" + ",".join(e["eventId"] for e in pending))

    # Reuse the proven Telegram formatting/selection/marker code, but feed it
    # only the current actionable finals and only their exact Sports.ru pages.
    bot.espn_completed_events_for_pt_day = lambda _d: pending
    bot.fetch_sports_games_for_pt_day = lambda _d: _sports_games_for_events(pending)
    bot.main()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("ERROR poll:", repr(exc), file=sys.stderr)
        sys.exit(1)
