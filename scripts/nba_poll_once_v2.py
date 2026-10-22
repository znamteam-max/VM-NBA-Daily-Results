#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Production hardening wrapper for minute-level NBA results.

Key guarantees:
- all NBA cards are checked on every tick through the lightweight Sports.ru day pages;
  finals that Sports.ru does not list (e.g. preseason games) come from ESPN; a final that
  Sports.ru shows before ESPN waits for ESPN (W-L records, full box) up to ESPN_FINAL_GRACE;
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

import requests
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
# A final that Sports.ru shows first is held until ESPN marks it final too (that is when ESPN
# has the W-L records and the full box score), but at most this long after ESPN's last play.
# ESPN flipped 0.5–3.5 min after the buzzer on 22.10; normal games post on the first tick after the flip.
ESPN_FINAL_GRACE = timedelta(minutes=10)
# If the ESPN summary cannot be fetched at all, keep waiting until this long after tip-off.
ESPN_SUMMARY_FAIL_CAP = timedelta(hours=3)
# Name particles that belong to the surname: «Тристан да Силва» -> «Т. да Силва».
# («ди» is left out: it is also the letter D in «Джей Ди Дэвисон».)
PARTICLES = {"да", "де", "дель", "делла", "ван", "фон", "ла", "ле", "дос", "дю", "дер", "ду"}
NAME_SUFFIXES = {"jr.", "jr", "sr.", "sr", "мл.", "ст.", "младший", "старший", "ii", "iii", "iv"}
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
    entry = _names().setdefault(key, {})
    entry.update({"en": en, "ru": ru})  # keeps «short» and «nba» if the entry had them
    _pending().pop(key, None)
    _names_dirty = True


def _norm_en(name: str) -> str:
    """English name for matching across sources: no accents, dots, apostrophes or Jr./II suffixes."""
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    parts = [p for p in re.split(r"[\s\-]+", re.sub(r"[.,'’]", "", s)) if p]
    if parts and parts[-1] in {"jr", "sr", "ii", "iii", "iv"}:
        parts = parts[:-1]
    return " ".join(parts)


def _lookup(row: dict) -> tuple[str, dict | None]:
    """Names-database key and entry of a player row. ESPN rows carry ESPN's id; NBA rows carry
    NBA's personId, found through the entry's «nba» field or, once, through a unique English-name match
    (the found id is then stored in the entry)."""
    global _names_dirty
    nba_id = str(row.get("nba_id") or "")
    if not nba_id:
        key = str(row.get("id") or "")
        return key, (_names().get(key) if key else None)
    for key, entry in _names().items():
        if entry.get("nba") == nba_id:
            return key, entry
    target = _norm_en(row["name"])
    same = [k for k, v in _names().items() if not v.get("nba") and _norm_en(v.get("en", "")) == target]
    if len(same) == 1:
        _names()[same[0]]["nba"] = nba_id
        _names_dirty = True
        return same[0], _names()[same[0]]
    key = f"nba:{nba_id}"
    return key, _names().get(key)


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


def _clean_profile_name(text: str) -> str | None:
    """Plain Russian name from a Sports.ru heading, or None.

    Sports.ru tells namesakes apart by birth year («Девин Букер 1991»), so years and
    bracketed notes are dropped; anything that is still not a plain Cyrillic name is rejected."""
    name = re.sub(r"\(.*?\)", " ", text or "")
    name = re.sub(r"\b(?:19|20)\d{2}\b", " ", name)
    name = re.sub(r"\s+(?:II|III|IV)\s*$", " ", name)  # «Гэри Пэйтон II» -> «Гэри Пэйтон»
    name = re.sub(r"\s+", " ", name).strip(" ,-")
    if CYRILLIC.search(name) and re.fullmatch(r"[А-Яа-яЁё'’′.\- ]+", name) and len(name.split()) <= 4:
        return name
    return None


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
        name = _clean_profile_name(h1.get_text(" ", strip=True) if h1 else "")
        if name:
            return name
    return None


def _display_name(row: dict) -> str:
    """Name to print for a player row. Sports.ru rows are already Russian.

    Order: «short» override -> a valid name in the names database -> the Sports.ru stat page of
    this very game (same team, identical and unique stat line; it also repairs a broken database
    entry) -> Sports.ru profile guessed from the English name -> English name."""
    en = row["name"]
    if CYRILLIC.search(en):
        return _clean_profile_name(en) or en
    key, entry = _lookup(row)
    if entry and entry.get("short"):
        return entry["short"]
    stored = _clean_profile_name(entry.get("ru", "")) if entry else None
    if stored:
        return stored
    ru = _clean_profile_name(row.get("ru_stat") or "")
    if ru:
        if key:
            _remember(key, en, ru)
        return ru
    ru = _sportsru_name(en)
    if ru:
        if key:
            _remember(key, en, ru)
        return ru
    if key:
        _queue(key, en, row.get("team", ""))
    return en


def _short_names() -> set[str]:
    return {v["short"] for v in _names().values() if v.get("short")}


def _initials(full: str) -> str:
    """«Кевин Дюрэнт» -> «К. Дюрэнт», «Тристан да Силва» -> «Т. да Силва»,
    «Майкл Портер младший» -> «М. Портер младший». Names with a «short» override print whole."""
    if full in _short_names():
        return full
    parts = full.split()
    if len(parts) < 2:
        return full
    start = len(parts) - 1
    if parts[-1].lower() in NAME_SUFFIXES and len(parts) >= 3:
        start -= 1
    while start > 1 and parts[start - 1].lower() in PARTICLES:
        start -= 1
    return f"{parts[0][0]}. {' '.join(parts[start:])}"


# Every player line (regular and Demin/Goldin format) is shortened through this.
bot.initials_ru = _initials


# ========================= PLAYERS =========================


def _pick_team(abbr: str, rows: list[dict]) -> list[tuple[dict, bool, bool]]:
    """Top player of the team, plus a second one only when he is standout or Demin/Goldin.

    Returns (row, bold, special_format) tuples."""
    rows = sorted(rows, key=bot.score_key, reverse=True)
    if not rows:
        return []
    keys = SPECIAL_KEYS.get(abbr, [])
    # NBA spells «Dëmin»: compare both the plain lower-case name and the accent-free one
    special = next((p for p in rows if any(k in (p["name"] or "").lower() or k in _norm_en(p["name"]) for k in keys)), None)
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


_summaries: dict[str, dict | None] = {}


def _espn_summary(espn_event_id: str) -> dict | None:
    """ESPN game summary (box score + play-by-play), fetched at most once per tick; None if the request failed."""
    if espn_event_id not in _summaries:
        data = None
        try:
            r = bot.S.get(ESPN_SUMMARY.format(eid=espn_event_id), headers=bot.ESPN_HEADERS, timeout=bot.HTTP_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
            else:
                bot.log(f"[DBG] ESPN summary HTTP {r.status_code} {espn_event_id}")
        except Exception as exc:
            bot.log(f"[DBG] ESPN summary failed {espn_event_id}: {exc!r}")
        _summaries[espn_event_id] = data
    return _summaries[espn_event_id]


def _last_play_utc(data: dict) -> datetime | None:
    stamps = [p.get("wallclock") for p in data.get("plays") or [] if p.get("wallclock")]
    try:
        return max(datetime.fromisoformat(s.replace("Z", "+00:00")) for s in stamps) if stamps else None
    except ValueError:
        return None


def _espn_day_events(d_pt) -> list[dict]:
    """All ESPN games of the PT day, finished or not, with the same canonical ids as the Sports.ru events."""
    raw = []
    for d in bot.espn_dates_for_pt_day(d_pt):
        raw.extend(poll._fetch_espn_events_for_day_all_types(d))
    by_pair: dict[frozenset, dict] = {}
    for e in raw:
        try:
            dt = bot._espn_pt_start_dt(e)
            if not dt or dt.date() != d_pt:
                continue
            pair = _event_pair(e)
            if len(pair) != 2 or "" in pair:
                continue  # placeholders such as TBD vs TBD
            if pair in by_pair and by_pair[pair].get("completed"):
                continue  # a game listed twice keeps its finished copy
            e = dict(e)
            e["eventId"] = poll._canonical_event_id(d_pt, *sorted(pair))
            by_pair[pair] = e
        except Exception as exc:  # one malformed ESPN event must not stop the tick
            bot.log(f"[DBG] ESPN event skipped: {exc!r}")
    return list(by_pair.values())


def _espn_grace_over(espn_ev: dict, event: dict) -> bool:
    """True once ESPN had ESPN_FINAL_GRACE after its last play to mark the game final."""
    data = _espn_summary(espn_ev["_raw_ev"]["id"])
    if data is None:  # request failed: keep waiting (next tick retries), but not forever
        try:
            tip = datetime.fromisoformat(event["utcDate"].replace("Z", "+00:00"))
        except (KeyError, ValueError, AttributeError):
            return True
        return datetime.now(timezone.utc) >= tip + ESPN_SUMMARY_FAIL_CAP
    last = _last_play_utc(data)
    if last is None:
        return True  # no play-by-play (e.g. exhibitions): nothing to wait for
    return datetime.now(timezone.utc) >= last + ESPN_FINAL_GRACE


def _box_matches_score(teams: dict, espn_ev: dict, event: dict) -> bool:
    """A box score is complete when each team's player points add up to its final score."""
    final = {bot.canon_abbr(event[s]["abbr"]): int(event[s]["score"]) for s in ("home", "away")}
    for s in ("home", "away"):
        abbr = bot.canon_abbr(espn_ev[s]["abbr"])
        rows = teams.get(espn_ev[s]["teamId"], [])
        if abbr not in final or sum(r["pts"] for r in rows) != final[abbr]:
            return False
    return True


def _copy_records(events: list[dict], espn_by_pair: dict, nba_by_pair: dict | None = None,
                  espn_any: dict | None = None) -> list[dict]:
    """W-L records of this tick's finished games: ESPN's once ESPN is final; otherwise NBA's, but only
    when NBA's record already counts this game (it differs from the pre-game record ESPN still shows)."""
    def sides(ev: dict | None) -> dict:
        return {bot.canon_abbr(ev[s]["abbr"]): ev[s] for s in ("home", "away")} if ev else {}

    for e in events:
        pair = _event_pair(e)
        espn_done = espn_by_pair.get(pair)
        nba_game = (nba_by_pair or {}).get(pair)
        pre_game = sides((espn_any or {}).get(pair))
        for side in ("home", "away"):
            abbr = bot.canon_abbr(e[side]["abbr"])
            if espn_done:
                rec = sides(espn_done).get(abbr, {}).get("record", "")
            elif nba_game and nba_game["final"]:
                rec = sides(nba_game).get(abbr, {}).get("record", "")
                if rec in ("", "0-0") or rec == pre_game.get(abbr, {}).get("record"):
                    rec = ""  # NBA has not counted this game yet
            else:
                rec = ""
            if rec:
                e[side]["record"] = rec
    return events


# ========================= NBA'S OWN DATA =========================
# cdn.nba.com refuses Russian and cloud addresses; our Cloudflare Worker nba-data-bridge forwards GETs to NBA
# when the request carries its key (GitHub secret NBA_BRIDGE_KEY). Without the key this source is off and the
# bot works on Sports.ru + ESPN exactly as before; any bridge failure also falls back to them.

NBA_BRIDGE = "https://nba-data-bridge.rngn.workers.dev"
NBA_KEY = os.environ.get("NBA_BRIDGE_KEY", "").strip()
# Own session WITHOUT retries and with a short timeout: a hanging bridge must not eat the 4-min job.
_nba_http = requests.Session()
_nba_down = False  # after the first bridge failure in this tick, NBA is skipped until the next tick
_nba_boxes: dict[str, dict] = {}
_nba_schedule: list | None = None


# Backup bridges on the owner's servers (deploy/nba-relay): same API and key, each with its own self-signed
# certificate kept in certs/ (the bot trusts exactly that certificate for that address).
NBA_RELAYS = (
    ("https://167.104.104.3:8790", ROOT / "certs" / "nba-relay-stockholm.pem"),
    ("https://217.179.50.92:8790", ROOT / "certs" / "nba-relay-warsaw.pem"),
)


def _all_nba_routes() -> list[tuple[str, object]]:
    for url, cert in NBA_RELAYS:
        if not cert.exists():
            bot.log(f"[DBG] NBA relay {url} is off: certificate {cert.name} is missing in certs/")
    return [(NBA_BRIDGE, True)] + [(url, str(cert)) for url, cert in NBA_RELAYS if cert.exists()]


_nba_routes = _all_nba_routes()  # a route that fails is dropped for the rest of the tick


def _nba_get(path: str, read_timeout: float = 8) -> dict | None:
    """NBA's JSON through the first working route: Cloudflare bridge, then Stockholm, then Warsaw."""
    global _nba_down
    if not NBA_KEY or _nba_down:
        return None
    what = path.split("?")[0]
    for route in list(_nba_routes):
        base, verify = route
        try:
            r = _nba_http.get(f"{base}/{path}", timeout=(3, read_timeout), verify=verify,
                              headers={"x-bridge-key": NBA_KEY, "User-Agent": bot.ESPN_HEADERS["User-Agent"]})
            if r.status_code == 200:
                return r.json()
        except Exception as exc:  # timeout, refused, TLS, broken JSON
            bot.log(f"[DBG] NBA route {base.split('//')[1]} failed for {what}: {type(exc).__name__}")
            _nba_routes.remove(route)
            continue
        bot.log(f"[DBG] NBA route {base.split('//')[1]} HTTP {r.status_code} for {what}")
        if r.status_code in (403, 404) and r.headers.get("content-type", "").startswith("application/xml"):
            return None  # NBA's own «no such file» (S3 XML): the same through every route
        _nba_routes.remove(route)  # anything else (Akamai, Cloudflare page, key, 5xx) is a problem of this route
    _nba_down = True
    return None


def _nba_utc(g: dict) -> str:
    """Tip-off time; the season schedule's gameTimeUTC is time-only (dated 1900), the real one is gameDateTimeUTC."""
    for key in ("gameDateTimeUTC", "gameTimeUTC"):
        value = str(g.get(key) or "")
        if value and not value.startswith("1900-"):
            return value
    return ""


def _nba_game(g: dict) -> dict | None:
    """One NBA game of the 30 clubs (+ preseason guests the bot knows, e.g. LON); All-Star events (003…) are skipped,
    their NBA codes differ from ESPN's and would post twice."""
    gid = str(g.get("gameId") or "")
    h, a = g.get("homeTeam") or {}, g.get("awayTeam") or {}
    abbrs = (bot.canon_abbr(h.get("teamTricode")), bot.canon_abbr(a.get("teamTricode")))
    if gid.startswith("003") or len(set(abbrs)) != 2 or any(x not in bot.ABBR_TO_RU for x in abbrs):
        return None

    def side(t: dict) -> dict:
        record = f"{t['wins']}-{t['losses']}" if "wins" in t and "losses" in t else ""
        return {"abbr": bot.canon_abbr(t.get("teamTricode")), "teamId": str(t.get("teamId") or ""),
                "score": int(t.get("score") or 0), "record": record}

    return {"gid": gid, "final": g.get("gameStatus") == 3, "pair": frozenset(abbrs),
            "utc": _nba_utc(g), "home": side(h), "away": side(a)}


def _nba_day_games(d_pt) -> dict:
    """NBA's games of the PT day by team pair: today's live scoreboard, or the season schedule for a past day."""
    if not NBA_KEY:
        return {}
    data = _nba_get("cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json")
    games = ((data or {}).get("scoreboard") or {}).get("games") or []

    def on_day(g: dict) -> bool:
        try:
            return datetime.fromisoformat(_nba_utc(g).replace("Z", "+00:00")).astimezone(poll.TZ_PT).date() == d_pt
        except ValueError:
            return False

    games = [g for g in games if on_day(g)]
    if not games and d_pt < datetime.now(timezone.utc).astimezone(poll.TZ_PT).date():
        global _nba_schedule
        if _nba_schedule is None:  # manual runs for a past day only; 7 MB, fetched once per process
            sched = _nba_get("cdn.nba.com/static/json/staticData/scheduleLeagueV2.json", read_timeout=30) or {}
            _nba_schedule = [g for d in (sched.get("leagueSchedule") or {}).get("gameDates") or [] for g in d.get("games") or []]
        games = [g for g in _nba_schedule if on_day(g)]
    out = {}
    for g in games:
        game = _nba_game(g)
        if game:
            out[game["pair"]] = game
    return out


def _nba_event(game: dict, d_pt) -> dict:
    """A final that only NBA lists, in the same shape as the Sports.ru / ESPN events."""
    h, a = game["home"], game["away"]
    return {"eventId": poll._canonical_event_id(d_pt, h["abbr"], a["abbr"]), "completed": True,
            "utcDate": game["utc"], "utcCompDate": game["utc"],
            "home": {**h, "winner": h["score"] > a["score"]}, "away": {**a, "winner": a["score"] > h["score"]}}


def _nba_box_rows(gid: str) -> dict[str, list[dict]]:
    """Official player lines per team abbreviation from NBA's box score (empty if unavailable)."""
    if gid not in _nba_boxes:
        _nba_boxes[gid] = (_nba_get(f"cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json") or {}).get("game") or {}
    game = _nba_boxes[gid]
    out: dict[str, list[dict]] = {}
    for side in ("homeTeam", "awayTeam"):
        team = game.get(side) or {}
        rows = []
        for p in team.get("players") or []:
            s = p.get("statistics") or {}
            row = {"nba_id": str(p.get("personId") or ""),
                   "name": f"{p.get('firstName', '')} {p.get('familyName', '')}".strip(),
                   "pts": int(s.get("points") or 0), "reb": int(s.get("reboundsTotal") or 0),
                   "ast": int(s.get("assists") or 0), "stl": int(s.get("steals") or 0), "blk": int(s.get("blocks") or 0)}
            if row["nba_id"] and row["name"] and any(row[k] for k in STAT_KEYS):
                rows.append(row)
        if rows:
            out[bot.canon_abbr(team.get("teamTricode"))] = rows
    return out


def _sums_match(rows_by_abbr: dict, event: dict) -> bool:
    """Each team's player points add up to its final score (the box score is complete)."""
    for s in ("home", "away"):
        abbr = bot.canon_abbr(event[s]["abbr"])
        if sum(r["pts"] for r in rows_by_abbr.get(abbr, [])) != int(event[s]["score"]):
            return False
    return True


def _espn_box_rows(espn_event_id: str) -> dict[str, list[dict]]:
    """Player lines per ESPN team id; empty while the box score is not there yet."""
    data = _espn_summary(espn_event_id) or {}
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
    """Pair each ESPN row with the Sports.ru name when the stat line is identical and unique on BOTH sides."""
    by_stats: dict[tuple, list[str]] = {}
    for row in sru_rows:
        by_stats.setdefault(tuple(row[k] for k in STAT_KEYS), []).append(row["name"])
    espn_count: dict[tuple, int] = {}
    for row in espn_rows:
        line = tuple(row[k] for k in STAT_KEYS)
        espn_count[line] = espn_count.get(line, 0) + 1
    out = []
    for row in espn_rows:
        line = tuple(row[k] for k in STAT_KEYS)
        found = by_stats.get(line, [])
        out.append((row, found[0] if len(found) == 1 and espn_count[line] == 1 else None))
    return out


def _ready_info(event: dict, espn_by_pair: dict, espn_any: dict, nba_by_pair: dict | None = None) -> dict | None:
    """Game info with player lines, or None while the game should wait.

    NBA's own data first: once NBA marks the game final and its box score adds up to the final score.
    Then ESPN: a final that Sports.ru shows first waits until ESPN marks it final as well (records +
    full box), at most ESPN_FINAL_GRACE after ESPN's last play. Last, the Sports.ru stat page."""
    pair = _event_pair(event)
    nba_game = (nba_by_pair or {}).get(pair)
    if nba_game and nba_game["final"]:
        box = _nba_box_rows(nba_game["gid"])
        if box and _sums_match(box, event):
            info = bot.synthesize_game_from_espn(event)
            sru = _sportsru_info(event)
            players = {}
            for side in ("teamA", "teamB"):
                abbr = info[side]["abbr"]
                ru_name = bot.ABBR_TO_RU.get(abbr, info[side]["name"])
                players[ru_name] = [
                    {**row, "team": abbr, "ru_stat": ru}
                    for row, ru in _match_stat_lines(box.get(abbr, []), _sportsru_rows(sru, abbr))
                ]
            info["players"] = players
            bot.log(f"[DBG] NBA box {event['eventId']} players={bot.players_count(info)}")
            return info
        bot.log(f"[DBG] NBA final but its box score is not complete yet {event['eventId']}")
    espn_ev = espn_by_pair.get(pair)
    if espn_ev is None and pair in espn_any:
        late = espn_any[pair]
        if not (_espn_grace_over(late, event) or _wait_expired(event)):
            bot.log(f"[DBG] waiting for ESPN to mark the final {event['eventId']}")
            return None
        # ESPN is late: its current box is used only if it is complete, otherwise the Sports.ru box.
        if _box_matches_score(_espn_box_rows(late["_raw_ev"]["id"]), late, event):
            espn_ev = late
            bot.log(f"[DBG] ESPN not final after the grace, its box adds up — using it {event['eventId']}")
        else:
            bot.log(f"[DBG] ESPN not final after the grace and its box is incomplete {event['eventId']}")
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
    espn_all = _espn_day_events(d_pt)
    espn_events = [e for e in espn_all if e.get("completed")]
    nba_by_pair = _nba_day_games(d_pt)
    # Sports.ru day pages also carry cards of neighbouring days (time only, no date), so a card can land on the
    # wrong PT day. Keep a Sports.ru final only if ESPN or NBA lists that pair on this PT day (when they answer).
    known = {_event_pair(e) for e in espn_all} | set(nba_by_pair)
    if known:
        stray = [e["eventId"] for e in sports_events if _event_pair(e) not in known]
        if stray:
            bot.log(f"[DBG] Sports.ru cards not on this PT day per ESPN/NBA, ignored: {','.join(stray)}")
        sports_events = [e for e in sports_events if _event_pair(e) in known]
    if sports_pages > 0:
        # Sports.ru is the main finish signal; ESPN adds finals Sports.ru does not list.
        listed = {_event_pair(e) for e in sports_events}
        completed = sports_events + [e for e in espn_events if _event_pair(e) not in listed]
    else:
        completed = espn_events
    # NBA's own final counts too (it may come before Sports.ru's card); same canonical ids, so no doubles.
    listed = {_event_pair(e) for e in completed}
    completed += [_nba_event(g, d_pt) for p, g in nba_by_pair.items() if g["final"] and p not in listed]

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
    espn_any = {_event_pair(e): e for e in espn_all}

    # Quick markers are upgraded silently once their box score exists.
    infos = []
    for event in quick:
        info = _ready_info(event, espn_by_pair, espn_any, nba_by_pair)
        if info is not None:
            infos.append(info)

    # At most one never-posted final per tick. It is posted only with its box score
    # (ESPN once final, see _ready_info), or score-only once PLAYERS_WAIT has passed since tip-off.
    publish = None
    for event in new:
        info = _ready_info(event, espn_by_pair, espn_any, nba_by_pair)
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

    # W-L records come from the ESPN games fetched above (no second ESPN round trip).
    # Sports.ru stays authoritative for finish + score.
    actionable = _copy_records(actionable, espn_by_pair, nba_by_pair, espn_any)

    print(
        "ACTION quick=" + ",".join(e["eventId"] for e in quick) +
        " publish=" + (publish["eventId"] if publish else "")
    )

    # Feed only the selected actions to the proven formatter/sender/marker code.
    bot.espn_completed_events_for_pt_day = lambda _d: actionable
    bot.fetch_sports_games_for_pt_day = lambda _d: infos
    bot.main()
    save_names()


def _check_names_db() -> None:
    """Every stored Russian name must be a plain name (no years, brackets, Latin)."""
    bad = [(k, v.get("ru")) for k, v in _names().items()
           if not v.get("ru") or _clean_profile_name(v["ru"]) != v["ru"]]
    if bad:
        raise RuntimeError(f"Names DB: {len(bad)} names are not plain Russian names, e.g. {bad[:5]}")
    print(f"OK names db={len(_names())} pending={len(_pending())}")


def _smoke_nba() -> None:
    """NBA bridge probe; never fails the smoke (without the key or with the bridge down the bot uses ESPN)."""
    if not NBA_KEY:
        print("NBA bridge: no key, NBA source is off")
        return
    for url, cert in NBA_RELAYS:
        if not cert.exists():
            print(f"NBA route {url.split('//')[1]}: NO CERT ({cert.name} missing in certs/)")
    for base, verify in _all_nba_routes():
        try:
            r = _nba_http.get(f"{base}/cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
                              timeout=(3, 8), verify=verify,
                              headers={"x-bridge-key": NBA_KEY, "User-Agent": bot.ESPN_HEADERS["User-Agent"]})
            games = len(r.json()["scoreboard"]["games"]) if r.status_code == 200 else "-"
            print(f"NBA route {base.split('//')[1]}: HTTP {r.status_code} games_today={games}")
        except Exception as exc:
            print(f"NBA route {base.split('//')[1]}: FAILED {type(exc).__name__}")


def main() -> None:
    if poll.SMOKE_TEST:
        # Current-source probe plus a fixed known slate regression.
        _check_names_db()
        _smoke_nba()
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
