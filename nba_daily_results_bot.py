#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

• Пары/рекорды (W-L): ESPN site.api (completed).
• Счёт/игроки на русском: Sports.ru (боксскор).
  Финальный счёт берётся как сумма четвертей/OT непосредственно перед словом
  «завершен/завершён» на странице матча. Если не найден — фоллбек: самая «крупная»
  пара X:Y на странице.
• Спец-логика игроков:
  – минимум один, максимум два на команду;
  – второй добавляется, если ≥ 20 очков ИЛИ дабл-дабл ИЛИ ≥ 6 перехватов/блок-шотов;
  – если играл Егор Дёмин (BKN) или Влад Голдин (MIA) — добавляется всегда и идёт жирным
    с 3 максимальными метриками (>0).
  – строки со статистикой: очки всегда; ≥5 подборов/передач, ≥4 перехватов/блок-шотов.
• Оформление: счёт и игроки — в спойлерах Telegram, счёт победителя — жирным.
• Эмодзи команд: по умолчанию базовые, можно передать кастом-пак в TEAM_EMOJI_JSON
  (JSON вида {"BOS":"<emoji>", ...}).

Переменные окружения:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID — обязательно
  TEAM_EMOJI_JSON — опционально (кастом-эмодзи)
  DEBUG_NBA=1 — расширенный лог в stderr
"""

from __future__ import annotations

import os
import sys
import re
import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry  # type: ignore
except Exception:
    Retry = None

from bs4 import BeautifulSoup

# ==================== ENV ====================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()
DEBUG = os.getenv("DEBUG_NBA", "").strip() != ""

# ==================== HTTP ====================

HTTP_TIMEOUT = 10

def _mk_adapter():
    if Retry is not None:
        r = Retry(
            total=3, connect=3, read=3, backoff_factor=0.4,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        return HTTPAdapter(max_retries=r)
    return HTTPAdapter(max_retries=2)

def make_session():
    s = requests.Session()
    ad = _mk_adapter()
    s.mount("https://", ad)
    s.mount("http://", ad)
    # ТОЛЬКО ASCII — чтобы не ловить UnicodeEncodeError в некоторых окружениях
    s.headers.update({
        "User-Agent": "NBA-DR/4.7 (sportsru-sum-frames, espn-records, spoilers, custom-emoji)",
        "Accept-Language": "ru-RU,ru;q=0.8,en;q=0.5",
        "Connection": "close",
    })
    return s

S = make_session()

def log(*a):
    if DEBUG:
        print(*a, file=sys.stderr)

def _get_json(url: str) -> dict:
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception:
        return {}

# ==================== DATES / RU ====================

RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    n0 = abs(int(n)) % 100
    n1 = n0 % 10
    if 11 <= n0 <= 19:
        return forms[2]
    if 2 <= n1 <= 4:
        return forms[1]
    if n1 == 1:
        return forms[0]
    return forms[2]

def pick_title_date_london() -> date:
    # Дата в заголовке — по Лондону, публикуем утром/днём по Европе
    now = datetime.now(ZoneInfo("Europe/London"))
    return now.date() if now.hour >= 11 else (now.date() - timedelta(days=1))

def candidate_days_for_espn() -> list[date]:
    # Чтобы корректно закрывать «хвосты» ночных матчей — гоняем ET +-1 и дату из заголовка
    now_et = datetime.now(ZoneInfo("America/New_York"))
    base_et = now_et.date() if now_et.hour >= 8 else (now_et.date() - timedelta(days=1))
    title = pick_title_date_london()
    st = {base_et - timedelta(days=1), base_et, base_et + timedelta(days=1), title}
    return sorted(st)

# ==================== TEAMS / EMOJI ====================

TEAM_RU_TO_ABBR = {
    "Атланта": "ATL",
    "Бостон": "BOS",
    "Бруклин": "BKN",
    "Шарлотт": "CHA",
    "Чикаго": "CHI",
    "Кливленд": "CLE",
    "Даллас": "DAL",
    "Денвер": "DEN",
    "Детройт": "DET",
    "Голден Стэйт": "GSW",
    "Хьюстон": "HOU",
    "Индиана": "IND",
    "Клипперс": "LAC",
    "Лейкерс": "LAL",
    "Мемфис": "MEM",
    "Майами": "MIA",
    "Милуоки": "MIL",
    "Миннесота": "MIN",
    "Новый Орлеан": "NOP",
    "Нью-Йорк": "NYK",
    "Оклахома-Сити": "OKC",
    "Орландо": "ORL",
    "Филадельфия": "PHI",
    "Финикс": "PHX",
    "Портленд": "POR",
    "Сакраменто": "SAC",
    "Сан-Антонио": "SAS",
    "Торонто": "TOR",
    "Юта": "UTA",
    "Вашингтон": "WAS",
}
ABBR_TO_RU = {v: k for k, v in TEAM_RU_TO_ABBR.items()}

TEAM_EMOJI_DEFAULT = {
    "ATL": "🦅", "BOS": "☘️", "BKN": "🕸️", "CHA": "🐝", "CHI": "🐂", "CLE": "🛡️",
    "DAL": "🐎", "DEN": "⛏️", "DET": "🔧", "GSW": "🗡️", "HOU": "🚀", "IND": "💫",
    "LAC": "✂️", "LAL": "⭐", "MEM": "🐻", "MIA": "🔥", "MIL": "🦌", "MIN": "🐺",
    "NOP": "🪶", "NYK": "🗽", "OKC": "⚡️", "ORL": "✨", "PHI": "🔔", "PHX": "☀️",
    "POR": "🧭", "SAC": "👑", "SAS": "🪙", "TOR": "🦖", "UTA": "🎷", "WAS": "🧙",
}

def load_team_emojis() -> dict[str, str]:
    if TEAM_EMOJI_JSON:
        try:
            d = json.loads(TEAM_EMOJI_JSON)
            if isinstance(d, dict):
                return {str(k).upper(): str(v) for k, v in d.items()}
        except Exception:
            pass
    return TEAM_EMOJI_DEFAULT

TEAM_EMOJI = load_team_emojis()

def emoji(abbr: str) -> str:
    return TEAM_EMOJI.get((abbr or "").upper(), "🏀")

# ==================== SPORTS.RU HELPERS ====================

def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def _normalize_match_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def _soup(url: str):
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None

def collect_day_links(d: date) -> list[str]:
    soup = _soup(day_url(d))
    if not soup:
        return []
    seen = set()
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href:
            continue
        full = _normalize_match_url(href)
        # Фильтр: реальные страницы матчей имеют формат .../team-a-vs-team-b/
        if not re.search(r"/basketball/match/[^/]+-vs-[^/]+/?$", full):
            continue
        if full in seen:
            continue
        seen.add(full)
        out.append(full)
    log("[DBG] SPORTS LINKS", len(out))
    return out

def _canonical_ru_team(raw: str) -> str | None:
    if not raw:
        return None
    t = raw.replace("«", "").replace("»", "").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t:
            return k
    return None

def _score_from_frames_near_finish(soup: BeautifulSoup) -> tuple[int, int, int] | None:
    """
    Ищем блок '... 31 : 38 27 : 33 27 : 21 22 : 31 завершен/завершён' прямо перед словом «заверш...».
    Берём 4–7 последних пар X:Y и суммируем. Возвращаем (left_total, right_total, ot_count).
    """
    try:
        text_full = soup.get_text(" ", strip=True)
    except Exception:
        return None

    t = re.sub(r"\s+", " ", (text_full or "")).lower()
    m = re.search(r"заверш[её]н", t)
    if not m:
        return None

    window = t[max(0, m.start() - 300): m.start()]
    seq = re.search(r"((?:\d{1,3}\s*:\s*\d{1,3}\s*){4,7})\s*$", window)
    if seq:
        pairs = re.findall(r"(\d{1,3})\s*:\s*(\d{1,3})", seq.group(1))
    else:
        pairs = re.findall(r"(\d{1,3})\s*:\s*(\d{1,3})", window)
        if len(pairs) < 4:
            return None
        pairs = pairs[-4:]

    try:
        left = sum(int(a) for a, _ in pairs)
        right = sum(int(b) for _, b in pairs)
    except Exception:
        return None

    ot = max(len(pairs) - 4, 0)
    return (left, right, ot)

def _players_rows_for_team(soup: BeautifulSoup, team_ru_key: str) -> list[dict]:
    """
    Находим заголовок «Статистика игроков. «{Команда}»», следующую за ним таблицу
    и читаем строки игроков. Возвращаем список словарей с метриками.
    """
    rows: list[dict] = []
    stamp = (team_ru_key or "").lower()

    # Найти нужный секционный заголовок
    anchor = None
    for h in soup.find_all(["h2", "h3", "h4"]):
        t = h.get_text(" ", strip=True)
        if "статистика игроков" in t.lower() and stamp in t.lower().split(".")[0]:
            anchor = h
            break
    if not anchor:
        return rows

    table = anchor.find_next("table")
    if not table:
        return rows

    for tr in table.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
        if not tds:
            continue
        if any(x.lower().startswith("игрок") for x in tds):
            continue
        # Найти ячейку с именем
        name_idx = None
        for i, cell in enumerate(tds[:3]):  # имя обычно в первых колонках
            if re.search(r"[^\d/:% ]", cell):
                name_idx = i
                break
        if name_idx is None:
            continue
        name = tds[name_idx]
        nums = tds[name_idx + 1 :]
        if len(nums) < 14:
            # в баскетболе sports.ru обычно ≥14 числовых ячеек
            continue

        def as_int(x: str) -> int:
            try:
                return int(x)
            except Exception:
                try:
                    return int(float(x))
                except Exception:
                    return 0

        # Типовой расклад: [PTS, ..., REB@7, AST@8, ..., STL@10, ..., BLK@12]
        pts = as_int(nums[0])
        reb = as_int(nums[7]) if len(nums) > 7 else 0
        ast = as_int(nums[8]) if len(nums) > 8 else 0
        stl = as_int(nums[10]) if len(nums) > 10 else 0
        blk = as_int(nums[12]) if len(nums) > 12 else 0

        if any([pts, reb, ast, stl, blk]):
            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})

    return rows

def parse_sports_match(url: str) -> dict | None:
    soup = _soup(url)
    if not soup:
        return None

    # --- Счёт (главный путь: сумма четвертей/OT прямо перед «завершен/завершён») ---
    scoreA = scoreB = 0
    ot = 0
    res = _score_from_frames_near_finish(soup)
    if res:
        scoreA, scoreB, ot = res
        log("[DBG] SCORE FRAMES SUM ->", f"{scoreA}:{scoreB}", f"(ot={ot})")
    else:
        # фоллбек — максимальная «крупная» пара по всей странице
        whole = soup.get_text(" ", strip=True) or ""
        pairs = re.findall(r"(\d{2,3})\s*:\s*(\d{2,3})", whole)
        if not pairs:
            return None
        best = max(pairs, key=lambda ab: int(ab[0]) + int(ab[1]))
        scoreA, scoreB = int(best[0]), int(best[1])
        ot = 0
        log("[DBG] SCORE FALLBACK MAXPAIR ->", f"{scoreA}:{scoreB}")

    # --- Команды (og:title или заголовки секций статистики) ---
    meta = soup.find("meta", attrs={"property": "og:title"})
    title = meta.get("content") if meta and meta.get("content") else (soup.title.string if soup.title else "")
    teamA = teamB = None
    if title and "—" in title:
        left, right = [x.strip() for x in title.split("—", 1)]
        teamA = _canonical_ru_team(left)
        teamB = _canonical_ru_team(right)

    if not (teamA and teamB) or teamA == teamB:
        heads = []
        for h in soup.find_all(["h2", "h3", "h4"]):
            t = h.get_text(" ", strip=True).lower()
            if "статистика игроков" in t:
                k = _canonical_ru_team(h.get_text(" ", strip=True).split(".")[0])
                if k:
                    heads.append(k)
        if len(heads) >= 2:
            teamA = teamA or heads[0]
            teamB = teamB or next((x for x in heads[1:] if x != teamA), None)

    if not (teamA and teamB) or teamA == teamB:
        return None

    a_abbr = TEAM_RU_TO_ABBR.get(teamA, "")
    b_abbr = TEAM_RU_TO_ABBR.get(teamB, "")
    if not a_abbr or not b_abbr:
        return None

    rowsA = _players_rows_for_team(soup, teamA)
    rowsB = _players_rows_for_team(soup, teamB)
    finished = ("заверш" in (soup.get_text(" ", strip=True).lower()))

    log("[DBG] OK", f"{teamA}-{teamB}", "SCORE", f"{scoreA}:{scoreB}",
        "A_rows", len(rowsA), "B_rows", len(rowsB))

    return {
        "teamA": {"name": teamA, "abbr": a_abbr, "emoji": emoji(a_abbr), "score": scoreA},
        "teamB": {"name": teamB, "abbr": b_abbr, "emoji": emoji(b_abbr), "score": scoreB},
        "ot": ot,
        "finished": bool(finished),
        "players": {teamA: rowsA, teamB: rowsB},
        "url": url,
    }

# ==================== ESPN (events + records) ====================

ESPN_SB = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"
ESPN_BOX = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eid}"

def _espn_record(c: dict) -> str:
    for r in c.get("records") or []:
        if r.get("type") == "total" and r.get("summary"):
            return r["summary"]
    return ""

def fetch_espn_events_for_day(d: date) -> list[dict]:
    j = _get_json(ESPN_SB.format(ymd=d.strftime("%Y%m%d")))
    out = []
    for ev in (j.get("events") or []):
        try:
            comp = (ev.get("competitions") or [None])[0] or {}
            comps = comp.get("competitors") or []
            if len(comps) != 2:
                continue
            home = next(c for c in comps if c.get("homeAway") == "home")
            away = next(c for c in comps if c.get("homeAway") == "away")
            th = (home.get("team") or {})
            ta = (away.get("team") or {})
            abbr_h = (th.get("abbreviation") or "").upper()
            abbr_a = (ta.get("abbreviation") or "").upper()
            if abbr_h == "GS":
                abbr_h = "GSW"
            if abbr_a == "GS":
                abbr_a = "GSW"

            status = (ev.get("status") or {}).get("type") or {}
            completed = bool(status.get("completed", False))
            try:
                period = int(status.get("period") or 0)
            except Exception:
                period = 0
            ot = max(period - 4, 0) if completed and period > 4 else 0

            def as_int(x):
                try:
                    return int(float(x))
                except Exception:
                    return 0

            out.append({
                "eventId": str(ev.get("id") or ""),
                "home": {
                    "abbr": abbr_h,
                    "teamId": str(th.get("id") or ""),
                    "score": as_int(home.get("score", 0)),
                    "winner": bool(home.get("winner", False)),
                    "record": _espn_record(home),
                },
                "away": {
                    "abbr": abbr_a,
                    "teamId": str(ta.get("id") or ""),
                    "score": as_int(away.get("score", 0)),
                    "winner": bool(away.get("winner", False)),
                    "record": _espn_record(away),
                },
                "completed": completed,
                "ot": ot,
            })
        except Exception:
            continue
    return out

def fetch_espn_events_multi(days: list[date]) -> dict[frozenset, dict]:
    seen: dict[frozenset, dict] = {}
    for d in days:
        evs = fetch_espn_events_for_day(d)
        for e in evs:
            if not e.get("completed"):
                continue
            key = frozenset([e["home"]["abbr"], e["away"]["abbr"]])
            if key in seen:
                continue
            seen[key] = e
    return seen  # pair -> event

def fetch_espn_players(event_id: str) -> dict[str, list[dict]]:
    j = _get_json(ESPN_BOX.format(eid=event_id))
    out: dict[str, list[dict]] = {}
    for team_block in (j.get("players") or []):
        team = team_block.get("team") or {}
        tid = str(team.get("id") or "")
        arr = []
        for grp in (team_block.get("statistics") or []):
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                nm = (ath.get("displayName") or "").strip()
                stats = {}
                for k, v in (a.get("stats") or {}).items():
                    stats[str(k).lower()] = v
                for k, v in (ath.get("stats") or {}).items():
                    stats.setdefault(str(k).lower(), v)

                def iget(*keys, default=0):
                    for k in keys:
                        if k in stats:
                            try:
                                return int(stats[k])
                            except Exception:
                                try:
                                    return int(float(stats[k]))
                                except Exception:
                                    pass
                    return default

                pts = iget("points", "pts")
                reb = iget("rebounds", "reb", "reboundstotal")
                ast = iget("assists", "ast")
                stl = iget("steals", "stl")
                blk = iget("blocks", "blk")
                if any([pts, reb, ast, stl, blk]):
                    arr.append({"name": nm, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        # merge by name
        merged: dict[str, dict] = {}
        for p in arr:
            if p["name"] not in merged:
                merged[p["name"]] = p
            else:
                m = merged[p["name"]]
                for k in ("pts", "reb", "ast", "stl", "blk"):
                    m[k] = max(m[k], p[k])
        out[tid] = list(merged.values())
    return out

# ==================== PLAYERS / LINES ====================

def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts:
        return full or ""
    if len(parts) == 1:
        return parts[0]
    first = parts[0]
    last = parts[-1]
    if last.lower() in {"jr.", "jr", "мл.", "ст.", "sr.", "sr"} and len(parts) >= 3:
        last = parts[-2] + " " + parts[-1]
    return f"{first[0]}. {last}"

def ru_forms(label: str, v: int) -> str:
    if label == "pts":
        return f"{v} {ru_plural(v, ('очко', 'очка', 'очков'))}"
    if label == "reb":
        return f"{v} {ru_plural(v, ('подбор', 'подбора', 'подборов'))}"
    if label == "ast":
        return f"{v} {ru_plural(v, ('передача', 'передачи', 'передач'))}"
    if label == "stl":
        return f"{v} {ru_plural(v, ('перехват', 'перехвата', 'перехватов'))}"
    if label == "blk":
        return f"{v} {ru_plural(v, ('блок-шот', 'блок-шота', 'блок-шотов'))}"
    return f"{v}"

def hot_mark(p: dict) -> str:
    if (p["pts"] >= 35) or (p["reb"] >= 15) or (p["ast"] >= 12) or (p["stl"] >= 5) or (p["blk"] >= 5):
        return " 🔥"
    return ""

def is_dd(p: dict) -> bool:
    return sum(x >= 10 for x in [p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]]) >= 2

def second_ok(p: dict) -> bool:
    return (p["pts"] >= 20) or is_dd(p) or (p["stl"] >= 6) or (p["blk"] >= 6)

def score_key(p: dict):
    return (p["pts"], p["reb"] + p["ast"], p["stl"] + p["blk"])

def pick_team_players(abbr: str, rows: list[dict]) -> list[tuple[dict, bool, bool]]:
    """
    Возвращает список [(player, bold, special_detail)] до 2 игроков.
    special_detail=True — форматируем «3 макс. метрики».
    """
    if not rows:
        return []
    rows = sorted(rows, key=score_key, reverse=True)

    # спец-игроки
    special_keys = []
    if abbr == "BKN":
        special_keys = ["дёмин", "демин", "demin"]
    if abbr == "MIA":
        special_keys = ["голдин", "goldin"]

    special = None
    for p in rows:
        nm = (p["name"] or "").lower().replace("ё", "е")
        if any(k in nm for k in special_keys):
            special = p
            break

    out: list[tuple[dict, bool, bool]] = []
    top = rows[0]

    if special and special["name"] == top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False))
        out.append((special, True, True))
    else:
        out.append((top, False, False))

    if len(out) < 2:
        for p in rows[1:]:
            if p["name"] == top["name"]:
                continue
            if second_ok(p):
                out.append((p, False, False))
                break

    return out[:2]

def format_player_regular(p: dict, bold: bool = False) -> str:
    name = initials_ru(p["name"])
    if bold:
        name = f"<b>{name}</b>"
    out = [ru_forms("pts", p["pts"])]
    if p["reb"] >= 5:
        out.append(ru_forms("reb", p["reb"]))
    if p["ast"] >= 5:
        out.append(ru_forms("ast", p["ast"]))
    if p["stl"] >= 4:
        out.append(ru_forms("stl", p["stl"]))
    if p["blk"] >= 4:
        out.append(ru_forms("blk", p["blk"]))
    return f"{name}: " + ", ".join(out) + hot_mark(p)

def format_player_special(p: dict) -> str:
    name = f"<b>{initials_ru(p['name'])}</b>"
    stats = [("pts", p["pts"]), ("reb", p["reb"]), ("ast", p["ast"]), ("stl", p["stl"]), ("blk", p["blk"])]
    stats = [(k, v) for k, v in stats if v > 0]
    stats.sort(key=lambda kv: kv[1], reverse=True)
    chosen = stats[:3]
    return f"{name}: " + ", ".join(ru_forms(k, v) for k, v in chosen) + hot_mark(p)

# ==================== FORMAT / SPOILER ====================

def sp(s: str) -> str:
    return f'<span class="tg-spoiler">{s}</span>'

SEP = "–––––––––––––––––––––––"

def format_score_line(name_ru: str, abbr: str, score: int, winner: bool, record: str, with_ot: str) -> str:
    # собираем текст счёта отдельно, чтобы не путаться в кавычках
    score_txt = f"{score}"
    if winner:
        score_txt = f"<b>{score}</b>"
    if with_ot:
        score_txt += with_ot
    if record:
        score_txt += f" ({record})"
    return f"{emoji(abbr)} {name_ru}: {sp(score_txt)}"

def build_block_from_sports(info: dict, records: dict[str, str]) -> str:
    A, B = info["teamA"], info["teamB"]
    ot_str = "" if info["ot"] == 0 else (" (ОТ)" if info["ot"] == 1 else f" ({info['ot']} ОТ)")
    a_win = A["score"] > B["score"]
    b_win = B["score"] > A["score"]

    head_lines = [
        format_score_line(A["name"], A["abbr"], A["score"], a_win, records.get(A["abbr"], ""), ""),
        format_score_line(B["name"], B["abbr"], B["score"], b_win, records.get(B["abbr"], ""), ot_str),
        "",
    ]

    rowsA = info["players"].get(A["name"], [])
    rowsB = info["players"].get(B["name"], [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p, bold, det) in pick_team_players(A["abbr"], rowsA)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p, bold, det) in pick_team_players(B["abbr"], rowsB)]

    lines: list[str] = []
    if al:
        lines.extend(al)
    if al and bl:
        lines.append("")  # пустая строка между командами
    if bl:
        lines.extend(bl)

    body = "\n".join(lines) if lines else ""
    return "\n".join(head_lines) + body

def build_block_from_espn(e: dict) -> str:
    h, a = e["home"], e["away"]
    name_h = ABBR_TO_RU.get(h["abbr"], h["abbr"])
    name_a = ABBR_TO_RU.get(a["abbr"], a["abbr"])
    ot_str = "" if e["ot"] == 0 else (" (ОТ)" if e["ot"] == 1 else f" ({e['ot']} ОТ)")

    head_lines = [
        format_score_line(name_h, h["abbr"], h["score"], h["winner"], h.get("record", ""), ""),
        format_score_line(name_a, a["abbr"], a["score"], a["winner"], a.get("record", ""), ot_str),
        "",
    ]

    players_by_tid = fetch_espn_players(e["eventId"])
    rowsH = players_by_tid.get(h["teamId"], [])
    rowsA = players_by_tid.get(a["teamId"], [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p, bold, det) in pick_team_players(h["abbr"], rowsH)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p, bold, det) in pick_team_players(a["abbr"], rowsA)]

    lines: list[str] = []
    if al:
        lines.extend(al)
    if al and bl:
        lines.append("")
    if bl:
        lines.extend(bl)

    body = "\n".join(lines) if lines else ""
    return "\n".join(head_lines) + body

# ==================== COLLECT DAY ====================

def fetch_sports_games_for_title_day(d_title: date) -> dict[frozenset, dict]:
    games: dict[frozenset, dict] = {}
    for url in collect_day_links(d_title):
        info = parse_sports_match(url)
        if not info or not info.get("finished"):
            continue
        pair = frozenset([info["teamA"]["abbr"], info["teamB"]["abbr"]])
        if pair in games:
            continue
        games[pair] = info
    return games  # pair -> sports.info

def build_post() -> str:
    d_title = pick_title_date_london()
    days = candidate_days_for_espn()

    # 1) Completed события ESPN (для рекордов и фоллбека)
    espn_by_pair = fetch_espn_events_multi(days)  # pair -> event

    # 2) Контент Sports.ru ровно на дату заголовка (финальные матчи)
    sports_by_pair = fetch_sports_games_for_title_day(d_title)

    # 3) Итоговый список пар: объединяем ключи, но приоритет показывать блоки Sports.ru
    all_pairs = list(espn_by_pair.keys() | sports_by_pair.keys())
    # Стабильный порядок: по русскому названию хозяев, если знаем из любого источника
    def pair_sort_key(p: frozenset) -> tuple:
        p_list = list(p)
        a = p_list[0]
        b = p_list[1] if len(p_list) > 1 else ""
        return (ABBR_TO_RU.get(a, a), ABBR_TO_RU.get(b, b))
    all_pairs.sort(key=pair_sort_key)

    title_count = len(all_pairs)
    title = f"НБА • {ru_date(d_title)} • {title_count} {ru_plural(title_count, ('матч', 'матча', 'матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if title_count == 0:
        return title.rstrip()

    blocks: list[str] = []
    for i, pair in enumerate(all_pairs, 1):
        # Карта рекордов abbr -> record (если есть)
        rec_map: dict[str, str] = {}
        if pair in espn_by_pair:
            ev = espn_by_pair[pair]
            rec_map[ev["home"]["abbr"]] = ev["home"].get("record", "")
            rec_map[ev["away"]["abbr"]] = ev["away"].get("record", "")

        if pair in sports_by_pair:
            blocks.append(build_block_from_sports(sports_by_pair[pair], rec_map))
        else:
            # ESPN fallback (если нет блока Sports.ru)
            blocks.append(build_block_from_espn(espn_by_pair[pair]))

        if i < title_count:
            blocks.append("\n" + SEP + "\n\n")

    return (title + "".join(blocks)).strip()

# ==================== TELEGRAM ====================

def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = S.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# ==================== MAIN ====================

if __name__ == "__main__":
    try:
        post = build_post()
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
