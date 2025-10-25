#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (русский формат sports.ru для фамилий)

Данные:
• Список матчей дня, счёты, рекорды (W-L) — liveData scoreboard от NBA:
  https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
• Игроки и бокскор — liveData boxscore по gameId:
  https://cdn.nba.com/static/json/liveData/boxscore/boxscore_<gameId>.json
(эти эндпойнты общедоступны и стабильны). 

Имена игроков:
• Строго как на sports.ru/ basketball (фамилия на кириллице).
• Файлы-кэши: ru_map_nba.json (id→фамилия), ru_pending_nba.json (очередь).
• Если профиль sports.ru не найден мгновенно — временно берём словарь исключений
  + транслит, а игрок попадает в очередь; отдельный скрипт добьёт кэш.

Формат поста:
НБА • 25 октября • 12 матчей
Результаты надёжно спрятаны 👇
–––––––––––––––––––––––
🏀 Дом: 116 (W-L)
🏀 Гости: 122 (W-L)

Фамилия: X очков, Y подборов, Z передач
Фамилия: ...
(потом две строки для другой команды)
–––––––––––––––––––––––

Выбор «двух лучших» — сортировка по очкам (tiebreak: REB+AST, затем +/-).
Маркер 🔥 если 35+ очков, либо трипл-дабл, либо 30+ и дабл-дабл.
"""

import os
import sys
import re
import json
import time
import unicodedata
from datetime import datetime
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# --------------------------- Telegram ---------------------------

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# --------------------------- NBA API ---------------------------

NBA_SCOREBOARD = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
NBA_BOXSCORE   = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"

# --------------------------- sports.ru -------------------------

SPORTS_RU_HOST   = "https://www.sports.ru"
SRU_PERSON_ROOT  = SPORTS_RU_HOST + "/basketball/person/"
SRU_PLAYER_ROOT  = SPORTS_RU_HOST + "/basketball/player/"
SRU_SEARCH       = SPORTS_RU_HOST + "/search/?q="

RU_MAP_PATH      = "ru_map_nba.json"
RU_PENDING_PATH  = "ru_pending_nba.json"

# --------------------------- Русификация -----------------------

RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}

TEAMS_RU = {
    "ATL":"Атланта","BOS":"Бостон","BKN":"Бруклин","CHA":"Шарлотт","CHI":"Чикаго","CLE":"Кливленд",
    "DAL":"Даллас","DEN":"Денвер","DET":"Детройт","GSW":"Голден Стэйт","HOU":"Хьюстон","IND":"Индиана",
    "LAC":"Клипперс","LAL":"Лейкерс","MEM":"Мемфис","MIA":"Майами","MIL":"Милуоки","MIN":"Миннесота",
    "NOP":"Новый Орлеан","NYK":"Нью-Йорк","OKC":"Оклахома-Сити","ORL":"Орландо","PHI":"Филадельфия",
    "PHX":"Финикс","POR":"Портленд","SAC":"Сакраменто","SAS":"Сан-Антонио","TOR":"Торонто","UTA":"Юта",
    "WAS":"Вашингтон"
}

# Наиболее частые фамилии (список будет пополняться апдейтером)
EXCEPT_LAST = {
    "Doncic":"Дончич","Jokic":"Йокич","Embiid":"Эмбиид","Curry":"Карри","James":"Джеймс","Davis":"Дэвис",
    "Durant":"Дюрэнт","Booker":"Букер","Irving":"Ирвинг","Tatum":"Тейтум","Brown":"Браун","Harden":"Харден",
    "George":"Джордж","Leonard":"Леонард","Antetokounmpo":"Адетокумбо","Young":"Янг","Adebayo":"Адебайо",
    "Williamson":"Уильямсон","Mitchell":"Митчелл","Brunson":"Брансон","Randle":"Рэндл","Sabonis":"Сабонис",
    "Markkanen":"Маркканен","Haliburton":"Халибертон","Wembanyama":"Вембаньяма","Edwards":"Эдвардс",
    "Siakam":"Сиакам","Anunoby":"Ануноби","Porzingis":"Порзингис","Gilgeous-Alexander":"Гилджес-Александер",
    "Fox":"Фокс","Kawhi":"Леонард","Maxey":"Макси","Holiday":"Холидэй","Lopez":"Лопес","Mobley":"Мобли",
    "Allen":"Аллен","Hachimura":"Хачимура","Vucevic":"Вучевич","Adebajo":"Адебайо","Ayton":"Эйтон",
}

# --------------------------- Утилиты ---------------------------

def log(*a): print(*a, file=sys.stderr)

def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent":"NBA-DailyResultsBot/1.0 (+cdn.nba.com liveData; sports.ru resolver)",
        "Accept":"application/json, text/html;q=0.8",
        "Accept-Language":"ru-RU,ru;q=0.9,en;q=0.6"
    })
    return s

S = make_session()

def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if n1 == 1: return forms[0]
    if 2 <= n1 <= 4: return forms[1]
    return forms[2]

def ru_date(d: datetime) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

RU_MAP = _load_json(RU_MAP_PATH, {})          # personId -> "Фамилия"
RU_PENDING = _load_json(RU_PENDING_PATH, [])  # [{id, first, last}]
_pending_ids_session: set[str] = set()

def pick_report_day_et() -> datetime:
    # Запуск по Лондону утром → в ET ещё «сегодняшний» день с играми
    now_et = datetime.now(ZoneInfo("America/New_York"))
    # Ждём пока всё точно финалится — после 06:00 ET обычно уже ок
    if now_et.hour < 6:
        return now_et  # всё равно берём «today» с liveData
    return now_et

# --------------------------- Спортс.ру имена ------------------

def _slugify(first: str, last: str) -> str:
    base = f"{first} {last}".strip()
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower().strip()
    base = re.sub(r"[^a-z0-9]+","-", base).strip("-")
    return base

def _sportsru_try_profile(first: str, last: str) -> str | None:
    slug = _slugify(first, last)
    for root in (SRU_PERSON_ROOT, SRU_PLAYER_ROOT):
        url = root + slug + "/"
        r = S.get(url, timeout=20)
        if r.status_code == 200 and ("/basketball/person/" in r.url or "/basketball/player/" in r.url):
            return url
    return None

def _sportsru_extract_surname(url: str) -> str | None:
    r = S.get(url, timeout=20)
    if r.status_code != 200: return None
    soup = BeautifulSoup(r.text, "html.parser")
    h = soup.find(["h1","h2"])
    if not h: return None
    full = " ".join(h.get_text(" ", strip=True).split())
    parts = [p for p in re.split(r"\s+", full) if p]
    if len(parts) >= 2:
        # последний «токен» обычно фамилия; покрывает «Джексон-мл.»
        return parts[-1]
    return None

def _sportsru_search_surname(first: str, last: str) -> str | None:
    q = quote_plus(f"{first} {last}".strip())
    r = S.get(SRU_SEARCH + q, timeout=20)
    if r.status_code != 200: return None
    soup = BeautifulSoup(r.text, "html.parser")
    link = soup.select_one('a[href*="/basketball/person/"]') or soup.select_one('a[href*="/basketball/player/"]')
    if not link or not link.get("href"): return None
    href = link["href"]
    if href.startswith("/"): href = SPORTS_RU_HOST + href
    return _sportsru_extract_surname(href)

def _queue_pending(pid: str, first: str, last: str):
    if not pid or pid in _pending_ids_session: return
    if pid in RU_MAP: return
    for it in RU_PENDING:
        if str(it.get("id")) == pid: return
    RU_PENDING.append({"id": pid, "first": first or "", "last": last or ""})
    _pending_ids_session.add(pid)

def ru_surname(pid: str, first: str, last: str) -> str:
    pid = str(pid)
    if pid in RU_MAP:
        return RU_MAP[pid]

    # словарь громких фамилий как fallback
    if last in EXCEPT_LAST:
        RU_MAP[pid] = EXCEPT_LAST[last]
        return RU_MAP[pid]

    # sports.ru slug → профиль
    try:
        url = _sportsru_try_profile(first, last)
        if url:
            sname = _sportsru_extract_surname(url)
            if sname:
                RU_MAP[pid] = sname
                return sname
    except Exception as e:
        log("[sports.ru slug] fail", first, last, e)

    # поиск
    try:
        sname = _sportsru_search_surname(first, last)
        if sname:
            RU_MAP[pid] = sname
            return sname
    except Exception as e:
        log("[sports.ru search] fail", first, last, e)

    # очередь на доразрешение
    _queue_pending(pid, first, last)
    # временно — латиница как есть (последнее слово фамилии)
    return last or first or pid

# --------------------------- Парсинг liveData ------------------

def fetch_scoreboard() -> list[dict]:
    r = S.get(NBA_SCOREBOARD, timeout=25)
    r.raise_for_status()
    j = r.json() or {}
    games = j.get("scoreboard", {}).get("games", []) or []
    out = []
    for g in games:
        # gameStatus: 1 — предматч, 2 — идёт, 3 — финал
        if int(g.get("gameStatus", 0)) != 3:
            continue
        gid = str(g.get("gameId"))
        h = g.get("homeTeam", {}) or {}
        a = g.get("awayTeam", {}) or {}
        out.append({
            "gameId": gid,
            "homeAbbr": (h.get("teamTricode") or h.get("triCode") or "").upper(),
            "awayAbbr": (a.get("teamTricode") or a.get("triCode") or "").upper(),
            "homeScore": int(h.get("score", 0)),
            "awayScore": int(a.get("score", 0)),
            "homeW": int(h.get("wins", 0)), "homeL": int(h.get("losses", 0)),
            "awayW": int(a.get("wins", 0)), "awayL": int(a.get("losses", 0)),
        })
    return out

def fetch_boxscore(game_id: str) -> dict:
    r = S.get(NBA_BOXSCORE.format(gid=game_id), timeout=25)
    r.raise_for_status()
    j = r.json() or {}
    game = j.get("game", {}) or {}
    return {
        "home": (game.get("homeTeam") or {}),
        "away": (game.get("awayTeam") or {}),
    }

def _extract_name_fields(p: dict) -> tuple[str,str]:
    # NBA liveData чаще даёт firstName/familyName; иначе разобьём name
    fn = (p.get("firstName") or "").strip()
    ln = (p.get("familyName") or "").strip()
    if not (fn and ln):
        nm = (p.get("name") or "").strip()
        parts = [x for x in re.split(r"\s+", nm) if x]
        if len(parts) >= 2:
            fn = fn or parts[0]
            ln = ln or parts[-1]
    return fn, ln

def _pick_top_two(players: list[dict]) -> list[dict]:
    act = []
    for p in players or []:
        st = (p.get("status") or "").upper()
        stats = p.get("statistics") or {}
        pts = int(stats.get("points") or 0)
        reb = int(stats.get("reboundsTotal") or stats.get("rebounds") or 0)
        ast = int(stats.get("assists") or 0)
        pm  = int(stats.get("plusMinusPoints") or 0)
        # отсекаем совсем DNP (у них обычно нет statistics)
        if pts == reb == ast == 0 and not stats:
            continue
        score_key = (pts, reb+ast, pm)
        act.append({"raw":p, "pts":pts, "reb":reb, "ast":ast, "pm":pm, "key":score_key})
    act.sort(key=lambda x: x["key"], reverse=True)
    return act[:2]

def _flame(pts:int, reb:int, ast:int, stl:int, blk:int) -> str:
    dbl = sum(v>=10 for v in (pts,reb,ast,stl,blk))
    if pts >= 35 or dbl >= 3 or (pts>=30 and dbl>=2):
        return " 🔥"
    return ""

def player_line_ru(p: dict, pid: str) -> str:
    fn, ln = _extract_name_fields(p)
    stats = p.get("statistics") or {}
    pts = int(stats.get("points") or 0)
    reb = int(stats.get("reboundsTotal") or stats.get("rebounds") or 0)
    ast = int(stats.get("assists") or 0)
    stl = int(stats.get("steals") or 0)
    blk = int(stats.get("blocks") or 0)

    fam = ru_surname(pid, fn, ln)

    pts_word = ru_plural(pts, ("очко","очка","очков"))
    reb_word = ru_plural(reb, ("подбор","подбора","подборов"))
    ast_word = ru_plural(ast, ("передача","передачи","передач"))

    return f"{fam}: {pts} {pts_word}, {reb} {reb_word}, {ast} {ast_word}{_flame(pts,reb,ast,stl,blk)}"

def build_game_block(gmeta: dict) -> str:
    # Заголовок
    home_ru = TEAMS_RU.get(gmeta["homeAbbr"], gmeta["homeAbbr"])
    away_ru = TEAMS_RU.get(gmeta["awayAbbr"], gmeta["awayAbbr"])
    head = (f"🏀 {home_ru}: {gmeta['homeScore']} ({gmeta['homeW']}-{gmeta['homeL']})\n"
            f"🏀 {away_ru}: {gmeta['awayScore']} ({gmeta['awayW']}-{gmeta['awayL']})\n\n")

    # Игроки
    box = fetch_boxscore(gmeta["gameId"])
    h_players = (box.get("home") or {}).get("players") or []
    a_players = (box.get("away") or {}).get("players") or []

    top_h = _pick_top_two(h_players)
    top_a = _pick_top_two(a_players)

    lines = []
    for x in top_h:
        pid = str((x["raw"].get("personId") or x["raw"].get("playerId") or ""))
        lines.append(player_line_ru(x["raw"], pid))
    lines.append("")  # пустая строка между командами
    for x in top_a:
        pid = str((x["raw"].get("personId") or x["raw"].get("playerId") or ""))
        lines.append(player_line_ru(x["raw"], pid))

    return head + "\n".join(lines)

def build_post_text(games: list[dict]) -> str:
    now_et = pick_report_day_et()
    title = f"НБА • {ru_date(now_et)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += "–––––––––––––––––––––––\n"
    parts = [title]
    for i, g in enumerate(games, 1):
        parts.append(build_game_block(g))
        if i < len(games):
            parts.append("–––––––––––––––––––––––")
    return "\n".join(parts)

def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    MAX = 3500
    rest = text
    while rest:
        if len(rest) <= MAX:
            chunk = rest
            rest = ""
        else:
            cut = rest.rfind("\n––––", 0, MAX)
            if cut == -1: cut = rest.rfind("\n", 0, MAX)
            if cut == -1: cut = MAX
            chunk, rest = rest[:cut], rest[cut:].lstrip()
        resp = S.post(url, json={
            "chat_id": CHAT_ID,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=25)
        if resp.status_code != 200:
            raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
        time.sleep(0.4)

if __name__ == "__main__":
    try:
        games = fetch_scoreboard()
        if not games:
            # Если ни одного FIN, всё равно отправим шапку «0 матчей» – редкий случай
            text = build_post_text([])
        else:
            text = build_post_text(games)
        tg_send(text)
        # Обновим файлы кэша/очереди (создадим, если не было)
        _save_json(RU_MAP_PATH, RU_MAP)
        _save_json(RU_PENDING_PATH, RU_PENDING)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
