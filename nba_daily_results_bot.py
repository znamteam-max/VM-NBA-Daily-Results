#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU), multi-source chain with hard 30s NBA deadline:
1) OFFICIAL NBA (cdn.nba.com) — суммарный предел на попытки 30 секунд
2) SofaScore (public)
3) ESPN (site.web.api)

• Табло:
  - NBA: https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_YYYYMMDD.json
         (+ fallback todaysScoreboard_00.json)
  - SofaScore: https://api.sofascore.com/api/v1/sport/basketball/events/{YYYY-MM-DD}
               или https://api.sofascore.com/api/v1/sport/basketball/competition/132/scheduled-events/{YYYY-MM-DD}
  - ESPN: https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates=YYYYMMDD

• Бокскор:
  - NBA: https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gameId}.json
  - ESPN: https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eventId}

• Русские фамилии: sports.ru (профиль/поиск); если не нашли — оставляем оригинальную латиницу.
• Игроки (1–2 на команду):
    - очки ≥ 30, ИЛИ
    - дабл-дабл (PTS/REB/AST любые 2 ≥ 10), ИЛИ
    - подборы ≥ 15, ИЛИ передачи ≥ 12, ИЛИ перехваты ≥ 4, ИЛИ блок-шоты ≥ 4.
  Спец-правило: Дёмин (BKN) и Голдин (MIA) — включить и выделить жирным, если играли.

• Отображение стат: всегда очки; дополнительно выводим REB (≥5), AST (≥5), STL (≥4), BLK (≥4).
• Команды: русское название + эмодзи/кастом-эмодзи (из team_emoji_ids.json).
• Отправка — одним сообщением (entities для кастом-эмодзи поддерживаются).
"""

import os, sys, re, json, time, random, unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------------- ENV ----------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ---------------- NBA (official) ----------------
NBA_SB_DATE   = "https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{date}.json"       # YYYYMMDD
NBA_SB_TODAY  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
NBA_BOX       = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"

# ---------------- SofaScore ----------------
SOFA_EVENTS_DAY    = "https://api.sofascore.com/api/v1/sport/basketball/events/{date_dash}"       # YYYY-MM-DD
SOFA_COMP_SCHEDULE = "https://api.sofascore.com/api/v1/sport/basketball/competition/132/scheduled-events/{date_dash}"  # 132 = NBA

# ---------------- ESPN ----------------
ESPN_SB   = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates={date}"  # YYYYMMDD
ESPN_BOX  = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eventId}"

# ---------------- sports.ru ----------------
SPORTS_RU   = "https://www.sports.ru"
SRU_PERSON  = SPORTS_RU + "/basketball/person/"
SRU_PLAYER  = SPORTS_RU + "/basketball/player/"
SRU_SEARCH  = SPORTS_RU + "/search/?q="

# ---------------- CACHE ----------------
RU_MAP_PATH          = "ru_map_nba.json"        # { athleteId: {"first":"Имя-ru","last":"Фамилия-ru"} | "Фамилия-legacy" }
RU_PENDING_PATH      = "ru_pending_nba.json"    # [{id, first, last}]
TEAM_CUSTOM_IDS_PATH = "team_emoji_ids.json"    # { "LAL": "custom_emoji_id", ... }

RU_MAP = {}
RU_PENDING = []
TEAM_CUSTOM_IDS = {}
_session_pending_ids = set()

# ---------------- DATES ----------------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, f: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return f[2]
    if 2 <= n1 <= 4:  return f[1]
    if n1 == 1:      return f[0]
    return f[2]

# ---------------- HTTP ----------------
def make_session():
    s = requests.Session()
    r = Retry(
        total=6, connect=6, read=6,
        backoff_factor=0.9,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) NBA-DailyResultsBot/7.1",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Origin": "https://www.nba.com",
        "Referer": "https://www.nba.com/",
        "Accept": "application/json,text/plain,*/*",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s
S = make_session()
def log(*a): print(*a, file=sys.stderr)

def _get_json(url: str, timeout=12, add_cache_buster=False) -> dict:
    u = url
    if add_cache_buster:
        u += ("&_=" if "?" in u else "?_=") + str(int(time.time()*1000))
    r = S.get(u, timeout=timeout)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

def _get_json_retries(url: str, tries=2, base_timeout=12, add_cache_buster=False) -> dict:
    for i in range(tries):
        try:
            j = _get_json(url, timeout=base_timeout + i*5, add_cache_buster=add_cache_buster)
            if j: return j
        except Exception as e:
            log(f"[GET fail try {i+1}/{tries}] {url} -> {e}")
        time.sleep(0.4 + i*0.4 + random.random()*0.3)
    return {}

def _get_json_until_deadline(url: str, deadline_ts: float, tries=3, per_try_timeout=10, add_cache_buster=False) -> dict:
    """
    Жёсткое ограничение по времени: не превышаем deadline_ts.
    Каждый заход — не дольше оставшегося времени.
    """
    for i in range(tries):
        remaining = deadline_ts - time.time()
        if remaining <= 0:
            return {}
        timeout = min(per_try_timeout + i*5, max(2, remaining - 0.25))
        try:
            j = _get_json(url, timeout=timeout, add_cache_buster=add_cache_buster)
            if j: return j
        except Exception as e:
            log(f"[GET deadline try {i+1}/{tries}] {url} -> {e}")
        time.sleep(min(0.4, max(0.0, (deadline_ts - time.time())/3)))
    return {}

# ---------------- TEAMS ----------------
TEAM_RU = {
    "ATL":"Атланта","BOS":"Бостон","BKN":"Бруклин","NY":"Нью-Йорк","NYK":"Нью-Йорк","PHI":"Филадельфия",
    "TOR":"Торонто","CHI":"Чикаго","CLE":"Кливленд","DET":"Детройт","IND":"Индиана","MIL":"Милуоки",
    "DEN":"Денвер","MIN":"Миннесота","OKC":"Оклахома-Сити","POR":"Портленд","UTA":"Юта","UTAH":"Юта",
    "GS":"Голден Стэйт","GSW":"Голден Стэйт","LAC":"Клипперс","LAL":"Лейкерс","PHX":"Финикс","SAC":"Сакраменто",
    "MIA":"Майами","ORL":"Орландо","DAL":"Даллас","HOU":"Хьюстон","MEM":"Мемфис","NO":"Новый Орлеан",
    "NOP":"Новый Орлеан","SA":"Сан-Антонио","SAS":"Сан-Антонио","WSH":"Вашингтон","WAS":"Вашингтон",
}
TEAM_EMOJI = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","NY":"🗽","NYK":"🗽","PHI":"🔔",
    "TOR":"🦖","CHI":"🐂","CLE":"🛡️","DET":"🔧","IND":"💫","MIL":"🦌",
    "DEN":"⛏️","MIN":"🐺","OKC":"⚡","POR":"🧭","UTA":"🎷","UTAH":"🎷",
    "GS":"🗡️","GSW":"🗡️","LAC":"✂️","LAL":"⭐","PHX":"☀️","SAC":"👑",
    "MIA":"🔥","ORL":"✨","DAL":"🐎","HOU":"🚀","MEM":"🐻","NO":"🪶",
    "NOP":"🪶","SA":"🪙","SAS":"🪙","WSH":"🧙","WAS":"🧙",
}
ALT_ABBR = {"GS":"GSW","NY":"NYK","BRK":"BKN","PHO":"PHX","NO":"NOP","NOR":"NOP","UTH":"UTA","WAS":"WSH"}
def canon_abbr(x: str) -> str: return ALT_ABBR.get((x or "").strip().upper(), (x or "").strip().upper())

# ---------------- CACHE I/O ----------------
def _load_json(path: str, default):
    if not os.path.exists(path): return default
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

# ---------------- RU NAMES (sports.ru) ----------------
def is_cyrillic(s: str) -> bool:
    return bool(s) and any("А" <= ch <= "я" for ch in s)

def _initial_from_en_to_ru(first_en: str) -> str:
    if not first_en: return "И"
    ch = first_en.strip()[:1].upper()
    table = {"A":"А","B":"Б","C":"К","D":"Д","E":"Е","F":"Ф","G":"Г","H":"Х","I":"И","J":"Д","K":"К",
             "L":"Л","M":"М","N":"Н","O":"О","P":"П","Q":"К","R":"Р","S":"С","T":"Т","U":"У","V":"В",
             "W":"В","X":"К","Y":"Й","Z":"З"}
    return table.get(ch, "И")

def _slugify(first: str, last: str) -> str:
    base = f"{first} {last}".strip()
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.lower()
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    return base

def _sportsru_try_profile(first: str, last: str) -> str | None:
    slug = _slugify(first, last)
    for root in (SRU_PERSON, SRU_PLAYER):
        url = root + slug + "/"
        r = S.get(url, timeout=10)
        if r.status_code == 200 and ("/basketball/person/" in r.url or "/basketball/player/" in r.url):
            return url
    return None

def _rus_first_last_from_header(text: str):
    full = " ".join(text.split())
    parts = [p for p in re.split(r"\s+", full) if p]
    if len(parts) >= 2:
        ru_first, ru_last = parts[0], parts[-1]
        if ru_last.lower() in {"мл.", "младший"} and len(parts) >= 3:
            ru_last = parts[-2] + " мл."
        return ru_first, ru_last
    return None

def _sportsru_from_profile(url: str):
    try:
        r = S.get(url, timeout=12)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        h = soup.find(["h1","h2"])
        if not h: return None
        return _rus_first_last_from_header(h.get_text(" ", strip=True))
    except Exception:
        return None

def _sportsru_search(first: str, last: str):
    try:
        q = quote_plus(f"{first} {last}".strip())
        r = S.get(SRU_SEARCH + q, timeout=12)
        if r.status_code != 200: return None
        soup = BeautifulSoup(r.text, "html.parser")
        a = soup.select_one('a[href*="/basketball/person/"]') or soup.select_one('a[href*="/basketball/player/"]')
        if not a or not a.get("href"): return None
        href = a["href"]
        if href.startswith("/"): href = SPORTS_RU + href
        return _sportsru_from_profile(href)
    except Exception:
        return None

EXCEPT_LAST = {
    "ingram":"Ингрэм","barrett":"Барретт","antetokounmpo":"Адетокумбо","anthony":"Энтони",
    "wagner":"Вагнер","bane":"Бэйн","young":"Янг","alexander-walker":"Александер-Уокер",
    "brunson":"Брансон","towns":"Таунс","brown":"Браун","hauser":"Хаузер","thomas":"Томас",
    "porter":"Портер","mitchell":"Митчелл","allen":"Аллен","durant":"Дюрэнт","sengun":"Шенгюн",
    "cunningham":"Каннингем","thompson":"Томпсон","jackson jr.":"Джексон-младший","jackson":"Джексон",
    "adebayo":"Адебайо","jovic":"Йович","williamson":"Уильямсон","murphy":"Мерфи",
    "wembanyama":"Вембаньяма","vassell":"Васселл","davis":"Дэвис","flagg":"Флэгг","george":"Джордж",
    "johnson":"Джонсон","doncic":"Дончич","dončić":"Дончич","reaves":"Ривз","edwards":"Эдвардс",
    "randle":"Рэндл","avdija":"Авдия","grant":"Грант","curry":"Карри","kuminga":"Куминга",
    "lavine":"Лавин","monk":"Монк","markkanen":"Маркканен","harden":"Харден","leonard":"Леонард",
    "brooks":"Брукс","booker":"Букер","porzingis":"Порзингис","gilgeous-alexander":"Гилджес-Александер",
    "demin":"Дёмин","goldin":"Голдин",
}
def _queue_pending(pid: str, first: str, last: str):
    if not pid or pid in _session_pending_ids: return
    if pid in RU_MAP: return
    for it in RU_PENDING:
        if it.get("id") == pid: return
    RU_PENDING.append({"id": pid, "first": first, "last": last})
    _session_pending_ids.add(pid)

def _improve_cached_if_needed(pid: str, first: str, last: str):
    cur = RU_MAP.get(pid)
    need = (not cur) or (isinstance(cur, str) and not is_cyrillic(cur)) \
           or (isinstance(cur, dict) and (not is_cyrillic(cur.get("last","")) or not cur.get("first")))
    if not need: return
    url = _sportsru_try_profile(first, last)
    got = _sportsru_from_profile(url) if url else None
    if not got: got = _sportsru_search(first, last)
    if got:
        RU_MAP[pid] = {"first": got[0], "last": got[1]}; return
    low_last = (last or "").strip().lower()
    if low_last in EXCEPT_LAST:
        RU_MAP[pid] = {"first":"", "last": EXCEPT_LAST[low_last]}; return
    RU_MAP[pid] = {"first":"", "last": (last or "").strip()}

def resolve_ru_name(first_en: str, last_en: str, athlete_id: str):
    if athlete_id:
        _improve_cached_if_needed(athlete_id, first_en or "", last_en or "")
        val = RU_MAP.get(athlete_id)
        if isinstance(val, dict): return (val.get("first",""), val.get("last",""))
        if isinstance(val, str):  return ("", val)
    url = _sportsru_try_profile(first_en or "", last_en or "")
    got = _sportsru_from_profile(url) if url else None
    if not got: got = _sportsru_search(first_en or "", last_en or "")
    if got: return got
    low_last = (last_en or "").strip().lower()
    ru_last = EXCEPT_LAST.get(low_last) or (last_en or "").strip()
    if athlete_id: _queue_pending(athlete_id, first_en or "", last_en or "")
    return ("", ru_last)

# ---------------- DATE PICK ----------------
def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()
def pick_candidate_days():
    base = pick_report_date()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

# ---------------- SCOREBOARD: NBA (≤30s суммарно) ----------------
def sb_nba(day: date, deadline_sec: float = 30.0):
    dstr = day.strftime("%Y%m%d")
    deadline_ts = time.time() + max(5.0, deadline_sec)

    j = _get_json_until_deadline(NBA_SB_DATE.format(date=dstr), deadline_ts, tries=2, per_try_timeout=10, add_cache_buster=True)
    out = _parse_sb_nba(j, dstr)
    if out: return out

    # если ещё осталось время — пробуем today's
    if time.time() < deadline_ts:
        j2 = _get_json_until_deadline(NBA_SB_TODAY, deadline_ts, tries=1, per_try_timeout=10, add_cache_buster=True)
        out2 = _parse_sb_nba(j2, dstr)
        if out2: return out2

    return []

def _parse_sb_nba(j: dict, dstr: str):
    if not isinstance(j, dict): return []
    sb = j.get("scoreboard") or {}
    games = sb.get("games") or []
    out = []
    for g in games:
        try:
            status = str(g.get("gameStatusText") or "").lower()
            if "final" not in status:  # только финальные
                continue
            gid = str(g.get("gameId") or "")
            ot = ""
            m = re.search(r'(\d+)\s*ot', status)
            if "ot" in status:
                ot = f" ({int(m.group(1))}ОТ)" if m else " (ОТ)"
            ht = g.get("homeTeam") or {}
            at = g.get("awayTeam") or {}
            comp = []
            for tm in (at, ht):  # гости → хозяева
                abbr = canon_abbr(tm.get("teamTricode") or "")
                score = int(tm.get("score") or 0)
                record = f"{tm.get('wins',0)}-{tm.get('losses',0)}"
                comp.append({
                    "abbr": abbr,
                    "score": score,
                    "winner": bool(tm.get("isWinner", False)),
                    "record": record,
                    "teamId": str(tm.get("teamId") or ""),
                })
            if len(comp) == 2:
                out.append({"eventId": gid, "competitors": comp, "ot": ot, "date": dstr, "source":"nba"})
        except Exception:
            continue
    return out

# ---------------- SCOREBOARD: SofaScore ----------------
def sb_sofa(day: date):
    d_dash = day.strftime("%Y-%m-%d")
    j = _get_json_retries(SOFA_EVENTS_DAY.format(date_dash=d_dash), tries=2, base_timeout=10)
    out = _parse_sb_sofa(j)
    if out: return out
    j2 = _get_json_retries(SOFA_COMP_SCHEDULE.format(date_dash=d_dash), tries=2, base_timeout=10)
    out2 = _parse_sb_sofa(j2)
    return out2

def _parse_sb_sofa(j: dict):
    events = []
    if isinstance(j, dict):
        if isinstance(j.get("events"), list):
            events = j.get("events") or []
        elif isinstance(j.get("events"), dict):
            for v in j.get("events").values():
                if isinstance(v, list): events.extend(v)
    out = []
    for ev in events:
        try:
            tourn = (ev.get("tournament") or {}).get("uniqueTournament") or {}
            if int(tourn.get("id") or 0) != 132:  # только NBA
                continue
            st = (ev.get("status") or {}).get("type") or ""
            if str(st).lower() not in {"finished","after overtime","ended"}:
                continue
            hid = ev.get("homeTeam") or {}
            aid = ev.get("awayTeam") or {}
            hs  = (ev.get("homeScore") or {}).get("current") or 0
            as_ = (ev.get("awayScore") or {}).get("current") or 0
            hp = ev.get("homeScore") or {}
            periods = 0
            for i in range(5, 10):  # ОТ — периоды 5+
                if hp.get(f"period{i}") is not None:
                    periods += 1
            ot = f" ({periods}ОТ)" if periods > 0 else ""
            comp = []
            for tm, sc in ((aid, as_), (hid, hs)):  # гости → хозяева
                abbr = canon_abbr((tm.get("nameCode") or tm.get("shortName") or tm.get("name") or "")[:3])
                comp.append({
                    "abbr": abbr,
                    "score": int(sc),
                    "winner": None,
                    "record": "",
                    "teamId": str(tm.get("id") or ""),
                })
            if len(comp) == 2:
                a, b = comp
                a["winner"] = a["score"] > b["score"]
                b["winner"] = b["score"] > a["score"]
                out.append({"eventId": str(ev.get("id") or ""), "competitors": comp, "ot": ot, "date": None, "source":"sofa"})
        except Exception:
            continue
    return out

# ---------------- SCOREBOARD: ESPN ----------------
def sb_espn(day: date):
    dstr = day.strftime("%Y%m%d")
    j = _get_json_retries(ESPN_SB.format(date=dstr), tries=2, base_timeout=10)
    events = j.get("events") or []
    out = []
    for ev in events:
        try:
            comp = (ev.get("competitions") or [])[0]
            status = (comp.get("status") or {}).get("type") or {}
            if not status.get("completed", False):
                continue
            detail = (status.get("description") or status.get("detail") or "").lower()
            ot = ""
            m = re.search(r'(\d+)\s*ot', detail)
            if "ot" in detail:
                ot = f" ({int(m.group(1))}ОТ)" if m else " (ОТ)"
            competitors = comp.get("competitors") or []
            game = {"eventId": str(ev.get("id") or ""), "competitors": [], "ot": ot, "date": dstr, "source":"espn"}
            for c in competitors:
                team = c.get("team") or {}
                abbr = canon_abbr(team.get("abbreviation") or "")
                score = int(float(c.get("score", 0)))
                win = c.get("winner", False)
                rec = ""
                for recobj in c.get("records") or []:
                    if recobj.get("type") == "total" and recobj.get("summary"):
                        rec = recobj["summary"]
                game["competitors"].append({
                    "abbr": abbr,
                    "score": score,
                    "winner": bool(win),
                    "record": rec or "",
                    "teamId": str(team.get("id") or ""),
                })
            if len(game["competitors"]) == 2:
                out.append(game)
        except Exception:
            continue
    return out

# ---------------- BOX: NBA ----------------
def box_nba(game_id: str):
    j = _get_json_retries(NBA_BOX.format(gid=game_id), tries=1, base_timeout=12, add_cache_buster=True)
    if not isinstance(j, dict): return []
    game = j.get("game") or {}
    out = []
    for side in ("awayTeam","homeTeam"):
        team = game.get(side) or {}
        tid = str(team.get("teamId") or "")
        players = []
        for p in (team.get("players") or []):
            info = p.get("person") or {}
            st   = p.get("statistics") or {}
            mins = st.get("minutes") or st.get("minutesCalculated") or ""
            if not any(st.get(k) for k in ("points","reboundsTotal","assists","steals","blocks")) and not mins:
                continue
            pid   = str(info.get("personId") or "")
            first = (info.get("firstName") or "").strip()
            last  = (info.get("familyName") or info.get("lastName") or "").strip()
            name  = (info.get("displayName") or f"{first} {last}").strip()
            def ig(key, *alts):
                for k in (key,)+alts:
                    v = st.get(k)
                    if v is None: continue
                    try: return int(v)
                    except:
                        try: return int(float(v))
                        except: pass
                return 0
            pts = ig("points","pointsTotal","pts")
            reb = ig("reboundsTotal","rebounds","reb")
            ast = ig("assists","ast")
            stl = ig("steals","stl")
            blk = ig("blocks","blk")
            players.append({"id":pid,"first":first,"last":last,"name":name,
                            "pts":pts,"reb":reb,"ast":ast,"stl":stl,"blk":blk})
        out.append({"teamId": tid, "players": players})
    return out

# ---------------- BOX: ESPN ----------------
def _espn_collect_stat_values(pobj):
    vals = {}
    stats = pobj.get("statistics")
    if isinstance(stats, list):
        for it in stats:
            if isinstance(it, dict):
                name = str(it.get("name") or it.get("shortDisplayName") or "").lower()
                val  = it.get("value", it.get("displayValue"))
                try:
                    if val is None: continue
                    val = int(float(val))
                except:
                    try: val = int(re.sub(r"[^\d\-\.]", "", str(val)) or 0)
                    except: continue
                if any(k in name for k in ("point","pts")):   vals["pts"] = max(vals.get("pts",0), val)
                if any(k in name for k in ("rebound","reb")): vals["reb"] = max(vals.get("reb",0), val)
                if any(k in name for k in ("assist","ast")):  vals["ast"] = max(vals.get("ast",0), val)
                if any(k in name for k in ("steal","stl")):   vals["stl"] = max(vals.get("stl",0), val)
                if any(k in name for k in ("block","blk")):   vals["blk"] = max(vals.get("blk",0), val)
    raw_stats = pobj.get("stats")
    if isinstance(raw_stats, dict):
        for k, v in raw_stats.items():
            lk = k.lower()
            try: iv = int(float(v))
            except:
                try: iv = int(re.sub(r"[^\d\-\.]", "", str(v)) or 0)
                except: iv = 0
            if lk in {"pts","points"}: vals["pts"] = max(vals.get("pts",0), iv)
            if lk in {"reb","reboundstotal","totreb"}: vals["reb"] = max(vals.get("reb",0), iv)
            if lk in {"ast","assists"}: vals["ast"] = max(vals.get("ast",0), iv)
            if lk in {"stl","steals"}: vals["stl"] = max(vals.get("stl",0), iv)
            if lk in {"blk","blocks"}: vals["blk"] = max(vals.get("blk",0), iv)
    ath = pobj.get("athlete") or {}
    a_stats = ath.get("stats")
    if isinstance(a_stats, dict):
        for k, v in a_stats.items():
            lk = str(k).lower()
            try: iv = int(float(v))
            except:
                try: iv = int(re.sub(r"[^\d\-\.]", "", str(v)) or 0)
                except: iv = 0
            if lk in {"pts","points"}: vals.setdefault("pts", iv)
            if lk in {"reb","totreb","reboundstotal"}: vals.setdefault("reb", iv)
            if lk in {"ast","assists"}: vals.setdefault("ast", iv)
            if lk in {"stl","steals"}: vals.setdefault("stl", iv)
            if lk in {"blk","blocks"}: vals.setdefault("blk", iv)
    return {"pts": int(vals.get("pts",0)), "reb": int(vals.get("reb",0)), "ast": int(vals.get("ast",0)), "stl": int(vals.get("stl",0)), "blk": int(vals.get("blk",0))}

def box_espn(event_id: str):
    j = _get_json_retries(ESPN_BOX.format(eventId=event_id), tries=2, base_timeout=10)
    out = []
    box = j.get("boxscore") or j
    for t in (box.get("teams") or []):
        team = t.get("team") or {}
        tid = str(team.get("id") or "")
        players_out = []
        for p in (t.get("players") or []):
            ath = p.get("athlete") or {}
            pid = str(ath.get("id") or "")
            name = ath.get("displayName") or ""
            parts = [pr for pr in re.split(r"\s+", name.strip()) if pr]
            first = parts[0] if parts else ""
            last  = " ".join(parts[1:]) if len(parts) > 1 else parts[0] if parts else ""
            vals = _espn_collect_stat_values(p)
            if any(vals.values()):
                players_out.append({
                    "id": pid, "first": first, "last": last, "name": name,
                    "pts": vals["pts"], "reb": vals["reb"], "ast": vals["ast"], "stl": vals["stl"], "blk": vals["blk"],
                })
        out.append({"teamId": tid, "players": players_out})
    return out

# ---------------- HIGHLIGHTS & RENDER ----------------
def _flame(pts, reb, ast, stl, blk):
    dd = sum(v>=10 for v in (pts,reb,ast))
    td = dd >= 3
    if pts >= 35 or td or (pts>=30 and dd>=2): return " 🔥"
    return ""

def is_highlight(p):
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
    dd = sum(v>=10 for v in (pts,reb,ast))
    return (pts >= 30) or (dd >= 2) or (reb >= 15) or (ast >= 12) or (stl >= 4) or (blk >= 4)

def _initial_ru(first_en, ru_first, fallback_name):
    if ru_first: return ru_first[:1].upper()
    base = first_en or (fallback_name.split()[0] if fallback_name else "")
    return _initial_from_en_to_ru(base)

def display_name_ru(p, ru_first, ru_last):
    init = _initial_ru(p.get("first",""), ru_first, p.get("name",""))
    surname = (ru_last or "").strip()
    if not surname:
        last_en = (p.get("last") or (p.get("name","").split()[-1] if p.get("name") else ""))
        surname = (last_en or "").strip()
    return f"{init}. {surname}"

def fmt_stat_line_ru(p, ru_first, ru_last, bold=False):
    name = display_name_ru(p, ru_first, ru_last)
    if bold: name = f"<b>{name}</b>"
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stл"], p["blk"] if False else (p["pts"], p["reb"], p["ast"], p["stl"], p["blk"])
    parts = [f"{name}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if reb >= 5: parts.append(f"{reb} {ru_plural(reb, ('подбор','подбора','подборов'))}")
    if ast >= 5: parts.append(f"{ast} {ru_plural(ast, ('передача','передачи','передач'))}")
    if stl >= 4: parts.append(f"{stl} {ru_plural(stl, ('перехват','перехвата','перехватов'))}")
    if blk >= 4: parts.append(f"{blk} {ru_plural(blk, ('блок-шот','блок-шота','блок-шотов'))}")
    return ", ".join(parts) + _flame(pts,reb,ast,stl,blk)

def select_highlights(players, abbr):
    if not players: return []
    want_special = "demin" if abbr=="BKN" else ("goldin" if abbr=="MIA" else None)
    def score_key(p): return (p.get("pts",0), p.get("reb",0)+p.get("ast",0), p.get("stl",0)+p.get("blk",0))
    sorted_all = sorted(players, key=score_key, reverse=True)
    picks = [p for p in sorted_all if is_highlight(p)][:2] or sorted_all[:1]
    spec = None
    if want_special:
        spec = next((p for p in sorted_all if (p.get("last","") or p.get("name","")).strip().lower().endswith(want_special)), None)
        if spec and all(spec["id"] != q["id"] for q in picks):
            if len(picks) == 2: picks[1] = spec
            else: picks.append(spec)
    return [(p, bool(spec and p["id"] == spec["id"])) for p in picks]

# ---------------- RENDER ----------------
SEP = "–––––––––––––––––––––––"

def _team_line_text(abbr, score, record, winner, ot_suffix, entities, offset_ref):
    name = TEAM_RU.get(abbr, abbr)
    s   = f"<b>{score}</b>" if winner else f"{score}"
    rec = f" ({record})" if record else ""
    use_custom = TEAM_CUSTOM_IDS.get(abbr)
    if use_custom:
        piece = f"■ {name}: {s}{rec}{ot_suffix}"
        entities.append({"type":"custom_emoji","offset":offset_ref[0],"length":1,"custom_emoji_id":use_custom})
        offset_ref[0] += len(piece) + 1
        return piece
    else:
        emo = TEAM_EMОJI.get(abbr, "🏀") if False else TEAM_EMOJI.get(abbr, "🏀")
        piece = f"{emo} {name}: {s}{rec}{ot_suffix}"
        offset_ref[0] += len(piece) + 1
        return piece

def build_game_block(game, players_by_teamid, entities, offset_ref):
    comp = game["competitors"]
    if len(comp) != 2: return ""
    a, b = comp[0], comp[1]  # гости → хозяева

    head_a = _team_line_text(a["abbr"], a["score"], a["record"], a["winner"], "", entities, offset_ref)
    head_b = _team_line_text(b["abbr"], b["score"], b["record"], b["winner"], game.get("ot",""), entities, offset_ref)
    head = head_a + "\n" + head_b + "\n"
    offset_ref[0] += 1

    def lines_for_team(c):
        arr = players_by_teamid.get(c["teamId"], [])
        picks = select_highlights(arr, c["abbr"])
        lines = []
        for p, bold in picks:
            ru_first, ru_last = resolve_ru_name(p.get("first",""), p.get("last",""), p.get("id",""))
            lines.append(fmt_stat_line_ru(p, ru_first, ru_last, bold))
        return lines

    la = lines_for_team(a)
    lb = lines_for_team(b)
    lines = []
    if la: lines.extend(la + [""])
    if lb: lines.extend(lb)
    text = head + ("\n".join(l for l in lines if l.strip()))
    offset_ref[0] += len(text) - len(head)
    return text.strip()

def build_post_text_and_entities():
    global TEAM_CUSTOM_IDS
    TEAM_CUSTOM_IDS = _load_json(TEAM_CUSTOM_IDS_PATH, {})

    chosen_day = None
    games = []
    source_used = None

    # 1) NBA с жёстким дедлайном 30с
    for d in pick_candidate_days():
        games = sb_nba(d, deadline_sec=30.0)
        if games:
            chosen_day = d; source_used = "nba"; break

    # 2) SofaScore
    if not games:
        for d in pick_candidate_days():
            games = sb_sofa(d)
            if games:
                chosen_day = d; source_used = "sofa"; break

    # 3) ESPN
    if not games:
        for d in pick_candidate_days():
            games = sb_espn(d)
            if games:
                chosen_day = d; source_used = "espn"; break

    if not chosen_day:
        chosen_day = pick_report_date()

    title = (
        f"НБА • {ru_date(chosen_day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
        "Результаты надёжно спрятаны 👇\n"
        f"{SEP}\n\n"
    )
    entities = []
    offset_ref = [len(title)]

    # Подготовим ESPN-индекс для SofaScore (сопоставление по аббревиатуре и счёту)
    espn_events_map = {}
    if source_used == "sofa":
        espn_list = sb_espn(chosen_day)
        for ev in espn_list:
            comp = ev["competitors"]
            if len(comp) != 2: continue
            a, b = comp[0], comp[1]
            key1 = (a["abbr"], b["abbr"], a["score"], b["score"])
            key2 = (b["abbr"], a["abbr"], b["score"], a["score"])
            espn_events_map[key1] = ev
            espn_events_map[key2] = ev

    blocks = []
    for i, g in enumerate(games, 1):
        players_by_teamid = {}

        if source_used == "nba" and g.get("eventId"):
            teams = box_nba(g["eventId"])
            players_by_teamid = {t["teamId"]: (t.get("players") or []) for t in teams}

        elif source_used == "espn" and g.get("eventId"):
            teams = box_espn(g["eventId"])
            players_by_teamid = {t["teamId"]: (t.get("players") or []) for t in teams}

        elif source_used == "sofa":
            comp = g["competitors"]
            if len(comp) == 2:
                a, b = comp[0], comp[1]
                ev = espn_events_map.get((a["abbr"], b["abbr"], a["score"], b["score"]))
                if ev:
                    teams = box_espn(ev["eventId"])
                    players_by_teamid = {t["teamId"]: (t.get("players") or []) for t in teams}

        blk = build_game_block(g, players_by_teamid, entities, offset_ref)
        blocks.append(blk.strip())
        if i < len(games):
            blocks.append(f"\n{SEP}\n")
            offset_ref[0] += len(SEP) + 2

    return (title + "\n".join(blocks)).strip(), entities

# ---------------- TELEGRAM ----------------
def tg_send_single(text, entities):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if entities: payload["entities"] = entities
    resp = S.post(url, json=payload, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    try:
        loaded_map = _load_json(RU_MAP_PATH, {})
        loaded_pending = _load_json(RU_PENDING_PATH, [])
        TEAM_CUSTOM_IDS.update(_load_json(TEAM_CUSTOM_IDS_PATH, {}))
        if isinstance(loaded_map, dict): RU_MAP.update(loaded_map)
        if isinstance(loaded_pending, list): RU_PENDING.extend(loaded_pending)

        text, entities = build_post_text_and_entities()
        tg_send_single(text, entities)

        _save_json(RU_PENDING_PATH, RU_PENDING)
        _save_json(RU_MAP_PATH, RU_MAP)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
