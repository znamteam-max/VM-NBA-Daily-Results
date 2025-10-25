#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU), OFFICIAL NBA (cdn.nba.com)

• Табло:   https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_YYYYMMDD.json
           (фоллбэк для «сегодня»: todaysScoreboard_00.json)
• Бокскор: https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gameId}.json

• Игроки к показу (1–2 на команду):
    - очки ≥ 30, ИЛИ
    - дабл-дабл (любые 2 из PTS/REB/AST ≥ 10), ИЛИ
    - подборы ≥ 15, ИЛИ передачи ≥ 12, ИЛИ перехваты ≥ 4, ИЛИ блок-шоты ≥ 4.
  Спец-правило: Дёмин (BKN) и Голдин (MIA) — включить и выделить жирным, если играли.

• Отображение стат: всегда очки; дополнительно выводим ТОЛЬКО если пороги пройдены:
    REB (≥5), AST (≥5), STL (≥4), BLK (≥4).

• Команды: название на русском + эмодзи.
  Обычные эмодзи — из словаря; кастом-эмодзи берём из team_emoji_ids.json ({ "LAL": "<custom_id>", ... }).

• Сообщение отправляется ОДНИМ постом.
"""

import os, sys, re, json, time, unicodedata
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
NBA_SB_DATE   = "https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{date}.json"      # YYYYMMDD
NBA_SB_TODAY  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
NBA_BOX       = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"

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
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.7,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    # важны заголовки для cdn.nba.com, чтобы не резало
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) NBA-DailyResultsBot/5.0",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Origin": "https://www.nba.com",
        "Referer": "https://www.nba.com/",
        "Accept": "application/json,text/plain,*/*",
        "Connection": "keep-alive",
    })
    return s
S = make_session()
def log(*a): print(*a, file=sys.stderr)

def _get_json(url: str) -> dict:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
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

def _latin_initial_to_cyr(first_en: str) -> str:
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
        r = S.get(url, timeout=15)
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
        r = S.get(url, timeout=20)
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
        r = S.get(SRU_SEARCH + q, timeout=20)
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
    # Ориентируемся на ET: публикуем прошедший игровой день до ~08:00 ET
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()

def pick_candidate_days():
    base = pick_report_date()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

# ---------------- NBA SCOREBOARD ----------------
def fetch_scoreboard(day: date):
    dstr = day.strftime("%Y%m%d")
    j = _get_json(NBA_SB_DATE.format(date=dstr))
    games = _parse_scoreboard_json(j)
    if games:
        return games
    # фоллбэк: если день = сегодня по ET, попробуем todaysScoreboard
    today_et = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
    if dstr == today_et:
        j2 = _get_json(NBA_SB_TODAY)
        g2 = _parse_scoreboard_json(j2)
        if g2:
            return g2
    return []

def _parse_scoreboard_json(j):
    if not isinstance(j, dict): return []
    sb = j.get("scoreboard") or {}
    games = sb.get("games") or []
    out = []
    for g in games:
        try:
            gid = str(g.get("gameId") or g.get("gameCode") or "")
            status = str(g.get("gameStatusText") or "").lower()
            completed = "final" in status
            if not completed:  # только финальные
                continue
            ot = ""
            m = re.search(r'(\d+)\s*ot', status)
            if "ot" in status:
                ot = f" ({int(m.group(1))}ОТ)" if m else " (ОТ)"
            # home/away
            ht = g.get("homeTeam") or {}
            at = g.get("awayTeam") or {}
            comp = []
            for tm in (at, ht):  # держим порядок: гостевая — первая, домашняя — вторая
                abbr = (tm.get("teamTricode") or "").upper()
                if abbr == "GS": abbr = "GSW"
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
                out.append({"eventId": gid, "competitors": comp, "ot": ot})
        except Exception:
            continue
    return out

# ---------------- NBA BOX ----------------
def fetch_box(game_id: str):
    j = _get_json(NBA_BOX.format(gid=game_id)) or {}
    game = j.get("game") or {}
    teams = []
    for side in ("awayTeam","homeTeam"):
        team = game.get(side) or {}
        tid = str(team.get("teamId") or "")
        players = []
        for p in (team.get("players") or []):
            info = p.get("person") or {}
            st   = p.get("statistics") or {}
            # фильтр «играл»: минуты или любые статы
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
        teams.append({"teamId": tid, "players": players})
    return teams

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

def merge_lists(players):
    # nothing to merge here (все из одного источника), но оставим для симметрии
    return list(players or [])

def _initial_ru(first_en, ru_first, fallback_name):
    if ru_first: return ru_first[:1].upper()
    base = first_en or (fallback_name.split()[0] if fallback_name else "")
    ch = base[:1].upper() if base else "И"
    table = {"A":"А","B":"Б","C":"К","D":"Д","E":"Е","F":"Ф","G":"Г","H":"Х","I":"И","J":"Д","K":"К",
             "L":"Л","M":"М","N":"Н","O":"О","P":"П","Q":"К","R":"Р","S":"С","T":"Т","U":"У","V":"В",
             "W":"В","X":"К","Y":"Й","Z":"З"}
    return table.get(ch, "И")

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
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]
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
        # entity: один символ '■' заменяем на custom emoji
        entities.append({"type":"custom_emoji","offset":offset_ref[0],"length":1,"custom_emoji_id":use_custom})
        offset_ref[0] += len(piece) + 1
        return piece
    else:
        emo = TEAM_EMOJI.get(abbr, "🏀")
        piece = f"{emo} {name}: {s}{rec}{ot_suffix}"
        offset_ref[0] += len(piece) + 1
        return piece

def build_game_block(game, entities, offset_ref):
    comp = game["competitors"]
    if len(comp) != 2: return ""
    a, b = comp[0], comp[1]  # порядок: «гости», затем «дом»

    head_a = _team_line_text(a["abbr"], a["score"], a["record"], a["winner"], "", entities, offset_ref)
    head_b = _team_line_text(b["abbr"], b["score"], b["record"], b["winner"], game.get("ot",""), entities, offset_ref)
    head = head_a + "\n" + head_b + "\n"
    offset_ref[0] += 1

    # бокс с официального API
    box_teams = fetch_box(game["eventId"])
    by_tid = {t["teamId"]: merge_lists(t.get("players", [])) for t in box_teams}

    def lines_for_team(c):
        arr = by_tid.get(c["teamId"], [])
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
    for d in pick_candidate_days():
        games = fetch_scoreboard(d)
        if games:
            chosen_day = d
            break
    if not chosen_day:
        chosen_day = pick_report_date()

    title = f"НБА • {ru_date(chosen_day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n" \
            "Результаты надёжно спрятаны 👇\n" \
            f"{SEP}\n\n"

    entities = []
    offset_ref = [len(title)]

    blocks = []
    for i, g in enumerate(games, 1):
        blk = build_game_block(g, entities, offset_ref)
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
    resp = S.post(url, json=payload, timeout=25)
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    try:
        # кэши
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
