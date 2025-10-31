#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

• Пары/рекорды (W-L): ESPN site.api (несколько дат вокруг «сегодня»).
• Счёт/игроки/русские фамилии: sports.ru (боксскор). Если игроков нет — фоллбек на ESPN boxscore.
• Заголовок: НБА • {дата (Лондон)} • {N матчей} + спойлеры (скрыты счёт и игроки).
• Победный счёт — жирным, OT/2ОТ/... указывается у проигравшего.
• Игроки (на каждую команду): минимум один, максимум два;
  второй добавляется, если ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 перехватов/блоков.
• Спец: если играл Егор Дёмин (BKN) или Влад Голдин (MIA) — обязательно показать его
  с 3 максимальными метриками (жирным).
• Эмодзи команд: по умолчанию встроенные; можно задать кастом-эмодзи через переменную окружения TEAM_EMOJI_JSON:
  {"BOS":"<tg-cemoji-id>", "NYK":"<tg-cemoji-id>", ...}
"""

import os, sys, re, json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    Retry = None
from bs4 import BeautifulSoup

# -------- ENV --------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # {"BOS":"<custom_emoji>", ...}
DEBUG = bool(os.getenv("DEBUG_NBA"))

# -------- HTTP --------
HTTP_TIMEOUT = 10

def _mk_adapter():
    if Retry is not None:
        r = Retry(total=3, connect=3, read=3, backoff_factor=0.4,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET","POST"])
        return HTTPAdapter(max_retries=r)
    return HTTPAdapter(max_retries=2)

def make_session():
    s = requests.Session()
    ad = _mk_adapter()
    s.mount("https://", ad); s.mount("http://", ad)
    # Только ASCII в UA, чтобы не ловить UnicodeEncodeError у некоторых окружений
    s.headers.update({
        "User-Agent": "NBA-DRB/4.7 (sportsru+espn; spoilers)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
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
        if r.status_code != 200: return {}
        return r.json()
    except Exception:
        return {}

# -------- DATES --------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def pick_report_date_london() -> date:
    now = datetime.now(ZoneInfo("Europe/London"))
    # После ~11:00 по Лондону считаем новым «днём поста»
    return now.date() if now.hour >= 11 else (now.date() - timedelta(days=1))

def candidate_days() -> list[date]:
    # Для ESPN/BDL берём ET ±1 и Лондон, чтобы накрыть граничные матчи
    now_et = datetime.now(ZoneInfo("America/New_York"))
    base_et = now_et.date() if now_et.hour >= 8 else (now_et.date() - timedelta(days=1))
    lon = pick_report_date_london()
    c = {base_et - timedelta(days=1), base_et, base_et + timedelta(days=1), lon}
    return sorted(c)

# -------- TEAMS / EMOJI --------
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
ABBR_TO_RU = {v:k for k,v in TEAM_RU_TO_ABBR.items()}

# Нормализация аббревиатур из разных фидов → канонические 3-буквенные
ESPN_ABBR_FIX = {
    # Частые «укороты»
    "GS": "GSW",
    "WSH": "WAS",
    "SA": "SAS",
    "NO": "NOP",
    "NY": "NYK",
    # Варианты «старого ESPN»
    "PHO": "PHX",
    "UTH": "UTA",
    "GSW": "GSW", "WAS":"WAS", "SAS":"SAS",
    "NOP":"NOP", "NYK":"NYK", "PHX":"PHX", "UTA":"UTA",
    # На всякий случай встречающиеся нестандартные:
    "LAK": "LAL",  # редко, но бывает
    "LAC": "LAC", "LAL":"LAL",
    "CLV": "CLE", "CLEV": "CLE", "CLE":"CLE",
    "POR":"POR", "OKC":"OKC", "ORL":"ORL", "MEM":"MEM",
    "DAL":"DAL", "DEN":"DEN", "DET":"DET", "HOU":"HOU",
    "IND":"IND", "MIA":"MIA", "MIL":"MIL", "MIN":"MIN",
    "PHI":"PHI", "SAC":"SAC", "TOR":"TOR",
    "BOS":"BOS", "ATL":"ATL", "BKN":"BKN", "CHI":"CHI",
    "GSWARRIORS":"GSW", "WIZARDS":"WAS", "SPURS":"SAS", "PELICANS":"NOP", "KNICKS":"NYK",
    "SUNS":"PHX", "JAZZ":"UTA", "WARRIORS":"GSW",
}

def norm_abbr(a: str) -> str:
    a = (a or "").upper()
    return ESPN_ABBR_FIX.get(a, a)

TEAM_EMOJI_DEFAULT = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡️","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}
def load_team_emojis():
    if TEAM_EMOJI_JSON:
        try:
            d = json.loads(TEAM_EMOJI_JSON)
            if isinstance(d, dict):
                # поддержка кастом-эмодзи/ID от Telegram
                out = TEAM_EMOJI_DEFAULT.copy()
                for k,v in d.items():
                    out[k.upper()] = str(v)
                return out
        except Exception:
            pass
    return TEAM_EMOJI_DEFAULT
TEAM_EMOJI = load_team_emojis()
def emoji(abbr: str) -> str: return TEAM_EMOJI.get(norm_abbr(abbr), "🏀")

# -------- SPORTS.RU (день + боксскоры на русском) --------
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def _normalize_match_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full); return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def _soup(url: str):
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None

def collect_day_links(d: date) -> list[str]:
    soup = _soup(day_url(d))
    if not soup: return []
    seen=set(); out=[]
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href: continue
        full = _normalize_match_url(href)
        if full in seen: continue
        seen.add(full); out.append(full)
    log("[DBG] SPORTS LINKS", len(out))
    return out

def _canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    t = raw.replace("«","").replace("»","").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t:
            return k
    return None

def _final_score_from_page_text(text: str) -> tuple[int,int,int]:
    """
    1) Ищем слово 'завершен/завершён' и ближайшую пару 'NN : NN' слева — это финальный счёт.
    2) Если не нашли: берём все пары счёта эпох по четвертям и суммируем каждую колонку.
    Возвращает (A,B,OTcount). OT считаем по количеству эпох >4, если удаётся.
    """
    # 1) «завершен» / «завершён»
    m_end = re.search(r"заверш[её]н", text, re.IGNORECASE)
    if m_end:
        before = text[:m_end.start()]
        pairs = list(re.finditer(r"(\d+)\s*:\s*(\d+)", before))
        if pairs:
            last = pairs[-1]
            try:
                a = int(last.group(1)); b = int(last.group(2))
                # попытаемся догадаться про OT: после финала часто идут 5-я/6-я «эпохи»
                tail = text[last.end(): last.end()+320]
                frames = re.findall(r"(\d+)\s*:\s*(\d+)", tail)
                ot = max(len(frames) - 4, 0)
                return a, b, ot
            except Exception:
                pass

    # 2) сумма по epoch-фреймам (первые 4 — четверти, следующие — OT)
    frames = re.findall(r"(\d+)\s*:\s*(\d+)", text)
    if frames:
        nums = [(int(x), int(y)) for x,y in frames]
        # heuristic: берём последние 4-7 пар, где «эпохи» идут подряд
        # чаще всего на странице блок с «31 : 38 27 : 33 ...», который идёт сразу после финала
        if len(nums) >= 4:
            last4 = nums[-4:]
            a = sum(p[0] for p in last4)
            b = sum(p[1] for p in last4)
            ot = 0
            # если есть ещё пары после первых 4 — считаем их как OT
            if len(nums) > 4:
                extra = nums[-(len(nums)-4):]
                if len(extra) > 0:
                    ot = max(len(extra), 0)
            return a, b, ot

    # 3) fallback — первая найденная пара (худший случай)
    m = re.search(r"(\d+)\s*:\s*(\d+)", text)
    if m:
        try:
            return int(m.group(1)), int(m.group(2)), 0
        except Exception:
            pass
    return 0, 0, 0

def parse_sports_match(url: str) -> dict | None:
    soup = _soup(url)
    if not soup: return None
    text = soup.get_text(" ", strip=True)

    # Команды: через og:title или заголовки «Статистика игроков. <команда>»
    meta = soup.find("meta", attrs={"property":"og:title"})
    title = meta.get("content") if meta and meta.get("content") else (soup.title.string if soup.title else "")
    teamA = teamB = None
    if title and "—" in title:
        left, right = [x.strip() for x in title.split("—", 1)]
        teamA = _canonical_ru_team(left); teamB = _canonical_ru_team(right)
    if not (teamA and teamB) or (teamA == teamB):
        heads=[]
        for h in soup.find_all(["h2","h3","h4"]):
            t = h.get_text(" ", strip=True).lower()
            if "статистика игроков" in t:
                k = _canonical_ru_team(h.get_text(" ", strip=True).split(".")[0])
                if k: heads.append(k)
        if len(heads) >= 2:
            teamA = teamA or heads[0]
            teamB = teamB or next((x for x in heads[1:] if x != teamA), None)
    if not (teamA and teamB) or teamA == teamB:
        return None

    a_abbr = TEAM_RU_TO_ABBR.get(teamA,""); b_abbr = TEAM_RU_TO_ABBR.get(teamB,"")
    if not a_abbr or not b_abbr:
        return None

    # Финальный счёт
    scoreA, scoreB, ot = _final_score_from_page_text(text)

    def read_rows(team_ru_key: str) -> list[dict]:
        """
        Ищем таблицу сразу после «Статистика игроков. <команда>», затем парсим строки.
        Колонки sports.ru бывают разными, поэтому:
          - ищем индекс «Имя» (первая ячейка, содержащая буквы, а не цифры/форматы),
          - дальше берём числа: очки — ближайшая числовая колонка (обычно первая),
            подборы/передачи/перехваты/блоки — по типовым смещениям (работает надёжно на страницах НБА).
        """
        rows=[]; stamp = team_ru_key.lower()
        anchor=None
        for h in soup.find_all(["h2","h3","h4"]):
            t = h.get_text(" ", strip=True)
            if "статистика игроков" in t.lower() and stamp in t.lower().split(".")[0]:
                anchor=h; break
        if not anchor:
            return rows
        table = anchor.find_next("table")
        if not table:
            return rows

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td","th"])
            if not cells: continue
            # пропустим заголовки
            headtxt = " ".join(c.get_text(" ", strip=True).lower() for c in cells)
            if "игрок" in headtxt: 
                continue

            # распознать имя
            tds = [td.get_text(" ", strip=True) for td in cells]
            name_idx=None
            for i,cell in enumerate(tds[:3]):
                if re.search(r"[^\d/:% ]", cell):
                    name_idx=i; break
            if name_idx is None: 
                continue

            name = tds[name_idx].strip()
            nums = tds[name_idx+1:]
            # Бывают дополнительные колонки, поэтому берём только цифры из первых ~14
            nums = [re.sub(r"[^\d]", "", x) for x in nums[:16]]
            nums = [x for x in nums if x != ""]
            if len(nums) < 5:
                continue

            def as_int(x: str) -> int:
                try: return int(x)
                except: 
                    try: return int(float(x))
                    except: return 0

            # Эвристика sports.ru (НБА): после имени идут очки, а затем проценты/дистанции,
            # ближе к концу блока — подборы/передачи/перехваты/блоки.
            pts = as_int(nums[0])
            # Чтобы не развалиться на редких разметках — берём из «хвоста» 4 величины как reb/ast/stl/blk.
            tail = [as_int(x) for x in nums[-6:]]  # в конце обычно: подборы, передачи, перехваты, потери, блоки, фолы
            # Попытаемся извлечь шаблонно: reb, ast, stl, blk — среди tail
            # Возьмём 4 максимума по порядку появления: это устойчивая эвристика для sports.ru
            # (перехваты/блоки обычно маленькие, но их порогами мы фильтруем при показе)
            reb = tail[0] if len(tail) >= 4 else 0
            ast = tail[1] if len(tail) >= 4 else 0
            stl = tail[2] if len(tail) >= 4 else 0
            blk = tail[4] if len(tail) >= 5 else (tail[3] if len(tail)>=4 else 0)

            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        return rows

    rowsA = read_rows(teamA)
    rowsB = read_rows(teamB)
    finished = bool(rowsA or rowsB) and (scoreA or scoreB)

    info = {
        "teamA": {"name": teamA, "abbr": a_abbr, "emoji": emoji(a_abbr), "score": scoreA},
        "teamB": {"name": teamB, "abbr": b_abbr, "emoji": emoji(b_abbr), "score": scoreB},
        "ot": max(0, int(ot)), "finished": finished,
        "players": {teamA: rowsA, teamB: rowsB},
        "url": url,
    }
    log(f"[DBG] OK {teamA}-{teamB} SCORE {scoreA}:{scoreB} A_rows {len(rowsA)} B_rows {len(rowsB)}")
    return info

# -------- ESPN site.api (пары, рекорды, игроки фоллбек) --------
ESPN_SB = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"
ESPN_BOX = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eid}"

def _espn_record(c: dict) -> str:
    for r in c.get("records") or []:
        if r.get("type") == "total" and r.get("summary"):
            return r["summary"]
    return ""

def fetch_espn_events_for_day(d: date) -> list[dict]:
    j = _get_json(ESPN_SB.format(ymd=d.strftime("%Y%m%d")))
    out=[]
    for ev in (j.get("events") or []):
        try:
            comp = (ev.get("competitions") or [None])[0] or {}
            comps = comp.get("competitors") or []
            if len(comps) != 2: continue
            home = next(c for c in comps if c.get("homeAway")=="home")
            away = next(c for c in comps if c.get("homeAway")=="away")
            th = (home.get("team") or {}); ta = (away.get("team") or {})
            abbr_h = norm_abbr(th.get("abbreviation") or "")
            abbr_a = norm_abbr(ta.get("abbreviation") or "")

            status = (ev.get("status") or {}).get("type") or {}
            completed = bool(status.get("completed", False))
            period = int(status.get("period") or 0)
            ot = max(period - 4, 0) if completed and period>4 else 0

            def as_int(x):
                try: return int(float(x))
                except: return 0

            out.append({
                "eventId": str(ev.get("id") or ""),
                "home": {
                    "abbr": abbr_h, "teamId": str(th.get("id") or ""),
                    "score": as_int(home.get("score", 0)),
                    "winner": bool(home.get("winner", False)),
                    "record": _espn_record(home),
                },
                "away": {
                    "abbr": abbr_a, "teamId": str(ta.get("id") or ""),
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
    seen={}
    for d in days:
        for e in fetch_espn_events_for_day(d):
            if not e.get("completed"):  # только финалы
                continue
            key = frozenset([e["home"]["abbr"], e["away"]["abbr"]])
            if key in seen:
                continue
            seen[key] = e
    return seen  # pair -> event

def fetch_espn_players(event_id: str) -> dict:
    j = _get_json(ESPN_BOX.format(eid=event_id))
    out={}
    for team_block in (j.get("players") or []):
        team = team_block.get("team") or {}
        tid = str(team.get("id") or "")
        arr=[]
        for grp in (team_block.get("statistics") or []):
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                nm = (ath.get("displayName") or "").strip()
                stats={}
                for k,v in (a.get("stats") or {}).items(): stats[k.lower()] = v
                for k,v in (ath.get("stats") or {}).items(): stats.setdefault(k.lower(), v)
                def iget(*keys, default=0):
                    for k in keys:
                        if k in stats:
                            try: return int(stats[k])
                            except:
                                try: return int(float(stats[k]))
                                except: pass
                    return default
                pts=iget("points","pts"); reb=iget("rebounds","reb","reboundstotal")
                ast=iget("assists","ast"); stl=iget("steals","stl"); blk=iget("blocks","blk")
                if any([pts,reb,ast,stl,blk]):
                    arr.append({"name": nm, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        # merge by name (max)
        merged={}
        for p in arr:
            if p["name"] not in merged: merged[p["name"]] = p
            else:
                m = merged[p["name"]]
                for k in ("pts","reb","ast","stl","blk"): m[k] = max(m[k], p[k])
        out[tid] = list(merged.values())
    return out

# -------- Игроки/формат --------
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts: return full or ""
    if len(parts) == 1: return parts[0]
    first = parts[0]; last = parts[-1]
    # «мл.»/«ст.» как часть фамилии
    if last.lower() in {"jr.","jr","мл.","ст.","sr.","sr"} and len(parts)>=3:
        last = parts[-2] + " " + parts[-1]
    return f"{first[0]}. {last}"

def ru_forms(label: str, v: int) -> str:
    if label=="pts": return f"{v} {ru_plural(v, ('очко','очка','очков'))}"
    if label=="reb": return f"{v} {ru_plural(v, ('подбор','подбора','подборов'))}"
    if label=="ast": return f"{v} {ru_plural(v, ('передача','передачи','передач'))}"
    if label=="stl": return f"{v} {ru_plural(v, ('перехват','перехвата','перехватов'))}"
    if label=="blk": return f"{v} {ru_plural(v, ('блок-шот','блок-шота','блок-шотов'))}"
    return f"{v}"

def hot_mark(p: dict) -> str:
    if (p["pts"]>=35) or (p["reb"]>=15) or (p["ast"]>=12) or (p["stl"]>=5) or (p["blk"]>=5):
        return " 🔥"
    return ""

def is_dd(p: dict) -> bool:
    return sum(x>=10 for x in [p["pts"],p["reb"],p["ast"],p["stl"],p["blk"]]) >= 2

def second_ok(p: dict) -> bool:
    return (p["pts"]>=20) or is_dd(p) or (p["stl"]>=6) or (p["blk"]>=6)

def score_key(p: dict): return (p["pts"], p["reb"]+p["ast"], p["stl"]+p["blk"])

def pick_team_players(abbr: str, rows: list[dict]) -> list[tuple[dict,bool,bool]]:
    # [(player, bold, special_detail)]
    if not rows: return []
    rows = sorted(rows, key=score_key, reverse=True)
    special_keys = []
    if norm_abbr(abbr)=="BKN": special_keys = ["дёмин","demin"]
    if norm_abbr(abbr)=="MIA": special_keys = ["голдин","goldin"]
    special=None
    for p in rows:
        nm = (p["name"] or "").lower()
        if any(k in nm for k in special_keys):
            special=p; break
    out=[]
    top = rows[0]
    if special and special["name"] == top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False)); out.append((special, True, True))
    else:
        out.append((top, False, False))
    if len(out)<2:
        for p in rows[1:]:
            if p["name"] == top["name"]: continue
            if second_ok(p): out.append((p, False, False)); break
    return out[:2]

def format_player_regular(p: dict, bold=False) -> str:
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    out = [ru_forms("pts", p["pts"])]
    if p["reb"]>=5: out.append(ru_forms("reb", p["reb"]))
    if p["ast"]>=5: out.append(ru_forms("ast", p["ast"]))
    if p["stl"]>=4: out.append(ru_forms("stl", p["stl"]))
    if p["blk"]>=4: out.append(ru_forms("blk", p["blk"]))
    return f"{name}: " + ", ".join(out) + hot_mark(p)

def format_player_special(p: dict) -> str:
    name = f"<b>{initials_ru(p['name'])}</b>"
    stats=[("pts",p["pts"]),("reb",p["reb"]),("ast",p["ast"]),("stl",p["stl"]),("blk",p["blk"])]
    stats=[(k,v) for k,v in stats if v>0]
    stats.sort(key=lambda kv: kv[1], reverse=True)
    chosen=stats[:3]
    return f"{name}: " + ", ".join(ru_forms(k,v) for k,v in chosen) + hot_mark(p)

# -------- Спойлер --------
def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'
SEP = "–––––––––––––––––––––––"

# -------- Блоки --------
def format_score_line(name_ru: str, abbr: str, score: int, winner: bool, record: str, ot_str: str) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    if ot_str and not winner: score_txt += ot_str
    if record: score_txt += f" ({record})"
    return f"{emoji(abbr)} {name_ru}: {sp(score_txt)}"

def build_block_from_sports(info: dict, records: dict[str,str]) -> str:
    A,B = info["teamA"], info["teamB"]
    ot_str = "" if info["ot"]==0 else (" (ОТ)" if info["ot"]==1 else f" ({info['ot']} ОТ)")
    a_win = A["score"] > B["score"]; b_win = B["score"] > A["score"]
    head = (
        f"{format_score_line(A['name'], A['abbr'], A['score'], a_win, records.get(A['abbr'],""), '')}\n"
        f"{format_score_line(B['name'], B['abbr'], B['score'], b_win, records.get(B['abbr'],""), ot_str)}\n\n"
    )
    rowsA = info["players"].get(A["name"], []); rowsB = info["players"].get(B["name"], [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(A["abbr"], rowsA)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(B["abbr"], rowsB)]
    lines=[]
    if al: lines.extend(al)
    if al and bl: lines.append("")  # пустая строка между командами
    if bl: lines.extend(bl)
    return head + ("\n".join(lines) if lines else "")

def build_block_from_espn(e: dict) -> str:
    h, a = e["home"], e["away"]
    name_h = ABBR_TO_RU.get(h["abbr"], h["abbr"]); name_a = ABBR_TO_RU.get(a["abbr"], a["abbr"])
    ot_str = "" if e["ot"]==0 else (" (ОТ)" if e["ot"]==1 else f" ({e['ot']} ОТ)")
    head = (
        f"{format_score_line(name_h, h['abbr'], h['score'], h['winner'], h.get('record',''), '')}\n"
        f"{format_score_line(name_a, a['abbr'], a['score'], a['winner'], a.get('record',''), ot_str)}\n\n"
    )
    players_by_tid = fetch_espn_players(e["eventId"])
    rowsH = players_by_tid.get(h["teamId"], []); rowsA = players_by_tid.get(a["teamId"], [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(h["abbr"], rowsH)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(a["abbr"], rowsA)]
    lines=[]
    if al: lines.extend(al)
    if al and bl: lines.append("")
    if bl: lines.extend(bl)
    return head + ("\n".join(lines) if lines else "")

# -------- Сбор матчей дня --------
def fetch_sports_games_for_title_day(d_title: date) -> dict[frozenset, dict]:
    games={}
    for url in collect_day_links(d_title):
        info = parse_sports_match(url)
        if not info or not info["finished"]:
            continue
        pair = frozenset([info["teamA"]["abbr"], info["teamB"]["abbr"]])
        if pair in games:
            continue
        games[pair] = info
    return games  # pair -> sports.info

def build_post() -> str:
    d_title = pick_report_date_london()
    days = candidate_days()

    # 1) Пары и рекорды: ESPN (completed) по нескольким дням
    espn_by_pair = fetch_espn_events_multi(days)  # pair -> event

    # 2) Контент: Sports.ru для даты заголовка (русские фамилии/статы/точные счёты)
    sports_by_pair = fetch_sports_games_for_title_day(d_title)

    # 3) Итоговый порядок: все пары ESPN; если есть sports.ru для пары — берём оттуда блок
    ordered_pairs = list(espn_by_pair.keys())
    title_count = len(ordered_pairs)
    title = f"НБА • {ru_date(d_title)} • {title_count} {ru_plural(title_count, ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if title_count == 0:
        return title.rstrip()

    blocks=[]
    for i, pair in enumerate(ordered_pairs, 1):
        if pair in sports_by_pair:
            ev = espn_by_pair.get(pair, {})
            rec_map = {}
            if ev:
                rec_map[ev["home"]["abbr"]] = ev["home"].get("record","")
                rec_map[ev["away"]["abbr"]] = ev["away"].get("record","")
            blocks.append(build_block_from_sports(sports_by_pair[pair], rec_map))
        else:
            # фоллбек на ESPN (англ. имена, но формат/спойлеры соблюдены)
            blocks.append(build_block_from_espn(espn_by_pair[pair]))
        if i < title_count:
            blocks.append("\n" + SEP + "\n\n")

    return (title + "".join(blocks)).strip()

# -------- Telegram --------
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

# -------- MAIN --------
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
