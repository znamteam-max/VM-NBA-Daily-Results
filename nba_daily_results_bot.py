#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

Режимы:
1) Обычный запуск (по CRON/Actions) — отправляет пост за «репорт-день» (ET) в CHAT_ID.
2) Слушатель (--listen или NBA_LISTEN=1) — long-poll Telegram:
   - /nba  → показать меню с 3 последними датами (ET) как инлайн-кнопки
   - /nba YYYY-MM-DD → сразу прислать пост за указанную дату (по ET)

Источники и формат (как было в последних версиях):
• Пары/W-L/OT: ESPN site.api (scoreboard) по нескольким датам вокруг выбранной.
• Счёт/игроки на русском: Sports.ru (боксскор). Если нет — фоллбек ESPN boxscore.
• Спойлеры: счёт и игроки завернуты, у победителя счёт жирным, рядом эмодзи (кастомные через TEAM_EMOJI_JSON).
• Выбор игроков:
  – минимум один, максимум два на команду;
  – второй добавляется, если ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 перехватов/блок-шотов;
  – спец: если играл Егор Дёмин (BKN) или Влад Голдин (MIA) — показываем его с 3 макс. метриками (жирным).
"""

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
    from urllib3.util.retry import Retry
except Exception:
    Retry = None
from bs4 import BeautifulSoup

# ---------------- ENV ----------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()  # для обычного режима (рассылка)
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # {"BOS":"<custom_emoji_id>", ...}
NBA_LISTEN = os.getenv("NBA_LISTEN", "").strip()

DEBUG = os.getenv("DEBUG_NBA", "").strip()


# ---------------- HTTP ----------------
HTTP_TIMEOUT = 10

def _mk_adapter():
    if Retry is not None:
        r = Retry(
            total=3, connect=3, read=3, backoff_factor=0.4,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        return HTTPAdapter(max_retries=r)
    return HTTPAdapter(max_retries=2)

def make_session():
    s = requests.Session()
    ad = _mk_adapter()
    s.mount("https://", ad)
    s.mount("http://", ad)
    # Без non-ASCII в UA (иначе у некоторых окружений codec падает)
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/4.8 (sports.ru+espn; spoilers; listen)",
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
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception:
        return {}

def _get_text(url: str) -> str:
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return ""
        return r.text
    except Exception:
        return ""


# ---------------- DATES ----------------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100
    n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def pick_et_report_day(now_et: datetime | None = None) -> date:
    now = now_et or datetime.now(ZoneInfo("America/New_York"))
    return now.date() if now.hour >= 8 else (now.date() - timedelta(days=1))

def et_last_three_days() -> list[date]:
    base = pick_et_report_day()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

def parse_ymd(s: str) -> date | None:
    try:
        y,m,d = map(int, s.split("-"))
        return date(y,m,d)
    except Exception:
        return None


# ---------------- TEAMS / EMOJI ----------------
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
ABBR_TO_RU = {v:k for k,v in TEAM_RU_TO_ABBR.items()}

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
                return {k.upper(): str(v) for k,v in d.items()}
        except Exception:
            pass
    return TEAM_EMOJI_DEFAULT

TEAM_EMOJI = load_team_emojis()
def emoji(abbr: str) -> str:
    return TEAM_EMOJI.get((abbr or "").upper(), "🏀")


# ---------------- SPORTS.RU (day + boxscores RU) ----------------
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def _normalize_match_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def _soup(url: str):
    try:
        html = _get_text(url)
        if not html:
            return None
        return BeautifulSoup(html, "html.parser")
    except Exception:
        return None

def collect_day_links(d: date) -> list[str]:
    soup = _soup(day_url(d))
    if not soup:
        return []
    seen=set()
    out=[]
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href:
            continue
        full = _normalize_match_url(href)
        if full in seen:
            continue
        seen.add(full)
        out.append(full)
    return out

def _canonical_ru_team(raw: str) -> str | None:
    if not raw:
        return None
    t = raw.replace("«","").replace("»","").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t:
            return k
    return None

def parse_sports_match(url: str) -> dict | None:
    soup = _soup(url)
    if not soup:
        return None

    # TRY exact score (итог) — ищем «завершен» или предфинальный блок с суммой четвертей
    text = soup.get_text("\n", strip=True)
    # Попытка: блок суммарного счёта «NNN : MMM» непосредственно перед/рядом с «завершен»
    score_final = None
    for m in re.finditer(r"(\d+)\s*:\s*(\d+)", text):
        tail = text[m.end(): m.end()+60].lower()
        if "заверш" in tail or "завершён" in tail or "завершен" in tail:
            try:
                score_final = (int(m.group(1)), int(m.group(2)))
                break
            except Exception:
                pass
    # Фоллбек: сумма по четвертям — ищем 4 (или 5+) последовательностей внутри одного блока
    if not score_final:
        # вытащим все N:N подряд в одной строке
        lines = [ln.strip() for ln in text.splitlines() if ":" in ln]
        for ln in lines:
            nums = re.findall(r"(\d+)\s*:\s*(\d+)", ln)
            if len(nums) >= 4:
                a = sum(int(x[0]) for x in nums[:4])
                b = sum(int(x[1]) for x in nums[:4])
                # возможны OT — если есть ещё пары, добавляем их
                if len(nums) > 4:
                    for extra in nums[4:]:
                        a += int(extra[0]); b += int(extra[1])
                score_final = (a, b)
                break
    if not score_final:
        return None

    scoreA, scoreB = score_final

    # Команды — из og:title
    meta = soup.find("meta", attrs={"property":"og:title"})
    title = meta.get("content") if meta and meta.get("content") else (soup.title.string if soup.title else "")
    teamA = teamB = None
    if title and "—" in title:
        left, right = [x.strip() for x in title.split("—", 1)]
        teamA = _canonical_ru_team(left)
        teamB = _canonical_ru_team(right)
    if not (teamA and teamB) or teamA == teamB:
        return None
    a_abbr = TEAM_RU_TO_ABBR.get(teamA, "")
    b_abbr = TEAM_RU_TO_ABBR.get(teamB, "")
    if not a_abbr or not b_abbr:
        return None

    # Игроки — таблицы «Статистика игроков. <КОМАНДА>»
    def read_rows(team_ru_key: str) -> list[dict]:
        rows=[]
        anchor=None
        stamp = team_ru_key.lower()
        for h in soup.find_all(["h2","h3","h4"]):
            t = h.get_text(" ", strip=True)
            if "статистика игроков" in t.lower() and stamp in t.lower().split(".")[0]:
                anchor=h
                break
        if not anchor:
            return rows
        table = anchor.find_next("table")
        if not table:
            return rows
        for tr in table.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
            if not tds:
                continue
            if any(x.lower().startswith("игрок") for x in tds):
                continue
            # имя — первая «нечисловая» ячейка
            name_idx=None
            for i,cell in enumerate(tds[:3]):
                if re.search(r"[^\d/:% ]", cell):
                    name_idx=i
                    break
            if name_idx is None:
                continue
            name = tds[name_idx]
            nums = tds[name_idx+1:]
            if len(nums) < 14:
                continue
            def as_int(x: str) -> int:
                try:
                    return int(x)
                except Exception:
                    try:
                        return int(float(x))
                    except Exception:
                        return 0
            pts = as_int(nums[0])
            reb = as_int(nums[7])
            ast = as_int(nums[8])
            stl = as_int(nums[10])
            blk = as_int(nums[12])
            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        return rows

    rowsA = read_rows(teamA)
    rowsB = read_rows(teamB)

    # OT: считаем как количество четвертей сверх 4, если есть строка с разбивкой
    ot = 0
    m_line = re.search(r"(?:\d+\s*:\s*\d+\s*){4,7}", text)
    if m_line:
        pairs = re.findall(r"(\d+)\s*:\s*(\d+)", m_line.group(0))
        if len(pairs) > 4:
            ot = len(pairs) - 4

    finished = True  # дошли до финального счёта

    return {
        "teamA": {"name": teamA, "abbr": a_abbr, "emoji": emoji(a_abbr), "score": scoreA},
        "teamB": {"name": teamB, "abbr": b_abbr, "emoji": emoji(b_abbr), "score": scoreB},
        "ot": ot, "finished": finished,
        "players": {teamA: rowsA, teamB: rowsB},
        "url": url,
    }


# ---------------- ESPN site.api (pairs, records, boxscore fallback) ----------------
ESPN_SB  = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"
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
            if len(comps) != 2:
                continue
            home = next(c for c in comps if c.get("homeAway")=="home")
            away = next(c for c in comps if c.get("homeAway")=="away")
            th = (home.get("team") or {})
            ta = (away.get("team") or {})
            abbr_h = (th.get("abbreviation") or "").upper()
            abbr_a = (ta.get("abbreviation") or "").upper()
            if abbr_h == "GS": abbr_h = "GSW"
            if abbr_a == "GS": abbr_a = "GSW"

            status = (ev.get("status") or {}).get("type") or {}
            completed = bool(status.get("completed", False))
            period = int(status.get("period") or 0)
            ot = max(period - 4, 0) if completed and period>4 else 0

            def as_int(x):
                try:
                    return int(float(x))
                except Exception:
                    return 0

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
            if not e.get("completed"):
                continue
            key = frozenset([e["home"]["abbr"], e["away"]["abbr"]])
            if key in seen:
                continue
            seen[key] = e
    return seen

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
                            except Exception:
                                try: return int(float(stats[k]))
                                except Exception: pass
                    return default
                pts=iget("points","pts"); reb=iget("rebounds","reb","reboundstotal")
                ast=iget("assists","ast"); stl=iget("steals","stl"); blk=iget("blocks","blk")
                if any([pts,reb,ast,stl,blk]):
                    arr.append({"name": nm, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        merged={}
        for p in arr:
            if p["name"] not in merged:
                merged[p["name"]] = p
            else:
                m = merged[p["name"]]
                for k in ("pts","reb","ast","stl","blk"):
                    m[k] = max(m[k], p[k])
        out[tid] = list(merged.values())
    return out


# ---------------- Players selection/format ----------------
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts:
        return full or ""
    if len(parts) == 1:
        return parts[0]
    first = parts[0]
    last = parts[-1]
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

def score_key(p: dict): 
    return (p["pts"], p["reb"]+p["ast"], p["stl"]+p["blk"])

def pick_team_players(abbr: str, rows: list[dict]) -> list[tuple[dict,bool,bool]]:
    if not rows:
        return []
    rows = sorted(rows, key=score_key, reverse=True)
    special_keys = []
    if abbr=="BKN": special_keys = ["дёмин","demin"]
    if abbr=="MIA": special_keys = ["голдин","goldin"]
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
            if p["name"] == top["name"]:
                continue
            if second_ok(p):
                out.append((p, False, False)); break
    return out[:2]

def format_player_regular(p: dict, bold=False) -> str:
    name = initials_ru(p["name"])
    if bold:
        name = f"<b>{name}</b>"
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


# ---------------- Spoilers ----------------
def sp(s: str) -> str:
    return f'<span class="tg-spoiler">{s}</span>'

SEP = "–––––––––––––––––––––––"


# ---------------- Blocks ----------------
def format_score_line(name_ru: str, abbr: str, score: int, winner: bool, record: str, ot_str: str) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    if ot_str and not winner:
        score_txt += ot_str
    if record:
        score_txt += f" ({record})"
    return f"{emoji(abbr)} {name_ru}: {sp(score_txt)}"

def build_block_from_sports(info: dict, records: dict[str,str]) -> str:
    A,B = info["teamA"], info["teamB"]
    ot_str = "" if info["ot"]==0 else (" (ОТ)" if info["ot"]==1 else f" ({info['ot']} ОТ)")
    a_win = A["score"] > B["score"]
    b_win = B["score"] > A["score"]
    head = (
        f"{format_score_line(A['name'], A['abbr'], A['score'], a_win, records.get(A['abbr'], ''), '')}\n"
        f"{format_score_line(B['name'], B['abbr'], B['score'], b_win, records.get(B['abbr'], ''), ot_str)}\n\n"
    )
    rowsA = info["players"].get(A["name"], [])
    rowsB = info["players"].get(B["name"], [])
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
    name_h = ABBR_TO_RU.get(h["abbr"], h["abbr"])
    name_a = ABBR_TO_RU.get(a["abbr"], a["abbr"])
    ot_str = "" if e["ot"]==0 else (" (ОТ)" if e["ot"]==1 else f" ({e['ot']} ОТ)")
    head = (
        f"{format_score_line(name_h, h['abbr'], h['score'], h['winner'], h.get('record',''), '')}\n"
        f"{format_score_line(name_a, a['abbr'], a['score'], a['winner'], a.get('record',''), ot_str)}\n\n"
    )
    players_by_tid = fetch_espn_players(e["eventId"]) if e.get("eventId") else {}
    rowsH = players_by_tid.get(h.get("teamId",""), [])
    rowsA = players_by_tid.get(a.get("teamId",""), [])
    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(h["abbr"], rowsH)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(a["abbr"], rowsA)]
    lines=[]
    if al: lines.extend(al)
    if al and bl: lines.append("")
    if bl: lines.extend(bl)
    return head + ("\n".join(lines) if lines else "")


# ---------------- Assemble games for a given ET day ----------------
def fetch_sports_games_for_day(d_title: date) -> dict[frozenset, dict]:
    games={}
    for url in collect_day_links(d_title):
        info = parse_sports_match(url)
        if not info or not info["finished"]:
            continue
        pair = frozenset([info["teamA"]["abbr"], info["teamB"]["abbr"]])
        if pair in games:
            continue
        games[pair] = info
    return games

def candidate_days_around(d: date) -> list[date]:
    # Для ESPN подстраховки по граничным часам
    return [d - timedelta(days=1), d, d + timedelta(days=1)]

def build_post_for_et_day(d_title: date) -> str:
    # пары/W-L через ESPN по окрестным датам
    espn_by_pair = fetch_espn_events_multi(candidate_days_around(d_title))
    sports_by_pair = fetch_sports_games_for_day(d_title)

    ordered_pairs = list(espn_by_pair.keys()) or list(sports_by_pair.keys())
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
            blocks.append(build_block_from_espn(espn_by_pair[pair]))
        if i < title_count:
            blocks.append("\n" + SEP + "\n\n")
    return (title + "".join(blocks)).strip()


# ---------------- Telegram send ----------------
def tg_send(text: str, chat_id: str | int | None = None):
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")
    to = chat_id or CHAT_ID
    if not to:
        raise RuntimeError("CHAT_ID не задан (и не передан явно)")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = S.post(url, json={
        "chat_id": to,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram {r.status_code}: {r.text}")

def tg_send_menu(chat_id, days: list[date]):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    keyboard = {"inline_keyboard": [[
        {"text": f"Сегодня (ET) • {days[0]:%Y-%m-%d}", "callback_data": f"NBA_ET:{days[0]:%Y-%m-%d}"},
    ],[
        {"text": f"Вчера (ET) • {days[1]:%Y-%m-%d}", "callback_data": f"NBA_ET:{days[1]:%Y-%m-%d}"},
    ],[
        {"text": f"-2 дня (ET) • {days[2]:%Y-%m-%d}", "callback_data": f"NBA_ET:{days[2]:%Y-%m-%d}"},
    ]]}
    S.post(url, json={
        "chat_id": chat_id,
        "text": "Выбери игровой день (по ET):",
        "reply_markup": keyboard,
    }, timeout=HTTP_TIMEOUT)

def tg_answer_callback(cb_id: str, text: str = ""):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery"
    S.post(url, json={"callback_query_id": cb_id, "text": text}, timeout=HTTP_TIMEOUT)


# ---------------- Telegram long-poll listener ----------------
def run_listener():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")
    url_updates = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    offset = None
    log("[LISTEN] started")
    while True:
        try:
            params = {"timeout": 50}
            if offset is not None:
                params["offset"] = offset
            r = S.get(url_updates, params=params, timeout=HTTP_TIMEOUT+50)
            if r.status_code != 200:
                continue
            data = r.json()
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                # callback
                if "callback_query" in upd:
                    cb = upd["callback_query"]
                    cb_id = cb.get("id")
                    msg = cb.get("message") or {}
                    chat_id = (msg.get("chat") or {}).get("id")
                    data_cb = cb.get("data","")
                    if data_cb.startswith("NBA_ET:"):
                        ymd = data_cb.split(":",1)[1]
                        d = parse_ymd(ymd)
                        if d:
                            tg_answer_callback(cb_id, f"Формирую пост за {d:%Y-%m-%d} (ET)…")
                            txt = build_post_for_et_day(d)
                            tg_send(txt, chat_id)
                    continue
                # message
                msg = upd.get("message") or {}
                chat_id = (msg.get("chat") or {}).get("id")
                text = (msg.get("text") or "").strip()
                if not text:
                    continue
                if text.startswith("/start") or text.startswith("/help"):
                    days = et_last_three_days()
                    tg_send("Привет! Команда /nba покажет выбор последних дат (ET), либо /nba YYYY-MM-DD для конкретной даты.", chat_id)
                    tg_send_menu(chat_id, days)
                elif text.startswith("/nba"):
                    parts = text.split(maxsplit=1)
                    if len(parts) == 2:
                        d = parse_ymd(parts[1].strip())
                        if d:
                            tg_send(f"Готовлю пост за {d:%Y-%m-%d} (ET)…", chat_id)
                            txt = build_post_for_et_day(d)
                            tg_send(txt, chat_id)
                            continue
                    days = et_last_three_days()
                    tg_send_menu(chat_id, days)
                else:
                    # по умолчанию показываем меню выбора
                    days = et_last_three_days()
                    tg_send_menu(chat_id, days)
        except Exception:
            continue


# ---------------- MAIN ----------------
def main():
    import argparse
    parser = argparse.ArgumentParser(description="NBA Daily Results Bot")
    parser.add_argument("--listen", action="store_true", help="Запустить long-poll слушатель Telegram")
    parser.add_argument("--date", type=str, help="Явно указать ET дату YYYY-MM-DD")
    args = parser.parse_args()

    if args.listen or NBA_LISTEN == "1":
        run_listener()
        return

    # Обычный режим: отправляем один пост за выбранный «репорт-день» (ET)
    d = parse_ymd(args.date) if args.date else pick_et_report_day()
    txt = build_post_for_et_day(d)
    tg_send(txt)

if __name__ == "__main__":
    try:
        main()
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
