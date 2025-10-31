#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

• Пары, финальный счёт, OT и рекорды (W-L): ESPN (оба эндпоинта) строго за выбранный день по ET
  с фильтрацией по реальной дате события (America/New_York).
• Игроки и русские фамилии: Sports.ru (боксскор), фоллбек — ESPN boxscore.
• Формат: названия и эмодзи видны; счёт и игроки — в спойлерах. Победитель — жирным; после счёта — (W-L).
• Выбор игроков:
  – минимум 1, максимум 2 на команду;
  – второй, если: ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 перехватов/блоков;
  – спец: если играл Егор Дёмин (BKN) или Влад Голдин (MIA), показываем всегда, жирным, 3 макс. метрики (>0).
  – показывать очки всегда; ≥5 реб/≥5 аст/≥4 стл/≥4 блк.

Новые возможности выбора дня (по ET):
  • ET_DAY_OFFSET=0|1|2 — выбрать «сегодня/вчера/позавчера».
  • PRINT_LAST3_MENU=1 — прислать интерактивное меню (кнопки) на 3 последних дня; при нажатии публикуется выбранный.
      TELEGRAM_ADMIN_ID=<id> — принимать нажатия только от указанного пользователя.
      INTERACTIVE_WAIT_SEC=75 — сколько секунд ждать нажатие; если не нажали — просто отправим меню и завершимся.
  • SEND_LAST3=1 — сразу отправить три поста подряд (ET-0, ET-1, ET-2).

ENV обязательно: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
Опционально: TEAM_EMOJI_JSON — JSON {"BOS":"<emoji>", ...} для кастом-эмодзи (например, из паков тг).
DEBUG_NBA=1 — расширенный лог.
"""

from __future__ import annotations

import os, sys, re, json, time
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

# ================== ENV / DEBUG ==================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()
DEBUG = os.getenv("DEBUG_NBA", "").strip() != ""

# Выбор дня / режимы
ET_DAY_OFFSET = int(os.getenv("ET_DAY_OFFSET", "0") or "0")  # 0..2
PRINT_LAST3_MENU = os.getenv("PRINT_LAST3_MENU", "").strip() == "1"
SEND_LAST3 = os.getenv("SEND_LAST3", "").strip() == "1"
TELEGRAM_ADMIN_ID = os.getenv("TELEGRAM_ADMIN_ID", "").strip()
INTERACTIVE_WAIT_SEC = int(os.getenv("INTERACTIVE_WAIT_SEC", "75") or "75")

def log(*a):
    if DEBUG:
        print(*a, file=sys.stderr)

# ================== HTTP ==================
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
    s.headers.update({
        "User-Agent": "NBA-DR/5.4 (espn dual endpoints; sportsru players; spoilers; last3 menu)",
        "Accept-Language": "ru-RU,ru;q=0.8,en;q=0.5",
        "Connection": "close",
    })
    return s

S = make_session()

def _get_json(url: str) -> dict:
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception:
        return {}

# ================== DATES / RU ==================
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n0 = abs(int(n)) % 100; n1 = n0 % 10
    if 11 <= n0 <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def et_today() -> date:
    now = datetime.now(ZoneInfo("America/New_York"))
    return now.date() if now.hour >= 8 else (now.date() - timedelta(days=1))

def title_date_for_et(d_et: date) -> date:
    # Для заголовка можно показывать ту же дату (по ET), чтобы не путать пользователя.
    return d_et

# ================== TEAMS / EMOJI ==================
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
ABBR_TO_RU = {v:k for k,v in TEAM_RU_TO_ABBR.items()}

ESPN_NORM = {
    "WSH":"WAS","SA":"SAS","GS":"GSW","NY":"NYK","NO":"NOP","PHO":"PHX","UTAH":"UTA",
    "NJN":"BKN","CHH":"CHA","SAN":"SAS","OKL":"OKC","WS":"WAS","GSW":"GSW"
}
def norm_abbr(a: str) -> str:
    a = (a or "").upper()
    return ESPN_NORM.get(a, a)

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
                return {str(k).upper(): str(v) for k,v in d.items()}
        except Exception:
            pass
    return TEAM_EMOJI_DEFAULT
TEAM_EMOJI = load_team_emojis()
def emoji(abbr: str) -> str: return TEAM_EMOJI.get(norm_abbr(abbr), "🏀")

# ================== SPORTS.RU (players/ru names) ==================
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def _normalize_match_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full); return urlunparse((p.scheme,p.netloc,p.path,"","",""))

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
        if not re.search(r"/basketball/match/[^/]+-vs-[^/]+/?$", full):
            continue
        if full in seen: continue
        seen.add(full); out.append(full)
    log("[DBG] SPORTS LINKS", len(out), "for", d)
    return out

def _canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    t = raw.replace("«","").replace("»","").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t:
            return k
    return None

def _players_rows_for_team(soup: BeautifulSoup, team_ru_key: str) -> list[dict]:
    rows=[]; stamp=(team_ru_key or "").lower().replace("ё","е")
    anchor=None
    for h in soup.find_all(["h2","h3","h4"]):
        t = (h.get_text(" ", strip=True) or "").lower().replace("ё","е")
        if "статистика игроков" in t and stamp in t.split(".")[0]:
            anchor=h; break
    if not anchor: return rows
    table = anchor.find_next("table")
    if not table: return rows
    for tr in table.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
        if not tds: continue
        if any(x.lower().startswith("игрок") for x in tds): continue
        name_idx=None
        for i,cell in enumerate(tds[:3]):
            if re.search(r"[^\d/:% ]", cell):
                name_idx=i; break
        if name_idx is None: continue
        name = tds[name_idx]
        nums = tds[name_idx+1:]
        if len(nums) < 14: continue
        def as_int(x: str) -> int:
            try: return int(x)
            except:
                try: return int(float(x))
                except: return 0
        pts = as_int(nums[0])
        reb = as_int(nums[7]) if len(nums)>7 else 0
        ast = as_int(nums[8]) if len(nums)>8 else 0
        stl = as_int(nums[10]) if len(nums)>10 else 0
        blk = as_int(nums[12]) if len(nums)>12 else 0
        if any([pts,reb,ast,stl,blk]):
            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
    return rows

def parse_sports_players(url: str) -> dict | None:
    soup = _soup(url)
    if not soup: return None

    meta = soup.find("meta", attrs={"property":"og:title"})
    title = meta.get("content") if meta and meta.get("content") else (soup.title.string if soup.title else "")
    teamA = teamB = None
    if title and "—" in title:
        left, right = [x.strip() for x in title.split("—", 1)]
        teamA = _canonical_ru_team(left); teamB = _canonical_ru_team(right)
    if not (teamA and teamB) or teamA == teamB:
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
    if not a_abbr or not b_abbr: return None

    rowsA = _players_rows_for_team(soup, teamA)
    rowsB = _players_rows_for_team(soup, teamB)

    return {
        "teamA": {"name": teamA, "abbr": a_abbr},
        "teamB": {"name": teamB, "abbr": b_abbr},
        "players": {teamA: rowsA, teamB: rowsB},
        "url": url,
    }

def collect_sports_players_for_pairs(pairs: set[frozenset[str]], d_et: date) -> dict[str, list[dict]]:
    # Собираем со sports.ru игроков для пар из ESPN, идя по 3 датам: ET-1, ET, ET+1 (на всякий случай).
    players: dict[str, list[dict]] = {}
    for dd in [d_et - timedelta(days=1), d_et, d_et + timedelta(days=1)]:
        for url in collect_day_links(dd):
            info = parse_sports_players(url)
            if not info: 
                continue
            a_abbr = info["teamA"]["abbr"]; b_abbr = info["teamB"]["abbr"]
            key = frozenset([a_abbr, b_abbr])
            if key not in pairs:
                continue
            rowsA = info["players"].get(info["teamA"]["name"], [])
            rowsB = info["players"].get(info["teamB"]["name"], [])
            if rowsA and a_abbr not in players: players[a_abbr] = rowsA
            if rowsB and b_abbr not in players: players[b_abbr] = rowsB
        # если закрыли все пары — прекращаем раньше
        if all(any(x in players for x in p) for p in pairs):
            break
    return players

# ================== ESPN (score/OT/W-L + fallback boxscore) ==================
ESPN_SITE_SB  = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"
ESPN_WEB_SB   = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates={ymd}"
ESPN_BOX      = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eid}"

def _espn_record(c: dict) -> str:
    for r in c.get("records") or []:
        if r.get("type") == "total" and r.get("summary"):
            return r["summary"]
    return ""

def _as_int(x):
    try: return int(float(x))
    except: return 0

def _parse_espn_events(j: dict, day_et: date) -> list[dict]:
    out=[]
    for ev in (j.get("events") or []):
        try:
            comp = (ev.get("competitions") or [None])[0] or {}
            comps = comp.get("competitors") or []
            if len(comps) != 2: 
                continue

            # дата события → ET → фильтр по дню
            dt_iso = comp.get("date") or ev.get("date")
            if not dt_iso: 
                continue
            try:
                dt_utc = datetime.fromisoformat(dt_iso.replace("Z","+00:00"))
            except Exception:
                continue
            dt_et = dt_utc.astimezone(ZoneInfo("America/New_York")).date()
            if dt_et != day_et: 
                continue

            status = (ev.get("status") or {}).get("type") or {}
            completed = bool(status.get("completed", False)) or str(status.get("state","")).lower()=="post"
            if not completed: 
                continue

            home = next(c for c in comps if c.get("homeAway")=="home")
            away = next(c for c in comps if c.get("homeAway")=="away")
            th = (home.get("team") or {})
            ta = (away.get("team") or {})
            abbr_h = norm_abbr(th.get("abbreviation",""))
            abbr_a = norm_abbr(ta.get("abbreviation",""))
            if abbr_h not in ABBR_TO_RU or abbr_a not in ABBR_TO_RU:
                continue

            try:
                period = int(status.get("period") or 0)
            except Exception:
                period = 0
            ot = max(period - 4, 0)

            h_score = _as_int(home.get("score", 0))
            a_score = _as_int(away.get("score", 0))
            h_win = bool(home.get("winner", False)) or (h_score > a_score)
            a_win = bool(away.get("winner", False)) or (a_score > h_score)

            out.append({
                "eventId": str(ev.get("id") or ""),
                "home": {
                    "abbr": abbr_h,
                    "teamId": str(th.get("id") or ""),
                    "score": h_score,
                    "winner": h_win,
                    "record": _espn_record(home),
                },
                "away": {
                    "abbr": abbr_a,
                    "teamId": str(ta.get("id") or ""),
                    "score": a_score,
                    "winner": a_win,
                    "record": _espn_record(away),
                },
                "ot": ot,
            })
        except Exception:
            continue
    return out

def fetch_espn_completed_for_day(day_et: date) -> list[dict]:
    endpoints = [ESPN_SITE_SB, ESPN_WEB_SB]
    dates = [day_et - timedelta(days=1), day_et, day_et + timedelta(days=1)]
    gathered=[]
    for d in dates:
        ymd = d.strftime("%Y%m%d")
        for ep in endpoints:
            j = _get_json(ep.format(ymd=ymd))
            parsed = _parse_espn_events(j, day_et)
            if parsed:
                log(f"[DBG] ESPN pull {ep.split('/apis/')[0].split('//')[-1]} {ymd} -> {len(parsed)} for ET {day_et}")
            gathered.extend(parsed)
    seen=set(); out=[]
    for e in gathered:
        key = (e["home"]["abbr"], e["away"]["abbr"], e["home"]["score"], e["away"]["score"])
        if key in seen: continue
        seen.add(key); out.append(e)
    log("[DBG] ESPN COMPLETED (merged)", len(out), "for", day_et)
    return out

def fetch_espn_players(event_id: str) -> dict[str, list[dict]]:
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
                for k,v in (a.get("stats") or {}).items(): stats[str(k).lower()] = v
                for k,v in (ath.get("stats") or {}).items(): stats.setdefault(str(k).lower(), v)
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
        merged={}
        for p in arr:
            if p["name"] not in merged: merged[p["name"]] = p
            else:
                m = merged[p["name"]]
                for k in ("pts","reb","ast","stl","blk"): m[k] = max(m[k], p[k])
        out[tid] = list(merged.values())
    return out

# ================== PLAYERS PICK/FORMAT ==================
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts: return full or ""
    if len(parts) == 1: return parts[0]
    first, last = parts[0], parts[-1]
    if last.lower() in {"jr.","jr","мл.","ст.","sr.","sr"} and len(parts)>=3:
        last = parts[-2] + " " + last
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
    if not rows: return []
    rows = sorted(rows, key=score_key, reverse=True)
    special_keys=[]
    if abbr=="BKN": special_keys=["дёмин","демин","demin"]
    if abbr=="MIA": special_keys=["голдин","goldin"]
    special=None
    for p in rows:
        nm = (p["name"] or "").lower().replace("ё","е")
        if any(k in nm for k in special_keys):
            special=p; break
    out=[]
    top=rows[0]
    if special and special["name"]==top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False)); out.append((special, True, True))
    else:
        out.append((top, False, False))
    if len(out)<2:
        for p in rows[1:]:
            if p["name"]==top["name"]: continue
            if second_ok(p): out.append((p, False, False)); break
    return out[:2]

def format_player_regular(p: dict, bold=False) -> str:
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    parts=[ru_forms("pts", p["pts"])]
    if p["reb"]>=5: parts.append(ru_forms("reb", p["reb"]))
    if p["ast"]>=5: parts.append(ru_forms("ast", p["ast"]))
    if p["stl"]>=4: parts.append(ru_forms("stl", p["stl"]))
    if p["blk"]>=4: parts.append(ru_forms("blk", p["blk"]))
    return f"{name}: " + ", ".join(parts) + hot_mark(p)

def format_player_special(p: dict) -> str:
    name = f"<b>{initials_ru(p['name'])}</b>"
    stats=[("pts",p["pts"]),("reb",p["reb"]),("ast",p["ast"]),("stl",p["stl"]),("blk",p["blk"])]
    stats=[(k,v) for k,v in stats if v>0]
    stats.sort(key=lambda kv: kv[1], reverse=True)
    chosen=stats[:3]
    return f"{name}: " + ", ".join(ru_forms(k,v) for k,v in chosen) + hot_mark(p)

# ================== SPOILERS / LINES ==================
def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'
SEP = "–––––––––––––––––––––––"

def format_score_line(name_ru: str, abbr: str, score: int, winner: bool, record: str, ot_str: str) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    if ot_str: score_txt += ot_str
    if record: score_txt += f" ({record})"
    return f"{emoji(abbr)} {name_ru}: {sp(score_txt)}"

def block_from_event(ev: dict, players_by_abbr: dict[str, list[dict]]) -> str:
    h, a = ev["home"], ev["away"]
    abbr_h = h["abbr"]; abbr_a = a["abbr"]
    name_h = ABBR_TO_RU.get(abbr_h, abbr_h)
    name_a = ABBR_TO_RU.get(abbr_a, abbr_a)
    ot = int(ev.get("ot",0))
    ot_str = "" if ot==0 else (" (ОТ)" if ot==1 else f" ({ot} ОТ)")

    head = (
        f"{format_score_line(name_h, abbr_h, h['score'], h['winner'], h.get('record',''), '')}\n"
        f"{format_score_line(name_a, abbr_a, a['score'], a['winner'], a.get('record',''), ot_str)}\n\n"
    )

    rowsH = players_by_abbr.get(abbr_h, [])
    rowsA = players_by_abbr.get(abbr_a, [])
    if (not rowsH or not rowsA) and ev.get("eventId"):
        espn_players = fetch_espn_players(ev["eventId"])
        rowsH = rowsH or espn_players.get(h["teamId"], [])
        rowsA = rowsA or espn_players.get(a["teamId"], [])

    al = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(abbr_h, rowsH)]
    bl = [sp(format_player_special(p) if det else format_player_regular(p, bold))
          for (p,bold,det) in pick_team_players(abbr_a, rowsA)]

    lines=[]
    if al: lines.extend(al)
    if al and bl: lines.append("")
    if bl: lines.extend(bl)
    return head + ("\n".join(lines) if lines else "")

# ================== BUILD POST ==================
def build_post_for_et_day(d_et: date) -> str:
    events = fetch_espn_completed_for_day(d_et)
    log("[DBG] DAY ESPN COUNT", len(events), "ET", d_et)

    pairs = {frozenset([e["home"]["abbr"], e["away"]["abbr"]]) for e in events}
    players_by_abbr: dict[str, list[dict]] = {}
    if events:
        players_by_abbr = collect_sports_players_for_pairs(pairs, d_et)

    def sort_key(ev: dict) -> tuple:
        return (ABBR_TO_RU.get(ev["home"]["abbr"], ev["home"]["abbr"]),
                ABBR_TO_RU.get(ev["away"]["abbr"], ev["away"]["abbr"]))
    events_sorted = sorted(events, key=sort_key)

    d_title = title_date_for_et(d_et)
    title_count = len(events_sorted)
    title = f"НБА • {ru_date(d_title)} • {title_count} {ru_plural(title_count, ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if title_count == 0:
        return title.rstrip()

    blocks=[]
    for i, ev in enumerate(events_sorted, 1):
        blocks.append(block_from_event(ev, players_by_abbr))
        if i < title_count:
            blocks.append("\n" + SEP + "\n\n")

    return (title + "".join(blocks)).strip()

# ================== TELEGRAM ==================
API = "https://api.telegram.org"

def tg_send(text: str) -> dict:
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"{API}/bot{BOT_TOKEN}/sendMessage"
    r = S.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")
    return r.json()

def tg_send_menu_last3(options: list[tuple[date,int]]) -> dict:
    # options: [(d_et, count), ...] len=3
    buttons=[]
    rows=[]
    for d, n in options:
        cap = f"{d:%d %b} (ET) — {n} {ru_plural(n, ('матч','матча','матчей'))}"
        cb  = f"NBA:DAY:{d.isoformat()}"
        rows.append([{"text": cap, "callback_data": cb}])
    kb={"inline_keyboard": rows}
    url = f"{API}/bot{BOT_TOKEN}/sendMessage"
    text = "Выберите день (по ET) для публикации:"
    r = S.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "reply_markup": kb,
    }, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")
    return r.json()

def tg_answer_callback(cb_id: str):
    url = f"{API}/bot{BOT_TOKEN}/answerCallbackQuery"
    S.post(url, json={"callback_query_id": cb_id}, timeout=HTTP_TIMEOUT)

def tg_get_updates(offset: int|None=None) -> dict:
    url = f"{API}/bot{BOT_TOKEN}/getUpdates"
    payload={"timeout": 0}
    if offset is not None: payload["offset"] = offset
    r = S.get(url, params=payload, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        return {}
    return r.json()

def tg_delete_message(chat_id: str, message_id: int):
    url = f"{API}/bot{BOT_TOKEN}/deleteMessage"
    S.post(url, json={"chat_id": chat_id, "message_id": message_id}, timeout=HTTP_TIMEOUT)

# ================== LAST3 MENU / FLOW ==================
def espn_count_for_et(d: date) -> int:
    return len(fetch_espn_completed_for_day(d))

def build_last3_options() -> list[tuple[date,int]]:
    base = et_today()
    days = [base - timedelta(days=k) for k in [0,1,2]]
    out=[]
    for d in days:
        c = espn_count_for_et(d)
        out.append((d,c))
    return out

def wait_callback_and_post(menu_msg: dict, options_set: set[str]):
    # Ждём нажатие до INTERACTIVE_WAIT_SEC
    deadline = time.time() + INTERACTIVE_WAIT_SEC
    last_update_id = None
    while time.time() < deadline:
        up = tg_get_updates(last_update_id+1 if last_update_id is not None else None)
        for upd in (up.get("result") or []):
            last_update_id = upd.get("update_id", last_update_id)
            cb = upd.get("callback_query")
            if not cb: continue
            data = cb.get("data","")
            from_user = str(((cb.get("from") or {}).get("id")) or "")
            if not data.startswith("NBA:DAY:"): continue
            if TELEGRAM_ADMIN_ID and from_user != TELEGRAM_ADMIN_ID:
                continue
            iso = data.split("NBA:DAY:",1)[1]
            if iso not in options_set:
                continue
            try:
                d = date.fromisoformat(iso)
            except Exception:
                continue
            try:
                tg_answer_callback(cb.get("id",""))
            except Exception:
                pass
            # по желанию — удалим меню
            try:
                msg = menu_msg.get("result",{}).get("message_id")
                if msg:
                    tg_delete_message(CHAT_ID, msg)
            except Exception:
                pass
            post = build_post_for_et_day(d)
            tg_send(post)
            return
        time.sleep(1)

# ================== MAIN ==================
if __name__ == "__main__":
    try:
        if PRINT_LAST3_MENU:
            opts = build_last3_options()  # [(date,count)*3]
            # Отправляем меню
            msg = tg_send_menu_last3(opts)
            # Ждём нажатие (если задан интервал >0). Иначе — просто завершаемся после отправки меню.
            if INTERACTIVE_WAIT_SEC > 0:
                options_set = {d.isoformat() for d, _ in opts}
                wait_callback_and_post(msg, options_set)
            print("OK")
            sys.exit(0)

        if SEND_LAST3:
            base = et_today()
            for delta in [0, 1, 2]:
                d = base - timedelta(days=delta)
                post = build_post_for_et_day(d)
                tg_send(post)
                time.sleep(0.5)
            print("OK")
            sys.exit(0)

        # Обычный режим: один отчёт за выбранный ET-офсет (0..2)
        base = et_today()
        off = max(0, min(2, ET_DAY_OFFSET))
        d = base - timedelta(days=off)
        post = build_post_for_et_day(d)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
