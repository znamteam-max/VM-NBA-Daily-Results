#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

• Дата отчёта: ровно 1 день по восточному времени США (ET).
  - Если в ET сейчас < 08:00 → берём вчерашнюю дату; иначе — сегодняшнюю.
• Каркас матчей и рекорды (W-L): ESPN (за выбранный день, только завершённые матчи).
• Счёт и русские названия команд: Sports.ru приоритетно (если матч найден); иначе — ESPN.
• Игроки: Sports.ru (парсим таблицу «статистика игроков»). Если её нет — фоллбек ESPN boxscore.
• Эмодзи:
  - По умолчанию — юникод-эмодзи.
  - Можно передать TEAM_EMOJI_JSON={"BOS":"5328...","NYK":"🗽"} — если значение похоже на числовой ID, рендерится как <tg-emoji emoji-id="...">🏀</tg-emoji>.
• Отображение:
  - Названия+эмодзи видны, счёт и игроки — внутри спойлеров.
  - Счёт победителя — полужирным; у проигравшего добавляется пометка (ОТ)/(N ОТ).
  - После счёта в скобках — сезонный W-L из ESPN.
  - После блока счёта — пустая строка; внутри блока игроков пустая строка между командами.
• Игроки:
  - Для каждой команды 1–2 игрока.
  - Всегда берём лучшего по очкам.
  - Второго добавляем, если: ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 перехватов/блок-шотов.
  - Егор Дёмин (BKN) и Влад Голдин (MIA) — включаются при наличии; имя жирным, выводим 3 максимальных показателя (>0).
• Зависимости: requests, beautifulsoup4
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
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()

HTTP_TIMEOUT = 9

# -------- HTTP --------
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
        "User-Agent": "NBA-DailyResultsBot/4.0 (espn-day, sportsru-stats, custom-emoji, spoilers)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Connection": "close",
    })
    return s

S = make_session()
def log(*a): print(*a, file=sys.stderr)

def _get_json(url: str) -> dict:
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return {}
        return r.json()
    except Exception:
        return {}

# -------- DATE HELPERS --------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def report_day_et() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return now_et.date() if now_et.hour >= 8 else (now_et.date() - timedelta(days=1))

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

ABBR_FIX = {
    "WSH":"WAS","SA":"SAS","NO":"NOP","NY":"NYK","PHO":"PHX","UTAH":"UTA","GS":"GSW"
}

TEAM_EMOJI_DEFAULT = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
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

def emoji_token(abbr: str) -> str:
    # Берём кастомную карту, если нет — дефолтный юникод
    val = TEAM_EMOJI.get((abbr or "").upper()) or TEAM_EMOJI_DEFAULT.get((abbr or "").upper()) or "🏀"
    val_str = str(val)
    # Если это ID кастом-эмодзи (цифры) — рендерим тегом Telegram с запасным символом внутри
    if re.fullmatch(r"\d{8,}", val_str):
        return f'<tg-emoji emoji-id="{val_str}">🏀</tg-emoji>'
    return val_str

def normalize_abbr(abbr: str) -> str:
    a = (abbr or "").upper()
    return ABBR_FIX.get(a, a)

# -------- SPORTS.RU --------
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
    return out

def _canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    t = raw.replace("«","").replace("»","").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t: return k
    return None

def parse_sports_match(url: str) -> dict | None:
    soup = _soup(url)
    if not soup:
        return None

    # --- счёт + признак ОТ ---
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d+)\s*:\s*(\d+)", text)
    if not m:
        return None
    scoreA, scoreB = int(m.group(1)), int(m.group(2))
    tail = text[m.end():m.end()+240]
    extra_pairs = re.findall(r"\d+\s*:\s*\d+", tail)
    ot = max(len(extra_pairs) - 4, 0) if extra_pairs else 0

    # --- команды ---
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
            t = h.get_text(" ", strip=True)
            if "статистика игроков" in t.lower():
                k = _canonical_ru_team(t.split(".")[0])
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

    # --- извлечение таблиц игроков Sports.ru с учётом шапки ---
    def read_rows_for(team_ru_key: str) -> list[dict]:
        anchor = None
        want = team_ru_key.lower()
        for h in soup.find_all(["h2", "h3", "h4"]):
            t = h.get_text(" ", strip=True).lower()
            if "статистика игроков" in t and want in t:
                anchor = h
                break
        if not anchor:
            return []

        table = anchor.find_next("table")
        if not table:
            return []

        # найдём строку-шапку
        header_tr = None
        for tr in table.find_all("tr"):
            if tr.find("th"):
                header_tr = tr
                break
        if not header_tr:
            return []

        header_cells = [th.get_text(" ", strip=True) for th in header_tr.find_all("th")]
        # индекс колонки имени
        name_idx = None
        for i, htxt in enumerate(header_cells[:4]):
            if re.search(r"игрок|фамилия|player", htxt, flags=re.I):
                name_idx = i
                break
        if name_idx is None:
            name_idx = 0

        # карта индексов по заголовкам
        def find_col(keys: list[str]) -> int | None:
            for i, htxt in enumerate(header_cells):
                ht = htxt.replace(" ", "").upper()
                for k in keys:
                    if k in ht:
                        return i
            return None

        idx_pts = find_col(["О", "ОЧК", "PTS"])
        idx_reb = find_col(["ПБ", "ПОДБ", "REB"])
        idx_ast = find_col(["АП", "ПЕРЕД", "AST"])
        idx_stl = find_col(["ПХ", "ПЕРЕХ", "СТИЛ", "STL"])
        idx_blk = find_col(["БШ", "БЛОК", "BLK"])

        fallback_after = True if None in (idx_pts, idx_reb, idx_ast, idx_stl, idx_blk) else False

        rows = []
        body_trs = []
        take = False
        for tr in table.find_all("tr"):
            if tr is header_tr:
                take = True
                continue
            if take:
                body_trs.append(tr)

        for tr in body_trs:
            # имя из ссылки
            a = tr.find("a", href=True)
            if not a:
                continue
            name = a.get_text(" ", strip=True)
            if not name:
                continue

            tds = tr.find_all("td")
            if not tds:
                continue

            def as_int(x: str) -> int:
                try:
                    return int(x)
                except Exception:
                    try:
                        return int(float(x))
                    except Exception:
                        return 0

            if fallback_after:
                # найдём позицию имени и считываем «фиксированные» смещения
                name_td = a.find_parent("td")
                name_pos = 0
                for i, td in enumerate(tds):
                    if td is name_td:
                        name_pos = i
                        break
                nums = [td.get_text(" ", strip=True) for td in tds[name_pos + 1:]]
                if len(nums) < 13:
                    continue
                pts = as_int(nums[0])
                reb = as_int(nums[7])
                ast = as_int(nums[8])
                stl = as_int(nums[10])
                blk = as_int(nums[12])
            else:
                cells = [td.get_text(" ", strip=True) for td in tds]
                def cell(i): return cells[i] if i is not None and i < len(cells) else "0"
                pts = as_int(cell(idx_pts))
                reb = as_int(cell(idx_reb))
                ast = as_int(cell(idx_ast))
                stl = as_int(cell(idx_stl))
                blk = as_int(cell(idx_blk))

            if not any([pts, reb, ast, stl, blk]):
                continue

            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})

        return rows

    rowsA = read_rows_for(teamA)
    rowsB = read_rows_for(teamB)
    finished = bool(rowsA or rowsB)

    return {
        "teamA": {"name": teamA, "abbr": a_abbr, "score": scoreA},
        "teamB": {"name": teamB, "abbr": b_abbr, "score": scoreB},
        "ot": ot,
        "finished": finished,
        "players": {teamA: rowsA, teamB: rowsB},
        "url": _normalize_match_url(url),
    }

# -------- ESPN (один день ET) --------
ESPN_SB  = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"
ESPN_BOX = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eid}"

def _espn_record(c: dict) -> str:
    for r in c.get("records") or []:
        if r.get("type") == "total" and r.get("summary"): return r["summary"]
    return ""

def fetch_espn_completed_for_day(d: date) -> dict[frozenset, dict]:
    """ESPN: только завершённые матчи за выбранную дату ET."""
    j = _get_json(ESPN_SB.format(ymd=d.strftime("%Y%m%d")))
    out={}
    for ev in (j.get("events") or []):
        try:
            status = (ev.get("status") or {}).get("type") or {}
            if not bool(status.get("completed", False)):
                continue
            comp = (ev.get("competitions") or [None])[0] or {}
            comps = comp.get("competitors") or []
            if len(comps) != 2: continue
            home = next(c for c in comps if c.get("homeAway")=="home")
            away = next(c for c in comps if c.get("homeAway")=="away")
            th=(home.get("team") or {}); ta=(away.get("team") or {})
            abbr_h = normalize_abbr(th.get("abbreviation") or "")
            abbr_a = normalize_abbr(ta.get("abbreviation") or "")
            def as_int(x):
                try: return int(float(x))
                except: return 0
            period = int(status.get("period") or 0)
            ot = max(period-4, 0) if period>4 else 0
            e = {
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
                "ot": ot,
            }
            key = frozenset([abbr_h, abbr_a])
            if key not in out: out[key] = e
        except Exception:
            continue
    return out

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

# -------- Игроки / формат --------
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts: return full or ""
    if len(parts) == 1: return parts[0]
    first = parts[0]; last = parts[-1]
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
    # спец: Дёмин / Голдин
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

# -------- Формирование блоков --------
def format_score_line(name_ru: str, abbr: str, score: int, winner: bool, record: str, ot_str: str) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    if ot_str and not winner: score_txt += ot_str
    if record: score_txt += f" ({record})"
    return f"{emoji_token(abbr)} {name_ru}: {sp(score_txt)}"

def merge_players_for_sports_block(s_info: dict, e: dict) -> tuple[list, list]:
    """Возвращает (rowsA, rowsB) — сперва из Sports.ru; если пусто — достаём из ESPN boxscore."""
    teamA = s_info["teamA"]; teamB = s_info["teamB"]
    rowsA = s_info["players"].get(teamA["name"], [])[:]
    rowsB = s_info["players"].get(teamB["name"], [])[:]
    if (rowsA and rowsB) or not e or not e.get("eventId"):
        return rowsA, rowsB
    # фоллбек к ESPN
    players_by_tid = fetch_espn_players(e["eventId"])
    # сопоставим abbr → teamId
    h, a = e["home"], e["away"]
    if normalize_abbr(teamA["abbr"]) == normalize_abbr(h["abbr"]):
        rowsA = rowsA or players_by_tid.get(h["teamId"], [])
    elif normalize_abbr(teamA["abbr"]) == normalize_abbr(a["abbr"]):
        rowsA = rowsA or players_by_tid.get(a["teamId"], [])
    if normalize_abbr(teamB["abbr"]) == normalize_abbr(h["abbr"]):
        rowsB = rowsB or players_by_tid.get(h["teamId"], [])
    elif normalize_abbr(teamB["abbr"]) == normalize_abbr(a["abbr"]):
        rowsB = rowsB or players_by_tid.get(a["teamId"], [])
    return rowsA, rowsB

def build_block_from_sports(info: dict, records: dict[str,str], e_for_players: dict | None) -> str:
    A,B = info["teamA"], info["teamB"]
    ot_str = "" if info["ot"]==0 else (" (ОТ)" if info["ot"]==1 else f" ({info['ot']} ОТ)")
    a_win = A["score"] > B["score"]; b_win = B["score"] > A["score"]

    line1 = format_score_line(A['name'], A['abbr'], A['score'], a_win, records.get(A['abbr'], ""), "")
    line2 = format_score_line(B['name'], B['abbr'], B['score'], b_win, records.get(B['abbr'], ""), ot_str)
    head = f"{line1}\n{line2}\n\n"

    rowsA, rowsB = merge_players_for_sports_block(info, e_for_players or {})

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

    line1 = format_score_line(name_h, h['abbr'], h['score'], h['winner'], h.get('record',''), "")
    line2 = format_score_line(name_a, a['abbr'], a['score'], a['winner'], a.get('record',''), ot_str)
    head = f"{line1}\n{line2}\n\n"

    players_by_tid = fetch_espn_players(e["eventId"]) if e.get("eventId") else {}
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
def fetch_sports_games_for_day(d: date) -> dict[frozenset, dict]:
    games={}
    for url in collect_day_links(d):
        info = parse_sports_match(url)
        if not info or not info["finished"]:
            continue
        pair = frozenset([info["teamA"]["abbr"], info["teamB"]["abbr"]])
        if pair in games: continue
        games[pair] = info
    return games

# -------- Пост --------
def build_post() -> str:
    d_et = report_day_et()
    sports_by_pair = fetch_sports_games_for_day(d_et)
    espn_by_pair   = fetch_espn_completed_for_day(d_et)

    ordered_pairs = list(espn_by_pair.keys())
    title_count = len(ordered_pairs)
    title = f"НБА • {ru_date(d_et)} • {title_count} {ru_plural(title_count, ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += "–––––––––––––––––––––––\n\n"

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
            blocks.append(build_block_from_sports(sports_by_pair[pair], rec_map, ev))
        else:
            blocks.append(build_block_from_espn(espn_by_pair[pair]))
        if i < title_count:
            blocks.append("\n" + "–––––––––––––––––––––––" + "\n\n")

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
