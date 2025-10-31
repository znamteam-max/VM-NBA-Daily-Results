#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)
Источник и русские фамилии/статы: Sports.ru (match pages, boxscore tables).
Формат: названия и эмодзи видны, счёт и игроки завернуты в спойлеры. У победителя счёт жирным.
Правила игроков:
  – минимум один, максимум два на команду;
  – второй добавляется, если ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 перехватов/блок-шотов;
  – спец: если играл Егор Дёмин (BKN) или Влад Голдин (MIA) — показываем его жирным и с 3 максимальными метриками.
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

# ========= ENV / CONFIG =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # JSON: {"BOS":"🟢", ...}
DEBUG = os.getenv("DEBUG", "0") == "1"

HTTP_TIMEOUT = 9

def log(*a):
    if DEBUG:
        print(*a, file=sys.stderr)

def _mk_adapter():
    if Retry:
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
        "User-Agent": "NBA-DailyResultsBot/4.2 (sports.ru only, robust tables)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Connection": "close",
    })
    return s

S = make_session()

# ========= RU dates/plurals =========
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def report_day_london() -> date:
    now = datetime.now(ZoneInfo("Europe/London"))
    return now.date() if now.hour >= 11 else (now.date() - timedelta(days=1))

# ========= Teams / Emojis =========
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
def emoji(abbr: str) -> str: return TEAM_EMOJI.get((abbr or "").upper(), "🏀")

# ========= Sports.ru helpers =========
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def _normalize_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full); return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def _soup(url: str):
    try:
        r = S.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None

def collect_day_match_urls(d: date) -> list[str]:
    soup = _soup(day_url(d))
    if not soup: return []
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href: continue
        u = _normalize_url(href)
        if u in seen: continue
        seen.add(u); out.append(u)
    # фильтруем «очевидные» мусорные урлы без названия команд
    out = [u for u in out if re.search(r"/basketball/match/[^/]+/?$", u)]
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

# ========= Parse a match page (robust) =========
def parse_match(url: str) -> dict | None:
    soup = _soup(url)
    if not soup: return None

    # Собираем обе таблицы «<TEAM>. статистика игроков» -> (team_ru, table)
    tables = []
    for h in soup.find_all(["h2","h3","h4"]):
        txt = h.get_text(" ", strip=True)
        tl = txt.lower()
        if "статистика игроков" not in tl:
            continue
        # левее точки — название команды
        left = txt.split(".", 1)[0].strip()
        team_ru = _canonical_ru_team(left)
        if not team_ru:  # не NBA или странный заголовок
            continue
        table = h.find_next("table")
        if not table:
            continue
        tables.append((team_ru, table))

    if len(tables) < 2:
        # Встречалось, когда на странице нет таблиц (раньше времени/очищено)
        return None

    # Прочтём игроков и «итоговую строку» (там очки команды)
    def to_int(x: str) -> int:
        x = (x or "").strip()
        try: return int(x)
        except:
            m = re.match(r"^\d+", x)
            return int(m.group(0)) if m else 0

    def rows_from_table(table) -> tuple[list[dict], int]:
        players = []
        team_pts = None
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
            if not cells: 
                continue
            low = " ".join(cells).lower()

            # заголовок
            if any("игрок" in c.lower() for c in cells):
                continue

            # итоговая строка команды: обычно заканчивается временем вида 240:00
            if re.search(r"\d{1,3}:\d{2}$", cells[-1]) and re.match(r"^\d+$", cells[0]):
                if team_pts is None:
                    team_pts = to_int(cells[0])
                continue

            # игрок: найдём первую ячейку с буквами (а не цифры/проценты)
            name_idx = None
            for i, c in enumerate(cells[:4]):  # имя всегда в первых столбцах
                if re.search(r"[A-Za-zА-Яа-яЁё]", c):
                    name_idx = i; break
            if name_idx is None:
                continue

            name = cells[name_idx]
            # после имени идёт «О» (очки)
            pts = cells[name_idx+1] if len(cells) > name_idx+1 else "0"

            # индексы метрик относительно имени (по лайауту sports.ru):
            # name | O | 2-очк | % | 3-очк | % | Штр | % | ПБ | АП | Ф | ПХ | П | БШ | Мин
            get = lambda off: to_int(cells[name_idx+off]) if len(cells) > name_idx+off else 0
            player = {
                "name": name,
                "pts": to_int(pts),
                "reb": get(8),   # ПБ
                "ast": get(9),   # АП
                "stl": get(11),  # ПХ
                "blk": get(13),  # БШ
            }
            # фильтруем пустые полностью строки
            if any(player[k] for k in ("pts","reb","ast","stl","blk")):
                players.append(player)

        return players, (team_pts or 0)

    team_blocks = []
    for team_ru, table in tables:
        rows, tpts = rows_from_table(table)
        if rows or tpts:
            team_blocks.append( (team_ru, rows, tpts) )

    # Должно быть две разные NBA-команды
    if len(team_blocks) < 2:
        return None
    # Иногда порядок может быть (гости, хозяева) — нам без разницы,
    # просто возьмём первые две различные команды из мапы NBA.
    # Если вдруг обе одинаковые (не должно) — отбрасываем.
    if team_blocks[0][0] == team_blocks[1][0]:
        return None

    A_ru, A_rows, A_pts = team_blocks[0]
    B_ru, B_rows, B_pts = team_blocks[1]

    a_abbr = TEAM_RU_TO_ABBR.get(A_ru, "")
    b_abbr = TEAM_RU_TO_ABBR.get(B_ru, "")
    if not (a_abbr and b_abbr):
        return None  # не NBA

    info = {
        "teamA": {"name": A_ru, "abbr": a_abbr, "emoji": emoji(a_abbr), "score": A_pts},
        "teamB": {"name": B_ru, "abbr": b_abbr, "emoji": emoji(b_abbr), "score": B_pts},
        "players": {A_ru: A_rows, B_ru: B_rows},
        "finished": True,
        "url": url,
    }
    log(f"[DBG] SPORTS TEAMS {A_ru} {B_ru} SCORE {A_pts} {B_pts} A_rows {len(A_rows)} B_rows {len(B_rows)}")
    return info

# ========= Players pick/format =========
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts: return ""
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

    # спец-игроки
    special_keys = []
    if abbr=="BKN": special_keys = ["дёмин","demin"]
    if abbr=="MIA": special_keys = ["голдин","goldin"]
    special=None
    for p in rows:
        if any(k in (p["name"] or "").lower() for k in special_keys):
            special = p; break

    out=[]
    top = rows[0]
    if special and special["name"] == top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False)); out.append((special, True, True))
    else:
        out.append((top, False, False))

    if len(out) < 2:
        for p in rows[1:]:
            if p["name"] == top["name"]: continue
            if second_ok(p):
                out.append((p, False, False)); break
    return out[:2]

def format_player_regular(p: dict, bold=False) -> str:
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    parts = [ru_forms("pts", p["pts"])]
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

# ========= Spoiler / formatting =========
def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'
SEP = "–––––––––––––––––––––––"

def format_score_line(name_ru: str, abbr: str, score: int, winner: bool) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    return f"{emoji(abbr)} {name_ru}: {sp(score_txt)}"

def build_block(info: dict) -> str:
    A,B = info["teamA"], info["teamB"]
    a_win = A["score"] > B["score"]; b_win = B["score"] > A["score"]

    head = (
        f"{format_score_line(A['name'], A['abbr'], A['score'], a_win)}\n"
        f"{format_score_line(B['name'], B['abbr'], B['score'], b_win)}\n\n"
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

# ========= Build post (Sports.ru only) =========
def build_post() -> str:
    d = report_day_london()
    urls = collect_day_match_urls(d)

    # парсим все матчи и оставляем только NBA
    games=[]
    for u in urls:
        try:
            info = parse_match(u)
            if info and info.get("finished"):
                games.append(info)
        except Exception:
            continue

    title = f"НБА • {ru_date(d)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if not games:
        return title.rstrip()

    blocks=[]
    for i, g in enumerate(games, 1):
        blocks.append(build_block(g))
        if i < len(games):
            blocks.append("\n" + SEP + "\n\n")

    return (title + "".join(blocks)).strip()

# ========= Telegram =========
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

# ========= MAIN =========
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
