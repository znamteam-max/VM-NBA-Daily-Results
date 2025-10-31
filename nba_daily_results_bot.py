#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)

• Источник контента (счёт, игроки, русские фамилии): Sports.ru (match pages).
• Пары/рекорды ESPN временно отключены (мешали сегодняшним матчам).
• Формат: названия команд видны, счёт и игроки — в спойлерах. У победителя счёт жирным.
• Правила игроков:
  – 1–2 на команду; второй — если ≥20 очков ИЛИ дабл-дабл ИЛИ ≥6 STL/BLK.
  – Спец: если играл Егор Дёмин (BKN) или Влад Голдин (MIA) — включаем обязательно и показываем 3 макс метрики >0 (жирным).
• Лого: поддержка кастом-эмодзи Telegram через TEAM_EMOJI_JSON (abbr->custom_emoji_id).
  Если переменная не задана — ставим дефолтные юникод-эмодзи.
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
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # {"BOS":"<custom_emoji_id>", ...}
DEBUG = bool(os.getenv("DEBUG_NBA", "").strip())

# -------- HTTP --------
HTTP_TIMEOUT = 12

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
    # Важно: без нестандартных символов в UA (латин-1 баг в CI)
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/4.1 (sports.ru only, spoilers, custom_emoji)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
        "Connection": "close",
    })
    return s

S = make_session()
def log(*a): 
    if DEBUG: print(*a, file=sys.stderr)

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
    # Чуть позже, чтобы дождаться выгрузки бокскора на sports.ru
    return now.date() if now.hour >= 8 else (now.date() - timedelta(days=1))

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

TEAM_EMOJI_DEFAULT = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}

def load_custom_emoji():
    if not TEAM_EMOJI_JSON: return {}
    try:
        d = json.loads(TEAM_EMOJI_JSON)
        if isinstance(d, dict):
            return {k.upper(): str(v) for k,v in d.items() if v}
    except Exception:
        pass
    return {}

CUSTOM_EMOJI = load_custom_emoji()

def emoji_token(abbr: str) -> str:
    """Возвращает маркер, который затем превратим в custom_emoji entity.
       Если кастомного нет — подставим обычный юникод в tg_send()."""
    ab = (abbr or "").upper()
    return f"{{EMO:{ab}}}"

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
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href: continue
        full = _normalize_match_url(href)
        if full in seen: continue
        seen.add(full); out.append(full)
    log(f"[DBG] SPORTS LINKS {len(out)}")
    return out

def _canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    t = raw.replace("«","").replace("»","").strip()
    t = re.sub(r"\(.*?\)", "", t).strip()
    for k in TEAM_RU_TO_ABBR:
        if t.startswith(k) or k in t:
            return k
    return None

def _anchor_team_players(soup: BeautifulSoup, team_ru: str):
    """Находим заголовок 'Команда. статистика игроков' (h2/h3/h4)"""
    if not soup: return None
    stamp = team_ru.lower()
    for h in soup.find_all(["h2","h3","h4"]):
        txt = h.get_text(" ", strip=True).lower()
        if "статистика игроков" in txt and stamp in txt:
            return h
    return None

def _find_table_after(anchor):
    """После заголовка ищем настоящую таблицу: <table> или div-таблицу."""
    if not anchor: return None
    # Сначала ищем <table>
    t = anchor.find_next("table")
    if t: return t
    # Иногда верстка без <table>: ищем контейнер, где есть заголовок 'Игрок О ...'
    node = anchor
    for _ in range(12):  # ограничим поиск
        node = node.find_next()
        if not node: break
        if getattr(node, "get_text", None):
            tx = node.get_text(" ", strip=True)
            if "игрок" in tx.lower() and " о " in (" "+tx.lower()+" "):
                return node
    return None

def _header_map(cells: list[str]) -> dict:
    """Строим карту колонок (index) по шапке."""
    label_to_key = {
        "о": "pts",
        "пб": "reb",
        "ап": "ast",
        "пх": "stl",
        "бш": "blk",
    }
    mp={}
    for i, raw in enumerate(cells):
        t = raw.strip().lower().replace(" ", "")
        if t in label_to_key and label_to_key[t] not in mp:
            mp[label_to_key[t]] = i
    return mp

def _as_int(x: str) -> int:
    x = (x or "").strip().replace("\u2009"," ")
    if x == "": return 0
    # пропускаем мин:сек
    if ":" in x: return 0
    # пропускаем дроби "7/10"
    if "/" in x:
        try:
            a, b = x.split("/", 1)
            return int(a)
        except Exception:
            return 0
    # проценты "75" — это %, нам они не нужны (берём базовые колонки по карте)
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0

def _parse_players_table(node) -> list[dict]:
    """Парсинг таблицы игроков из узла (table/div)."""
    rows_out=[]
    # Соберём строки как списки ячеек
    def cells_of_tr(tr):
        tds = tr.find_all(["td","th"])
        return [td.get_text(" ", strip=True) for td in tds]

    # найдём кандидаты строк
    trs = []
    if hasattr(node, "find_all"):
        trs = node.find_all("tr")
    # Если tr нет, попробуем по роли: берём все строки-контейнеры с цифрами
    if not trs:
        ch = []
        for div in node.find_all(True, recursive=True):
            txt = div.get_text(" ", strip=True)
            if txt and any(ch.isdigit() for ch in txt):
                ch.append(div)
        # Слишком шумно — лучше выйти
        return rows_out

    # заголовок
    header = None
    for tr in trs:
        cells = cells_of_tr(tr)
        joined = " ".join(cells).strip().lower()
        if joined.startswith("игрок ") or (" игрок " in (" "+joined+" ")):
            header = cells
            break
    if not header:
        return rows_out
    col = _header_map(header)
    # Индексы обязательных колонок
    idx_pts = col.get("pts")
    idx_reb = col.get("reb")
    idx_ast = col.get("ast")
    idx_stl = col.get("stl")
    idx_blk = col.get("blk")

    # Разбираем реальные строки
    for tr in trs:
        cells = cells_of_tr(tr)
        if not cells: continue
        j = " ".join(cells).strip().lower()
        if j.startswith("игрок "):  # шапка
            continue
        # «Итого»/командная строка
        if j.startswith("итого ") or j.startswith("о –"):
            continue
        # имя — первая ячейка, где есть буквы (а не числа/дроби)
        name = None
        for c in cells[:3]:
            if re.search(r"[^\d/:%\s\-]", c):
                name = c.strip()
                break
        if not name:
            continue
        # статистика
        def safe_get(idx):
            if idx is None or idx >= len(cells): return 0
            return _as_int(cells[idx])

        pts = safe_get(idx_pts)
        reb = safe_get(idx_reb)
        ast = safe_get(idx_ast)
        stl = safe_get(idx_stl)
        blk = safe_get(idx_blk)

        if not any([pts, reb, ast, stl, blk]):
            # пустая строка игрока нам не нужна
            continue

        rows_out.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})

    return rows_out

def _extract_score_from_context(anchor_b) -> tuple[int,int]:
    """После второго заголовка (команда B) ищем первую строку 'NNN : NNN'."""
    if not anchor_b: return (0,0)
    rx = re.compile(r"\b(\d{2,3})\s*:\s*(\d{2,3})\b")
    cur = anchor_b
    for _ in range(40):  # ограничим поиск
        cur = cur.find_next(string=True)
        if not cur: break
        m = rx.search(cur)
        if m:
            return int(m.group(1)), int(m.group(2))
    return (0,0)

def parse_sports_match(url: str) -> dict | None:
    soup = _soup(url)
    if not soup: return None

    # команды из <title> / og:title
    meta = soup.find("meta", attrs={"property":"og:title"})
    title = meta.get("content") if meta and meta.get("content") else (soup.title.string if soup.title else "")
    teamA = teamB = None
    if title and " - " in title:
        left, right = [x.strip() for x in title.split(" - ", 1)]
        teamA = _canonical_ru_team(left)
        # после дефиса в заголовке часто ещё дата — отрежем по ':' если есть
        right = right.split(":")[0].strip()
        teamB = _canonical_ru_team(right)
    # запасной способ — по крупным заголовкам карточки
    if not (teamA and teamB):
        heads = []
        for h in soup.find_all(["h2","h3"]):
            t = h.get_text(" ", strip=True).strip()
            if t in TEAM_RU_TO_ABBR:
                heads.append(t)
        if len(heads) >= 2:
            teamA = teamA or heads[0]
            teamB = teamB or next((x for x in heads[1:] if x != teamA), None)
    if not (teamA and teamB) or teamA == teamB:
        return None

    a_abbr = TEAM_RU_TO_ABBR.get(teamA,""); b_abbr = TEAM_RU_TO_ABBR.get(teamB,"")
    if not a_abbr or not b_abbr: return None

    # таблицы игроков
    ancA = _anchor_team_players(soup, teamA)
    ancB = _anchor_team_players(soup, teamB)
    if not ancA or not ancB:
        log("[DBG] HEADERS not found for", teamA, teamB, url)
        return None

    tabA = _find_table_after(ancA)
    tabB = _find_table_after(ancB)
    if not tabA or not tabB:
        log("[DBG] TABLES not found right after headers:", bool(tabA), bool(tabB), url)
        return None

    rowsA = _parse_players_table(tabA)
    rowsB = _parse_players_table(tabB)
    if not (rowsA or rowsB):
        log("[DBG] PLAYERS parsed 0+0 for", url)
        return None

    # счёт из близкого контекста
    scoreA, scoreB = _extract_score_from_context(ancB)

    log(f"[DBG] OK {teamA}-{teamB} SCORE {scoreA}:{scoreB} A_rows {len(rowsA)} B_rows {len(rowsB)}")
    return {
        "teamA": {"name": teamA, "abbr": a_abbr, "score": scoreA},
        "teamB": {"name": teamB, "abbr": b_abbr, "score": scoreB},
        "players": {teamA: rowsA, teamB: rowsB},
        "url": url,
    }

# -------- Игроки/формат --------
def initials_ru(full: str) -> str:
    parts = [p for p in re.split(r"\s+", (full or "").strip()) if p]
    if not parts: return full or ""
    if len(parts) == 1: return parts[0]
    first = parts[0]; last = parts[-1]
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
    # [(player, bold, special_detail)]
    if not rows: return []
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
def format_score_line(name_ru: str, abbr: str, score: int, winner: bool) -> str:
    score_txt = f"<b>{score}</b>" if winner else f"{score}"
    return f"{emoji_token(abbr)} {name_ru}: {sp(score_txt)}"

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

# -------- Сбор матчей дня --------
def fetch_sports_games_for_day(d: date) -> list[dict]:
    out=[]
    for url in collect_day_links(d):
        info = parse_sports_match(url)
        if info:
            out.append(info)
    return out

def build_post() -> str:
    d_title = pick_report_date_london()
    games = fetch_sports_games_for_day(d_title)

    title_count = len(games)
    title = f"НБА • {ru_date(d_title)} • {title_count} {ru_plural(title_count, ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if title_count == 0:
        return title.rstrip()

    blocks=[]
    for i, g in enumerate(games, 1):
        blocks.append(build_block(g))
        if i < title_count:
            blocks.append("\n" + SEP + "\n\n")

    return (title + "".join(blocks)).strip()

# -------- Telegram (custom emoji entities) --------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")

    # Собираем entities для кастом-эмодзи
    entities=[]
    out_parts=[]
    last=0
    # Маркер {EMO:XXX}
    for m in re.finditer(r"\{EMO:([A-Z]{2,3})\}", text):
        abbr = m.group(1)
        out_parts.append(text[last:m.start()])
        start_offset = sum(len(p) for p in out_parts)
        if abbr in CUSTOM_EMOJI:
            # Вставим один символ-заглушку (его Telegram заменит на кастом)
            out_parts.append("⬤")
            entities.append({
                "type": "custom_emoji",
                "offset": start_offset,
                "length": 1,
                "custom_emoji_id": CUSTOM_EMOJI[abbr]
            })
        else:
            # Юникод-эмодзи по умолчанию
            out_parts.append(TEAM_EMOJI_DEFAULT.get(abbr, "🏀"))
        last = m.end()
    out_parts.append(text[last:])
    final_text = "".join(out_parts)

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = S.post(url, json={
        "chat_id": CHAT_ID,
        "text": final_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "entities": entities if entities else None,
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
