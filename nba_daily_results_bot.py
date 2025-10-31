#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU)
Источник данных: Sports.ru (страницы конкретных матчей).
- День: https://www.sports.ru/stat/basketball/center/end/YYYY/MM/DD.html
- Матч: https://www.sports.ru/basketball/match/<slug>/

Что исправлено:
• Парсинг статистики игроков по текстовым блокам "### <Команда>. статистика игроков".
• Извлечение очков, подборов, передач, перехватов и блок-шотов.
• Выбор «выдающихся» игроков (min 1, max 2 на команду):
  - Всегда лучший скорер команды.
  - Плюс ещё один, если: ≥20 очков, ИЛИ дабл-дабл (по любой из пяти категорий: PTS/REB/AST/STL/BLK),
    ИЛИ ≥5 перехватов, ИЛИ ≥5 блок-шотов.
• Если играли Егор Дёмин (BKN) или Влад Голдин (MIA) — включаем обязательно и показываем 3 наибольших ненулевых показателя.
• Сообщение одним постом; счёт и все строки со статистикой спрятаны в спойлеры.
• Жирным выделяется счёт победившей команды.
• Только НБА (любой не-НБА матч со страницы дня отфильтровывается по контенту матча).

Примечание по эмодзи-логотипам:
- Чтобы использовать кастом-эмодзи (пак), в переменную окружения TEAM_EMOJI_JSON положите JSON-словарь
  вида {"MIL":"<custom_emoji_id>", "NYK":"<id>", ...}. Если не задано — будет обычный "🏀".
- В этом файле мы остаёмся на HTML-разметке (для спойлеров и <b>), поэтому логотип — обычный символ в тексте.
  В случае, если вы хотите именно кастом-эмодзи из паков с entity type=custom_emoji, нужно переводить весь рендер
  на entities без parse_mode. Это сделаю отдельным коммитом по запросу.
"""

import os, sys, re, json, time, unicodedata
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DEBUG     = bool(os.getenv("DEBUG_NBA", "").strip())

# (опционально) словарь кастом-эмодзи: {"MIL":"<id>", ...}
try:
    TEAM_EMOJI_MAP = json.loads(os.getenv("TEAM_EMOJI_JSON", "{}") or "{}")
except Exception:
    TEAM_EMOJI_MAP = {}

# ---------- DATES (America/New_York) ----------
def pick_report_date() -> date:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    # отчёт делаем по вчерашнему дню, если сейчас ещё очень рано по ET
    return (now_et.date() - timedelta(days=1)) if now_et.hour < 8 else now_et.date()

RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня",
    7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def logdbg(*a):
    if DEBUG:
        print("[DBG]", *a, file=sys.stderr)

# ---------- HTTP ----------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.7,
              status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    # user-agent только ASCII (иначе на некоторых раннерах даёт UnicodeEncodeError)
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/4.0 (sports.ru)",
        "Accept-Language": "ru,en;q=0.7",
    })
    return s

S = make_session()

# ---------- UTILS ----------
DAY_URL_TMPL = "https://www.sports.ru/stat/basketball/center/end/{y:04d}/{m:02d}/{d:02d}.html"
MATCH_PREFIX = "https://www.sports.ru/basketball/match/"

NBA_RU_NAMES = {
    # минимальный набор для надёжного «человечного» названия
    "ATL":"Атланта","BOS":"Бостон","BKN":"Бруклин","CHA":"Шарлотт","CHI":"Чикаго","CLE":"Кливленд",
    "DAL":"Даллас","DEN":"Денвер","DET":"Детройт","GSW":"Голден Стэйт","HOU":"Хьюстон","IND":"Индиана",
    "LAC":"Клипперс","LAL":"Лейкерс","MEM":"Мемфис","MIA":"Майами","MIL":"Милуоки","MIN":"Миннесота",
    "NOP":"Новый Орлеан","NYK":"Нью-Йорк","OKC":"Оклахома-Сити","ORL":"Орландо","PHI":"Филадельфия",
    "PHX":"Финикс","POR":"Портленд","SAC":"Сакраменто","SAS":"Сан-Антонио","TOR":"Торонто","UTA":"Юта","WAS":"Вашингтон",
}

# для эмодзи — по аббревиатуре
def team_emoji(abbr: str) -> str:
    if abbr in TEAM_EMOJI_MAP:
        # здесь используем обычный символ для текста; кастом-эмодзи через entities — отдельная версия
        return "🏀"  # плейсхолдер (визуально); сам кастом ID пока не встроен в текст
    return "🏀"

# попытка определить аббревиатуру из slug URL
SLUG2ABBR = {
    "atlanta-hawks":"ATL","boston-celtics":"BOS","brooklyn-nets":"BKN","charlotte-hornets":"CHA",
    "chicago-bulls":"CHI","cleveland-cavaliers":"CLE","dallas-mavericks":"DAL","denver-nuggets":"DEN",
    "detroit-pistons":"DET","golden-state-warriors":"GSW","houston-rockets":"HOU","indiana-pacers":"IND",
    "los-angeles-clippers":"LAC","los-angeles-lakers":"LAL","memphis-grizzlies":"MEM","miami-heat":"MIA",
    "milwaukee-bucks":"MIL","minnesota-timberwolves":"MIN","new-orleans-pelicans":"NOP","new-york-knicks":"NYK",
    "oklahoma-city-thunder":"OKC","orlando-magic":"ORL","philadelphia-76ers":"PHI","phoenix-suns":"PHX",
    "portland-trail-blazers":"POR","sacramento-kings":"SAC","san-antonio-spurs":"SAS","toronto-raptors":"TOR",
    "utah-jazz":"UTA","washington-wizards":"WAS",
}

def abbr_from_url(url: str, side: int) -> str | None:
    """
    side: 0 — хозяева в slug слева, 1 — гости справа.
    Пример: .../charlotte-hornets-vs-orlando-magic/  -> 0: CHA, 1: ORL
    """
    m = re.search(r"/basketball/match/([a-z0-9\-]+)-vs-([a-z0-9\-]+)/", url)
    if not m: return None
    slug = m.group(1 + side)
    return SLUG2ABBR.get(slug)

# ---------- FETCH LINKS OF THE DAY ----------
def fetch_day_links(d: date) -> list[str]:
    url = DAY_URL_TMPL.format(y=d.year, m=d.month, d=d.day)
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("SPORTS DAY URL FAIL", url, r.status_code)
        return []
    logdbg("SPORTS DAY URL", url, "OK")
    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if not h.startswith("/basketball/match/") and not h.startswith(MATCH_PREFIX):
            continue
        # отбросим «/live/», пустые даты и т.п.
        if "/live/" in h:
            continue
        full = urljoin("https://www.sports.ru", h)
        links.append(full if full.endswith("/") else full + "/")
    # dedup & только «vs» внутри slug (иначе это не матч 1х1)
    uniq = []
    seen = set()
    for u in links:
        if "vs" not in u: 
            continue
        if u not in seen:
            uniq.append(u); seen.add(u)
    logdbg("SPORTS LINKS", len(uniq))
    return uniq

# ---------- PARSE ONE MATCH (Sports.ru) ----------
TEAM_BLOCK_RE = re.compile(r"^###\s*([^\n]+)\.\s*статистика игроков\s*$", re.IGNORECASE)

def _text_lines(html: str) -> list[str]:
    # Специально не убираем квадратные спец-ссылки  — они помогают достать имя
    soup = BeautifulSoup(html, "html.parser")
    # Оставляем только видимый текст
    txt = soup.get_text("\n", strip=False)
    # нормализуем переносы
    lines = [ln.strip() for ln in txt.split("\n")]
    return [ln for ln in lines if ln is not None]

def _ensure_is_nba(lines: list[str]) -> bool:
    joined = "\n".join(lines)
    return ("НБА" in joined) or ("NBA" in joined.upper())

def _extract_team_blocks(lines: list[str]) -> list[tuple[str, int, int]]:
    """
    Находит все заголовки "### <Команда>. статистика игроков" и возвращает
    [(team_name, start_line_index, end_line_index_exclusive)] — два блока (для матча).
    """
    idxs = []
    for i, ln in enumerate(lines):
        m = TEAM_BLOCK_RE.match(ln)
        if m:
            team = m.group(1).strip()
            idxs.append((team, i))
    blocks = []
    for k in range(len(idxs)):
        team, i = idxs[k]
        j = idxs[k+1][1] if k + 1 < len(idxs) else len(lines)
        blocks.append((team, i, j))
    # В идеале на матче ровно 2 таких блока
    return blocks[:2]

PLAYER_LINK_RE = re.compile(r"^【\d+†([^】]+)】$")

def _parse_players_from_block(block_lines: list[str]) -> list[dict]:
    """
    block_lines — кусок текста между "### <Команда>. статистика игроков" и следующим "### ..."
    Возвращает: [{"name_ru": "Ф И", "pts":int,"reb":int,"ast":int,"stl":int,"blk":int}]
    """
    out = []
    i = 0
    while i < len(block_lines):
        ln = block_lines[i].strip()
        m = PLAYER_LINK_RE.match(ln)
        if m:
            name_ru = m.group(1).strip()
            # пропускаем возможные одиночные номера/мусорные строки
            j = i + 1
            while j < len(block_lines) and (not block_lines[j] or re.fullmatch(r"\d+", block_lines[j])):
                j += 1
            if j < len(block_lines):
                stat_ln = block_lines[j].strip()
                # ожидаем, что первая «ячейка» — очки, последняя — Мин
                # пример: "17 7/10 70 1/9 11 0/0 0 7 13 4 2 3 0 36:16"
                tokens = stat_ln.split()
                # нужна минимум структура: PTS ... REB AST F STL TO BLK TIME  -> >= 8 элементов + бросковые
                if len(tokens) >= 8 and re.search(r":", tokens[-1]):
                    # возьмём с конца: time, blk, to, stl, f, ast, reb
                    time_tok = tokens.pop()  # "36:16"
                    try:
                        blk = int(tokens.pop())
                        tov = int(tokens.pop())
                        stl = int(tokens.pop())
                        fou = int(tokens.pop())
                        ast = int(tokens.pop())
                        reb = int(tokens.pop())
                        pts = int(tokens[0])
                    except Exception:
                        blk = stl = ast = reb = pts = 0
                    out.append({
                        "name_ru": name_ru,
                        "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk
                    })
                    i = j  # перепрыгиваем на строку статистики
        i += 1
    return out

def _team_total_from_block(block_lines: list[str]) -> int | None:
    """
    Ищем итоговую строку команды (обычно в самом конце секции) вида:
    "    123 29/49 ... 240:00" — берём первое целое число.
    """
    for k in range(len(block_lines)-1, -1, -1):
        ln = block_lines[k].strip()
        if re.search(r"\d+:\d{2}$", ln):
            m = re.match(r"^\s*(\d+)\b", ln)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None
    return None

def parse_sports_match(url: str) -> dict | None:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        logdbg("PARSE START", url, "HTTP", r.status_code)
        return None
    lines = _text_lines(r.text)
    if not _ensure_is_nba(lines):
        # не НБА
        return None

    blocks = _extract_team_blocks(lines)
    if len(blocks) < 2:
        logdbg("PARSE START", url, "NO TEAM BLOCKS")
        return None

    # Два блока по порядку — это и есть команды матча
    teamA, iA, jA = blocks[0]
    teamB, iB, jB = blocks[1]

    # Вырезаем «содержимое» секций (после заголовка)
    blockA = lines[iA+1:jA]
    blockB = lines[iB+1:jB]

    playersA = _parse_players_from_block(blockA)
    playersB = _parse_players_from_block(blockB)

    # Счёт берём из «итоговых» строк блоков
    scoreA = _team_total_from_block(blockA) or 0
    scoreB = _team_total_from_block(blockB) or 0

    # ОТ детектируем по количеству «периодов» в заголовке к матчу (5 пар = овертайм)
    joined = "\n".join(lines)
    # возьмём ближайшую строку "## <Команда>" .. "## <Команда>" .. затем строку "XX : YY"
    # если не найдём — помолчим об ОТ
    ot = False
    m = re.search(r"\b(\d{1,3})\s*:\s*(\d{1,3})\b.*?\n([ \d:]{5,})", joined, flags=re.S)
    if m:
        per_line = m.group(3)
        pairs = re.findall(r"\d{1,2}\s*:\s*\d{1,2}", per_line)
        ot = len(pairs) > 4

    # Аббревиатуры по URL (для эмодзи)
    abbrA = abbr_from_url(url, 0) or ""
    abbrB = abbr_from_url(url, 1) or ""

    game = {
        "url": url,
        "teams": [
            {"name_ru": teamA, "abbr": abbrA, "score": scoreA, "players": playersA},
            {"name_ru": teamB, "abbr": abbrB, "score": scoreB, "players": playersB},
        ],
        "ot": ot,
    }
    logdbg("PARSE TEAMS", teamA, teamB, "SCORES", scoreA, scoreB, "A_rows", len(playersA), "B_rows", len(playersB))
    return game

# ---------- PICKING PLAYERS ----------
def is_double_double(p: dict) -> bool:
    cats = [p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]]
    return sum(1 for v in cats if v >= 10) >= 2

def pick_players_for_team(team_abbr: str, plist: list[dict]) -> list[dict]:
    """
    Возвращает список 1..2 игроков согласно правилам.
    Спец-случаи: Дёмин (BKN), Голдин (MIA) — включаем обязательно.
    """
    if not plist:
        return []

    # отсортируем для выбора «лучшего скорера»
    plist_sorted = sorted(plist, key=lambda x: (x["pts"], x["reb"], x["ast"]), reverse=True)
    picked = []

    # специальные игроки
    special_last = None
    if team_abbr == "BKN":
        special_last = "Дёмин"
    elif team_abbr == "MIA":
        special_last = "Голдин"

    special = None
    if special_last:
        for p in plist:
            if p["name_ru"].split()[-1].strip().lower() == special_last.lower():
                special = p
                break

    # основной — лучший скорер
    best = plist_sorted[0]
    picked.append(best)

    # пометка спец-игрока (если он есть и не best)
    if special and special not in picked:
        picked.append(special)
    else:
        # второй по условиям: ≥20 очков или дабл-дабл или ≥5 STL/BLK
        for p in plist_sorted[1:]:
            if p in picked: 
                continue
            if p["pts"] >= 20 or is_double_double(p) or p["stl"] >= 5 or p["blk"] >= 5:
                picked.append(p)
                break

    # максимум 2
    return picked[:2]

def initials_plus_surname(name_ru: str) -> str:
    # "Ламело Болл" -> "Л. Болл"
    parts = [w for w in name_ru.split() if w and w != "—"]
    if not parts:
        return name_ru
    if len(parts) == 1:
        return parts[0]
    init = parts[0][0] + "."
    surname = parts[-1]
    # Некоторые «младший/старший» можно сократить, но не будем усложнять сейчас
    return f"{init} {surname}"

def fmt_player_line(p: dict, force_top3: bool = False) -> str:
    """
    Форматируем строку для игрока:
    - Всегда очки.
    - Далее добавляем те категории, которые проходят пороги:
      ≥5 подборов | ≥5 передач | ≥4 перехвата | ≥4 блок-шота.
    - Если force_top3=True (для Дёмина/Голдина) — берём 3 наибольших ненулевых категорий.
    """
    name = initials_plus_surname(p["name_ru"])
    pts, reb, ast, stl, blk = p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]

    if force_top3:
        # соберём все категории, исключая очки, и возьмём топ-3 по значению > 0
        extras = [
            ("подбор", "подбора", "подборов", reb, "подборов"),
            ("передача", "передачи", "передач", ast, "передач"),
            ("перехват", "перехвата", "перехватов", stl, "перехватов"),
            ("блок-шот", "блок-шота", "блок-шотов", blk, "блок-шотов"),
        ]
        extras = [(w1,w2,w3,val,label) for (w1,w2,w3,val,label) in extras if val and val>0]
        extras.sort(key=lambda t: t[3], reverse=True)
        extras = extras[:3]
        parts = [f"{name}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
        for (w1,w2,w3,val,label) in extras:
            parts.append(f"{val} {ru_plural(val,(w1,w2,w3))}")
        return "<span class=\"tg-spoiler\">" + ", ".join(parts) + "</span>"

    parts = [f"{name}: {pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if reb >= 5: parts.append(f"{reb} {ru_plural(reb, ('подбор','подбора','подборов'))}")
    if ast >= 5: parts.append(f"{ast} {ru_plural(ast, ('передача','передачи','передач'))}")
    if stl >= 4: parts.append(f"{stl} {ru_plural(stl, ('перехват','перехвата','перехватов'))}")
    if blk >= 4: parts.append(f"{blk} {ru_plural(blk, ('блок-шот','блок-шота','блок-шотов'))}")
    return "<span class=\"tg-spoiler\">" + ", ".join(parts) + "</span>"

# ---------- BUILD POST ----------
def build_post(d: date) -> str:
    title = f"НБА • {ru_date(d)} • {{N}} {ru_plural('{N}', ('матч','матча','матчей'))}\n" \
            "Результаты надёжно спрятаны 👇\n" \
            "–––––––––––––––––––––––\n\n"

    links = fetch_day_links(d)
    games = []
    for u in links:
        try:
            g = parse_sports_match(u)
            if g:
                # отбрасываем матчи без счёта (на всякий случай)
                if g["teams"][0]["score"] == 0 and g["teams"][1]["score"] == 0:
                    continue
                games.append(g)
        except Exception as e:
            logdbg("PARSE ERROR", u, repr(e))
            continue

    # сортировка как на Sports.ru: по времени появления на странице не гарантируется, просто оставим как есть
    n = len(games)
    out = title.replace("{N}", str(n)).replace("'{N}'", str(n))

    lines = []
    for idx, g in enumerate(games):
        t1, t2 = g["teams"][0], g["teams"][1]
        # определим победителя
        a_win = t1["score"] > t2["score"]
        b_win = t2["score"] > t1["score"]

        # эмодзи (простая версия)
        e1 = team_emoji(t1["abbr"])
        e2 = team_emoji(t2["abbr"])

        # строка со счётом (скрываем всё после двоеточия)
        scoreA = f"<b>{t1['score']}</b>" if a_win else f"{t1['score']}"
        scoreB = f"<b>{t2['score']}</b>" if b_win else f"{t2['score']}"
        ot_tag = " (ОТ)" if g["ot"] else ""

        lines.append(f"{e1} {t1['name_ru']}: <span class=\"tg-spoiler\">{scoreA}</span>")
        lines.append(f"{e2} {t2['name_ru']}: <span class=\"tg-spoiler\">{scoreB}</span>{ot_tag}")

        # пустая строка между счётом и игроками
        lines.append("")

        # игроки команды 1
        chosen1 = pick_players_for_team(t1["abbr"], t1["players"])
        for p in chosen1:
            force = False
            if t1["abbr"] == "BKN" and p["name_ru"].split()[-1] == "Дёмин":
                force = True
            if t1["abbr"] == "MIA" and p["name_ru"].split()[-1] == "Голдин":
                force = True
            lines.append(fmt_player_line(p, force_top3=force))

        # раздел между командами в блоке игроков
        if chosen1:
            lines.append("")

        # игроки команды 2
        chosen2 = pick_players_for_team(t2["abbr"], t2["players"])
        for p in chosen2:
            force = False
            if t2["abbr"] == "BKN" and p["name_ru"].split()[-1] == "Дёмин":
                force = True
            if t2["abbr"] == "MIA" and p["name_ru"].split()[-1] == "Голдин":
                force = True
            lines.append(fmt_player_line(p, force_top3=force))

        # разделитель матчей
        if idx + 1 < len(games):
            lines.append("")
            lines.append("–––––––––––––––––––––––")
            lines.append("")

    return out + "\n".join(lines).strip()

# ---------- TELEGRAM ----------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    # Спойлеры работают только в HTML-режиме
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = S.post(url, json=payload, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# ---------- MAIN ----------
if __name__ == "__main__":
    try:
        day = pick_report_date()
        logdbg("DAY", day.isoformat())
        post = build_post(day)
        tg_send(post)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
