#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU) — Sports.ru only, cross-check with ESPN

Фильтрация:
  • Матч считается НБА, только если ОБЕ команды распознаны как 30 команд НБА
    по русскому названию (словарь ниже).
  • Дополнительно, если доступен ESPN scoreboard за ту же дату, оставляем только
    те пары команд, которые есть у ESPN (официальная валидация количества матчей).

Источник основных данных: Sports.ru
  - День:   https://www.sports.ru/stat/basketball/center/end/YYYY/MM/DD.html
  - Матч:   https://www.sports.ru/basketball/match/<slug>/

Вывод:
  - Заголовок: НБА • {дата} • {N матчей}
  - На матч:
      <эмодзи> <Команда A>: <счёт A>
      <эмодзи> <Команда B>: <счёт B> [ (ОТ / N ОТ) ]
      Игроки (1–2 на команду):
        • всегда топ-скорер;
        • второй включается, если:
            — очки ≥20, ИЛИ
            — дабл-дабл (любые 2 из PTS/REB/AST/STL/BLK ≥10), ИЛИ
            — перехваты ≥6, ИЛИ блок-шоты ≥6.
      Печатаем только «значимые» статы: REB ≥5, AST ≥5, STL ≥4, BLK ≥4.
      🔥 если: PTS ≥35, REB ≥15, AST ≥12, STL ≥5, BLK ≥5.
  - Спец-правила: Дёмин (Бруклин) и Голдин (Майами) — включаем обязательно, если играли.
    У них строка — минимум 3 самых больших положительных показателя и жирное начертание.

Окружение: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, (опц.) TEAM_EMOJI_JSON
"""

import os, sys, re, json, time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # опционально: кастом-эмодзи по аббревиатурам

# ---------- HTTP ----------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/2.3 (Sports.ru + ESPN cross-check)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s
S = make_session()

# ---------- DATE / RU ----------
RU_MONTHS = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря",
}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def pick_report_date() -> date:
    now = datetime.now(ZoneInfo("Europe/London"))
    base = now.date()
    if now.hour < 11:  # до полудня считаем «вчерашний» игровой день
        base = base - timedelta(days=1)
    return base
def pick_candidate_days():
    d = pick_report_date()
    return [d, d - timedelta(days=1), d - timedelta(days=2)]

# ---------- TEAMS / EMOJI ----------
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
TEAM_EMOJI_FALLBACK = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}
def load_team_emoji_map():
    if TEAM_EMOJI_JSON:
        try:
            m = json.loads(TEAM_EMOJI_JSON)
            if isinstance(m, dict):
                return {k.upper(): str(v) for k, v in m.items()}
        except Exception:
            pass
    return TEAM_EMOJI_FALLBACK
TEAM_EMOJI = load_team_emoji_map()
def team_emoji_by_abbr(abbr: str) -> str:
    return TEAM_EMOJI.get((abbr or "").upper(), "🏀")

def canonical_ru_team(raw: str) -> str | None:
    """
    Приводим любое «Нью-Йорк Никс», «Голден Стэйт Уорриорз» и т.п. к нашему ключу
    («Нью-Йорк», «Голден Стэйт»). Ищем по префиксу/вхождению.
    """
    if not raw: return None
    txt = (raw or "").strip()
    # сначала точное совпадение
    if txt in TEAM_RU_TO_ABBR:
        return txt
    # затем — если начинается с ключа
    for key in TEAM_RU_TO_ABBR.keys():
        if txt.startswith(key):
            return key
    # затем — если ключ встречается как подстрока
    for key in TEAM_RU_TO_ABBR.keys():
        if key in txt:
            return key
    return None

# ---------- ESPN CROSS-CHECK ----------
ESPN_SB = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates={yyyy}{mm}{dd}"
def fetch_espn_pairs(d: date) -> set[frozenset]:
    """
    Возвращает множество пар команд по аббревиатурам (frozenset({'BOS','NYK'})),
    только для завершённых матчей. Если ESPN недоступен — пустое множество.
    """
    url = ESPN_SB.format(yyyy=d.year, mm=str(d.month).zfill(2), dd=str(d.day).zfill(2))
    try:
        r = S.get(url, timeout=25)
        if r.status_code != 200:
            return set()
        j = r.json()
    except Exception:
        return set()
    pairs = set()
    for ev in j.get("events") or []:
        completed = bool(((ev.get("status") or {}).get("type") or {}).get("completed", False))
        if not completed: 
            continue
        comp = (ev.get("competitions") or [None])[0] or {}
        comps = comp.get("competitors") or []
        if len(comps) != 2: 
            continue
        abbrs = []
        for c in comps:
            team = c.get("team") or {}
            abbr = (team.get("abbreviation") or "").upper()
            if abbr == "GS":  # ESPN местами даёт «GS» вместо GSW
                abbr = "GSW"
            abbrs.append(abbr)
        if len(abbrs) == 2 and all(abbrs):
            pairs.add(frozenset(abbrs))
    return pairs

# ---------- FETCH / DAY ----------
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def get_html(url: str):
    r = S.get(url, timeout=25)
    if r.status_code != 200: return None
    return BeautifulSoup(r.text, "lxml")

def collect_day_match_links(d: date) -> list[str]:
    soup = get_html(day_url(d))
    if not soup: return []
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]; txt = a.get_text(" ", strip=True)
        if not href.startswith("/basketball/match/"): 
            continue
        # берём только «ссылки-счёты»
        if re.search(r"\d+\s:\s\d+", txt):
            full = "https://www.sports.ru" + href if href.startswith("/") else href
            links.append(full)
    # уникализируем
    seen=set(); out=[]
    for u in links:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

# ---------- PARSE MATCH ----------
def parse_match(url: str) -> dict | None:
    soup = get_html(url)
    if not soup: return None

    page_text = soup.get_text(" ", strip=True)

    # команды (обычно первые два h2/h1)
    h2s = [h.get_text(" ", strip=True) for h in soup.find_all(["h2","h1"])]
    teams = [t for t in h2s if t and t not in {"Онлайн","Видео"} and len(t) <= 60]
    if len(teams) < 2:
        return None
    rawA, rawB = teams[0], teams[1]
    teamA = canonical_ru_team(rawA)
    teamB = canonical_ru_team(rawB)
    if not teamA or not teamB:
        return None  # не НБА

    abbrA = TEAM_RU_TO_ABBR.get(teamA,"")
    abbrB = TEAM_RU_TO_ABBR.get(teamB,"")

    # финальный счёт
    m_score = re.search(r"(\d+)\s:\s(\d+)", page_text)
    if not m_score: 
        return None
    scoreA, scoreB = int(m_score.group(1)), int(m_score.group(2))

    # завершён ли матч
    low = page_text.lower()
    finished = ("завершен" in low) or ("завершён" in low) or ("матч заверш" in low)

    # овертаймы — считаем пары счётов после финального
    tail = page_text[m_score.end(): m_score.end()+240]
    pairs = re.findall(r"\d+\s:\s\d+", tail)
    ot = max(len(pairs) - 4, 0) if pairs else 0

    # парсим таблицы «<Команда>. статистика игроков»
    def take_team_rows(team_ru_key: str) -> list[dict]:
        rows: list[dict] = []
        # ищем секцию, где заголовок начинается с названия команды (без точки)
        hdr = None
        key_low = team_ru_key.lower()
        for tag in soup.find_all(["h3","h4"]):
            text = tag.get_text(" ", strip=True)
            lowtxt = text.lower()
            if "статистика игроков" in lowtxt and lowtxt.startswith(key_low):
                hdr = tag; break
        if not hdr: 
            # запасной вариант: разрешим небольшие вставки между названием и точкой
            for tag in soup.find_all(["h3","h4"]):
                text = tag.get_text(" ", strip=True)
                lowtxt = text.lower()
                if "статистика игроков" in lowtxt and key_low in lowtxt[: max(5, len(key_low)+6) ]:
                    hdr = tag; break
        if not hdr: 
            return rows
        table = hdr.find_next("table")
        if not table: return rows

        for tr in table.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
            if not tds or "Игрок" in (tds[0] or ""): 
                continue
            # имя в первых 2–3 ячейках
            name_idx = None
            for i, cell in enumerate(tds[:3]):
                if re.search(r"[^\d/:% ]", cell):
                    name_idx = i; break
            if name_idx is None:
                continue
            name = tds[name_idx]
            nums = tds[name_idx+1:]
            if len(nums) < 14:
                continue

            def as_int(x: str) -> int:
                try: return int(x)
                except:
                    try: return int(float(x))
                    except: return 0

            pts = as_int(nums[0])
            reb = as_int(nums[7])
            ast = as_int(nums[8])
            stl = as_int(nums[10])
            blk = as_int(nums[12])

            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        return rows

    rowsA = take_team_rows(teamA)
    rowsB = take_team_rows(teamB)

    return {
        "teamA": {"name": teamA, "abbr": abbrA, "emoji": team_emoji_by_abbr(abbrA), "score": scoreA},
        "teamB": {"name": teamB, "abbr": abbrB, "emoji": team_emoji_by_abbr(abbrB), "score": scoreB},
        "finished": finished,
        "ot": ot,
        "players": {teamA: rowsA, teamB: rowsB},
        "url": url,
    }

# ---------- PICK / FORMAT ----------
def initials_ru(full_name: str) -> str:
    parts = [p for p in re.split(r"\s+", (full_name or "").strip()) if p]
    if not parts: return full_name or ""
    if len(parts) == 1: return parts[0]
    first = parts[0]
    suffixes = {"мл.","ст."}
    if parts[-1].lower() in suffixes and len(parts) >= 3:
        last = parts[-2] + " " + parts[-1]
    else:
        last = parts[-1]
    return f"{first[0]}. {last}"

def hot_mark(p: dict) -> str:
    if (p["pts"] >= 35) or (p["reb"] >= 15) or (p["ast"] >= 12) or (p["stl"] >= 5) or (p["blk"] >= 5):
        return " 🔥"
    return ""

def format_player_line_regular(p: dict, bold=False) -> str:
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    parts = [f"{p['pts']} {ru_plural(p['pts'], ('очко','очка','очков'))}"]
    if p["reb"] >= 5: parts.append(f"{p['reb']} {ru_plural(p['reb'], ('подбор','подбора','подборов'))}")
    if p["ast"] >= 5: parts.append(f"{p['ast']} {ru_plural(p['ast'], ('передача','передачи','передач'))}")
    if p["stl"] >= 4: parts.append(f"{p['stl']} {ru_plural(p['stl'], ('перехват','перехвата','перехватов'))}")
    if p["blk"] >= 4: parts.append(f"{p['blk']} {ru_plural(p['blk'], ('блок-шот','блок-шота','блок-шотов'))}")
    return f"{name}: " + ", ".join(parts) + hot_mark(p)

def format_player_line_special_detail(p: dict, bold=True) -> str:
    """
    Для Дёмина/Голдина: минимум 3 самых больших положительных показателя из {PTS, REB, AST, STL, BLK}.
    """
    name = initials_ru(p["name"])
    if bold: name = f"<b>{name}</b>"
    stats = [
        ("очки", p["pts"], p["pts"]),
        ("подбор", p["reb"], p["reb"]),
        ("передача", p["ast"], p["ast"]),
        ("перехват", p["stl"], p["stl"]),
        ("блок-шот", p["blk"], p["blk"]),
    ]
    stats = [(lab, val, raw) for (lab, val, raw) in stats if raw and raw > 0]
    stats.sort(key=lambda x: x[2], reverse=True)
    chosen = stats[:3] if len(stats) >= 3 else stats

    parts = []
    for lab, val, raw in chosen:
        if lab == "очки":
            parts.append(f"{val} {ru_plural(val, ('очко','очка','очков'))}")
        elif lab == "подбор":
            parts.append(f"{val} {ru_plural(val, ('подбор','подбора','подборов'))}")
        elif lab == "передача":
            parts.append(f"{val} {ru_plural(val, ('передача','передачи','передач'))}")
        elif lab == "перехват":
            parts.append(f"{val} {ru_plural(val, ('перехват','перехвата','перехватов'))}")
        elif lab == "блок-шот":
            parts.append(f"{val} {ru_plural(val, ('блок-шот','блок-шота','блок-шотов'))}")
    return f"{name}: " + ", ".join(parts) + hot_mark(p)

def _score_key(p: dict):
    return (p["pts"], p["reb"] + p["ast"], p["stl"] + p["blk"])

def _is_double_double(p: dict) -> bool:
    cats = [p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]]
    return sum(v >= 10 for v in cats) >= 2

def second_player_condition(p: dict) -> bool:
    return (p["pts"] >= 20) or _is_double_double(p) or (p["stl"] >= 6) or (p["blk"] >= 6)

def pick_players_for_team(team_ru: str, abbr: str, rows: list[dict]) -> list[tuple[dict,bool,bool]]:
    """
    Возвращает до двух игроков (player, bold, special_detail).
      • всегда топ-скорер;
      • спец-игрок (Дёмин для BKN, Голдин для MIA) — включаем обязательно, жирным и с подробной строкой;
      • второй по условию (очки ≥20 ИЛИ дабл-дабл ИЛИ STL ≥6 ИЛИ BLK ≥6).
    """
    if not rows: return []
    rows = sorted(rows, key=_score_key, reverse=True)
    out: list[tuple[dict,bool,bool]] = []

    # 1) топ
    top = rows[0]

    # 2) спец-игрок
    special_key = "дёмин" if abbr == "BKN" else ("голдин" if abbr == "MIA" else None)
    special = None
    if special_key:
        for p in rows:
            if special_key in (p["name"] or "").lower():
                special = p; break

    if special and special["name"] == top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False))
        out.append((special, True, True))
    else:
        out.append((top, False, False))

    # 3) дополним по условию (если ещё нет двух)
    if len(out) < 2:
        for p in rows[1:]:
            if p["name"] == top["name"]:
                continue
            if second_player_condition(p):
                out.append((p, False, False))
                break

    # максимум два
    return out[:2]

# ---------- BUILD MESSAGE ----------
SEP = "–––––––––––––––––––––––"

def build_post() -> str:
    chosen_day = None
    games = []

    # 1) собираем кандидатный день со Sports.ru
    for d in pick_candidate_days():
        links = collect_day_match_links(d)
        day_games = []
        for u in links:
            info = parse_match(u)
            if not info: 
                continue
            if not info["finished"]:
                continue
            # доп. фильтр: только если обе команды распознаны и есть аббревиатуры
            if not info["teamA"]["abbr"] or not info["teamB"]["abbr"]:
                continue
            day_games.append(info)
        if day_games:
            chosen_day = d
            games = day_games
            break

    if not chosen_day:
        chosen_day = pick_report_date()

    # 2) валидация количеством матчей по ESPN (если доступен)
    espn_pairs = fetch_espn_pairs(chosen_day)
    if espn_pairs:
        games = [g for g in games if frozenset({g["teamA"]["abbr"], g["teamB"]["abbr"]}) in espn_pairs]

    title = f"НБА • {ru_date(chosen_day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if not games:
        return title.rstrip()

    blocks = []
    for i, g in enumerate(games, 1):
        A, B = g["teamA"], g["teamB"]
        ot_str = "" if g["ot"] == 0 else (" (ОТ)" if g["ot"] == 1 else f" ({g['ot']} ОТ)")
        head = f"{A['emoji']} {A['name']}: {A['score']}\n{B['emoji']} {B['name']}: {B['score']}{ot_str}\n"

        lines = []
        rowsA = g["players"].get(A["name"], [])
        rowsB = g["players"].get(B["name"], [])

        for p, bold, special_detail in pick_players_for_team(A["name"], A["abbr"], rowsA):
            lines.append(
                format_player_line_special_detail(p, bold=True) if special_detail
                else format_player_line_regular(p, bold)
            )
        if lines: lines.append("")
        for p, bold, special_detail in pick_players_for_team(B["name"], B["abbr"], rowsB):
            lines.append(
                format_player_line_special_detail(p, bold=True) if special_detail
                else format_player_line_regular(p, bold)
            )

        blocks.append(head + ("\n".join(lines) if lines else ""))
        if i < len(games):
            blocks.append("\n" + SEP + "\n")

    return (title + "\n".join(blocks)).strip()

# ---------- TELEGRAM ----------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = S.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=25)
    if resp.status_code != 200:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")

# ---------- MAIN ----------
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
