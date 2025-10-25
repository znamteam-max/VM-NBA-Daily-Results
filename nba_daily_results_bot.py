#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU) — Sports.ru only

• Источник: Sports.ru
  - День:   https://www.sports.ru/stat/basketball/center/end/YYYY/MM/DD.html
  - Матч:   https://www.sports.ru/basketball/match/<slug>/
  - В матче парсим:
      * названия команд (рус)
      * финальный счёт + последовательность четвертей/ОТ → пометка (ОТ/N ОТ)
      * 2 таблицы «<Команда>. статистика игроков» → русские имена и О/ПБ/АП/ПХ/БШ

• Вывод:
  - Заголовок: НБА • {дата} • {N матчей}
  - Блок на матч:
      <эмодзи> <Команда A>: <счёт A> [ (N ОТ) ]
      <эмодзи> <Команда B>: <счёт B>
      <игроки A, 1–2 строки>
      <пустая строка>
      <игроки B, 1–2 строки>
  - Игроки: минимум 1 (топ по очкам), максимум 2 (если «выдающийся»):
        выдающийся: очки ≥ 30 ИЛИ подборы ≥ 12 ИЛИ передачи ≥ 10 ИЛИ перехваты ≥ 4 ИЛИ блок-шоты ≥ 4
      * Печатаем показатели только если они «существенные»:
          подборы ≥ 5, передачи ≥ 5, перехваты ≥ 4, блок-шоты ≥ 4
      * «Огонёк» 🔥 ставим, если очки ≥ 35 или подборы ≥ 15 или передачи ≥ 12 или перехваты ≥ 5 или блок-шоты ≥ 5
      * Имя в формате «И. Фамилия» (русская фамилия с Sports.ru).
      * Спец-правило: у «Бруклин» (BKN) включаем игрока с фамилией, содержащей «Дёмин» (жирным), у «Майами» (MIA) — «Голдин».

• Кастом-эмодзи:
  - Если в окружении TELEGRAM добавлен пак got_ball_team, положите JSON в TEAM_EMOJI_JSON: {"BOS":"🟩…", ...}
  - Если переменная не задана — используем аккуратные дефолт-эмодзи.

• Переменные окружения:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""

import os, sys, re, json, time, unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Опционально: JSON с кастом-эмодзи по аббревиатурам НБА (из вашего пака)
_TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()

# ---------- HTTP ----------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/2.0 (Sports.ru-only)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()

# ---------- DATE / RU ----------
RU_MONTHS = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}
def ru_date(d: date) -> str:
    return f"{d.day} {RU_MONTHS[d.month]}"

def ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def pick_report_date() -> date:
    # Завершаем игровой день, когда в Лондоне уже утро
    now = datetime.now(ZoneInfo("Europe/London"))
    base = now.date()
    if now.hour < 11:  # до полудня берём вчерашний день
        base = base - timedelta(days=1)
    return base

def pick_candidate_days() -> list[date]:
    base = pick_report_date()
    return [base, base - timedelta(days=1), base - timedelta(days=2)]

# ---------- TEAMS / EMOJI ----------
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
# Дефолт-эмодзи (если TEAM_EMOJI_JSON не задан)
TEAM_EMOJI_FALLBACK = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}

def load_team_emoji_map() -> dict[str,str]:
    if _TEAM_EMOJI_JSON:
        try:
            m = json.loads(_TEAM_EMOJI_JSON)
            if isinstance(m, dict):
                return {k.upper(): str(v) for k, v in m.items()}
        except Exception:
            pass
    return TEAM_EMOJI_FALLBACK

TEAM_EMOJI = load_team_emoji_map()

def team_emoji_by_ru(team_ru: str) -> str:
    abbr = TEAM_RU_TO_ABBR.get((team_ru or "").strip(), "")
    return TEAM_EMOJI.get(abbr, "🏀")

# ---------- FETCH UTILS ----------
def get_html(url: str) -> BeautifulSoup | None:
    r = S.get(url, timeout=25)
    if r.status_code != 200:
        return None
    return BeautifulSoup(r.text, "lxml")

def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def collect_day_match_links(d: date) -> list[str]:
    """
    Со страницы дня собираем ссылки на все матчи (включая не НБА), затем
    уже на странице матча фильтруем «НБА» и «завершен».
    Берём именно ссылки со счётом — это устойчивый способ.
    """
    soup = get_html(day_url(d))
    if not soup: return []
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = a.get_text(" ", strip=True)
        if not href.startswith("/basketball/match/"):
            continue
        # у "ссылка-счёт" текст в формате "NNN : NNN"
        if re.search(r"\d+\s:\s\d+", txt):
            full = "https://www.sports.ru" + href if href.startswith("/") else href
            links.append(full)
    # уникализируем, сохраняя порядок
    seen = set(); out = []
    for u in links:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

# ---------- PARSE MATCH ----------
def parse_match(url: str) -> dict | None:
    """
    Возвращает:
    {
      "url": ...,
      "tour": "НБА",
      "finished": True/False,
      "teamA": {"name":"Индиана","abbr":"IND","emoji":"...","score":135},
      "teamB": {"name":"Оклахома-Сити","abbr":"OKC","emoji":"...","score":141},
      "ot": 0|1|2|...,
      "players": {
          "Индиана": [ {name, pts, reb, ast, stl, blk}, ... ],
          "Оклахома-Сити": [...]
      }
    }
    """
    soup = get_html(url)
    if not soup: return None

    page_text = soup.get_text(" ", strip=True)

    # турнир
    tour = None
    # попробуем взять по хлебным крошкам/тегам
    for a in soup.find_all("a"):
        t = (a.get_text(" ", strip=True) or "").strip()
        if t == "НБА":
            tour = t
            break

    # команды (заголовки "## <Команда>")
    h2s = [h.get_text(" ", strip=True) for h in soup.find_all(["h2","h1"])]
    team_names = [t for t in h2s if t and t not in {"Онлайн", "Видео"} and len(t) <= 40]
    # На странице обычно подряд: "<Команда A>", "<Команда B>"
    teamA = team_names[0] if len(team_names) >= 2 else None
    # Иногда первое h2 — не то, поэтому продублируем извлечение из блока с логотипами
    # но в большинстве случаев этого достаточно:
    teamB = team_names[1] if len(team_names) >= 2 else None

    # финальный счёт
    m_score = re.search(r"(\d+)\s:\s(\d+)", page_text)
    if not (teamA and teamB and m_score):
        return None

    scoreA = int(m_score.group(1))
    scoreB = int(m_score.group(2))

    # послематчевый статус
    finished = "завершен" in page_text.lower()

    # оценка кол-ва овертаймов: ищем пары "NN : NN" сразу после финального счёта
    after = page_text[m_score.end(): m_score.end()+200]
    pairs = re.findall(r"\d+\s:\s\d+", after)
    ot = max(len(pairs) - 4, 0) if pairs else 0

    # парсим 2 таблицы «<Команда>. статистика игроков»
    players_by_team: dict[str, list[dict]] = {}

    for h3 in soup.find_all(["h3","h4"]):
        title = h3.get_text(" ", strip=True).lower()
        if "статистика игроков" not in title:
            continue
        # имя команды — до точки
        raw = h3.get_text(" ", strip=True)
        team_name = raw.split(".")[0].strip()

        table = h3.find_next("table")
        if not table:
            continue
        rows = []
        for tr in table.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
            if not tds or "Игрок" in tds[0]:
                continue
            # вычищаем пустые
            tds = [x for x in tds if x]
            # ищем ячейку с именем (первая «не чисто числовая» и без «/»)
            name_idx = None
            for i, cell in enumerate(tds[:3]):  # имя находится в первых ячейках
                if re.search(r"[^\d/:% ]", cell):  # содержит буквы/кириллицу
                    name_idx = i
                    break
            if name_idx is None:
                continue
            name = tds[name_idx]

            nums = tds[name_idx+1:]
            # ожидаем минимум 14 столбцов после имени
            if len(nums) < 14:
                continue

            def as_int(x: str) -> int:
                try:
                    # «6/11», «45:07» не должны падать
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

            rows.append({
                "name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk
            })

        if rows:
            players_by_team[team_name] = rows

    def abbr_of(team_ru: str) -> str:
        return TEAM_RU_TO_ABBR.get(team_ru, "")

    return {
        "url": url,
        "tour": tour,
        "finished": finished,
        "teamA": {"name": teamA, "abbr": abbr_of(teamA), "emoji": team_emoji_by_ru(teamA), "score": scoreA},
        "teamB": {"name": teamB, "abbr": abbr_of(teamB), "emoji": team_emoji_by_ru(teamB), "score": scoreB},
        "ot": ot,
        "players": players_by_team
    }

# ---------- PICK PLAYERS / FORMAT ----------
def initials_ru(full_name: str) -> str:
    # «Имя Фамилия [мл./ст.]» → «И. Фамилия[ мл.]»
    parts = [p for p in re.split(r"\s+", (full_name or "").strip()) if p]
    if len(parts) == 0:
        return full_name or ""
    if len(parts) == 1:
        return parts[0]
    first = parts[0]
    # Фамилия — последнее слово; если оканчивается на «мл.»/«ст.», берём два последних
    suffixes = {"мл.", "ст."}
    if parts[-1].lower() in suffixes and len(parts) >= 3:
        surname = parts[-2] + " " + parts[-1]
    else:
        surname = parts[-1]
    initial = (first[0] + ".") if first else ""
    return f"{initial} {surname}"

def is_standout(p: dict) -> bool:
    return (p["pts"] >= 30) or (p["reb"] >= 12) or (p["ast"] >= 10) or (p["stl"] >= 4) or (p["blk"] >= 4)

def hot_mark(p: dict) -> str:
    if (p["pts"] >= 35) or (p["reb"] >= 15) or (p["ast"] >= 12) or (p["stl"] >= 5) or (p["blk"] >= 5):
        return " 🔥"
    return ""

def format_line_for_player(p: dict, bold: bool=False) -> str:
    # Печатаем только «значимые» статпункты
    pts = p["pts"]
    parts = [f"{pts} {ru_plural(pts, ('очко','очка','очков'))}"]
    if p["reb"] >= 5:
        parts.append(f"{p['reb']} {ru_plural(p['reb'], ('подбор','подбора','подборов'))}")
    if p["ast"] >= 5:
        parts.append(f"{p['ast']} {ru_plural(p['ast'], ('передача','передачи','передач'))}")
    if p["stl"] >= 4:
        parts.append(f"{p['stl']} {ru_plural(p['stl'], ('перехват','перехвата','перехватов'))}")
    if p["blk"] >= 4:
        parts.append(f"{p['blk']} {ru_plural(p['blk'], ('блок-шот','блок-шота','блок-шотов'))}")

    name = initials_ru(p["name"])
    if bold:
        name = f"<b>{name}</b>"
    return f"{name}: " + ", ".join(parts) + hot_mark(p)

def pick_players_for_team(team_ru: str, abbr: str, rows: list[dict]) -> list[tuple[dict,bool]]:
    """
    Выбираем 1–2 строки:
      • всегда топ по очкам
      • второй — если «выдающийся»
      • спец-правило: BKN → фамилия содержит «Дёмин», MIA → «Голдин» (жирным)
    """
    if not rows: return []
    # сортируем по очкам, затем по (REB+AST)
    rows = sorted(rows, key=lambda p: (p["pts"], p["reb"]+p["ast"]), reverse=True)
    out: list[tuple[dict,bool]] = []

    top = rows[0]
    out.append((top, False))

    # спец-игрок
    special_name = None
    if abbr == "BKN":
        special_name = "дёмин"
    elif abbr == "MIA":
        special_name = "голдин"

    special = None
    if special_name:
        for p in rows:
            if special_name in (p["name"] or "").lower():
                special = p
                break
        if special and special["name"] != top["name"]:
            out.append((special, True))

    # второй «выдающийся», если ещё не добавили двоих
    if len(out) < 2:
        for p in rows[1:]:
            if is_standout(p):
                # не дублируем
                if all(p["name"] != x[0]["name"] for x in out):
                    out.append((p, False))
                    break

    return out

# ---------- BUILD MESSAGE ----------
SEP = "–––––––––––––––––––––––"

def build_post() -> str:
    chosen_day = None
    games: list[dict] = []

    # ищем первый день с матчами НБА среди кандидатов
    for d in pick_candidate_days():
        links = collect_day_match_links(d)
        day_games = []
        for u in links:
            info = parse_match(u)
            if not info: continue
            if info["tour"] != "НБА":  # фильтр — только НБА
                continue
            if not info["finished"]:
                continue
            day_games.append(info)
        if day_games:
            chosen_day = d
            games = day_games
            break

    if not chosen_day:
        # совсем пусто — всё равно покажем базовую дату (0 матчей)
        chosen_day = pick_report_date()

    title = f"НБА • {ru_date(chosen_day)} • {len(games)} {ru_plural(len(games), ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if not games:
        return title.rstrip()

    blocks = []
    for i, g in enumerate(games, 1):
        A = g["teamA"]; B = g["teamB"]
        ot_str = ""
        if g["ot"] > 0:
            if g["ot"] == 1:
                ot_str = " (ОТ)"
            else:
                ot_str = f" ({g['ot']} ОТ)"
        head = f"{A['emoji']} {A['name']}: {A['score']}\n{B['emoji']} {B['name']}: {B['score']}{ot_str}\n"

        # игроки
        lines = []
        rowsA = g["players"].get(A["name"], [])
        rowsB = g["players"].get(B["name"], [])

        for p, bold in pick_players_for_team(A["name"], A["abbr"], rowsA):
            lines.append(format_line_for_player(p, bold))
        if lines: lines.append("")  # разделение
        for p, bold in pick_players_for_team(B["name"], B["abbr"], rowsB):
            lines.append(format_line_for_player(p, bold))

        block = head + ("\n".join(lines) if lines else "")
        blocks.append(block)

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
