#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU) — Sports.ru primary, ESPN cross-check
Добавлены: отступы и спойлеры (скрыт счёт и статистика игроков; названия команд видны).
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
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # опционально

# ---------- HTTP ----------
def make_session():
    s = requests.Session()
    r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET","POST"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/2.5 (Sports.ru + ESPN cross-check, spoilers)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s
S = make_session()

def log(*a): print(*a, file=sys.stderr)

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
    if now.hour < 11:  # утро -> вчерашний игровой день
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

# ---------- ESPN CROSS-CHECK ----------
ESPN_SB = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates={yyyy}{mm}{dd}"
def fetch_espn_pairs(d: date) -> set[frozenset]:
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
            if abbr == "GS": abbr = "GSW"
            abbrs.append(abbr)
        if len(abbrs) == 2 and all(abbrs):
            pairs.add(frozenset(abbrs))
    return pairs

# ---------- UTILS ----------
def unique_preserve(seq):
    seen=set(); out=[]
    for x in seq:
        if x in seen: 
            continue
        seen.add(x); out.append(x)
    return out

def clean_team_label(s: str) -> str:
    s = (s or "").strip()
    s = re.split(r"—|\-|/|\|", s, maxsplit=1)[0].strip()
    s = s.replace("«","").replace("»","").replace("“","").replace("”","").replace('"',"")
    s = re.sub(r"\(.*?\)", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s)
    return s

def canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    txt = clean_team_label(raw)
    if txt in TEAM_RU_TO_ABBR: return txt
    for key in TEAM_RU_TO_ABBR.keys():
        if txt.startswith(key): return key
    for key in TEAM_RU_TO_ABBR.keys():
        if key in txt: return key
    return None

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
        if re.search(r"\d+\s:\s\d+", txt):
            full = "https://www.sports.ru" + href if href.startswith("/") else href
            links.append(full)
    return unique_preserve(links)

# ---------- TEAM EXTRACTION ----------
def _extract_teams_via_meta(soup: BeautifulSoup) -> tuple[str|None,str|None]:
    mt = soup.find("meta", attrs={"property":"og:title"})
    title = mt["content"] if mt and mt.get("content") else (soup.title.string if soup.title and soup.title.string else "")
    if not title: return (None, None)
    parts = [p.strip() for p in title.split("—")]
    if len(parts) >= 2:
        a = canonical_ru_team(parts[0]); b = canonical_ru_team(parts[1])
        return (a, b)
    return (None, None)

def _extract_teams_via_stat_headers(soup: BeautifulSoup) -> list[str]:
    found = []
    for tag in soup.find_all(["h3","h4"]):
        t = tag.get_text(" ", strip=True)
        if "статистика игроков" in t.lower():
            t0 = t.split(".")[0].strip()
            k = canonical_ru_team(t0)
            if k: found.append(k)
    return unique_preserve(found)

# ---------- PARSE MATCH ----------
def parse_match(url: str) -> dict | None:
    soup = get_html(url)
    if not soup: return None
    page_text = soup.get_text(" ", strip=True)

    m_score = re.search(r"(\d+)\s:\s(\d+)", page_text)
    if not m_score: return None
    scoreA, scoreB = int(m_score.group(1)), int(m_score.group(2))

    low = page_text.lower()
    finished = ("завершен" in low) or ("завершён" in low) or ("матч заверш" in low)

    tail = page_text[m_score.end(): m_score.end()+240]
    pairs = re.findall(r"\d+\s:\s\d+", tail)
    ot = max(len(pairs) - 4, 0) if pairs else 0

    teamA = teamB = None
    a1, b1 = _extract_teams_via_meta(soup)
    if a1 and b1: teamA, teamB = a1, b1
    if not (teamA and teamB) or teamA == teamB:
        headers = _extract_teams_via_stat_headers(soup)
        if len(headers) >= 2:
            if not teamA: teamA = headers[0]
            if not teamB or teamB == teamA:
                teamB = next((x for x in headers[1:] if x != teamA), teamB)
    if not (teamA and teamB) or teamA == teamB:
        h2s = [h.get_text(" ", strip=True) for h in soup.find_all(["h2","h1"])]
        candidates = []
        for t in h2s:
            t = t.strip()
            if not t or t in {"Онлайн","Видео"}: continue
            k = canonical_ru_team(t)
            if k: candidates.append(k)
        candidates = unique_preserve(candidates)
        if len(candidates) >= 2:
            teamA, teamB = candidates[0], candidates[1]
    if not (teamA and teamB) or teamA == teamB:
        return None

    abbrA = TEAM_RU_TO_ABBR.get(teamA,""); abbrB = TEAM_RU_TO_ABBR.get(teamB,"")
    if not abbrA or not abbrB: return None

    def take_team_rows(team_ru_key: str) -> list[dict]:
        rows: list[dict] = []
        key_low = team_ru_key.lower()
        hdr = None
        for tag in soup.find_all(["h3","h4"]):
            text = tag.get_text(" ", strip=True)
            lowtxt = text.lower()
            if "статистика игроков" in lowtxt and key_low in lowtxt.split(".")[0]:
                hdr = tag; break
        if not hdr: return rows
        table = hdr.find_next("table")
        if not table: return rows

        for tr in table.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
            if not tds: continue
            if any(x.lower().startswith("игрок") for x in tds): continue
            name_idx = None
            for i, cell in enumerate(tds[:3]):
                if re.search(r"[^\d/:% ]", cell):
                    name_idx = i; break
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

# ---------- PLAYERS / FORMAT ----------
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
    if not rows: return []
    rows = sorted(rows, key=_score_key, reverse=True)
    out: list[tuple[dict,bool,bool]] = []

    top = rows[0]
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

    if len(out) < 2:
        for p in rows[1:]:
            if p["name"] == top["name"]:
                continue
            if second_player_condition(p):
                out.append((p, False, False))
                break

    return out[:2]

# ---------- SPOILER ----------
def sp(s: str) -> str:
    # Телеграм поддерживает HTML-спойлер: <span class="tg-spoiler">...</span>
    return f'<span class="tg-spoiler">{s}</span>'

# ---------- BUILD MESSAGE ----------
SEP = "–––––––––––––––––––––––"

def build_post() -> str:
    chosen_day = None
    games = []

    for d in pick_candidate_days():
        links = collect_day_match_links(d)
        day_games = []
        for u in links:
            info = parse_match(u)
            if not info: 
                continue
            if not info["finished"]:
                continue
            if not info["teamA"]["abbr"] or not info["teamB"]["abbr"]:
                continue
            if info["teamA"]["abbr"] == info["teamB"]["abbr"]:
                continue
            day_games.append(info)
        if day_games:
            chosen_day = d
            games = day_games
            break

    if not chosen_day:
        chosen_day = pick_report_date()

    # ESPN cross-check
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

        # Счёт и (ОТ) — под спойлером; названия — видимы
        head = (
            f"{A['emoji']} {A['name']}: {sp(str(A['score']))}\n"
            f"{B['emoji']} {B['name']}: {sp(str(B['score']) + ot_str)}\n\n"  # двойной отступ до блока игроков
        )

        lines = []
        rowsA = g["players"].get(A["name"], [])
        rowsB = g["players"].get(B["name"], [])

        # Команда A — строки игроков, каждая под спойлером
        a_lines = []
        for p, bold, special_detail in pick_players_for_team(A["name"], A["abbr"], rowsA):
            txt = (
                format_player_line_special_detail(p, bold=True) if special_detail
                else format_player_line_regular(p, bold)
            )
            a_lines.append(sp(txt))

        # Пустая строка между командами внутри блока игроков
        b_lines = []
        for p, bold, special_detail in pick_players_for_team(B["name"], B["abbr"], rowsB):
            txt = (
                format_player_line_special_detail(p, bold=True) if special_detail
                else format_player_line_regular(p, bold)
            )
            b_lines.append(sp(txt))

        # удалим дубли строк
        def dedupe_strs(arr): 
            seen=set(); res=[]
            for sline in arr:
                if sline in seen: continue
                seen.add(sline); res.append(sline)
            return res
        a_lines = dedupe_strs([ln for ln in a_lines if ln.strip()])
        b_lines = dedupe_strs([ln for ln in b_lines if ln.strip()])

        if a_lines:
            lines.extend(a_lines)
        if a_lines and b_lines:
            lines.append("")  # пустая строка между командами
        if b_lines:
            lines.extend(b_lines)

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
        "parse_mode": "HTML",  # важно для <span class="tg-spoiler">
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
