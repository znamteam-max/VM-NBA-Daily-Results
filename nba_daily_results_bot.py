#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NBA Daily Results → Telegram (RU) — Sports.ru primary, ESPN-ordered, spoilers

Главные правки:
• Собираем матчи sports.ru за окно (день−1, день, день+1) — исключает пропажу матчей из-за таймзон.
• Берём список финалов у ESPN за те же три даты и формируем финальный список строго по ESPN-порядку.
  Для каждого матча пытаемся найти sports.ru-страницу; если не нашли — используем ESPN как резерв.
• Счёт/ОТ и строки игроков — под HTML-спойлером. Видимы только названия команд и эмодзи.
"""

import os, sys, re, json, time
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

# ---------- ENV ----------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()  # можно передать JSON с кастом-эмодзи

# ---------- HTTP ----------
def _make_adapter():
    if Retry is not None:
        r = Retry(total=6, connect=6, read=6, backoff_factor=0.6,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET","POST"])
        return HTTPAdapter(max_retries=r)
    return HTTPAdapter(max_retries=3)

def make_session():
    s = requests.Session()
    ad = _make_adapter()
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({
        "User-Agent": "NBA-DailyResultsBot/2.7 (Sports.ru primary, ESPN-ordered, spoilers)",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
    })
    return s

S = make_session()
def log(*a): print(*a, file=sys.stderr)

# ---------- RU DATES ----------
RU_MONTHS = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
             7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
def ru_date(d: date) -> str: return f"{d.day} {RU_MONTHS[d.month]}"
def ru_plural(n: int, forms: tuple[str,str,str]) -> str:
    n = abs(int(n)) % 100; n1 = n % 10
    if 11 <= n <= 19: return forms[2]
    if 2 <= n1 <= 4:  return forms[1]
    if n1 == 1:      return forms[0]
    return forms[2]

def pick_report_date() -> date:
    now = datetime.now(ZoneInfo("Europe/London"))
    d = now.date()
    if now.hour < 11: d = d - timedelta(days=1)
    return d

def window_days(center: date) -> list[date]:
    return [center - timedelta(days=1), center, center + timedelta(days=1)]

# ---------- TEAMS / EMOJI ----------
TEAM_RU_TO_ABBR = {
    "Атланта":"ATL","Бостон":"BOS","Бруклин":"BKN","Шарлотт":"CHA","Чикаго":"CHI",
    "Кливленд":"CLE","Даллас":"DAL","Денвер":"DEN","Детройт":"DET","Голден Стэйт":"GSW",
    "Хьюстон":"HOU","Индиана":"IND","Клипперс":"LAC","Лейкерс":"LAL","Мемфис":"MEM",
    "Майами":"MIA","Милуоки":"MIL","Миннесота":"MIN","Новый Орлеан":"NOP","Нью-Йорк":"NYK",
    "Оклахома-Сити":"OKC","Орландо":"ORL","Филадельфия":"PHI","Финикс":"PHX","Портленд":"POR",
    "Сакраменто":"SAC","Сан-Антонио":"SAS","Торонто":"TOR","Юта":"UTA","Вашингтон":"WAS",
}
ABBR_TO_TEAM_RU = {v:k for k,v in TEAM_RU_TO_ABBR.items()}

TEAM_EMOJI_FALLBACK = {
    "ATL":"🦅","BOS":"☘️","BKN":"🕸️","CHA":"🐝","CHI":"🐂","CLE":"🛡️","DAL":"🐎","DEN":"⛏️","DET":"🔧",
    "GSW":"🗡️","HOU":"🚀","IND":"💫","LAC":"✂️","LAL":"⭐","MEM":"🐻","MIA":"🔥","MIL":"🦌","MIN":"🐺",
    "NOP":"🪶","NYK":"🗽","OKC":"⚡","ORL":"✨","PHI":"🔔","PHX":"☀️","POR":"🧭","SAC":"👑","SAS":"🪙",
    "TOR":"🦖","UTA":"🎷","WAS":"🧙",
}
def load_team_emoji_map():
    if TEAM_EMOJI_JSON:
        try:
            d = json.loads(TEAM_EMOJI_JSON)
            if isinstance(d, dict):
                return {k.upper(): str(v) for k,v in d.items()}
        except Exception:
            pass
    return TEAM_EMOJI_FALLBACK
TEAM_EMOJI = load_team_emoji_map()
def team_emoji_by_abbr(abbr: str) -> str:
    return TEAM_EMOJI.get((abbr or "").upper(), "🏀")

def canonical_ru_team(raw: str) -> str | None:
    if not raw: return None
    txt = (raw or "").strip()
    txt = re.split(r"—|\-|/|\|", txt, maxsplit=1)[0].strip()
    txt = txt.replace("«","").replace("»","").replace("“","").replace("”","").replace('"',"")
    txt = re.sub(r"\(.*?\)", "", txt).strip()
    txt = re.sub(r"\s{2,}", " ", txt)
    if txt in TEAM_RU_TO_ABBR: return txt
    for key in TEAM_RU_TO_ABBR:
        if txt.startswith(key): return key
    for key in TEAM_RU_TO_ABBR:
        if key in txt: return key
    return None

# ---------- ESPN ----------
ESPN_SB = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates={yyyy}{mm}{dd}"
ESPN_BOX = "https://site.web.api.espn.com/apis/v2/sports/basketball/nba/boxscore?event={eid}"

def fetch_espn_completed(center_day: date) -> list[dict]:
    """Финальные игры за окно day-1..day+1, в ESPN-порядке (как в ленте)."""
    out = []
    for d in window_days(center_day):
        url = ESPN_SB.format(yyyy=d.year, mm=str(d.month).zfill(2), dd=str(d.day).zfill(2))
        try:
            r = S.get(url, timeout=25)
            if r.status_code != 200: continue
            j = r.json()
        except Exception:
            continue
        for ev in j.get("events") or []:
            st = ((ev.get("status") or {}).get("type") or {})
            if not bool(st.get("completed", False)):
                continue
            comp = (ev.get("competitions") or [None])[0] or {}
            comps = comp.get("competitors") or []
            if len(comps) != 2: continue
            teams = []
            for c in comps:
                t = c.get("team") or {}
                ab = (t.get("abbreviation") or "").upper()
                if ab == "GS": ab = "GSW"
                teams.append({
                    "abbr": ab,
                    "score": int(float(c.get("score", 0))),
                    "winner": bool(c.get("winner", False)),
                    "teamId": str(t.get("id") or ""),
                })
            if len(teams) != 2: continue
            # период для ОТ (если есть)
            ot = 0
            try:
                period = int((ev.get("status") or {}).get("period") or 0)
                if period and period > 4:
                    ot = period - 4
            except Exception:
                pass
            out.append({
                "eventId": str(ev.get("id") or ""),
                "teams": teams,
                "ot": ot,
                "when": ev.get("date") or "",
            })
    # уникализируем по парам (последний встреченный оставляем)
    seen=set(); uniq=[]
    for g in out:
        pair = frozenset([g["teams"][0]["abbr"], g["teams"][1]["abbr"]])
        if pair in seen: continue
        seen.add(pair); uniq.append(g)
    return uniq

def fetch_espn_players(event_id: str) -> dict:
    """Возвращает teamId -> [ {name, pts, reb, ast, stl, blk} ]."""
    try:
        r = S.get(ESPN_BOX.format(eid=event_id), timeout=25)
        if r.status_code != 200: return {}
        j = r.json()
    except Exception:
        return {}
    out = {}
    for team_block in (j.get("players") or []):
        team = team_block.get("team") or {}
        tid = str(team.get("id") or "")
        arr = []
        for grp in (team_block.get("statistics") or []):
            for a in (grp.get("athletes") or []):
                ath = a.get("athlete") or {}
                name = (ath.get("displayName") or "").strip()
                stats_map = {}
                for k,v in (a.get("stats") or {}).items(): stats_map[k.lower()] = v
                for k,v in (ath.get("stats") or {}).items(): stats_map.setdefault(k.lower(), v)
                def iget(*keys, default=0):
                    for k in keys:
                        if k in stats_map:
                            try: return int(stats_map[k])
                            except Exception:
                                try: return int(float(stats_map[k]))
                                except Exception: pass
                    return default
                pts = iget("points","pts")
                reb = iget("rebounds","reb","totreb","reboundstotal")
                ast = iget("assists","ast")
                stl = iget("steals","stl")
                blk = iget("blocks","blk")
                if any(v for v in (pts,reb,ast,stl,blk)):
                    arr.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        # слить дубли, взять максимумы
        merged = {}
        for p in arr:
            key = p["name"]
            if key not in merged: merged[key] = p
            else:
                m = merged[key]
                for k in ("pts","reb","ast","stl","blk"):
                    m[k] = max(m[k], p[k])
        out[tid] = list(merged.values())
    return out

# ---------- SPORTS.RU ----------
def day_url(d: date) -> str:
    return f"https://www.sports.ru/stat/basketball/center/end/{d:%Y/%m/%d}.html"

def get_html(url: str):
    try:
        r = S.get(url, timeout=25)
        if r.status_code != 200: return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None

def _normalize_match_url(u: str) -> str:
    full = "https://www.sports.ru" + u if u.startswith("/") else u
    p = urlparse(full)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def collect_day_match_links(d: date) -> list[str]:
    soup = get_html(day_url(d))
    if not soup: return []
    seen=set(); out=[]
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/basketball/match/" not in href: continue
        full = _normalize_match_url(href)
        if full not in seen:
            seen.add(full); out.append(full)
    return out

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
    seen=set(); out=[]
    for x in found:
        if x in seen: continue
        seen.add(x); out.append(x)
    return out

def parse_match(url: str) -> dict | None:
    soup = get_html(url)
    if not soup: return None
    page_text = soup.get_text(" ", strip=True)
    m_score = re.search(r"(\d+)\s:\s(\d+)", page_text)
    if not m_score: return None
    scoreA, scoreB = int(m_score.group(1)), int(m_score.group(2))
    low = page_text.lower()
    finished = ("заверш" in low) or ("итог" in low)  # шире
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
        return None
    abbrA = TEAM_RU_TO_ABBR.get(teamA,""); abbrB = TEAM_RU_TO_ABBR.get(teamB,"")
    if not abbrA or not abbrB: return None

    def take_team_rows(team_ru_key: str) -> list[dict]:
        rows=[]; key_low = team_ru_key.lower()
        hdr=None
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
            pts = as_int(nums[0]); reb = as_int(nums[7]); ast = as_int(nums[8])
            stl = as_int(nums[10]); blk = as_int(nums[12])
            rows.append({"name": name, "pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk})
        return rows

    rowsA = take_team_rows(teamA); rowsB = take_team_rows(teamB)
    return {
        "teamA": {"name": teamA, "abbr": abbrA, "emoji": team_emoji_by_abbr(abbrA), "score": scoreA},
        "teamB": {"name": teamB, "abbr": abbrB, "emoji": team_emoji_by_abbr(abbrB), "score": scoreB},
        "finished": finished, "ot": ot,
        "players": {teamA: rowsA, teamB: rowsB},
        "url": url,
    }

# ---------- PLAYER FORMAT ----------
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
    stats = [("очки", p["pts"]), ("подбор", p["reb"]), ("передача", p["ast"]),
             ("перехват", p["stl"]), ("блок-шот", p["blk"])]
    stats = [(lab,val) for (lab,val) in stats if val and val > 0]
    stats.sort(key=lambda x: x[1], reverse=True)
    chosen = stats[:3]
    parts=[]
    for lab,val in chosen:
        if lab == "очки": parts.append(f"{val} {ru_plural(val, ('очко','очка','очков'))}")
        elif lab == "подбор": parts.append(f"{val} {ru_plural(val, ('подбор','подбора','подборов'))}")
        elif lab == "передача": parts.append(f"{val} {ru_plural(val, ('передача','передачи','передач'))}")
        elif lab == "перехват": parts.append(f"{val} {ru_plural(val, ('перехват','перехвата','перехватов'))}")
        elif lab == "блок-шот": parts.append(f"{val} {ru_plural(val, ('блок-шот','блок-шота','блок-шотов'))}")
    return f"{name}: " + ", ".join(parts) + hot_mark(p)

def _score_key(p: dict): return (p["pts"], p["reb"] + p["ast"], p["stl"] + p["blk"])
def _is_double_double(p: dict) -> bool:
    cats = [p["pts"], p["reb"], p["ast"], p["stl"], p["blk"]]
    return sum(v >= 10 for v in cats) >= 2
def second_player_condition(p: dict) -> bool:
    return (p["pts"] >= 20) or _is_double_double(p) or (p["stl"] >= 6) or (p["blk"] >= 6)

def pick_players_for_team(abbr: str, rows: list[dict]) -> list[tuple[dict,bool,bool]]:
    """[(player, bold, special_detail)] — спец: BKN Дёмин, MIA Голдин (подробно, минимум 3 метрики)."""
    if not rows: return []
    rows = sorted(rows, key=_score_key, reverse=True)
    out=[]
    top = rows[0]
    special_key = "дёмин" if abbr == "BKN" else ("голдин" if abbr == "MIA" else None)
    special=None
    if special_key:
        for p in rows:
            if special_key in (p["name"] or "").lower():
                special = p; break
    if special and special["name"] == top["name"]:
        out.append((special, True, True))
    elif special:
        out.append((top, False, False)); out.append((special, True, True))
    else:
        out.append((top, False, False))
    if len(out) < 2:
        for p in rows[1:]:
            if p["name"] == top["name"]: continue
            if second_player_condition(p):
                out.append((p, False, False)); break
    return out[:2]

# ---------- SPOILER ----------
def sp(s: str) -> str: return f'<span class="tg-spoiler">{s}</span>'
SEP = "–––––––––––––––––––––––"

# ---------- BUILD ----------
def build_post() -> str:
    center = pick_report_date()

    # ESPN — финальные матчи и их порядок
    espn_list = fetch_espn_completed(center)
    if not espn_list:
        # если по какой-то причине ESPN недоступен — остаёмся на sports.ru как есть
        chosen = center
        espn_pairs = []
    else:
        chosen = center
        espn_pairs = [frozenset([g["teams"][0]["abbr"], g["teams"][1]["abbr"]]) for g in espn_list]

    # соберём sports.ru матчи за окно day-1..day+1
    sports_games_by_pair = {}
    for d in window_days(center):
        for link in collect_day_match_links(d):
            info = parse_match(link)
            if not info or not info["finished"]: continue
            a = info["teamA"]["abbr"]; b = info["teamB"]["abbr"]
            if not a or not b or a == b: continue
            key = frozenset([a,b])
            # не перезаписываем уже собранное
            if key not in sports_games_by_pair:
                sports_games_by_pair[key] = info

    # итоговые игры — в ESPN-порядке; если нет на sports.ru, строим из ESPN
    final_blocks = []
    games_count = 0
    for g in espn_list:
        t1, t2 = g["teams"][0], g["teams"][1]
        a_abbr, b_abbr = t1["abbr"], t2["abbr"]
        key = frozenset([a_abbr, b_abbr])
        ot = g.get("ot", 0)

        if key in sports_games_by_pair:
            info = sports_games_by_pair[key]
            A, B = info["teamA"], info["teamB"]
            ot_str = "" if info["ot"] == 0 else (" (ОТ)" if info["ot"] == 1 else f" ({info['ot']} ОТ)")
            head = f"{A['emoji']} {A['name']}: {sp(str(A['score']))}\n" \
                   f"{B['emoji']} {B['name']}: {sp(str(B['score']) + ot_str)}\n\n"
            rowsA = info["players"].get(A["name"], [])
            rowsB = info["players"].get(B["name"], [])
            a_lines = [sp(format_player_line_special_detail(p, True) if det else format_player_line_regular(p, bold))
                       for (p,bold,det) in pick_players_for_team(A["abbr"], rowsA)]
            b_lines = [sp(format_player_line_special_detail(p, True) if det else format_player_line_regular(p, bold))
                       for (p,bold,det) in pick_players_for_team(B["abbr"], rowsB)]
            # раздел между командами
            lines = []
            a_lines = [x for x in a_lines if x.strip()]
            b_lines = [x for x in b_lines if x.strip()]
            if a_lines: lines.extend(a_lines)
            if a_lines and b_lines: lines.append("")
            if b_lines: lines.extend(b_lines)
            final_blocks.append(head + ("\n".join(lines) if lines else ""))
        else:
            # резерв: ESPN-версии
            # названия на русском
            a_name = ABBR_TO_TEAM_RU.get(a_abbr, a_abbr); b_name = ABBR_TO_TEAM_RU.get(b_abbr, b_abbr)
            a_emo = team_emoji_by_abbr(a_abbr); b_emo = team_emoji_by_abbr(b_abbr)
            ot_str = "" if ot == 0 else (" (ОТ)" if ot == 1 else f" ({ot} ОТ)")
            head = f"{a_emo} {a_name}: {sp(str(t1['score']))}\n" \
                   f"{b_emo} {b_name}: {sp(str(t2['score']) + ot_str)}\n\n"

            # игроки из ESPN boxscore
            players_by_tid = fetch_espn_players(g["eventId"])
            rowsA = players_by_tid.get(t1["teamId"], [])
            rowsB = players_by_tid.get(t2["teamId"], [])
            a_lines = [sp(format_player_line_special_detail(p, True) if det else format_player_line_regular(p, bold))
                       for (p,bold,det) in pick_players_for_team(a_abbr, rowsA)]
            b_lines = [sp(format_player_line_special_detail(p, True) if det else format_player_line_regular(p, bold))
                       for (p,bold,det) in pick_players_for_team(b_abbr, rowsB)]
            lines=[]
            a_lines = [x for x in a_lines if x.strip()]
            b_lines = [x for x in b_lines if x.strip()]
            if a_lines: lines.extend(a_lines)
            if a_lines and b_lines: lines.append("")
            if b_lines: lines.extend(b_lines)
            final_blocks.append(head + ("\n".join(lines) if lines else ""))

        games_count += 1
        if games_count < len(espn_list):
            final_blocks.append("\n" + SEP + "\n")

    title = f"НБА • {ru_date(chosen)} • {games_count} {ru_plural(games_count, ('матч','матча','матчей'))}\n"
    title += "Результаты надёжно спрятаны 👇\n"
    title += SEP + "\n\n"

    if not final_blocks:
        return title.rstrip()
    return (title + "".join(final_blocks)).strip()

# ---------- TELEGRAM ----------
def tg_send(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = S.post(url, json={
        "chat_id": CHAT_ID, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text}")

# ---------- MAIN ----------
if __name__ == "__main__":
    try:
        text = build_post()
        tg_send(text)
        print("OK")
    except Exception as e:
        print("ERROR:", repr(e), file=sys.stderr)
        sys.exit(1)
