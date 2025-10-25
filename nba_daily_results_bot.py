# --- НОВОЕ: источники пар и рекордов дня (BBR + ESPN site.api) ---

from bs4 import BeautifulSoup

# полные англ. названия -> аббревиатуры ESPN
TEAM_ABBR = {
    "Atlanta Hawks":"ATL","Boston Celtics":"BOS","Brooklyn Nets":"BKN","Charlotte Hornets":"CHA",
    "Chicago Bulls":"CHI","Cleveland Cavaliers":"CLE","Dallas Mavericks":"DAL","Denver Nuggets":"DEN",
    "Detroit Pistons":"DET","Golden State Warriors":"GSW","Houston Rockets":"HOU","Indiana Pacers":"IND",
    "Los Angeles Clippers":"LAC","Los Angeles Lakers":"LAL","Memphis Grizzlies":"MEM","Miami Heat":"MIA",
    "Milwaukee Bucks":"MIL","Minnesota Timberwolves":"MIN","New Orleans Pelicans":"NOP","New York Knicks":"NYK",
    "Oklahoma City Thunder":"OKC","Orlando Magic":"ORL","Philadelphia 76ers":"PHI","Phoenix Suns":"PHX",
    "Portland Trail Blazers":"POR","Sacramento Kings":"SAC","San Antonio Spurs":"SAS","Toronto Raptors":"TOR",
    "Utah Jazz":"UTA","Washington Wizards":"WAS",
}

def _bbr_schedule_url():
    # Сезон 2025-26 — это NBA_2026_games.html
    return "https://www.basketball-reference.com/leagues/NBA_2026_games.html"

def _norm_txt(s: str) -> str:
    return " ".join((s or "").replace("\xa0"," ").split())

def fetch_pairs_from_bbr(day: date) -> list[dict]:
    """
    Читает таблицу расписания за день на Basketball-Reference и возвращает
    финальные игры со счётом (12 для 24.10.2025).
    Возврат: [{"home_en","away_en","home_abbr","away_abbr","home_score","away_score","ot"}]
    """
    url = _bbr_schedule_url()
    r = S.get(url, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    tbl = soup.find("table", id="schedule")
    if not tbl:
        return []

    want_month = day.strftime("%b")       # Oct
    want_day   = str(int(day.strftime("%d")))  # '24' без ведущего нуля
    want_year  = day.strftime("%Y")       # 2025

    out = []
    for tr in (tbl.select("tbody tr") or []):
        # На некоторых строках могут быть заголовки/разделители
        th = tr.find("th", attrs={"data-stat":"date_game"})
        if not th: 
            continue
        dtxt = _norm_txt(th.get_text())
        # Пример: 'Fri, Oct 24, 2025'
        if (want_month not in dtxt) or (want_year not in dtxt) or (want_day not in dtxt):
            continue

        # Должен быть боксскор (финал), иначе пропускаем
        bs = tr.find("td", attrs={"data-stat":"box_score_text"})
        if not bs or not bs.find("a"):
            continue

        away_en = _norm_txt(tr.find("td", attrs={"data-stat":"visitor_team_name"}).get_text())
        home_en = _norm_txt(tr.find("td", attrs={"data-stat":"home_team_name"}).get_text())
        a_pts   = tr.find("td", attrs={"data-stat":"visitor_pts"})
        h_pts   = tr.find("td", attrs={"data-stat":"home_pts"})
        if not a_pts or not h_pts or not a_pts.get_text(strip=True) or not h_pts.get_text(strip=True):
            continue  # не финал

        away_pts = int(a_pts.get_text())
        home_pts = int(h_pts.get_text())

        ot = ""
        ot_td = tr.find("td", attrs={"data-stat":"overtimes"})
        if ot_td:
            ot = _norm_txt(ot_td.get_text())  # '', 'OT', '2OT' ...

        # маппим полные имена к ESPN-аббревиатурам (для рекордов)
        away_abbr = TEAM_ABBR.get(away_en, "")
        home_abbr = TEAM_ABBR.get(home_en, "")

        out.append({
            "home_en": home_en, "away_en": away_en,
            "home_abbr": home_abbr, "away_abbr": away_abbr,
            "home_score": home_pts, "away_score": away_pts,
            "ot": ot,
        })
    return out

def fetch_team_records_from_espn(day: date) -> dict[tuple[str,str], dict]:
    """
    Берём «послематчевые» W-L у ESPN (ВАЖНО: используем site.api, а не site.web.api).
    Возвращает словарь ключом (home_abbr, away_abbr) в любом порядке, значением:
      {"home_abbr":..., "home_record": "2-0", "away_abbr":..., "away_record":"0-2"}
    """
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={day.strftime('%Y%m%d')}"
    j = _get_json(url)
    rec_map = {}  # (H,A) или (A,H) -> data

    for ev in (j.get("events") or []):
        try:
            comp = (ev.get("competitions") or [])[0]
            comps = comp.get("competitors") or []
            if len(comps) != 2:
                continue
            # ESPN может не отдавать «home/away» флаг одинаково, поэтому вытащим isHome
            home_c = next(c for c in comps if c.get("homeAway") == "home")
            away_c = next(c for c in comps if c.get("homeAway") == "away")
            def rec_of(c):
                for r in c.get("records") or []:
                    if r.get("type") == "total" and r.get("summary"):
                        return r["summary"]  # '2-0'
                return ""
            h_abbr = (home_c.get("team") or {}).get("abbreviation","")
            a_abbr = (away_c.get("team") or {}).get("abbreviation","")
            rec_map[(h_abbr, a_abbr)] = {
                "home_abbr": h_abbr, "away_abbr": a_abbr,
                "home_record": rec_of(home_c), "away_record": rec_of(away_c),
            }
        except Exception:
            continue
    return rec_map

# --- НОВОЕ: замена прежнего «списка матчей» на BBR, с фоллбеком на прежний источник, если что-то пойдёт не так
def collect_games_for_day(day: date) -> list[dict]:
    """
    Главный список матчей дня. Сперва берём Basketball-Reference (полный набор).
    Если пусто — фоллбек на прежний метод (если он у тебя назывался иначе, скорректируй вызов).
    Каждый элемент:
      {
        "home_en","away_en","home_abbr","away_abbr",
        "home_score","away_score","ot",
        # + "home_record","away_record" (добавим ниже)
      }
    """
    games = fetch_pairs_from_bbr(day)
    if not games:
        # Fallback: если раньше у тебя был сбор пар из sports.ru, можешь подставить его здесь:
        # games = fetch_pairs_from_sportsru(day)  # <- оставь как было у тебя
        return []

    # приклеим W-L из ESPN (site.api)
    rec_map = fetch_team_records_from_espn(day)
    for g in games:
        key = (g["home_abbr"], g["away_abbr"])
        # иногда ESPN даёт другие аббревиатуры? Тогда попробуем и обратный ключ
        data = rec_map.get(key) or rec_map.get((key[1], key[0]))
        if data:
            g["home_record"] = data.get("home_record","")
            g["away_record"] = data.get("away_record","")
        else:
            g["home_record"] = ""
            g["away_record"] = ""
    return games
