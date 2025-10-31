#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, sys, requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TEAM_EMOJI_JSON = os.getenv("TEAM_EMOJI_JSON", "").strip()

if not TOKEN:
    sys.exit("ERROR: TELEGRAM_BOT_TOKEN не задан")

try:
    mapping = json.loads(TEAM_EMOJI_JSON) if TEAM_EMOJI_JSON else {}
    if not isinstance(mapping, dict):
        raise ValueError("TEAM_EMOJI_JSON должен быть объектом {ABBR: custom_emoji_id}")
except Exception as e:
    sys.exit(f"ERROR: некорректный TEAM_EMOJI_JSON: {e}")

ids = sorted({str(v) for v in mapping.values() if v})
if not ids:
    sys.exit("Нет custom_emoji_id в TEAM_EMOJI_JSON")

url = f"https://api.telegram.org/bot{TOKEN}/getCustomEmojiStickers"
try:
    r = requests.post(url, json={"custom_emoji_ids": ids}, timeout=15)
except Exception as e:
    sys.exit(f"HTTP error: {e}")

print("HTTP:", r.status_code)
data = {}
try:
    data = r.json()
except Exception:
    print("RAW:", r.text[:500])
    sys.exit("ERROR: не удалось распарсить ответ Telegram")

if not data.get("ok"):
    print("Telegram API error:", data)
    sys.exit(1)

found_ids = {s.get("custom_emoji_id") for s in (data.get("result") or [])}
missing = [(abbr, cid) for abbr, cid in mapping.items() if str(cid) not in found_ids]

print(f"Всего ID в env: {len(ids)}; Найдено Telegram: {len(found_ids)}")
if missing:
    print("НЕ найдены/некорректны:")
    for abbr, cid in missing:
        print(f"  {abbr}: {cid}")
    sys.exit(2)
else:
    print("Все custom_emoji_id валидны ✨")

# Опционально: сгенерить превью-текст с токенами (без отправки)
def preview_text(mp: dict[str, str]) -> str:
    return " ".join([f'{{EMO:\\n({abbr})\\n}}{abbr}' for abbr in sorted(mp)])

print("\nPreview с токенами (как подставляется в тексте):")
print(preview_text(mapping))
