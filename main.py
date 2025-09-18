import os
import logging
import asyncio
from typing import Dict, List, Tuple, Set
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
import pytz
import re
import hashlib

# =========================
# Config por .env
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
ALERTS_CHAT_ID = os.getenv("ALERTS_CHAT_ID")
SOURCE_CHAT_ID = os.getenv("SOURCE_CHAT_ID")

TIMEZONE = "America/Chihuahua"
tz = pytz.timezone(TIMEZONE)

missing = [k for k, v in {
    "TELEGRAM_TOKEN": TOKEN,
    "SUMMARY_CHAT_ID": SUMMARY_CHAT_ID,
    "ALERTS_CHAT_ID": ALERTS_CHAT_ID,
    "SOURCE_CHAT_ID": SOURCE_CHAT_ID,
}.items() if not v]
if missing:
    raise RuntimeError("Faltan variables de entorno: " + ", ".join(missing))

# =========================
# Bot & Dispatcher
# =========================
bot = Bot(token=TOKEN)
dp = Dispatcher()

# Memoria en ejecución (se reinicia al redeploy)
# Guardamos items a nivel PÁRRAFO, pero con un ID de COLUMNA (link)
all_items: List[Dict] = []

# =========================
# Utilidades
# =========================
ICON_SET = ("🟢", "🟡", "🔴", "⚠️")
ICON_WEIGHT = {"⚠️": 4, "🔴": 3, "🟡": 2, "🟢": 1}

# … (aquí va todo tu bloque de utilidades, parseadores y funciones exactamente como lo tenías) …

# =========================
# Handlers
# =========================
@dp.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(message: Message):
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)
    titulo = now.strftime("%a %d %b %Y – %H:%M")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), disable_web_page_preview=True)

@dp.message(Command("links"))
async def cmd_links(message: Message):
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)
    seen_cols = set()
    lines = []
    for it in subset:
        if not it.get("link"):
            continue
        if it["col_id"] in seen_cols:
            continue
        seen_cols.add(it["col_id"])
        lines.append(f"{it['color']} {it['medio']} – {', '.join(it['actors'])}: {it['link']}")
    if not lines:
        await bot.send_message(SUMMARY_CHAT_ID, "🔗 Hoy no hay links registrados.")
        return
    await bot.send_message(SUMMARY_CHAT_ID, "🔗 Links de hoy\n" + "\n".join(lines), disable_web_page_preview=True)

@dp.message()
async def recibir_columnas(message: Message):
    try:
        if SOURCE_CHAT_ID and str(message.chat.id) != str(SOURCE_CHAT_ID):
            return
        texto = message.text or ""
        if "/" not in texto:
            return
        items = parse_message(texto)
        if not items:
            return
        all_items.extend(items)

        # Alertas inmediatas
        for it in items:
            if it["color"] in ("🔴", "⚠️") and ALERTS_CHAT_ID:
                try:
                    await bot.send_message(ALERTS_CHAT_ID, armar_alerta(it), disable_web_page_preview=True)
                except Exception as e:
                    logging.error(f"Error enviando alerta: {e}")
    except Exception as e:
        logging.error(f"Error en recibir_columnas: {e}")

# =========================
# Tarea programada 08:30 (06–08)
# =========================
async def enviar_resumen_autom():
    now = ahora_tz()
    start = now.replace(hour=6, minute=0, second=0, microsecond=0)
    end   = now.replace(hour=8, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, end)
    titulo = now.strftime("%a %d %b %Y – 08:30")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), disable_web_page_preview=True)

# =========================
# Main
# =========================
async def main():
    logging.basicConfig(level=logging.INFO)
    scheduler = AsyncIOScheduler(timezone=tz)
    scheduler.add_job(enviar_resumen_autom, "cron", hour=8, minute=30)
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
