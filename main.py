import os
import logging
import asyncio
from typing import Dict, List
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
import pytz

# =========================
# Config por .env
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
ALERTS_CHAT_ID = os.getenv("ALERTS_CHAT_ID")
SOURCE_CHAT_ID = os.getenv("SOURCE_CHAT_ID")

TIMEZONE = "America/Chihuahua"
tz = pytz.timezone(TIMEZONE)

# Validación temprana (ayuda en logs si falta algo)
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

# Guardamos TODAS las columnas del día (con timestamp)
all_columnas: List[Dict] = []

# =========================
# Utilidades
# =========================
def ahora_tz() -> datetime:
    return datetime.now(tz)

def parse_column(text: str) -> Dict | None:
    """
    Espera:
    🟡 BONILLA / JUÁREZ / EL BORDO
    <cuerpo...>
    https://link
    """
    try:
        lines = [ln for ln in (text or "").strip().split("\n") if ln.strip()]
        if not lines:
            return None

        header = lines[0]
        color = header[0:2].strip()
        header_rest = header[2:].strip()
        actor, alcance, medio = [x.strip() for x in header_rest.split(" / ", 3)]

        link = lines[-1].strip() if lines else ""
        cuerpo = "\n".join(lines[1:-1]).strip() if len(lines) > 2 else ""

        if color not in ("🟢", "🟡", "🔴", "⚠️"):
            return None
        if not link.startswith("http"):
            link = ""

        return {
            "color": color,
            "actor": actor,
            "alcance": alcance,
            "medio": medio,
            "cuerpo": cuerpo,
            "link": link,
        }
    except Exception as e:
        logging.error(f"Error parseando columna: {e}")
        return None

def filtrar_por_rango(cols: List[Dict], start: datetime, end: datetime) -> List[Dict]:
    return [c for c in cols if start <= c["ts"] < end]

def generar_resumen(cols: List[Dict]) -> str:
    if not cols:
        return "🔵 COLUMNAS AM / Hoy no se recibieron columnas en el periodo solicitado."

    total = len(cols)
    colores = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0}
    actores: Dict[str, Dict[str, int]] = {}

    for col in cols:
        colores[col["color"]] = colores.get(col["color"], 0) + 1
        a = col["actor"]
        if a not in actores:
            actores[a] = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0, "total": 0}
        actores[a][col["color"]] += 1
        actores[a]["total"] += 1

    fecha_str = ahora_tz().strftime("%a %d %b %Y – %H:%M")
    out = f"## 🔵 COLUMNAS / {fecha_str}\n\n"

    out += "### 🚦 Semáforo\n"
    out += f"🟢 Positivas: {colores.get('🟢', 0)}\n"
    out += f"🟡 Neutras:   {colores.get('🟡', 0)}\n"
    out += f"🔴 Negativas: {colores.get('🔴', 0)}\n"
    out += f"⚠️ Alertas:   {colores.get('⚠️', 0)}\n"
    out += f"**Total columnas: {total}**\n\n"

    out += "### 👥 Actores top\n```\n"
    for actor, data in sorted(actores.items(), key=lambda x: x[1]["total"], reverse=True):
        out += (f"{actor:<18} "
                f"🟢{data['🟢']} 🟡{data['🟡']} 🔴{data['🔴']} ⚠️{data['⚠️']} "
                f"| Total {data['total']}\n")
    out += "```\n\n"

    out += "### 🔗 Links\n"
    for col in cols:
        medio_fmt = col['medio'].replace("*", "")
        out += f"{col['color']} *{medio_fmt}* ({col['alcance']}) – {col['actor']}: [Abrir]({col['link']})\n"
    return out

def armar_alerta(col: Dict) -> str:
    frase = (col.get("cuerpo") or "").replace("\n", " ").strip()
    if len(frase) > 120:
        frase = frase[:120] + "…"
    alerta = (
        f"[ALERTA {col['color']}] {col['actor']}\n"
        f"Medio: {col['medio']} ({col['alcance']})\n\n"
        f"Frase clave:\n\"{frase}\"\n\n"
    )
    if col.get("link"):
        alerta += f"🔗 [Abrir nota]({col['link']})"
    return alerta

# =========================
# Handlers
# =========================
@dp.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(message: Message):
    """Resumen de TODO lo recibido HOY (00:00 → ahora)."""
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_columnas, start, now)
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset), parse_mode="Markdown")

@dp.message(Command("links"))
async def cmd_links(message: Message):
    """Lista solo los links de HOY."""
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_columnas, start, now)
    if not subset:
        await bot.send_message(SUMMARY_CHAT_ID, "🔗 Hoy no hay links registrados.")
        return
    text = "### 🔗 Links de hoy\n" + "\n".join(
        f"{c['color']} {c['medio']} – {c['actor']}: {c['link']}" for c in subset if c.get("link")
    )
    await bot.send_message(SUMMARY_CHAT_ID, text, parse_mode="Markdown")

@dp.message()
async def recibir_columnas(message: Message):
    """Ingesta de columnas desde el grupo origen."""
    try:
        if SOURCE_CHAT_ID and str(message.chat.id) != str(SOURCE_CHAT_ID):
            return

        texto = message.text or ""
        if not any(icon in texto for icon in ("🟢", "🟡", "🔴", "⚠️")):
            return

        parsed = parse_column(texto)
        if not parsed:
            return

        now_local = ahora_tz()
        parsed["ts"] = now_local
        all_columnas.append(parsed)  # Guardamos SIEMPRE (para /resumen_hoy y /links)

        # Enviar alertas de inmediato
        if parsed["color"] in ("🔴", "⚠️") and ALERTS_CHAT_ID:
            try:
                await bot.send_message(ALERTS_CHAT_ID, armar_alerta(parsed), parse_mode="Markdown")
            except Exception as e:
                logging.error(f"Error enviando alerta: {e}")

    except Exception as e:
        logging.error(f"Error en recibir_columnas: {e}")

# =========================
# Tarea programada 08:30 (solo 06:00–08:00)
# =========================
async def enviar_resumen_autom():
    now = ahora_tz()
    start = now.replace(hour=6, minute=0, second=0, microsecond=0)
    end   = now.replace(hour=8, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_columnas, start, end)
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset), parse_mode="Markdown")

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
