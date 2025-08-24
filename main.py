import os
import logging
import asyncio
from typing import Dict, List
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
import pytz

# =========================
# Configuración por .env
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")  # Grupo/canal donde se manda el Resumen AM
ALERTS_CHAT_ID = os.getenv("ALERTS_CHAT_ID")    # Grupo/canal donde se mandan ALERTAS 🔴⚠️
SOURCE_CHAT_ID = os.getenv("SOURCE_CHAT_ID")    # (Opcional) Solo leer de este chat

TIMEZONE = "America/Chihuahua"
tz = pytz.timezone(TIMEZONE)

# =========================
# Bot & Dispatcher
# =========================
bot = Bot(token=TOKEN)
dp = Dispatcher()

# Buffer en memoria para columnas válidas (las que llegan 06:00–08:00)
columnas: List[Dict] = []

# =========================
# Utilidades
# =========================
def ahora_tz() -> datetime:
    return datetime.now(tz)

def dentro_ventana(dt: datetime) -> bool:
    """True si la hora local está entre 06:00 y 08:00 (incluye 08:00)."""
    start = dt.replace(hour=6, minute=0, second=0, microsecond=0)
    end   = dt.replace(hour=8, minute=0, second=0, microsecond=0)
    return start <= dt <= end

def parse_column(text: str) -> Dict | None:
    """
    Formato esperado:
    🟡 BONILLA / JUÁREZ / EL BORDO
    <cuerpo de la columna …>
    https://link-final
    """
    try:
        lines = [ln for ln in (text or "").strip().split("\n") if ln.strip()]
        if not lines:
            return None

        header = lines[0]
        # Color (primer emoji) + resto del encabezado
        color = header[0:2].strip()  # emoji típico ocupa 2 chars
        header_rest = header[2:].strip()

        # BONILLA / JUÁREZ / EL BORDO
        actor, alcance, medio = [x.strip() for x in header_rest.split(" / ", 3)]

        # Última línea debe ser el link
        link = lines[-1].strip() if lines else ""
        cuerpo = "\n".join(lines[1:-1]).strip() if len(lines) > 2 else ""

        # Validaciones básicas
        if not link.startswith("http"):
            # Si no hay link al final, lo dejamos vacío pero seguimos
            link = ""

        if color not in ("🟢", "🟡", "🔴", "⚠️"):
            return None

        return {
            "color": color,
            "actor": actor,
            "alcance": alcance,
            "medio": medio,
            "cuerpo": cuerpo,
            "link": link
        }
    except Exception as e:
        logging.error(f"Error parseando columna: {e}")
        return None

def generar_resumen() -> str:
    """Arma el mensaje del Resumen AM con semáforo + actores + links (formato Telegram)."""
    if not columnas:
        return "🔵 COLUMNAS AM / Hoy no se recibieron columnas entre 6–8 am."

    total = len(columnas)
    colores = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0}
    actores: Dict[str, Dict[str, int]] = {}

    for col in columnas:
        colores[col["color"]] = colores.get(col["color"], 0) + 1
        a = col["actor"]
        if a not in actores:
            actores[a] = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0, "total": 0}
        actores[a][col["color"]] += 1
        actores[a]["total"] += 1

    fecha_str = ahora_tz().strftime("%a %d %b %Y – 08:30")
    out = f"## 🔵 COLUMNAS AM / {fecha_str}\n\n"

    # Semáforo general
    out += "### 🚦 Semáforo general\n"
    out += f"🟢 Positivas: {colores.get('🟢', 0)}\n"
    out += f"🟡 Neutras:   {colores.get('🟡', 0)}\n"
    out += f"🔴 Negativas: {colores.get('🔴', 0)}\n"
    out += f"⚠️ Alertas:   {colores.get('⚠️', 0)}\n"
    out += f"**Total columnas: {total}**\n\n"

    # Actores (bloque monoespaciado para verse alineado en Telegram)
    out += "### 👥 Actores top\n```\n"
    for actor, data in sorted(actores.items(), key=lambda x: x[1]["total"], reverse=True):
        out += (f"{actor:<18} "
                f"🟢{data['🟢']} 🟡{data['🟡']} 🔴{data['🔴']} ⚠️{data['⚠️']} "
                f"| Total {data['total']}\n")
    out += "```\n\n"

    # Links (orden natural de llegada)
    out += "### 🔗 Links\n"
    for col in columnas:
        # Ej: 🟡 *El Bordo* (Juárez) – BONILLA: [Abrir](https://...)
        medio_fmt = col['medio'].replace("*", "")  # evitar romper Markdown
        out += f"{col['color']} *{medio_fmt}* ({col['alcance']}) – {col['actor']}: [Abrir]({col['link']})\n"
    return out

async def enviar_resumen():
    """Envía el resumen y limpia el buffer."""
    try:
        msg = generar_resumen()
        await bot.send_message(SUMMARY_CHAT_ID, msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error enviando resumen: {e}")
    finally:
        columnas.clear()  # limpiar para el siguiente día

def armar_alerta(col: Dict) -> str:
    """Mensaje de alerta compacto (Telegram-friendly)."""
    # Frase clave: primeros 120 caracteres del cuerpo
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
    """Permite pedir el resumen manualmente."""
    await enviar_resumen()

@dp.message()
async def recibir_columnas(message: Message):
    """Ingesta de columnas desde el grupo origen."""
    try:
        # Si definiste SOURCE_CHAT_ID, ignora mensajes de otros chats
        if SOURCE_CHAT_ID and str(message.chat.id) != str(SOURCE_CHAT_ID):
            return

        texto = message.text or ""
        # Sólo procesar si trae alguno de los íconos del semáforo
        if not any(icon in texto for icon in ("🟢", "🟡", "🔴", "⚠️")):
            return

        col = parse_column(texto)
        if not col:
            return

        now_local = ahora_tz()

        # ALERTAS a su grupo dedicado (en cualquier horario)
        if col["color"] in ("🔴", "⚠️") and ALERTS_CHAT_ID:
            try:
                await bot.send_message(ALERTS_CHAT_ID, armar_alerta(col), parse_mode="Markdown")
            except Exception as e:
                logging.error(f"Error enviando alerta: {e}")

        # Contabilizar para el resumen solo si llega entre 06:00 y 08:00
        if dentro_ventana(now_local):
            columnas.append(col)

    except Exception as e:
        logging.error(f"Error en recibir_columnas: {e}")

# =========================
# Main
# =========================
async def main():
    logging.basicConfig(level=logging.INFO)
    # Programar envío 08:30 AM
    scheduler = AsyncIOScheduler(timezone=tz)
    scheduler.add_job(enviar_resumen, "cron", hour=8, minute=30)
    scheduler.start()

    # Iniciar polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
