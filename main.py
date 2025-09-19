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

# ===============================
# Configuración por .env
# ===============================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
ALERTS_CHAT_ID = os.getenv("ALERTS_CHAT_ID")

bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# ===============================
# Funciones auxiliares
# ===============================
def ahora_tz():
    tz = pytz.timezone("America/Chihuahua")
    return datetime.now(tz)

def normalizar_actor(actor: str) -> str:
    actor = actor.strip().upper()
    reemplazos = {
        "ALCALDE MARCO": "MARCO BONILLA",
        "MARCO ALCALDE": "MARCO BONILLA",
        "BONILLA": "MARCO BONILLA",
        "PRESIDENTE MUNICIPAL": "MARCO BONILLA",
        "GOBERNADORA": "MARU CAMPOS",
        "MARU": "MARU CAMPOS",
    }
    for k, v in reemplazos.items():
        if k in actor:
            return v
    return actor

def generar_resumen(entradas: List[Dict], titulo: str) -> str:
    positivos = sum(1 for e in entradas if e["color"] == "🟢")
    neutros = sum(1 for e in entradas if e["color"] == "🟡")
    negativos = sum(1 for e in entradas if e["color"] == "🔴")
    alertas = sum(1 for e in entradas if e["color"] == "⚠️")

    # Actores
    conteo_actores: Dict[str, Dict[str, int]] = {}
    for e in entradas:
        actores = e.get("actor", "").split("/")
        for a in actores:
            norm = normalizar_actor(a)
            if norm not in conteo_actores:
                conteo_actores[norm] = {"total": 0, "🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0}
            conteo_actores[norm]["total"] += 1
            conteo_actores[norm][e["color"]] += 1

    resumen = [
        f"🔵 {titulo}\n",
        "🪫 Semáforo",
        f"🟢 Positivas: {positivos}",
        f"🟡 Neutras:   {neutros}",
        f"🔴 Negativas: {negativos}",
        f"⚠️ Alertas:   {alertas}",
        f"Total entradas: {len(entradas)}",
        "",
        "👥 Actores top",
        "-------------------------",
    ]

    for actor, datos in conteo_actores.items():
        resumen.append(
            f"{actor}\n| Total {datos['total']}   🟢{datos['🟢']} 🟡{datos['🟡']} 🔴{datos['🔴']} ⚠️{datos['⚠️']}"
        )

    resumen.append("\n📰 Medios con publicación")
    medios = sorted(set(e["medio"] for e in entradas if e.get("medio")))
    for m in medios:
        resumen.append(f"- {m}")

    return "\n".join(resumen)

def armar_alerta(entrada: Dict) -> str:
    return f"{entrada['color']} {entrada['actor']} / {entrada['medio']} / {entrada['texto']} {entrada['link']}"

# ===============================
# Handlers
# ===============================
@dp.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(message: Message):
    now = ahora_tz()
    entradas = []  # aquí iría tu lógica para recolectar entradas
    titulo = f"COLUMNAS / {now.strftime('%a %d %b %Y – %H:%M')}"
    resumen = generar_resumen(entradas, titulo)
    await message.answer(resumen)

# ===============================
# Main
# ===============================
async def main():
    # BORRAR WEBHOOK para evitar conflictos con polling
    await bot.delete_webhook(drop_pending_updates=True)

    # Iniciar scheduler si lo usas
    scheduler.start()

    # Iniciar polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
