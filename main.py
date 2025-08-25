import os
import logging
import asyncio
from typing import Dict, List, Tuple
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
import pytz
import re

# =========================
# Config por .env
# =========================
TOKEN = os.getenv("TELEGRAM_TOKEN")
SUMMARY_CHAT_ID = os.getenv("SUMMARY_CHAT_ID")
ALERTS_CHAT_ID = os.getenv("ALERTS_CHAT_ID")
SOURCE_CHAT_ID = os.getenv("SOURCE_CHAT_ID")

TIMEZONE = "America/Chihuahua"
tz = pytz.timezone(TIMEZONE)

# Validación temprana
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

# Guardamos TODAS las entradas del día (una por párrafo)
all_items: List[Dict] = []

# =========================
# Utilidades
# =========================
ICON_SET = ("🟢", "🟡", "🔴", "⚠️")
# Severidad: mayor es más grave
ICON_WEIGHT = {"⚠️": 4, "🔴": 3, "🟡": 2, "🟢": 1}

def ahora_tz() -> datetime:
    return datetime.now(tz)

def choose_severity(found_icons: List[str]) -> str:
    if not found_icons:
        return "🟡"
    return max(found_icons, key=lambda ic: ICON_WEIGHT.get(ic, 0))

def split_paragraphs(lines: List[str]) -> List[str]:
    """Separa en párrafos por líneas en blanco; conserva texto."""
    paras, buf = [], []
    for ln in lines:
        if ln.strip() == "":
            if buf:
                paras.append("\n".join(buf).strip())
                buf = []
        else:
            buf.append(ln)
    if buf:
        paras.append("\n".join(buf).strip())
    return paras

def clean_leading_icons(text: str) -> Tuple[str, List[str]]:
    """Quita íconos/bullets iniciales y recoge todos los íconos del párrafo."""
    # Remueve bullets simples al inicio
    t = text.lstrip(" -–—•*\t")
    start_icons = []
    i = 0
    while i < len(t):
        token2 = t[i:i+2]
        if token2 in ICON_SET:
            start_icons.append(token2)
            i += 2
            continue
        if t[i].isspace() or t[i] in "-–—•*":
            i += 1
            continue
        break
    body = t[i:].strip()
    inner = [m.group(0) for m in re.finditer(r"(🟢|🟡|🔴|⚠️)", body)]
    icons = start_icons + inner
    # Normaliza espacios raros
    body = re.sub(r"\s+", " ", body).strip()
    return body, list(dict.fromkeys(icons))

def parse_header(header: str) -> Dict | None:
    """
    Soporta:
    - '🟡 BONILLA / JUÁREZ / EL DIARIO'
    - 'PAN / MORENA / ALCALDE BONILLA / ENTRELÍNEAS'
    Retorna dict con actors (list), alcance (str|''), medio (str)
    """
    h = header.strip()
    if h[:2] in ICON_SET:
        h = h[2:].strip()
    parts = [p.strip() for p in h.split("/") if p.strip()]
    if len(parts) < 2:
        return None
    if len(parts) == 3:
        actor, alcance, medio = parts
        actors = [actor]
        return {"actors": actors, "alcance": alcance, "medio": medio}
    else:
        medio = parts[-1]
        actors = parts[:-1]
        return {"actors": actors, "alcance": "", "medio": medio}

def parse_message(text: str) -> List[Dict]:
    """
    Devuelve una lista de items (uno por párrafo detectado).
    Cada item: {color, actors[], alcance, medio, cuerpo, link, ts}
    """
    try:
        lines = [ln for ln in (text or "").split("\n")]
        # Link (último http*)
        link = ""
        for ln in reversed(lines):
            if ln.strip().startswith("http"):
                link = ln.strip()
                break
        # Header
        first_nonempty_idx = next((i for i, ln in enumerate(lines) if ln.strip()), None)
        if first_nonempty_idx is None:
            return []
        header = lines[first_nonempty_idx].strip()
        meta = parse_header(header)
        if not meta:
            return []

        # Cuerpo entre header y link
        end_idx = len(lines)
        if link:
            end_idx = next((i for i, ln in enumerate(lines) if ln.strip() == link), len(lines))
        body_lines = lines[first_nonempty_idx + 1:end_idx]

        paragraphs = split_paragraphs(body_lines)
        items: List[Dict] = []
        for p in paragraphs:
            if not p.strip():
                continue
            cuerpo, icons = clean_leading_icons(p)
            color = choose_severity([ic for ic in icons if ic in ICON_SET])
            items.append({
                "color": color,
                "actors": meta["actors"],
                "alcance": meta["alcance"],
                "medio": meta["medio"],
                "cuerpo": cuerpo,
                "link": link,
                "ts": ahora_tz(),
            })
        if not items:
            items.append({
                "color": "🟡",
                "actors": meta["actors"],
                "alcance": meta["alcance"],
                "medio": meta["medio"],
                "cuerpo": "",
                "link": link,
                "ts": ahora_tz(),
            })
        return items
    except Exception as e:
        logging.error(f"Error parseando mensaje: {e}")
        return []

def filtrar_por_rango(cols: List[Dict], start: datetime, end: datetime) -> List[Dict]:
    return [c for c in cols if start <= c["ts"] < end]

def extract_topic(texto: str) -> str:
    """
    Toma la primera línea/frase del párrafo como 'tema'.
    Limpia bullets y recorta a 60-80 chars para que sea legible.
    """
    if not texto:
        return ""
    # primera línea
    first_line = texto.split("\n", 1)[0].strip()
    first_line = first_line.lstrip("-–—•* ").strip()
    # si hay punto, toma la primera oración (pero no de 1-2 palabras)
    sentence = re.split(r"[.!?]\s", first_line)[0].strip() or first_line
    sentence = re.sub(r"\s+", " ", sentence)
    if len(sentence) < 4:
        return ""
    if len(sentence) > 80:
        sentence = sentence[:80].rstrip() + "…"
    return sentence

def generar_resumen(cols: List[Dict], titulo_hora: str) -> str:
    if not cols:
        return "🔵 COLUMNAS / Hoy no se recibieron columnas en el periodo solicitado."

    total = len(cols)
    colores = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0}
    actores: Dict[str, Dict[str, int]] = {}
    medios_count: Dict[str, int] = {}
    topics_count: Dict[str, int] = {}

    for it in cols:
        # semáforo
        colores[it["color"]] = colores.get(it["color"], 0) + 1
        # actores
        for a in it["actors"]:
            if a not in actores:
                actores[a] = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0, "total": 0}
            actores[a][it["color"]] += 1
            actores[a]["total"] += 1
        # medios
        medio = it["medio"].strip()
        medios_count[medio] = medios_count.get(medio, 0) + 1
        # temas
        t = extract_topic(it.get("cuerpo", ""))
        if t:
            topics_count[t] = topics_count.get(t, 0) + 1

    out = f"🔵 COLUMNAS / {titulo_hora}\n\n"

    # Semáforo
    out += "🚦 Semáforo\n"
    out += f"🟢 Positivas: {colores.get('🟢', 0)}\n"
    out += f"🟡 Neutras:   {colores.get('🟡', 0)}\n"
    out += f"🔴 Negativas: {colores.get('🔴', 0)}\n"
    out += f"⚠️ Alertas:   {colores.get('⚠️', 0)}\n"
    out += f"Total entradas: {total}\n\n"

    # Actores top (en bloque monoespaciado para que alinee)
    out += "👥 Actores top\n"
    out += "```\n"
    for actor, data in sorted(actores.items(), key=lambda x: x[1]["total"], reverse=True):
        out += (f"{actor:<22} "
                f"🟢{data['🟢']} 🟡{data['🟡']} 🔴{data['🔴']} ⚠️{data['⚠️']} "
                f"| Total {data['total']}\n")
    out += "```\n\n"

    # Medios con publicación
    out += "📰 Medios con publicación\n"
    for medio, cnt in sorted(medios_count.items(), key=lambda x: x[1], reverse=True):
        out += f"- {medio}\n"
    out += "\n"

    # Temas principales (top 5)
    if topics_count:
        out += "📌 Temas principales\n"
        for tema, cnt in sorted(topics_count.items(), key=lambda x: x[1], reverse=True)[:5]:
            out += f"- {tema}\n"

    return out

def armar_alerta(it: Dict) -> str:
    frase = (it.get("cuerpo") or "").replace("\n", " ").strip()
    if len(frase) > 160:
        frase = frase[:160] + "…"
    actors_str = ", ".join(it["actors"])
    alcance = f" ({it['alcance']})" if it['alcance'] else ""
    alerta = (
        f"[ALERTA {it['color']}] {actors_str}\n"
        f"Medio: {it['medio']}{alcance}\n\n"
        f"Frase clave:\n\"{frase}\"\n\n"
    )
    if it.get("link"):
        alerta += f"🔗 [Abrir nota]({it['link']})"
    return alerta

# =========================
# Handlers
# =========================
@dp.message(Command("resumen_hoy"))
async def cmd_resumen_hoy(message: Message):
    """Resumen de TODO lo recibido HOY (00:00 → ahora)."""
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)
    titulo = now.strftime("%a %d %b %Y – %H:%M")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), parse_mode="Markdown")

@dp.message(Command("links"))
async def cmd_links(message: Message):
    """Lista solo los links de HOY (por si lo necesitas)."""
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)
    links = []
    seen = set()
    for it in subset:
        if it.get("link") and it["link"] not in seen:
            seen.add(it["link"])
            links.append(f"{it['color']} {it['medio']} – {', '.join(it['actors'])}: {it['link']}")
    if not links:
        await bot.send_message(SUMMARY_CHAT_ID, "🔗 Hoy no hay links registrados.")
        return
    await bot.send_message(SUMMARY_CHAT_ID, "🔗 Links de hoy\n" + "\n".join(links))

@dp.message()
async def recibir_columnas(message: Message):
    """Ingesta de columnas desde el grupo origen."""
    try:
        if SOURCE_CHAT_ID and str(message.chat.id) != str(SOURCE_CHAT_ID):
            return

        texto = message.text or ""
        if "/" not in texto:
            return  # requerimos encabezado con '/'

        items = parse_message(texto)
        if not items:
            return

        all_items.extend(items)

        # Alertas inmediatas
        for it in items:
            if it["color"] in ("🔴", "⚠️") and ALERTS_CHAT_ID:
                try:
                    await bot.send_message(ALERTS_CHAT_ID, armar_alerta(it), parse_mode="Markdown")
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
    subset = filtrar_por_rango(all_items, start, end)
    titulo = now.strftime("%a %d %b %Y – 08:30")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), parse_mode="Markdown")

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
