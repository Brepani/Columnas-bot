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
ICON_WEIGHT = {"⚠️": 4, "🔴": 3, "🟡": 2, "🟢": 1}

STOPWORDS_ES = {
    "a","acá","ahi","ahí","al","algo","algún","alguna","algunas","algunos","allá","alli","allí","ante","antes",
    "aquel","aquella","aquellas","aquellos","aqui","aquí","así","aun","aún","aunque","cada","como","con","contra",
    "cual","cuales","cualquier","cualquiera","cualquieras","cuando","cuanto","cuanta","cuantas","cuantos","de",
    "debe","deben","debido","del","desde","donde","dos","el","él","ella","ellas","ellos","en","entre","era","eran",
    "es","esa","esas","ese","eso","esos","esta","está","están","estaba","estaban","estado","estados","estar",
    "estas","este","esto","estos","fue","ha","han","hasta","hay","he","hemos","la","las","le","les","lo","los",
    "más","me","mi","mis","mismo","mucha","muchas","mucho","muchos","muy","nada","ni","no","nos","nosotros",
    "o","otra","otras","otro","otros","para","pero","poco","por","porque","que","qué","quien","quién","quienes",
    "quienesquiera","se","sea","según","ser","si","sí","sido","sin","sobre","solo","solamente","son","su","sus",
    "tal","también","tan","tanto","te","tendrá","tienen","toda","todas","todo","todos","tras","tu","tus","un",
    "una","unas","uno","unos","ya","y","e","u"
}

ACCENT_MAP = str.maketrans("áéíóúüñÁÉÍÓÚÜÑ", "aeiouunaeiouun")

def ahora_tz() -> datetime:
    return datetime.now(tz)

def choose_severity(found_icons: List[str]) -> str:
    if not found_icons:
        return "🟡"
    return max(found_icons, key=lambda ic: ICON_WEIGHT.get(ic, 0))

def split_paragraphs(lines: List[str]) -> List[str]:
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
    body = re.sub(r"\s+", " ", body).strip()
    return body, list(dict.fromkeys(icons))

def parse_header(header: str) -> Dict | None:
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
    try:
        lines = [ln for ln in (text or "").split("\n")]
        link = ""
        for ln in reversed(lines):
            if ln.strip().startswith("http"):
                link = ln.strip()
                break
        first_nonempty_idx = next((i for i, ln in enumerate(lines) if ln.strip()), None)
        if first_nonempty_idx is None:
            return []
        header = lines[first_nonempty_idx].strip()
        meta = parse_header(header)
        if not meta:
            return []
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

# -------- Temas principales (n-gramas 2-3 palabras) --------
def normalize_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"http\S+|www\.\S+", " ", s)
    s = re.sub(r"[\"'”“‘’´`]", " ", s)
    s = s.translate(ACCENT_MAP)
    s = re.sub(r"[^a-z0-9áéíóúüñ\s]", " ", s)  # después de translate quedan ascii
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokenize(s: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9áéíóúüñ]+", s) if t not in STOPWORDS_ES and len(t) >= 3]

def ngrams(tokens: List[str], n: int) -> List[str]:
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]

def top_topics(items: List[Dict], k: int = 5) -> List[Tuple[str, int, str]]:
    """
    Devuelve lista de (frase, conteo, snippet ejemplo)
    Solo considera bigramas/trigramas que aparecen en >=2 párrafos.
    """
    counts: Dict[str, int] = {}
    occurrences: Dict[str, List[str]] = {}  # frase -> lista de párrafos originales
    for it in items:
        text = it.get("cuerpo", "") or ""
        norm = normalize_text(text)
        toks = tokenize(norm)
        cands = set(ngrams(toks, 2) + ngrams(toks, 3))
        for cand in cands:
            if len(cand) < 7:  # filtra cosas muy cortas
                continue
            counts[cand] = counts.get(cand, 0) + 1
            occurrences.setdefault(cand, []).append(text)

    # filtra por frecuencia mínima 2
    freq_items = [(p, c) for p, c in counts.items() if c >= 2]
    if not freq_items:
        return []

    # ordena por conteo desc y longitud (prefiere 2-3 palabras compactas)
    freq_items.sort(key=lambda x: (x[1], len(x[0])), reverse=True)
    top = freq_items[:k]

    results: List[Tuple[str, int, str]] = []
    for phrase, cnt in top:
        # snippet: buscar una oración que contenga todas las palabras del n-grama
        words = set(phrase.split())
        snippet = ""
        for para in occurrences.get(phrase, []):
            sentences = re.split(r"(?<=[.!?])\s+", para)
            found = None
            for s in sentences:
                s_norm = normalize_text(s)
                if all(w in s_norm.split() for w in words):
                    found = s.strip()
                    break
            snippet = found or para.strip()
            if snippet:
                break
        if len(snippet) > 110:
            snippet = snippet[:110].rstrip() + "…"
        # formateo frase a “título”: capitaliza palabras
        title = " ".join(w.capitalize() for w in phrase.split())
        results.append((title, cnt, snippet))
    return results

# -------- Generación de resumen --------
def generar_resumen(cols: List[Dict], titulo_hora: str) -> str:
    if not cols:
        return "🔵 COLUMNAS / Hoy no se recibieron columnas en el periodo solicitado."

    total = len(cols)
    colores = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0}
    actores: Dict[str, Dict[str, int]] = {}
    medios_count: Dict[str, int] = {}

    for it in cols:
        colores[it["color"]] = colores.get(it["color"], 0) + 1
        for a in it["actors"]:
            if a not in actores:
                actores[a] = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0, "total": 0}
            actores[a][it["color"]] += 1
            actores[a]["total"] += 1
        medio = it["medio"].strip()
        medios_count[medio] = medios_count.get(medio, 0) + 1

    out = f"🔵 COLUMNAS / {titulo_hora}\n\n"

    # Semáforo
    out += "🚦 Semáforo\n"
    out += f"🟢 Positivas: {colores.get('🟢', 0)}\n"
    out += f"🟡 Neutras:   {colores.get('🟡', 0)}\n"
    out += f"🔴 Negativas: {colores.get('🔴', 0)}\n"
    out += f"⚠️ Alertas:   {colores.get('⚠️', 0)}\n"
    out += f"Total entradas: {total}\n\n"

    # Actores top
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

    # Temas principales (n-gramas)
    topics = top_topics(cols, k=5)
    if topics:
        out += "📌 Temas principales\n"
        for title, cnt, snip in topics:
            out += f"- {title} ({cnt}) · {snip}\n"

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
    now = ahora_tz()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    subset = filtrar_por_rango(all_items, start, now)
    titulo = now.strftime("%a %d %b %Y – %H:%M")
    await bot.send_message(SUMMARY_CHAT_ID, generar_resumen(subset, titulo), parse_mode="Markdown")

@dp.message(Command("links"))
async def cmd_links(message: Message):
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
        for it in items:
            if it["color"] in ("🔴", "⚠️") and ALERTS_CHAT_ID:
                try:
                    await bot.send_message(ALERTS_CHAT_ID, armar_alerta(it), parse_mode="Markdown")
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
