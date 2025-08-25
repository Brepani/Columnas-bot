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

STOPWORDS_ES = {
    "a","acá","ahi","ahí","al","algo","algún","alguna","algunas","algunos","allá","alli","allí","ante","antes",
    "aquel","aquella","aquellas","aquellos","aqui","aquí","así","aun","aún","aunque","cada","como","con","contra",
    "cual","cuales","cualquier","cualquiera","cualquieras","cuando","cuanto","cuanta","cuantas","cuantos","de",
    "debe","deben","debido","del","desde","donde","dos","el","él","ella","ellas","ellos","en","entre","era","eran",
    "es","esa","esas","ese","eso","esos","esta","está","están","estaba","estaban","estado","estados","estar",
    "estas","este","esto","estos","fue","ha","han","hasta","hay","he","hemos","la","las","le","les","lo","los",
    "más","me","mi","mis","mismo","mucha","muchas","mucho","muchos","muy","nada","ni","no","nos","nosotros",
    "o","otra","otras","otro","otros","para","pero","poco","por","porque","que","qué","quien","quién","quienes",
    "se","sea","según","ser","si","sí","sido","sin","sobre","solo","solamente","son","su","sus",
    "tal","también","tan","tanto","te","tiene","tienen","toda","todas","todo","todos","tras","tu","tus","un",
    "una","unas","uno","unos","ya","y","e","u"
}

ACCENT_MAP = str.maketrans("áéíóúüñÁÉÍÓÚÜÑ", "aeiouunaeiouun")

# Normalizadores (alias) para temas/actores
ALIASES = {
    r"\b(alcalde\s+)?marco\s+bonilla(\s+mendoza)?\b": "Marco Bonilla",
    r"\bmaru\s+campos\b|\bmaria\s+eugenia\s+campos\b": "Maru Campos",
    r"\bcruz\s+p(e|é)rez\s+cu(e|é)llar\b": "Cruz Pérez Cuéllar",
    r"\bdaniela\s+al(v|b)arez\b": "Daniela Álvarez",
    r"\bcesar\s+ja(u|á)regui(\s+moreno)?\b": "César Jáuregui",
    r"\bkomaba\b": "César Komaba",
    r"\bdorados\s+vs?\.?\s+indios\b|\bindios\s+vs?\.?\s+dorados\b": "Dorados vs Indios",
    r"\bbeisbol\s+estatal\b|\bliga\s+estatal\s+de\s+beisbol\b": "Béisbol estatal",
    r"\bparlamento\s+juvenil\b": "Parlamento Juvenil",
    r"\bbienestar\b": "Bienestar",
}
TITLE_WORDS = {"alcalde","gobernador","gobernadora","diputado","diputada",
               "presidente","presidenta","secretario","secretaria","regidor","regidora"}

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

def make_column_id(link: str, medio: str, header: str, body: str) -> str:
    """
    Columna = por LINK. Si no hubiera link, generamos un id estable con hash.
    """
    if link:
        return link.strip()
    key = f"{medio}|{header}|{body[:200]}"
    return "nolink:" + hashlib.sha1(key.encode("utf-8")).hexdigest()

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

        # Genera un ID de columna
        col_id = make_column_id(link, meta["medio"], header, "\n".join(body_lines))

        items: List[Dict] = []
        if not paragraphs:
            paragraphs = [""]  # columna vacía (evita perder el registro)

        for p in paragraphs:
            cuerpo, icons = clean_leading_icons(p)
            color = choose_severity([ic for ic in icons if ic in ICON_SET])
            items.append({
                "col_id": col_id,
                "color": color,
                "actors": meta["actors"],
                "alcance": meta["alcance"],
                "medio": meta["medio"],
                "cuerpo": cuerpo,
                "link": link,
                "ts": ahora_tz(),
            })
        return items
    except Exception as e:
        logging.error(f"Error parseando mensaje: {e}")
        return []

def filtrar_por_rango(cols: List[Dict], start: datetime, end: datetime) -> List[Dict]:
    return [c for c in cols if start <= c["ts"] < end]

# ---------- Temas ----------
def normalize_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"http\S+|www\.\S+", " ", s)
    s = re.sub(r"[\"'”“‘’´`]", " ", s)
    s = s.translate(ACCENT_MAP)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokenize(s: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9]+", s) if t not in STOPWORDS_ES and len(t) >= 3]

def ngrams(tokens: List[str], n: int) -> List[str]:
    return [" ".join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]

def title_case(s: str) -> str:
    return " ".join(w.capitalize() for w in s.split())

def unify_alias(phrase: str) -> str:
    p_norm = normalize_text(phrase)
    parts = p_norm.split()
    if parts and parts[0] in TITLE_WORDS and len(parts) >= 2:
        p_norm = " ".join(parts[1:])
    for pat, canon in ALIASES.items():
        if re.search(pat, p_norm, flags=re.IGNORECASE):
            return canon
    return title_case(p_norm.strip())

def collapse_similars(counters: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    keys = sorted(counters.keys(), key=len)
    keep: Dict[str, Set[str]] = {}
    for k in keys:
        placed = False
        for base in list(keep.keys()):
            if k.lower() in base.lower() or base.lower() in k.lower():
                shortest = base if len(base) <= len(k) else k
                longest = k if shortest == base else base
                merged = counters.get(shortest, set()) | counters.get(longest, set())
                keep[shortest] = merged
                if longest in keep:
                    del keep[longest]
                placed = True
                break
        if not placed:
            keep[k] = counters[k]
    return keep

def top_topics_by_column(items: List[Dict], k: int = 5) -> List[Tuple[str, int, str]]:
    """
    Cuenta temas por COLUMNA (col_id) única.
    Devuelve (tema, #columnas, snippet).
    """
    # Agrupa textos por columna
    col_texts: Dict[str, List[str]] = {}
    for it in items:
        col_texts.setdefault(it["col_id"], []).append(it.get("cuerpo", "") or "")

    # Candidatos: tema -> set(col_id)
    candidates: Dict[str, Set[str]] = {}
    # Para snippet por col
    any_text_by_col: Dict[str, str] = {cid: " ".join(txts).strip() for cid, txts in col_texts.items()}

    for cid, texts in col_texts.items():
        whole = " ".join(texts)
        norm = normalize_text(whole)
        toks = tokenize(norm)
        grams = set(ngrams(toks, 2) + ngrams(toks, 3))

        # intenta alias por presencia (personas/eventos)
        for pat, canon in ALIASES.items():
            if re.search(pat, norm, flags=re.IGNORECASE):
                grams.add(canon.lower())

        grams = {g for g in grams if len(g) >= 7}
        for g in grams:
            canon = unify_alias(g)
            candidates.setdefault(canon, set()).add(cid)

    # Colapsa similares y filtra frecuencia mínima 2 columnas
    collapsed = collapse_similars(candidates)
    freq = [(topic, len(cols)) for topic, cols in collapsed.items() if len(cols) >= 2]
    if not freq:
        return []

    freq.sort(key=lambda x: (x[1], -len(x[0])), reverse=True)
    top = freq[:k]

    # Snippets: toma una oración de cualquiera de las columnas del tema
    results: List[Tuple[str, int, str]] = []
    for topic, cnt in top:
        snippet = ""
        for cid in collapsed[topic]:
            para = any_text_by_col.get(cid, "")
            sentences = re.split(r"(?<=[.!?])\s+", para)
            words = normalize_text(topic).split()
            found = None
            for s in sentences:
                s_norm = normalize_text(s)
                if all(w in s_norm.split() for w in words):
                    found = s.strip()
                    break
            snippet = (found or para.strip())
            if snippet:
                break
        if len(snippet) > 110:
            snippet = snippet[:110].rstrip() + "…"
        results.append((topic, cnt, snippet))
    return results

# ---------- Generación de resumen (POR COLUMNA/LINK) ----------
def generar_resumen(items: List[Dict], titulo_hora: str) -> str:
    if not items:
        return "🔵 COLUMNAS / Hoy no se recibieron columnas en el periodo solicitado."

    # 1) Agrega por columna
    cols: Dict[str, Dict] = {}  # col_id -> {medio, actores(set), color_max, textos(list)}
    for it in items:
        cid = it["col_id"]
        if cid not in cols:
            cols[cid] = {
                "medio": it["medio"],
                "actores": set(),
                "color": "🟢",
                "textos": [],
            }
        cols[cid]["actores"].update(it["actors"])
        # color más grave
        if ICON_WEIGHT[it["color"]] > ICON_WEIGHT[cols[cid]["color"]]:
            cols[cid]["color"] = it["color"]
        if it.get("cuerpo"):
            cols[cid]["textos"].append(it["cuerpo"])

    total_cols = len(cols)

    # 2) Semáforo por columna
    colores = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0}
    for c in cols.values():
        colores[c["color"]] += 1

    # 3) Actores top (1 por columna)
    actores: Dict[str, Dict[str, int]] = {}
    for c in cols.values():
        for a in c["actores"]:
            if a not in actores:
                actores[a] = {"🟢": 0, "🟡": 0, "🔴": 0, "⚠️": 0, "total": 0}
            actores[a][c["color"]] += 1
            actores[a]["total"] += 1

    # 4) Medios con publicación (por columna)
    medios_count: Dict[str, int] = {}
    for c in cols.values():
        m = c["medio"].strip()
        medios_count[m] = (medios_count.get(m, 0) + 1)

    out = f"🔵 COLUMNAS / {titulo_hora}\n\n"

    out += "🚦 Semáforo\n"
    out += f"🟢 Positivas: {colores.get('🟢', 0)}\n"
    out += f"🟡 Neutras:   {colores.get('🟡', 0)}\n"
    out += f"🔴 Negativas: {colores.get('🔴', 0)}\n"
    out += f"⚠️ Alertas:   {colores.get('⚠️', 0)}\n"
    out += f"Total columnas: {total_cols}\n\n"

    out += "👥 Actores top\n"
    out += "```\n"
    for actor, data in sorted(actores.items(), key=lambda x: x[1]["total"], reverse=True):
        out += (f"{actor:<22} "
                f"🟢{data['🟢']} 🟡{data['🟡']} 🔴{data['🔴']} ⚠️{data['⚠️']} "
                f"| Total {data['total']}\n")
    out += "```\n\n"

    out += "📰 Medios con publicación\n"
    for medio, cnt in sorted(medios_count.items(), key=lambda x: x[1], reverse=True):
        out += f"- {medio}\n"
    out += "\n"

    # 5) Temas principales POR COLUMNA
    topics = top_topics_by_column(items, k=5)
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
    await bot.send_message(SUMMARY_CHAT_ID, "🔗 Links de hoy\n" + "\n".join(lines))

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

        # Alertas inmediatas (por párrafo, para no perder sensibilidad)
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
