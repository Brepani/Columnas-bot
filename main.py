def generar_resumen(items: list[dict], titulo: str) -> str:
    """
    items: lista de dicts con al menos:
      - 'header_line': str (la primera línea con color + actores + medio)
      - 'medio': str (nombre del medio normalizado)
      - 'sentiment': str in {'pos','neu','neg','alt'}  # positiva/neutra/negativa/alerta
    """
    # ---- semáforo global ----
    pos = sum(1 for x in items if x.get("sentiment") == "pos")
    neu = sum(1 for x in items if x.get("sentiment") == "neu")
    neg = sum(1 for x in items if x.get("sentiment") == "neg")
    alt = sum(1 for x in items if x.get("sentiment") == "alt")
    total = len(items)

    # ---- conteo de actores (desde encabezado) ----
    actores_stats = {}  # nombre -> dict(count,total_pos,neu,neg,alt)
    for x in items:
        header = x.get("header_line", "")
        sent = x.get("sentiment")
        actores = extraer_actores_desde_header(header)
        for a in actores:
            d = actores_stats.setdefault(a, {"total": 0, "pos": 0, "neu": 0, "neg": 0, "alt": 0})
            d["total"] += 1
            if sent in d:
                d[sent] += 1

    # ordena por total desc, luego por pos desc, y alfabético
    actores_orden = sorted(
        actores_stats.items(),
        key=lambda kv: (-kv[1]["total"], -kv[1]["pos"], kv[0].lower())
    )

    # ---- medios con publicación (solo nombres, sin links) ----
    medios = []
    for x in items:
        m = (x.get("medio") or "").strip()
        if m and _norm(m) != "sin medio":
            medios.append(m)
    medios = sorted(set(medios), key=lambda s: s.lower())

    # ---- render ----
    lineas = []
    lineas.append(f"🔵 {titulo}")
    lineas.append("")
    lineas.append("🧾 Semáforo")
    lineas.append(f"🟢 Positivas: {pos}")
    lineas.append(f"🟡 Neutras:   {neu}")
    lineas.append(f"🔴 Negativas: {neg}")
    lineas.append(f"⚠️ Alertas:   {alt}")
    lineas.append(f"Total entradas: {total}")
    lineas.append("")
    lineas.append("👥 Actores top")
    lineas.append("-------------------------")

    if actores_orden:
        for nombre, c in actores_orden[:12]:  # muestra top 12
            lineas.append(f"{nombre}\n| Total {c['total']}   🟢{c['pos']} 🟡{c['neu']} 🔴{c['neg']} ⚠️{c['alt']}")
    else:
        lineas.append("(sin actores detectados)")

    lineas.append("")
    lineas.append("📰 Medios con publicación")
    if medios:
        for m in medios:
            lineas.append(f"- {m}")
    else:
        lineas.append("- (sin medios)")

    # NOTA: ya no incluimos “📌 Temas principales”
    return "\n".join(lineas)
