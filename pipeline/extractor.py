"""
pipeline/extractor.py
=====================
Extrae métricas de YouTube Analytics API y las carga en PostgreSQL.

Uso:
  python -m pipeline.run
  python -m pipeline.run --start 2025-01-01 --end 2025-03-31
  python -m pipeline.run --subcuenta "SharkTank Brasil"
"""

import os
import re
import pickle
import logging
from datetime import date

import pandas as pd
from sqlalchemy import text
from google.auth.transport.requests import Request
import google_auth_oauthlib.flow
import googleapiclient.discovery

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import (
    TOKENS_DIR, CLIENT_SECRET_FILE, YT_SCOPES,
    TRAFFIC_SOURCE_MAP, DEVICE_MAP, CUENTAS_CON_USPR
)
from db.connection import engine

log = logging.getLogger(__name__)

# ============================================================
# 🔑  AUTENTICACIÓN
# ============================================================

def parse_token_filename(filename: str):
    """
    Extrae (nombre_subcuenta, channel_id) del nombre del .pickle.
    Formato: token_{nombre_subcuenta}_{UCxxxxxxx}.pickle
    """
    base  = os.path.splitext(filename)[0]
    match = re.match(r"^token_(.+)_(UC[\w-]{22})$", base)
    if not match:
        log.warning(f"  Nombre no reconocido, omitido: {filename}")
        return None, None
    return match.group(1), match.group(2)


def load_credentials(token_path: str, label: str):
    """Carga, refresca o re-autentica credenciales OAuth."""
    creds = None
    if os.path.exists(token_path):
        with open(token_path, "rb") as f:
            creds = pickle.load(f)

    if creds and creds.expired and creds.refresh_token:
        try:
            log.info(f"  🔄 Refrescando token: {label}")
            creds.refresh(Request())
            _save_creds(creds, token_path)
            log.info("  ✅ Token refrescado.")
        except Exception as e:
            log.warning(f"  ⚠️  Refresh falló ({e}). Re-autenticando...")
            creds = None

    if not creds or not creds.valid:
        log.info(f"  🌐 Flujo OAuth para: {label}")
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
            CLIENT_SECRET_FILE, YT_SCOPES
        )
        creds = flow.run_local_server(port=0, prompt="select_account")
        _save_creds(creds, token_path)
        log.info("  ✅ Autenticación exitosa.")

    return creds


def _save_creds(creds, path):
    with open(path, "wb") as f:
        pickle.dump(creds, f)


# ============================================================
# 📡  HELPERS API
# ============================================================

def _api_query(yt, start, end, metrics, dimensions,
               sort=None, filters=None, max_results=None):
    """Ejecuta una consulta a YouTube Analytics API → DataFrame."""
    kwargs = dict(
        ids="channel==MINE",
        startDate=start,
        endDate=end,
        metrics=metrics,
        dimensions=dimensions,
    )
    if sort:        kwargs["sort"]       = sort
    if filters:     kwargs["filters"]    = filters
    if max_results: kwargs["maxResults"] = max_results

    try:
        resp = yt.reports().query(**kwargs).execute()
    except Exception as e:
        log.warning(f"    API [{dimensions}|{filters}]: {e}")
        return pd.DataFrame()

    if not resp.get("rows"):
        return pd.DataFrame()
    return pd.DataFrame(
        resp["rows"],
        columns=[c["name"] for c in resp["columnHeaders"]]
    )


# ============================================================
# 📊  EXTRACCIÓN — MÉTRICAS DIARIAS
# ============================================================

def _get_general(yt, start, end):
    df = _api_query(yt, start, end,
                    "views,estimatedMinutesWatched,estimatedRevenue,cpm,likes,subscribersGained",
                    "day", sort="day")
    if df.empty:
        return df
    df.rename(columns={"estimatedMinutesWatched": "watch_time_hours"}, inplace=True)
    df["watch_time_hours"] /= 60
    return df


def _get_content_type(yt, start, end):
    df = _api_query(yt, start, end,
                    "views,estimatedMinutesWatched,estimatedRevenue",
                    "day,creatorContentType", sort="day")
    if df.empty:
        return df
    df.rename(columns={"estimatedMinutesWatched": "watch_time_hours"}, inplace=True)
    df["watch_time_hours"] /= 60
    piv = df.pivot(index="day", columns="creatorContentType",
                   values=["views", "watch_time_hours", "estimatedRevenue"])
    piv.columns = [f"{m}_{c}" for m, c in piv.columns]
    piv.reset_index(inplace=True)
    return piv


def _get_traffic_sources(yt, start, end, geo_filter=None):
    df = _api_query(yt, start, end, "views", "day,insightTrafficSourceType",
                    sort="day", filters=geo_filter)
    if df.empty:
        return df
    df = df[df["insightTrafficSourceType"].isin(TRAFFIC_SOURCE_MAP.keys())].copy()
    df["insightTrafficSourceType"] = df["insightTrafficSourceType"].map(TRAFFIC_SOURCE_MAP)
    piv = df.pivot(index="day", columns="insightTrafficSourceType", values="views")
    piv.columns = [f"{c}_views" for c in piv.columns]
    piv.reset_index(inplace=True)
    return piv


def _get_devices(yt, start, end, geo_filter=None):
    df = _api_query(yt, start, end,
                    "views,estimatedMinutesWatched", "day,deviceType",
                    sort="day", filters=geo_filter)
    if df.empty:
        return df
    df = df[df["deviceType"].isin(DEVICE_MAP.keys())].copy()
    df["deviceType"] = df["deviceType"].map(DEVICE_MAP)
    df["estimatedMinutesWatched"] /= 60  # convertir a horas (consistente con el resto del pipeline)
    piv_v = df.pivot(index="day", columns="deviceType", values="views")
    piv_v.columns = [f"{c}_views" for c in piv_v.columns]
    piv_w = df.pivot(index="day", columns="deviceType", values="estimatedMinutesWatched")
    piv_w.columns = [f"{c}_watchtime" for c in piv_w.columns]
    piv   = piv_v.join(piv_w)
    piv.reset_index(inplace=True)
    return piv


def extract_daily(yt, start, end):
    """Combina todas las consultas diarias en un solo DataFrame."""
    df_g = _get_general(yt, start, end)
    if df_g.empty:
        return pd.DataFrame()

    result = df_g
    for df in [_get_content_type(yt, start, end),
               _get_traffic_sources(yt, start, end),
               _get_devices(yt, start, end)]:
        if not df.empty:
            result = result.merge(df, on="day", how="outer")

    result.sort_values("day", inplace=True)
    result["day"] = pd.to_datetime(result["day"])
    return result


# ============================================================
# 📊  EXTRACCIÓN — DATOS DE PERÍODO
# ============================================================

def extract_demographics(yt, start, end, geo_filter=None):
    df = _api_query(yt, start, end, "viewerPercentage", "ageGroup,gender",
                    sort="-viewerPercentage", filters=geo_filter)
    if df.empty:
        return df
    df.columns = ["age_group", "gender", "viewer_pct"]
    df["geo_group"] = "US_PR" if geo_filter else "WW"
    return df


def extract_country_views(yt, start, end, max_results=20):
    df = _api_query(yt, start, end, "views", "country",
                    sort="-views", max_results=max_results)
    if df.empty:
        return df
    df.columns = ["country", "views"]
    return df


def extract_us_states(yt, start, end, max_results=25):
    df = _api_query(yt, start, end, "views", "province",
                    sort="-views", max_results=max_results,
                    filters="country==US")
    if df.empty:
        return df
    df.columns = ["state", "views"]
    df["state"] = df["state"].str.replace("US-", "", regex=False)
    return df


def extract_search_terms(yt, start, end, geo_filter=None, max_results=25):
    base_filter = "insightTrafficSourceType==YT_SEARCH"

    # US+PR: la API rechaza country==US,PR combinado con otros filtros vía ";"
    # Solución: consultar US y PR por separado y sumar resultados.
    if geo_filter:
        acum = {}
        for country in ["US", "PR"]:
            f = f"country=={country};{base_filter}"
            df_c = _api_query(yt, start, end, "views", "insightTrafficSourceDetail",
                              sort="-views", max_results=max_results, filters=f)
            if df_c.empty:
                continue
            df_c.columns = ["search_term", "views"]
            for _, row in df_c.iterrows():
                acum[row["search_term"]] = acum.get(row["search_term"], 0) + row["views"]
        if not acum:
            return pd.DataFrame()
        df = pd.DataFrame(list(acum.items()), columns=["search_term", "views"])
        df = df.sort_values("views", ascending=False).head(max_results).reset_index(drop=True)
        df["geo_group"] = "US_PR"
        return df

    # WW: consulta normal sin filtro de país
    df = _api_query(yt, start, end, "views", "insightTrafficSourceDetail",
                    sort="-views", max_results=max_results, filters=base_filter)
    if df.empty:
        return df
    df.columns = ["search_term", "views"]
    df["geo_group"] = "WW"
    return df


def extract_top_videos(yt, yt_data, channel_id, start, end,
                       geo_filter=None, max_results=15):
    """
    Top videos por views en el período, sin importar cuándo se publicaron.

    Estrategia: consultar Analytics directamente con dimensions=video y
    sort=-views. La API devuelve hasta 200 resultados por llamada.
    Para US+PR se itera país por país (la API no acepta country==US,PR
    combinado con otros filtros) y se suman las views antes de rankear.
    """
    # Pedir más resultados a la API para luego quedarnos con los top reales
    api_limit = 200

    countries = ["US", "PR"] if geo_filter else [None]
    acum_views = {}

    for country in countries:
        geo_f = f"country=={country}" if country else None
        df_chunk = _api_query(yt, start, end, "views", "video",
                              sort="-views", max_results=api_limit,
                              filters=geo_f)
        if df_chunk.empty:
            continue
        df_chunk.columns = ["video_id", "views"]
        for _, row in df_chunk.iterrows():
            acum_views[row["video_id"]] = acum_views.get(row["video_id"], 0) + row["views"]

    if not acum_views:
        return pd.DataFrame()

    df = pd.DataFrame(list(acum_views.items()), columns=["video_id", "views"])
    df = df.sort_values("views", ascending=False).head(max_results).reset_index(drop=True)

    # Obtener títulos desde la Data API
    titles = {}
    id_list = df["video_id"].tolist()
    # La Data API acepta hasta 50 IDs por llamada
    for i in range(0, len(id_list), 50):
        batch = id_list[i:i+50]
        try:
            resp = yt_data.videos().list(part="snippet", id=",".join(batch)).execute()
            for item in resp.get("items", []):
                titles[item["id"]] = item["snippet"]["title"]
        except Exception as e:
            log.warning(f"    No se obtuvieron títulos (batch {i}): {e}")

    df["title"]     = df["video_id"].map(titles).fillna("Sin título")
    df["geo_group"] = "US_PR" if geo_filter else "WW"
    return df[["title", "views", "geo_group"]]


# ============================================================
# 🗄️  CARGA EN BD
# ============================================================

# Mapeo columnas API → columnas BD
RENAME_DAILY = {
    "day":                            "fecha",
    "views":                          "views_total",
    "watch_time_hours":               "watch_time_total",
    "estimatedRevenue":               "revenue_total",
    "cpm":                            "cpm_promedio",
    "likes":                          "likes_total",
    "subscribersGained":              "suscriptores_total",
    "views_videoOnDemand":            "views_videos",
    "views_shorts":                   "views_shorts",
    "views_liveStream":               "views_lives",
    "watch_time_hours_videoOnDemand": "watchtime_videos",
    "watch_time_hours_shorts":        "watchtime_shorts",
    "watch_time_hours_liveStream":    "watchtime_lives",
    "estimatedRevenue_videoOnDemand": "revenue_videos",
    "estimatedRevenue_shorts":        "revenue_shorts",
    "estimatedRevenue_liveStream":    "revenue_lives",
    "Búsqueda YouTube_views":        "views_search",
    "Videos sugeridos_views":        "views_suggested",
    "Externos_views":                "views_external",
    "Browse / Suscriptores_views":   "views_browse",
    "Playlists_views":               "views_playlist",
    "Shorts feed_views":             "views_short_feed",
    "Directo/Desconocido_views":     "views_directortunknown",
    "mobile_views":                  "views_mobile",
    "tv_views":                      "views_tv",
    "computer_views":                "views_computer",
    "tablet_views":                  "views_tablet",
    "mobile_watchtime":              "watchtime_mobile",
    "tv_watchtime":                  "watchtime_tv",
    "computer_watchtime":            "watchtime_computer",
    "tablet_watchtime":              "watchtime_tablet",
    # aliases legacy por si la API devuelve revenue_* por dispositivo

}

COLS_FINAL = [
    "id_subcuenta", "id_fecha",
    "views_total", "watch_time_total", "revenue_total", "cpm_promedio",
    "likes_total", "suscriptores_total",
    "views_videos", "views_shorts", "views_lives",
    "watchtime_videos", "watchtime_shorts", "watchtime_lives",
    "revenue_videos", "revenue_shorts", "revenue_lives",
    "views_search", "views_suggested", "views_external", "views_browse",
    "views_playlist", "views_short_feed", "views_directortunknown",
    "views_mobile", "views_tv", "views_computer", "views_tablet",
    "watchtime_mobile", "watchtime_tv", "watchtime_computer", "watchtime_tablet",
]


def _get_id_subcuenta(nombre):
    df = pd.read_sql(
        "SELECT id_subcuenta FROM dim_subcuenta WHERE nombre_subcuenta = %s LIMIT 1",
        con=engine, params=(nombre,)
    )
    return int(df.iloc[0]["id_subcuenta"]) if not df.empty else None


def save_daily(df_raw: pd.DataFrame, nombre_subcuenta: str):
    """Upsert de métricas diarias en hechos_metricas."""
    id_sub = _get_id_subcuenta(nombre_subcuenta)
    if not id_sub:
        log.error(f"  ❌ Subcuenta no encontrada: '{nombre_subcuenta}'")
        return False

    df = df_raw.rename(columns=RENAME_DAILY)
    # Rellenar numéricos
    num_cols = df.select_dtypes(include="number").columns
    df[num_cols] = df[num_cols].fillna(0)

    # Join con dim_fecha
    fechas = pd.read_sql("SELECT id_fecha, fecha FROM dim_fecha", con=engine)
    fechas["fecha"] = pd.to_datetime(fechas["fecha"]).dt.date
    df["fecha"]     = pd.to_datetime(df["fecha"]).dt.date
    df = df.merge(fechas, on="fecha", how="left")

    missing = df["id_fecha"].isna().sum()
    if missing:
        log.warning(f"  ⚠️  {missing} fechas sin id_fecha. Se descartan.")
        df = df.dropna(subset=["id_fecha"])

    df["id_subcuenta"] = id_sub
    for c in COLS_FINAL:
        if c not in df.columns:
            df[c] = 0
    df = df[COLS_FINAL]

    # Eliminar datos previos del rango (idempotencia)
    fids = df["id_fecha"].astype(int).tolist()
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM hechos_metricas WHERE id_subcuenta=:s AND id_fecha=ANY(:f)"),
            {"s": id_sub, "f": fids}
        )
    df.to_sql("hechos_metricas", con=engine, if_exists="append", index=False)
    log.info(f"  ✅ {len(df)} filas → hechos_metricas")
    return True


def _upsert_period(table: str, df: pd.DataFrame, nombre_subcuenta: str,
                   start: str, end: str):
    """Borra el período y re-inserta en tablas de período.
    Si el df tiene columna geo_group, el DELETE filtra también por geo_group
    para no borrar WW cuando se inserta US_PR y viceversa.
    """
    if df.empty:
        return
    id_sub = _get_id_subcuenta(nombre_subcuenta)
    if not id_sub:
        return
    df = df.copy()
    df["id_subcuenta"] = id_sub
    df["fecha_inicio"]  = start
    df["fecha_fin"]     = end
    with engine.begin() as conn:
        if "geo_group" in df.columns:
            geo = df["geo_group"].iloc[0]
            conn.execute(
                text(f"DELETE FROM {table} WHERE id_subcuenta=:s "
                     "AND fecha_inicio=:fi AND fecha_fin=:ff AND geo_group=:g"),
                {"s": id_sub, "fi": start, "ff": end, "g": geo}
            )
        else:
            conn.execute(
                text(f"DELETE FROM {table} WHERE id_subcuenta=:s "
                     "AND fecha_inicio=:fi AND fecha_fin=:ff"),
                {"s": id_sub, "fi": start, "ff": end}
            )
    df.to_sql(table, con=engine, if_exists="append", index=False)
    log.info(f"  ✅ {len(df)} filas → {table}")


def save_demographics(df, sub, start, end):
    _upsert_period("hechos_demograficos", df, sub, start, end)

def save_country_views(df, sub, start, end):
    _upsert_period("hechos_geografia_paises", df, sub, start, end)

def save_us_states(df, sub, start, end):
    _upsert_period("hechos_us_estados", df, sub, start, end)

def save_search_terms(df, sub, start, end):
    _upsert_period("hechos_search_terms", df, sub, start, end)

def save_top_videos(df, sub, start, end):
    _upsert_period("hechos_top_videos", df, sub, start, end)


# ============================================================
# 📡  EXTRACCIÓN US+PR — KPIs, FUENTES, DISPOSITIVOS
# ============================================================

def extract_uspr_kpis(yt, start, end):
    """KPIs totales US+PR: suma US + PR por separado y combina."""
    metricas = "views,estimatedMinutesWatched,estimatedRevenue,subscribersGained,subscribersLost,likes"
    total = {
        "views": 0, "watch_time": 0, "revenue": 0,
        "subs_gained": 0, "subs_lost": 0, "likes": 0
    }
    for country in ["US", "PR"]:
        df = _api_query(yt, start, end, metricas, "day",
                        sort="day", filters=f"country=={country}")
        if df.empty:
            continue
        total["views"]       += df["views"].sum()
        total["watch_time"]  += df["estimatedMinutesWatched"].sum() / 60
        total["revenue"]     += df["estimatedRevenue"].sum()
        total["subs_gained"] += df["subscribersGained"].sum()
        total["likes"]       += df["likes"].sum()
    return pd.DataFrame([{
        "views_total":      total["views"],
        "watch_time_total": round(total["watch_time"], 2),
        "revenue_total":    round(total["revenue"], 2),
        "likes_total":      total["likes"],
        "suscriptores_net": total["subs_gained"] - total["subs_lost"],
    }])


def extract_uspr_traffic_sources(yt, start, end):
    """Fuentes de tráfico US+PR: suma US + PR."""
    traffic = {}
    for country in ["US", "PR"]:
        df = _api_query(yt, start, end, "views", "insightTrafficSourceType",
                        filters=f"country=={country}")
        if df.empty:
            continue
        for _, row in df.iterrows():
            src = TRAFFIC_SOURCE_MAP.get(row["insightTrafficSourceType"])
            if src:
                traffic[src] = traffic.get(src, 0) + row["views"]
    if not traffic:
        return pd.DataFrame()
    return pd.DataFrame([{
        "views_search":           traffic.get("Búsqueda YouTube", 0),
        "views_suggested":        traffic.get("Videos sugeridos", 0),
        "views_external":         traffic.get("Externos", 0),
        "views_browse":           traffic.get("Browse / Suscriptores", 0),
        "views_playlist":         traffic.get("Playlists", 0),
        "views_short_feed":       traffic.get("Shorts feed", 0),
        "views_directortunknown": traffic.get("Directo/Desconocido", 0),
    }])


def extract_uspr_devices(yt, start, end):
    """Dispositivos US+PR: suma US + PR."""
    devices = {}
    for country in ["US", "PR"]:
        df = _api_query(yt, start, end, "views,estimatedMinutesWatched",
                        "deviceType", filters=f"country=={country}")
        if df.empty:
            continue
        for _, row in df.iterrows():
            dev = DEVICE_MAP.get(row["deviceType"])
            if dev:
                if dev not in devices:
                    devices[dev] = {"views": 0, "watchtime": 0}
                devices[dev]["views"]    += row["views"]
                devices[dev]["watchtime"] += row["estimatedMinutesWatched"] / 60
    if not devices:
        return pd.DataFrame()
    return pd.DataFrame([{
        "views_mobile":      devices.get("mobile",   {}).get("views",    0),
        "views_tv":          devices.get("tv",       {}).get("views",    0),
        "views_computer":    devices.get("computer", {}).get("views",    0),
        "views_tablet":      devices.get("tablet",   {}).get("views",    0),
        "watchtime_mobile":  round(devices.get("mobile",   {}).get("watchtime", 0), 2),
        "watchtime_tv":      round(devices.get("tv",       {}).get("watchtime", 0), 2),
        "watchtime_computer":round(devices.get("computer", {}).get("watchtime", 0), 2),
        "watchtime_tablet":  round(devices.get("tablet",   {}).get("watchtime", 0), 2),
    }])


def save_uspr_periodo(kpis_df, traffic_df, devices_df, nombre_subcuenta, start, end):
    """Une KPIs + fuentes + dispositivos US+PR y hace upsert en hechos_uspr_periodo."""
    id_sub = _get_id_subcuenta(nombre_subcuenta)
    if not id_sub:
        return
    # Combinar los tres DataFrames en una sola fila
    row = {}
    for df in [kpis_df, traffic_df, devices_df]:
        if not df.empty:
            row.update(df.iloc[0].to_dict())
    if not row:
        return
    row["id_subcuenta"] = id_sub
    row["fecha_inicio"]  = start
    row["fecha_fin"]     = end

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM hechos_uspr_periodo "
                 "WHERE id_subcuenta=:s AND fecha_inicio=:fi AND fecha_fin=:ff"),
            {"s": id_sub, "fi": start, "ff": end}
        )
    pd.DataFrame([row]).to_sql("hechos_uspr_periodo", con=engine,
                                if_exists="append", index=False)
    log.info(f"  ✅ 1 fila → hechos_uspr_periodo")


# ============================================================
# 🔍  HELPERS
# ============================================================

def cuenta_tiene_uspr(nombre_subcuenta: str) -> bool:
    """Verifica si la subcuenta pertenece a una cuenta con análisis US+PR."""
    df = pd.read_sql("""
        SELECT c.nombre_cuenta FROM dim_subcuenta s
        JOIN dim_cuenta c ON s.id_cuenta = c.id_cuenta
        WHERE s.nombre_subcuenta = %s LIMIT 1
    """, con=engine, params=(nombre_subcuenta,))
    if df.empty:
        return False
    return df.iloc[0]["nombre_cuenta"].upper() in CUENTAS_CON_USPR


# ============================================================
# 🚀  PIPELINE PRINCIPAL
# ============================================================

def run(start: str, end: str, only_subcuenta: str = None, tokens_dir: str = None):
    # tokens_dir puede venir de run.py (--grupo) o usar el default de settings
    if tokens_dir is None:
        tokens_dir = TOKENS_DIR

    if not os.path.isdir(tokens_dir):
        log.error(f"Carpeta de tokens no encontrada: '{tokens_dir}'")
        return

    token_files = sorted(f for f in os.listdir(tokens_dir) if f.endswith(".pickle"))
    if not token_files:
        log.error("No hay archivos .pickle en la carpeta de tokens.")
        return

    log.info(f"🔎 {len(token_files)} tokens encontrados")
    log.info(f"📅 Rango: {start} → {end}\n")

    ok, err = [], []

    for fname in token_files:
        nombre_sub, channel_id = parse_token_filename(fname)
        if not nombre_sub:
            err.append(fname); continue
        if only_subcuenta and nombre_sub != only_subcuenta:
            continue

        log.info(f"{'='*60}")
        log.info(f"📺 {nombre_sub}  ({channel_id})")

        uspr = cuenta_tiene_uspr(nombre_sub)
        if uspr:
            log.info("  🔵 Cuenta con análisis US+PR activado")

        try:
            creds = load_credentials(os.path.join(tokens_dir, fname), nombre_sub)
            yt      = googleapiclient.discovery.build("youtubeAnalytics", "v2", credentials=creds)
            yt_data = googleapiclient.discovery.build("youtube",          "v3", credentials=creds)

            # ── Métricas diarias WW ──────────────────────────────
            df_daily = extract_daily(yt, start, end)
            if df_daily.empty:
                log.warning("  ⚠️  Sin datos diarios.")
                err.append(nombre_sub); continue
            save_daily(df_daily, nombre_sub)

            # ── Demografía WW ────────────────────────────────────
            save_demographics(extract_demographics(yt, start, end),
                              nombre_sub, start, end)

            # ── Países WW ────────────────────────────────────────
            save_country_views(extract_country_views(yt, start, end),
                               nombre_sub, start, end)

            # ── Términos de búsqueda WW ──────────────────────────
            save_search_terms(extract_search_terms(yt, start, end),
                              nombre_sub, start, end)

            # ── Top videos WW ────────────────────────────────────
            save_top_videos(
                extract_top_videos(yt, yt_data, channel_id, start, end),
                nombre_sub, start, end
            )

            # ── EXTRAS SOLO CUENTAS CON US+PR ───────────────────
            if uspr:
                uspr_f = "country==US,PR"

                # KPIs + fuentes + dispositivos US+PR (totales período)
                save_uspr_periodo(
                    extract_uspr_kpis(yt, start, end),
                    extract_uspr_traffic_sources(yt, start, end),
                    extract_uspr_devices(yt, start, end),
                    nombre_sub, start, end
                )

                save_demographics(
                    extract_demographics(yt, start, end, geo_filter=uspr_f),
                    nombre_sub, start, end
                )
                save_us_states(
                    extract_us_states(yt, start, end),
                    nombre_sub, start, end
                )
                save_search_terms(
                    extract_search_terms(yt, start, end, geo_filter=uspr_f),
                    nombre_sub, start, end
                )
                save_top_videos(
                    extract_top_videos(yt, yt_data, channel_id, start, end,
                                       geo_filter=uspr_f),
                    nombre_sub, start, end
                )

            ok.append(nombre_sub)

        except Exception as e:
            log.error(f"  ❌ Error con '{nombre_sub}': {e}", exc_info=True)
            err.append(nombre_sub)

    log.info(f"\n{'='*60}")
    log.info(f"📊 RESUMEN: ✅ {len(ok)} exitosas → {ok}")
    log.info(f"           ❌ {len(err)} con error  → {err}")