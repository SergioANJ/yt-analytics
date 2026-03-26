"""
dashboard/queries.py
====================
Todas las consultas SQL del dashboard en un solo lugar.
"""

import pandas as pd
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db.connection import engine


def get_daily(id_cuenta, ids_sub, f0, f1) -> pd.DataFrame:
    if not ids_sub:
        return pd.DataFrame()
    return pd.read_sql("""
        SELECT
            fecha,
            SUM(views_total)          AS views_total,
            SUM(watch_time_total)     AS watch_time_total,
            SUM(revenue_total)        AS revenue_total,
            ROUND(AVG(cpm_promedio),2) AS cpm_promedio,
            SUM(likes_total)          AS likes_total,
            SUM(suscriptores_total)   AS suscriptores_total,
            SUM(views_videos)         AS views_videos,
            SUM(views_shorts)         AS views_shorts,
            SUM(views_lives)          AS views_lives,
            SUM(watchtime_videos)     AS watchtime_videos,
            SUM(watchtime_shorts)     AS watchtime_shorts,
            SUM(watchtime_lives)      AS watchtime_lives,
            SUM(revenue_videos)       AS revenue_videos,
            SUM(revenue_shorts)       AS revenue_shorts,
            SUM(revenue_lives)        AS revenue_lives,
            SUM(views_search)         AS views_search,
            SUM(views_suggested)      AS views_suggested,
            SUM(views_external)       AS views_external,
            SUM(views_browse)         AS views_browse,
            SUM(views_playlist)       AS views_playlist,
            SUM(views_short_feed)     AS views_short_feed,
            SUM(views_directortunknown) AS views_directortunknown,
            SUM(views_mobile)         AS views_mobile,
            SUM(views_tv)             AS views_tv,
            SUM(views_computer)       AS views_computer,
            SUM(views_tablet)         AS views_tablet,
            SUM(watchtime_mobile)     AS watchtime_mobile,
            SUM(watchtime_tv)         AS watchtime_tv,
            SUM(watchtime_computer)   AS watchtime_computer,
            SUM(watchtime_tablet)     AS watchtime_tablet
        FROM vw_metricas
        WHERE id_cuenta = %s
          AND id_subcuenta = ANY(%s)
          AND fecha BETWEEN %s AND %s
        GROUP BY fecha
        ORDER BY fecha
    """, con=engine, params=(id_cuenta, ids_sub, f0, f1))


def get_demograficos(id_cuenta, ids_sub, f0, f1, geo="WW") -> pd.DataFrame:
    if not ids_sub:
        return pd.DataFrame()
    return pd.read_sql("""
        SELECT age_group, gender, SUM(viewer_pct) AS viewer_pct
        FROM vw_demograficos
        WHERE id_cuenta = %s AND id_subcuenta = ANY(%s)
          AND geo_group = %s
          AND fecha_inicio <= %s AND fecha_fin >= %s
        GROUP BY age_group, gender
    """, con=engine, params=(id_cuenta, ids_sub, geo, f1, f0))


def get_paises(id_cuenta, ids_sub, f0, f1) -> pd.DataFrame:
    if not ids_sub:
        return pd.DataFrame()
    return pd.read_sql("""
        SELECT country, SUM(views) AS views
        FROM vw_geografia_paises
        WHERE id_cuenta = %s AND id_subcuenta = ANY(%s)
          AND fecha_inicio <= %s AND fecha_fin >= %s
        GROUP BY country
        ORDER BY views DESC
        LIMIT 20
    """, con=engine, params=(id_cuenta, ids_sub, f1, f0))


def get_estados(id_cuenta, ids_sub, f0, f1) -> pd.DataFrame:
    if not ids_sub:
        return pd.DataFrame()
    return pd.read_sql("""
        SELECT state, SUM(views) AS views
        FROM vw_us_estados
        WHERE id_cuenta = %s AND id_subcuenta = ANY(%s)
          AND fecha_inicio <= %s AND fecha_fin >= %s
        GROUP BY state
        ORDER BY views DESC
        LIMIT 25
    """, con=engine, params=(id_cuenta, ids_sub, f1, f0))


def get_search_terms(id_cuenta, ids_sub, f0, f1, geo="WW") -> pd.DataFrame:
    if not ids_sub:
        return pd.DataFrame()
    return pd.read_sql("""
        SELECT search_term, SUM(views) AS views
        FROM vw_search_terms
        WHERE id_cuenta = %s AND id_subcuenta = ANY(%s)
          AND geo_group = %s
          AND fecha_inicio <= %s AND fecha_fin >= %s
        GROUP BY search_term
        ORDER BY views DESC
        LIMIT 25
    """, con=engine, params=(id_cuenta, ids_sub, geo, f1, f0))


def get_top_videos(id_cuenta, ids_sub, f0, f1, geo="WW") -> pd.DataFrame:
    if not ids_sub:
        return pd.DataFrame()
    return pd.read_sql("""
        SELECT title, SUM(views) AS views
        FROM vw_top_videos
        WHERE id_cuenta = %s AND id_subcuenta = ANY(%s)
          AND geo_group = %s
          AND fecha_inicio <= %s AND fecha_fin >= %s
        GROUP BY title
        ORDER BY views DESC
        LIMIT 15
    """, con=engine, params=(id_cuenta, ids_sub, geo, f1, f0))


def get_uspr_periodo(id_cuenta, ids_sub, f0, f1) -> pd.DataFrame:
    """KPIs, fuentes de tráfico y dispositivos US+PR agregados del período."""
    if not ids_sub:
        return pd.DataFrame()
    return pd.read_sql("""
        SELECT
            COALESCE(SUM(views_total), 0)            AS views_total,
            COALESCE(SUM(watch_time_total), 0)       AS watch_time_total,
            COALESCE(SUM(revenue_total), 0)          AS revenue_total,
            COALESCE(SUM(likes_total), 0)            AS likes_total,
            COALESCE(SUM(suscriptores_net), 0)       AS suscriptores_net,
            COALESCE(SUM(views_search), 0)           AS views_search,
            COALESCE(SUM(views_suggested), 0)        AS views_suggested,
            COALESCE(SUM(views_external), 0)         AS views_external,
            COALESCE(SUM(views_browse), 0)           AS views_browse,
            COALESCE(SUM(views_playlist), 0)         AS views_playlist,
            COALESCE(SUM(views_short_feed), 0)       AS views_short_feed,
            COALESCE(SUM(views_directortunknown), 0) AS views_directortunknown,
            COALESCE(SUM(views_mobile), 0)           AS views_mobile,
            COALESCE(SUM(views_tv), 0)               AS views_tv,
            COALESCE(SUM(views_computer), 0)         AS views_computer,
            COALESCE(SUM(views_tablet), 0)           AS views_tablet,
            COALESCE(SUM(watchtime_mobile), 0)       AS watchtime_mobile,
            COALESCE(SUM(watchtime_tv), 0)           AS watchtime_tv,
            COALESCE(SUM(watchtime_computer), 0)     AS watchtime_computer,
            COALESCE(SUM(watchtime_tablet), 0)       AS watchtime_tablet
        FROM hechos_uspr_periodo u
        JOIN dim_subcuenta s ON u.id_subcuenta = s.id_subcuenta
        WHERE s.id_cuenta = %s
          AND u.id_subcuenta = ANY(%s)
          AND fecha_inicio >= %s AND fecha_fin <= %s
    """, con=engine, params=(id_cuenta, ids_sub, f0, f1))


def get_proyecciones(id_cuenta, anio) -> pd.DataFrame:
    return pd.read_sql("""
        SELECT mes, views_proyectadas, watchtime_proyectado, revenue_proyectado
        FROM proyecciones_mensuales
        WHERE id_cuenta = %s AND anio = %s
        ORDER BY mes
    """, con=engine, params=(id_cuenta, anio))


def get_metricas_anuales(id_cuenta, ids_sub, anio) -> pd.DataFrame:
    if ids_sub:
        return pd.read_sql("""
            SELECT
                EXTRACT(MONTH FROM fecha)::INT AS mes,
                SUM(views_total)       AS views_total,
                SUM(watch_time_total)  AS watch_time_total,
                SUM(revenue_total)     AS revenue_total
            FROM vw_metricas
            WHERE id_cuenta = %s AND EXTRACT(YEAR FROM fecha) = %s
              AND id_subcuenta = ANY(%s)
            GROUP BY 1 ORDER BY 1
        """, con=engine, params=(id_cuenta, anio, ids_sub))
    else:
        return pd.read_sql("""
            SELECT
                EXTRACT(MONTH FROM fecha)::INT AS mes,
                SUM(views_total)       AS views_total,
                SUM(watch_time_total)  AS watch_time_total,
                SUM(revenue_total)     AS revenue_total
            FROM vw_metricas
            WHERE id_cuenta = %s AND EXTRACT(YEAR FROM fecha) = %s
            GROUP BY 1 ORDER BY 1
        """, con=engine, params=(id_cuenta, anio))


def get_fecha_range(id_cuenta):
    df = pd.read_sql("""
        SELECT MIN(f.fecha) AS fmin, MAX(f.fecha) AS fmax
        FROM hechos_metricas h
        JOIN dim_fecha f      ON h.id_fecha     = f.id_fecha
        JOIN dim_subcuenta s  ON h.id_subcuenta = s.id_subcuenta
        WHERE s.id_cuenta = %s
    """, con=engine, params=(id_cuenta,))
    return df["fmin"].iloc[0], df["fmax"].iloc[0]


def get_nombre_cuenta(id_cuenta) -> str:
    df = pd.read_sql(
        "SELECT nombre_cuenta FROM dim_cuenta WHERE id_cuenta = %s LIMIT 1",
        con=engine, params=(int(id_cuenta),)
    )
    return df.iloc[0]["nombre_cuenta"].upper() if not df.empty else ""


def verificar_usuario(nombre, password):
    df = pd.read_sql(
        "SELECT id_usuario, id_cuenta, nombre_usuario, password_hash "
        "FROM dim_usuario WHERE nombre_usuario = %s AND activo = TRUE",
        con=engine, params=(nombre,)
    )
    if len(df) == 1 and password == df.loc[0, "password_hash"]:
        return df.iloc[0]
    return None


def get_subcuentas(id_cuenta) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT id_subcuenta, nombre_subcuenta FROM dim_subcuenta "
        "WHERE id_cuenta = %s ORDER BY nombre_subcuenta",
        con=engine, params=(id_cuenta,)
    )