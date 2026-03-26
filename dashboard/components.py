"""
dashboard/components.py
=======================
Componentes Streamlit reutilizables para el dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def _kpi_card(col, icon, label, value, color="#1971C2"):
    """Tarjeta KPI con texto siempre visible, sin truncar."""
    col.markdown(f"""
    <div style="background:#fff;border:1px solid #e0e0e0;border-top:3px solid {color};
                border-radius:10px;padding:14px 10px;text-align:center;
                box-shadow:0 1px 4px rgba(0,0,0,0.06);">
        <div style="font-size:10px;color:#888;text-transform:uppercase;
                    letter-spacing:1px;margin-bottom:6px;white-space:nowrap;">
            {icon} {label}
        </div>
        <div style="font-size:clamp(12px,1.5vw,20px);font-weight:700;color:#212529;
                    word-break:break-word;line-height:1.3;">
            {value}
        </div>
    </div>
    """, unsafe_allow_html=True)


def kpis(df: pd.DataFrame):
    if df.empty:
        st.warning("Sin datos para el período seleccionado.")
        return
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    _kpi_card(c1, "👁️",  "Views",         f"{df['views_total'].sum():,}",           "#1971C2")
    _kpi_card(c2, "⏱️",  "Watch Time (h)", f"{df['watch_time_total'].sum():,.0f}",   "#2F9E44")
    _kpi_card(c3, "💰",  "Revenue",        f"${df['revenue_total'].sum():,.2f}",     "#F76707")
    _kpi_card(c4, "📊",  "CPM promedio",   f"${df['cpm_promedio'].mean():,.2f}",     "#7048E8")
    _kpi_card(c5, "👍",  "Likes",          f"{df['likes_total'].sum():,}",           "#E03131")
    _kpi_card(c6, "📈",  "Suscriptores",   f"{df['suscriptores_total'].sum():,}",    "#1971C2")
    st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)


def kpis_uspr(row: pd.Series):
    """KPIs para datos de período US+PR (una sola fila agregada)."""
    c1, c2, c3, c4, c5 = st.columns(5)
    _kpi_card(c1, "👁️",  "Views US+PR",    f"{int(row.get('views_total', 0)):,}",              "#1971C2")
    _kpi_card(c2, "⏱️",  "Watch Time (h)", f"{float(row.get('watch_time_total', 0)):,.0f}",     "#2F9E44")
    _kpi_card(c3, "💰",  "Revenue",        f"${float(row.get('revenue_total', 0)):,.2f}",       "#F76707")
    _kpi_card(c4, "👍",  "Likes",          f"{int(row.get('likes_total', 0)):,}",               "#E03131")
    _kpi_card(c5, "📈",  "Suscriptores",   f"{int(row.get('suscriptores_net', 0)):,}",          "#7048E8")
    st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)


def fuentes_trafico_uspr(row: pd.Series, key_suffix="_uspr"):
    """Fuentes de tráfico para datos de período US+PR."""
    st.subheader("🚦 Fuentes de tráfico — US+PR")
    fuentes_cols = {
        "Búsqueda YouTube":      "views_search",
        "Videos sugeridos":      "views_suggested",
        "Externos":              "views_external",
        "Browse / Suscriptores": "views_browse",
        "Playlists":             "views_playlist",
        "Shorts feed":           "views_short_feed",
        "Directo/Desconocido":   "views_directortunknown",
    }
    totales = {k: int(row.get(v, 0)) for k, v in fuentes_cols.items()}
    df_f = pd.DataFrame(list(totales.items()), columns=["Fuente", "Views"])
    df_f = df_f[df_f["Views"] > 0].sort_values("Views", ascending=True)
    if df_f.empty:
        st.info("Sin datos de fuentes de tráfico para US+PR.")
        return
    df_f["Porcentaje"] = (df_f["Views"] / df_f["Views"].sum() * 100).round(2)
    c1, c2 = st.columns([0.6, 0.4])
    with c1:
        fig = px.bar(df_f, x="Views", y="Fuente", orientation="h",
                     color="Fuente",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(showlegend=False, margin=dict(l=0))
        st.plotly_chart(fig, use_container_width=True, key=f"fuentes{key_suffix}")
    with c2:
        st.dataframe(
            df_f[["Fuente", "Views", "Porcentaje"]].sort_values("Views", ascending=False)
            .style.format({"Views": "{:,.0f}", "Porcentaje": "{:.2f}%"}),
            hide_index=True, use_container_width=True
        )


def dispositivos_uspr(row: pd.Series, key_suffix="_uspr"):
    """Dispositivos para datos de período US+PR."""
    st.subheader("💻 Dispositivos — US+PR")
    mode = st.radio("Métrica:", ["Views", "Watch Time"],
                    horizontal=True, key=f"dev{key_suffix}")
    mapa = {
        "Views":      {"Móvil": "views_mobile",      "TV": "views_tv",      "Computador": "views_computer",      "Tablet": "views_tablet"},
        "Watch Time": {"Móvil": "watchtime_mobile",  "TV": "watchtime_tv",  "Computador": "watchtime_computer",  "Tablet": "watchtime_tablet"},
    }
    res = {k: float(row.get(v, 0)) for k, v in mapa[mode].items()}
    df_d = pd.DataFrame(list(res.items()), columns=["Dispositivo", mode])
    df_d = df_d[df_d[mode] > 0]
    if df_d.empty:
        st.info("Sin datos de dispositivos para US+PR.")
        return
    df_d["Porcentaje"] = (df_d[mode] / df_d[mode].sum() * 100).round(2)
    c1, c2 = st.columns([0.6, 0.4])
    with c1:
        fig = px.bar(df_d.sort_values(mode, ascending=True),
                     x=mode, y="Dispositivo", orientation="h",
                     text=df_d["Porcentaje"].map("{:.1f}%".format),
                     color="Dispositivo",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key=f"dispositivos{key_suffix}")
    with c2:
        st.dataframe(df_d.style.format({mode: "{:,.0f}", "Porcentaje": "{:.2f}%"}),
                     hide_index=True, use_container_width=True)


def evolucion(df: pd.DataFrame, key_suffix=""):
    if df.empty:
        return
    st.subheader("📊 Evolución temporal")
    opciones = {"Views": "views_total", "Watch Time": "watch_time_total", "Revenue": "revenue_total"}
    c1, c2 = st.columns([0.3, 0.7])
    with c1:
        agrup = st.radio("Agrupar por:", ["Mensual", "Trimestral", "Acumulado"],
                         horizontal=False, key=f"agrup{key_suffix}")
    with c2:
        sel = st.multiselect("Métricas:", list(opciones.keys()),
                             default=list(opciones.keys()), key=f"sel_met{key_suffix}")
    if not sel:
        return
    cols  = [opciones[m] for m in sel]
    df2   = df.copy()
    df2["fecha"] = pd.to_datetime(df2["fecha"])
    df2   = df2.sort_values("fecha").set_index("fecha")
    freq  = {"Mensual": "ME", "Trimestral": "QE", "Acumulado": "ME"}[agrup]
    dfp   = df2[cols].resample(freq).sum()
    if agrup == "Acumulado":
        dfp = dfp.cumsum()
    st.line_chart(dfp)


def tipo_contenido(df: pd.DataFrame, key_suffix=""):
    if df.empty:
        return
    st.subheader("🥧 Distribución por tipo de contenido")
    opcion = st.selectbox("Métrica:", ["Views", "Watch Time", "Revenue"],
                          key=f"pie_tipo{key_suffix}")
    mapa = {
        "Views":      {"Videos": "views_videos",    "Shorts": "views_shorts",    "Lives": "views_lives"},
        "Watch Time": {"Videos": "watchtime_videos", "Shorts": "watchtime_shorts","Lives": "watchtime_lives"},
        "Revenue":    {"Videos": "revenue_videos",   "Shorts": "revenue_shorts",  "Lives": "revenue_lives"},
    }
    res = {k: df[v].sum() for k, v in mapa[opcion].items()}
    df_p = pd.DataFrame(list(res.items()), columns=["Tipo", opcion])
    df_p["Porcentaje"] = (df_p[opcion] / df_p[opcion].sum() * 100).round(2)
    c1, c2 = st.columns([0.6, 0.4])
    with c1:
        fig = px.pie(df_p, values=opcion, names="Tipo",
                     color_discrete_sequence=px.colors.qualitative.Set2, hole=0.4)
        fig.update_traces(textinfo="percent+label", textposition="inside")
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True, key=f"tipo{key_suffix}")
    with c2:
        st.dataframe(df_p.style.format({opcion: "{:,.0f}", "Porcentaje": "{:.2f}%"}),
                     hide_index=True, use_container_width=True)


def fuentes_trafico(df: pd.DataFrame, key_suffix=""):
    if df.empty:
        return
    st.subheader("🚦 Fuentes de tráfico")
    fuentes_cols = {
        "Búsqueda YouTube":      "views_search",
        "Videos sugeridos":      "views_suggested",
        "Externos":              "views_external",
        "Browse / Suscriptores": "views_browse",
        "Playlists":             "views_playlist",
        "Shorts feed":           "views_short_feed",
        "Directo/Desconocido":   "views_directortunknown",
    }
    totales = {k: df[v].sum() for k, v in fuentes_cols.items() if v in df.columns}
    df_f    = pd.DataFrame(list(totales.items()), columns=["Fuente", "Views"])
    df_f    = df_f.sort_values("Views", ascending=True)
    df_f["Porcentaje"] = (df_f["Views"] / df_f["Views"].sum() * 100).round(2)
    c1, c2  = st.columns([0.6, 0.4])
    with c1:
        fig = px.bar(df_f, x="Views", y="Fuente", orientation="h",
                     color="Fuente",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(showlegend=False, margin=dict(l=0))
        st.plotly_chart(fig, use_container_width=True, key=f"fuentes{key_suffix}")
    with c2:
        st.dataframe(
            df_f[["Fuente","Views","Porcentaje"]].sort_values("Views", ascending=False)
            .style.format({"Views": "{:,.0f}", "Porcentaje": "{:.2f}%"}),
            hide_index=True, use_container_width=True
        )


def dispositivos(df: pd.DataFrame, key_suffix=""):
    if df.empty:
        return
    st.subheader("💻 Dispositivos")
    mode = st.radio("Métrica:", ["Views", "Watch Time"],
                    horizontal=True, key=f"dev{key_suffix}")
    
    mapa = {
        "Views":      {"Móvil": "views_mobile",      "TV": "views_tv",      "Computador": "views_computer",      "Tablet": "views_tablet"},
        "Watch Time": {"Móvil": "watchtime_mobile",  "TV": "watchtime_tv",  "Computador": "watchtime_computer",  "Tablet": "watchtime_tablet"},
    }
    
    res  = {k: df[v].sum() for k, v in mapa[mode].items() if v in df.columns}
    df_d = pd.DataFrame(list(res.items()), columns=["Dispositivo", mode])
    df_d["Porcentaje"] = (df_d[mode] / df_d[mode].sum() * 100).round(2)
    
    # --- CORRECCIÓN: Ordenar el dataframe antes ---
    df_d = df_d.sort_values(mode, ascending=True)
    
    c1, c2 = st.columns([0.6, 0.4])
    with c1:
        # Usamos el dataframe ya ordenado y pasamos el nombre de la columna a 'text'
        fig = px.bar(df_d,
                     x=mode, y="Dispositivo", orientation="h",
                     text="Porcentaje", # Pasamos solo el nombre de la columna
                     color="Dispositivo",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        
        # Formateamos el texto para que aparezca con % dentro del gráfico
        fig.update_traces(texttemplate='%{text:.1f}%', textposition="outside")
        
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key=f"dispositivos{key_suffix}")
    
    with c2:
        # El dataframe ya está ordenado también para la tabla
        st.dataframe(df_d.style.format({mode: "{:,.0f}", "Porcentaje": "{:.2f}%"}),
                     hide_index=True, use_container_width=True)

def demografia(df_demo: pd.DataFrame, key_suffix=""):
    if df_demo.empty:
        st.info("Sin datos demográficos para el período/filtro.")
        return
    st.subheader("👥 Edad y Género")
    c1, c2 = st.columns(2)
    with c1:
        df_a = df_demo.groupby("age_group")["viewer_pct"].sum().reset_index()
        fig  = px.bar(df_a.sort_values("age_group"),
                      x="age_group", y="viewer_pct",
                      title="Distribución por edad",
                      labels={"age_group": "Grupo de edad", "viewer_pct": "% espectadores"},
                      color_discrete_sequence=["#636EFA"])
        st.plotly_chart(fig, use_container_width=True, key=f"demo_edad{key_suffix}")
    with c2:
        df_g = df_demo.groupby("gender")["viewer_pct"].sum().reset_index()
        fig  = px.pie(df_g, values="viewer_pct", names="gender",
                      title="Distribución por género",
                      color_discrete_sequence=["#EF553B","#636EFA","#00CC96"], hole=0.4)
        st.plotly_chart(fig, use_container_width=True, key=f"demo_genero{key_suffix}")
    # Heatmap
    piv = df_demo.pivot_table(values="viewer_pct", index="age_group",
                               columns="gender", aggfunc="sum", fill_value=0)
    fig_h = px.imshow(piv, title="Mapa de calor Edad × Género",
                      labels=dict(x="Género", y="Grupo de edad", color="%"),
                      color_continuous_scale="YlOrRd", text_auto=".1f")
    st.plotly_chart(fig_h, use_container_width=True, key=f"demo_heatmap{key_suffix}")


def top_videos(df: pd.DataFrame, title="🎬 Top Videos"):
    st.subheader(title)
    if df.empty:
        st.info("Sin datos de videos para el período.")
        return
    top10 = df.sort_values("views", ascending=False).head(10).reset_index(drop=True)
    rank_colors = {1: "#E03131", 2: "#F76707", 3: "#1971C2"}
    col_a, col_b = st.columns(2)
    for col, start in [(col_a, 0), (col_b, 5)]:
        with col:
            for i in range(start, min(start + 5, len(top10))):
                row = top10.iloc[i]
                rank = i + 1
                color = rank_colors.get(rank, "#868E96")
                st.markdown(f"""
                <div style="background:#fff;border:1px solid #e8e8e8;border-radius:9px;
                            padding:11px 15px;margin-bottom:8px;display:flex;
                            align-items:center;gap:12px;
                            box-shadow:0 1px 3px rgba(0,0,0,0.05);">
                    <div style="font-size:16px;font-weight:800;color:{color};
                                min-width:24px;font-family:monospace;">
                        {rank:02d}
                    </div>
                    <div style="flex:1;overflow:hidden;">
                        <div style="font-size:13px;font-weight:600;color:#212529;
                                    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">
                            {row['title']}
                        </div>
                        <div style="font-size:11px;color:#868E96;margin-top:2px;">
                            👁️ {int(row['views']):,} views
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)


def search_terms(df: pd.DataFrame, title="🔍 Términos de búsqueda"):
    st.subheader(title)
    if df.empty:
        st.info("Sin datos de búsquedas para el período.")
        return
    c1, c2 = st.columns([0.6, 0.4])
    with c1:
        fig = px.bar(df.head(15).sort_values("views"),
                     x="views", y="search_term", orientation="h",
                     color_discrete_sequence=["#AB63FA"])
        fig.update_layout(yaxis_title=None, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.dataframe(df[["search_term","views"]].style.format({"views": "{:,.0f}"}),
                     hide_index=True, use_container_width=True)


def geografia_paises(df: pd.DataFrame):
    if df.empty:
        st.info("Sin datos geográficos disponibles.")
        return
    st.subheader("🌍 Análisis Geográfico — Países")

    # YouTube Analytics devuelve ISO-2 (US, MX…); Plotly choropleth necesita ISO-3 (USA, MEX…)
    ISO2_TO_ISO3 = {
        "AF":"AFG","AL":"ALB","DZ":"DZA","AO":"AGO","AR":"ARG","AM":"ARM","AU":"AUS",
        "AT":"AUT","AZ":"AZE","BS":"BHS","BH":"BHR","BD":"BGD","BY":"BLR","BE":"BEL",
        "BZ":"BLZ","BJ":"BEN","BO":"BOL","BA":"BIH","BW":"BWA","BR":"BRA","BN":"BRN",
        "BG":"BGR","BF":"BFA","BI":"BDI","KH":"KHM","CM":"CMR","CA":"CAN","CV":"CPV",
        "CF":"CAF","TD":"TCD","CL":"CHL","CN":"CHN","CO":"COL","CG":"COG","CD":"COD",
        "CR":"CRI","CI":"CIV","HR":"HRV","CU":"CUB","CY":"CYP","CZ":"CZE","DK":"DNK",
        "DJ":"DJI","DO":"DOM","EC":"ECU","EG":"EGY","SV":"SLV","GQ":"GNQ","ER":"ERI",
        "EE":"EST","ET":"ETH","FI":"FIN","FR":"FRA","GA":"GAB","GM":"GMB","GE":"GEO",
        "DE":"DEU","GH":"GHA","GR":"GRC","GT":"GTM","GN":"GIN","GW":"GNB","GY":"GUY",
        "HT":"HTI","HN":"HND","HK":"HKG","HU":"HUN","IN":"IND","ID":"IDN","IR":"IRN",
        "IQ":"IRQ","IE":"IRL","IL":"ISR","IT":"ITA","JM":"JAM","JP":"JPN","JO":"JOR",
        "KZ":"KAZ","KE":"KEN","KW":"KWT","KG":"KGZ","LA":"LAO","LV":"LVA","LB":"LBN",
        "LR":"LBR","LY":"LBY","LT":"LTU","LU":"LUX","MK":"MKD","MG":"MDG","MW":"MWI",
        "MY":"MYS","MV":"MDV","ML":"MLI","MT":"MLT","MR":"MRT","MX":"MEX","MD":"MDA",
        "MN":"MNG","ME":"MNE","MA":"MAR","MZ":"MOZ","MM":"MMR","NA":"NAM","NP":"NPL",
        "NL":"NLD","NZ":"NZL","NI":"NIC","NE":"NER","NG":"NGA","NO":"NOR","OM":"OMN",
        "PK":"PAK","PA":"PAN","PG":"PNG","PY":"PRY","PE":"PER","PH":"PHL","PL":"POL",
        "PT":"PRT","PR":"PRI","QA":"QAT","RO":"ROU","RU":"RUS","RW":"RWA","SA":"SAU",
        "SN":"SEN","RS":"SRB","SL":"SLE","SO":"SOM","ZA":"ZAF","SS":"SSD","ES":"ESP",
        "LK":"LKA","SD":"SDN","SR":"SUR","SZ":"SWZ","SE":"SWE","CH":"CHE","SY":"SYR",
        "TW":"TWN","TJ":"TJK","TZ":"TZA","TH":"THA","TL":"TLS","TG":"TGO","TT":"TTO",
        "TN":"TUN","TR":"TUR","TM":"TKM","UG":"UGA","UA":"UKR","AE":"ARE","GB":"GBR",
        "US":"USA","UY":"URY","UZ":"UZB","VE":"VEN","VN":"VNM","YE":"YEM","ZM":"ZMB",
        "ZW":"ZWE","IS":"ISL","EC":"ECU","MU":"MUS","MO":"MAC","PS":"PSE","XK":"XKX",
        "CW":"CUW","AW":"ABW","BQ":"BES","SX":"SXM","TC":"TCA","VG":"VGB","KY":"CYM",
        "BB":"BRB","LC":"LCA","VC":"VCT","GD":"GRD","AG":"ATG","DM":"DMA","KN":"KNA",
    }
    df_map = df.copy()
    df_map["iso3"] = df_map["country"].map(ISO2_TO_ISO3)
    df_map = df_map.dropna(subset=["iso3"])

    c1, c2 = st.columns([0.65, 0.35])
    with c1:
        fig = px.choropleth(
            df_map,
            locations="iso3",
            locationmode="ISO-3",
            color="views",
            color_continuous_scale="Reds",
            hover_name="country",
            hover_data={"views": ":,", "iso3": False},
            title="Views por País",
        )
        fig.update_geos(
            showframe=False,
            showcoastlines=True,  coastlinecolor="#cccccc",
            showland=True,        landcolor="#f5f5f5",
            showocean=True,       oceancolor="#dce9f5",
            showcountries=True,   countrycolor="#cccccc",
        )
        fig.update_layout(margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig2 = px.bar(df.head(10).sort_values("views"),
                      x="views", y="country", orientation="h",
                      title="Top 10 países",
                      color_discrete_sequence=["#EF553B"])
        fig2.update_layout(yaxis_title=None, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)


def geografia_estados(df: pd.DataFrame):
    if df.empty:
        st.info("Sin datos de estados disponibles.")
        return
    st.subheader("🗺️ Análisis por Estado — EE.UU.")
    c1, c2 = st.columns([0.65, 0.35])
    with c1:
        fig = px.choropleth(df, locations="state", locationmode="USA-states",
                            color="views", color_continuous_scale="Blues",
                            scope="usa", title="Views por Estado en EE.UU.")
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.dataframe(df.head(15).style.format({"views": "{:,.0f}"}),
                     hide_index=True, use_container_width=True)


def progreso_anual(id_cuenta, ids_sub, get_proyecciones_fn, get_metricas_fn):
    st.header("📆 Progreso Global Anual")
    anio = st.selectbox("Año:", [2026, 2025], index=1, key="anio_prog")

    proy    = get_proyecciones_fn(id_cuenta, anio)
    df_year = get_metricas_fn(id_cuenta, ids_sub, anio)

    if proy.empty and df_year.empty:
        st.warning(f"Sin datos de proyecciones ni métricas para {anio}.")
        return

    df = pd.merge(proy, df_year, on="mes", how="outer").fillna(0)
    df["views_real_acum"] = df["views_total"].cumsum()
    df["rev_real_acum"]   = df["revenue_total"].cumsum()
    df["views_proj_acum"] = df["views_proyectadas"].cumsum()
    df["rev_proj_acum"]   = df["revenue_proyectado"].cumsum()
    df["views_pct"] = (df["views_real_acum"] / df["views_proj_acum"].replace(0,1) * 100)
    df["rev_pct"]   = (df["rev_real_acum"]   / df["rev_proj_acum"].replace(0,1)   * 100)

    row = df.iloc[-1]
    c1, c2 = st.columns(2)
    c1.metric("Views — Cumplimiento", f"{row['views_pct']:.1f}%",
              f"Real: {row['views_real_acum']:,.0f} | Proy: {row['views_proj_acum']:,.0f}")
    c2.metric("Revenue — Cumplimiento", f"{row['rev_pct']:.1f}%",
              f"Real: {row['rev_real_acum']:,.2f} | Proy: {row['rev_proj_acum']:,.2f}")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["mes"], y=df["views_real_acum"],
                             mode="lines+markers", name="Real", hovertemplate="%{y:,}"))
    fig.add_trace(go.Scatter(x=df["mes"], y=df["views_proj_acum"],
                             mode="lines+markers", name="Proyectado",
                             line=dict(dash="dash"), hovertemplate="%{y:,}"))
    fig.update_layout(title=f"Views Acumuladas {anio}: Real vs Proyectado",
                      xaxis_title="Mes", yaxis_title="Views acumuladas",
                      template="plotly_white", hovermode="x unified",
                      yaxis=dict(tickformat=","))
    st.plotly_chart(fig, use_container_width=True)