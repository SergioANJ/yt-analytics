-- 1. Eliminar vistas dependientes
DROP VIEW IF EXISTS vw_metricas;
DROP VIEW IF EXISTS vw_demograficos;

-- 2. Renombrar columnas revenue → watchtime por dispositivo
ALTER TABLE hechos_metricas
    RENAME COLUMN revenue_mobile   TO watchtime_mobile;
ALTER TABLE hechos_metricas
    RENAME COLUMN revenue_tv       TO watchtime_tv;
ALTER TABLE hechos_metricas
    RENAME COLUMN revenue_computer TO watchtime_computer;
ALTER TABLE hechos_metricas
    RENAME COLUMN revenue_tablet   TO watchtime_tablet;

-- 3. Cambiar tipo a NUMERIC(16,2) para horas
ALTER TABLE hechos_metricas
    ALTER COLUMN watchtime_mobile   TYPE NUMERIC(16,2) USING watchtime_mobile::NUMERIC,
    ALTER COLUMN watchtime_tv       TYPE NUMERIC(16,2) USING watchtime_tv::NUMERIC,
    ALTER COLUMN watchtime_computer TYPE NUMERIC(16,2) USING watchtime_computer::NUMERIC,
    ALTER COLUMN watchtime_tablet   TYPE NUMERIC(16,2) USING watchtime_tablet::NUMERIC;

-- 4. Corregir gender
ALTER TABLE hechos_demograficos
    ALTER COLUMN gender TYPE VARCHAR(50);

-- 5. Recrear vw_metricas
CREATE OR REPLACE VIEW vw_metricas AS
SELECT
    h.id_hecho, c.id_cuenta, c.nombre_cuenta, s.id_subcuenta, s.nombre_subcuenta,
    f.fecha, f.anio, f.mes, f.trimestre, f.nombre_mes,
    h.views_total, h.watch_time_total, h.revenue_total, h.cpm_promedio,
    h.likes_total, h.suscriptores_total,
    h.views_videos,     h.views_shorts,     h.views_lives,
    h.watchtime_videos, h.watchtime_shorts, h.watchtime_lives,
    h.revenue_videos,   h.revenue_shorts,   h.revenue_lives,
    h.views_search,     h.views_suggested,  h.views_external,
    h.views_browse,     h.views_playlist,   h.views_short_feed,
    h.views_directortunknown,
    h.views_mobile,       h.views_tv,           h.views_computer,     h.views_tablet,
    h.watchtime_mobile,   h.watchtime_tv,       h.watchtime_computer, h.watchtime_tablet
FROM hechos_metricas h
JOIN dim_subcuenta s ON h.id_subcuenta = s.id_subcuenta
JOIN dim_cuenta    c ON s.id_cuenta    = c.id_cuenta
JOIN dim_fecha     f ON h.id_fecha     = f.id_fecha;

-- 6. Recrear vw_demograficos
CREATE OR REPLACE VIEW vw_demograficos AS
SELECT d.*, s.nombre_subcuenta, c.nombre_cuenta, c.id_cuenta
FROM hechos_demograficos d
JOIN dim_subcuenta s ON d.id_subcuenta = s.id_subcuenta
JOIN dim_cuenta    c ON s.id_cuenta    = c.id_cuenta;

select *from vw_metricas;



-- ELIMINACIÓN DE TODA LA BASE DE DATOS, DE MANERA EN CASCADA
TRUNCATE TABLE hechos_metricas, hechos_demograficos, hechos_geografia_paises, 
               hechos_us_estados, hechos_search_terms, hechos_top_videos 
RESTART IDENTITY CASCADE;