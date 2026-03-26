-- ============================================================
-- db/schema.sql
-- Base de datos completa para YouTube Analytics Dashboard
-- Ejecutar una sola vez sobre una BD vacía llamada "yt_analytics"
-- ============================================================

-- ============================================================
-- DIMENSIONES
-- ============================================================

-- Cuentas (grupos de canales: SONY, TELEMUNDO, etc.)
CREATE TABLE dim_cuenta (
    id_cuenta     SERIAL PRIMARY KEY,
    nombre_cuenta VARCHAR(100) NOT NULL UNIQUE
);

-- Usuarios del dashboard (uno por cuenta)
CREATE TABLE dim_usuario (
    id_usuario     SERIAL PRIMARY KEY,
    nombre_usuario VARCHAR(50)  NOT NULL UNIQUE,
    password_hash  VARCHAR(255) NOT NULL,
    id_cuenta      INT          NOT NULL,
    rol            VARCHAR(20)  NOT NULL DEFAULT 'viewer',  -- 'viewer' | 'admin'
    activo         BOOLEAN      NOT NULL DEFAULT TRUE,
    CONSTRAINT fk_usuario_cuenta FOREIGN KEY (id_cuenta)
        REFERENCES dim_cuenta(id_cuenta) ON DELETE CASCADE
);

-- Subcuentas / canales de YouTube
CREATE TABLE dim_subcuenta (
    id_subcuenta    SERIAL PRIMARY KEY,
    id_cuenta       INT          NOT NULL,
    nombre_subcuenta VARCHAR(150) NOT NULL,
    CONSTRAINT fk_subcuenta_cuenta FOREIGN KEY (id_cuenta)
        REFERENCES dim_cuenta(id_cuenta) ON DELETE CASCADE
);

-- Calendario de fechas (2020-01-01 → 2035-12-31)
CREATE TABLE dim_fecha (
    id_fecha   SERIAL PRIMARY KEY,
    fecha      DATE NOT NULL UNIQUE,
    anio       INT  NOT NULL,
    mes        INT  NOT NULL,
    dia        INT  NOT NULL,
    trimestre  INT  NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL
);

INSERT INTO dim_fecha (fecha, anio, mes, dia, trimestre, nombre_mes)
SELECT
    d::DATE,
    EXTRACT(YEAR  FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    EXTRACT(DAY   FROM d)::INT,
    CEIL(EXTRACT(MONTH FROM d)::INT / 3.0)::INT,
    TO_CHAR(d, 'TMMonth')
FROM generate_series('2020-01-01'::date, '2035-12-31'::date, interval '1 day') t(d);

-- ============================================================
-- TABLA DE HECHOS — MÉTRICAS DIARIAS
-- ============================================================
-- Una fila por (subcuenta × día).
-- Almacena métricas WW (worldwide) en todas las columnas.
-- Las métricas US+PR de Telemundo se guardan en tablas aparte.

CREATE TABLE hechos_metricas (
    id_hecho      SERIAL PRIMARY KEY,
    id_subcuenta  INT NOT NULL,
    id_fecha      INT NOT NULL,

    -- Generales
    views_total        BIGINT         DEFAULT 0,
    watch_time_total   NUMERIC(16,2)  DEFAULT 0,   -- horas
    revenue_total      NUMERIC(12,2)  DEFAULT 0,
    cpm_promedio       NUMERIC(10,2)  DEFAULT 0,
    likes_total        BIGINT         DEFAULT 0,
    suscriptores_total BIGINT         DEFAULT 0,

    -- Por tipo de contenido — Views
    views_videos   BIGINT  DEFAULT 0,
    views_shorts   BIGINT  DEFAULT 0,
    views_lives    BIGINT  DEFAULT 0,

    -- Por tipo de contenido — Watch Time (horas)
    watchtime_videos   NUMERIC(16,2)  DEFAULT 0,
    watchtime_shorts   NUMERIC(16,2)  DEFAULT 0,
    watchtime_lives    NUMERIC(16,2)  DEFAULT 0,

    -- Por tipo de contenido — Revenue
    revenue_videos   NUMERIC(12,2)  DEFAULT 0,
    revenue_shorts   NUMERIC(12,2)  DEFAULT 0,
    revenue_lives    NUMERIC(12,2)  DEFAULT 0,

    -- Por fuente de tráfico — Views
    views_search          BIGINT  DEFAULT 0,
    views_suggested       BIGINT  DEFAULT 0,
    views_external        BIGINT  DEFAULT 0,
    views_browse          BIGINT  DEFAULT 0,
    views_playlist        BIGINT  DEFAULT 0,
    views_short_feed      BIGINT  DEFAULT 0,
    views_directortunknown BIGINT DEFAULT 0,

    -- Por dispositivo — Views
    views_mobile   BIGINT  DEFAULT 0,
    views_tv       BIGINT  DEFAULT 0,
    views_computer BIGINT  DEFAULT 0,
    views_tablet   BIGINT  DEFAULT 0,

    -- Por dispositivo — Revenue
    revenue_mobile   NUMERIC(12,2)  DEFAULT 0,
    revenue_tv       NUMERIC(12,2)  DEFAULT 0,
    revenue_computer NUMERIC(12,2)  DEFAULT 0,
    revenue_tablet   NUMERIC(12,2)  DEFAULT 0,

    CONSTRAINT fk_hecho_subcuenta FOREIGN KEY (id_subcuenta)
        REFERENCES dim_subcuenta(id_subcuenta) ON DELETE CASCADE,
    CONSTRAINT fk_hecho_fecha FOREIGN KEY (id_fecha)
        REFERENCES dim_fecha(id_fecha)       ON DELETE CASCADE,
    CONSTRAINT uq_hecho UNIQUE (id_subcuenta, id_fecha)
);

CREATE INDEX idx_hecho_subcuenta ON hechos_metricas (id_subcuenta);
CREATE INDEX idx_hecho_fecha     ON hechos_metricas (id_fecha);

-- ============================================================
-- TABLAS COMPLEMENTARIAS — PERÍODO (no diarias)
-- ============================================================
-- Usan fecha_inicio / fecha_fin porque la API devuelve
-- el dato agregado para el rango consultado, no día a día.

-- Demografía (edad + género)
-- geo_group: 'WW' para todos | 'US_PR' solo Telemundo
CREATE TABLE hechos_demograficos (
    id             SERIAL PRIMARY KEY,
    id_subcuenta   INT          NOT NULL,
    fecha_inicio   DATE         NOT NULL,
    fecha_fin      DATE         NOT NULL,
    geo_group      VARCHAR(10)  NOT NULL DEFAULT 'WW',
    age_group      VARCHAR(20)  NOT NULL,
    gender         VARCHAR(10)  NOT NULL,
    viewer_pct     NUMERIC(8,4) NOT NULL DEFAULT 0,
    CONSTRAINT fk_demo_sub FOREIGN KEY (id_subcuenta)
        REFERENCES dim_subcuenta(id_subcuenta) ON DELETE CASCADE
);

CREATE INDEX idx_demo_sub    ON hechos_demograficos (id_subcuenta);
CREATE INDEX idx_demo_geo    ON hechos_demograficos (geo_group);
CREATE INDEX idx_demo_period ON hechos_demograficos (fecha_inicio, fecha_fin);

-- Geografía — Top países (WW, todas las cuentas)
CREATE TABLE hechos_geografia_paises (
    id            SERIAL PRIMARY KEY,
    id_subcuenta  INT         NOT NULL,
    fecha_inicio  DATE        NOT NULL,
    fecha_fin     DATE        NOT NULL,
    country       VARCHAR(5)  NOT NULL,
    views         BIGINT      NOT NULL DEFAULT 0,
    CONSTRAINT fk_geopais_sub FOREIGN KEY (id_subcuenta)
        REFERENCES dim_subcuenta(id_subcuenta) ON DELETE CASCADE
);

CREATE INDEX idx_geopais_sub    ON hechos_geografia_paises (id_subcuenta);
CREATE INDEX idx_geopais_period ON hechos_geografia_paises (fecha_inicio, fecha_fin);

-- Geografía — Top estados EE.UU. (solo Telemundo)
CREATE TABLE hechos_us_estados (
    id            SERIAL PRIMARY KEY,
    id_subcuenta  INT         NOT NULL,
    fecha_inicio  DATE        NOT NULL,
    fecha_fin     DATE        NOT NULL,
    state         VARCHAR(10) NOT NULL,
    views         BIGINT      NOT NULL DEFAULT 0,
    CONSTRAINT fk_estados_sub FOREIGN KEY (id_subcuenta)
        REFERENCES dim_subcuenta(id_subcuenta) ON DELETE CASCADE
);

CREATE INDEX idx_estados_sub ON hechos_us_estados (id_subcuenta);

-- Términos de búsqueda
-- geo_group: 'WW' para todos | 'US_PR' solo Telemundo
CREATE TABLE hechos_search_terms (
    id            SERIAL PRIMARY KEY,
    id_subcuenta  INT          NOT NULL,
    fecha_inicio  DATE         NOT NULL,
    fecha_fin     DATE         NOT NULL,
    geo_group     VARCHAR(10)  NOT NULL DEFAULT 'WW',
    search_term   VARCHAR(500) NOT NULL,
    views         BIGINT       NOT NULL DEFAULT 0,
    CONSTRAINT fk_search_sub FOREIGN KEY (id_subcuenta)
        REFERENCES dim_subcuenta(id_subcuenta) ON DELETE CASCADE
);

CREATE INDEX idx_search_sub    ON hechos_search_terms (id_subcuenta);
CREATE INDEX idx_search_geo    ON hechos_search_terms (geo_group);
CREATE INDEX idx_search_period ON hechos_search_terms (fecha_inicio, fecha_fin);

-- Top Videos
-- geo_group: 'WW' para todos | 'US_PR' solo Telemundo
CREATE TABLE hechos_top_videos (
    id            SERIAL PRIMARY KEY,
    id_subcuenta  INT          NOT NULL,
    fecha_inicio  DATE         NOT NULL,
    fecha_fin     DATE         NOT NULL,
    geo_group     VARCHAR(10)  NOT NULL DEFAULT 'WW',
    title         VARCHAR(500) NOT NULL,
    views         BIGINT       NOT NULL DEFAULT 0,
    CONSTRAINT fk_topvid_sub FOREIGN KEY (id_subcuenta)
        REFERENCES dim_subcuenta(id_subcuenta) ON DELETE CASCADE
);

CREATE INDEX idx_topvid_sub    ON hechos_top_videos (id_subcuenta);
CREATE INDEX idx_topvid_geo    ON hechos_top_videos (geo_group);
CREATE INDEX idx_topvid_period ON hechos_top_videos (fecha_inicio, fecha_fin);

-- Proyecciones mensuales por cuenta
CREATE TABLE proyecciones_mensuales (
    id_proyeccion        SERIAL PRIMARY KEY,
    id_cuenta            INT            NOT NULL,
    anio                 INT            NOT NULL,
    mes                  INT            NOT NULL,
    views_proyectadas    BIGINT,
    watchtime_proyectado BIGINT,
    revenue_proyectado   NUMERIC(12,2),
    creado_en            TIMESTAMP      NOT NULL DEFAULT NOW(),
    actualizado_en       TIMESTAMP      NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_proyeccion UNIQUE (id_cuenta, anio, mes),
    CONSTRAINT fk_proy_cuenta FOREIGN KEY (id_cuenta)
        REFERENCES dim_cuenta(id_cuenta) ON DELETE CASCADE
);

-- ============================================================
-- VISTAS
-- ============================================================

-- Vista principal de métricas diarias enriquecidas
CREATE OR REPLACE VIEW vw_metricas AS
SELECT
    h.id_hecho,
    c.id_cuenta,
    c.nombre_cuenta,
    s.id_subcuenta,
    s.nombre_subcuenta,
    f.fecha,
    f.anio,
    f.mes,
    f.trimestre,
    f.nombre_mes,
    h.views_total,
    h.watch_time_total,
    h.revenue_total,
    h.cpm_promedio,
    h.likes_total,
    h.suscriptores_total,
    h.views_videos,       h.views_shorts,       h.views_lives,
    h.watchtime_videos,   h.watchtime_shorts,   h.watchtime_lives,
    h.revenue_videos,     h.revenue_shorts,     h.revenue_lives,
    h.views_search,       h.views_suggested,    h.views_external,
    h.views_browse,       h.views_playlist,     h.views_short_feed,
    h.views_directortunknown,
    h.views_mobile,       h.views_tv,           h.views_computer,   h.views_tablet,
    h.revenue_mobile,     h.revenue_tv,         h.revenue_computer, h.revenue_tablet
FROM hechos_metricas    h
JOIN dim_subcuenta      s ON h.id_subcuenta = s.id_subcuenta
JOIN dim_cuenta         c ON s.id_cuenta    = c.id_cuenta
JOIN dim_fecha          f ON h.id_fecha     = f.id_fecha;

-- Vista de demografía
CREATE OR REPLACE VIEW vw_demograficos AS
SELECT d.*, s.nombre_subcuenta, c.nombre_cuenta, c.id_cuenta
FROM hechos_demograficos d
JOIN dim_subcuenta s ON d.id_subcuenta = s.id_subcuenta
JOIN dim_cuenta    c ON s.id_cuenta    = c.id_cuenta;

-- Vista de geografía — países
CREATE OR REPLACE VIEW vw_geografia_paises AS
SELECT g.*, s.nombre_subcuenta, c.nombre_cuenta, c.id_cuenta
FROM hechos_geografia_paises g
JOIN dim_subcuenta s ON g.id_subcuenta = s.id_subcuenta
JOIN dim_cuenta    c ON s.id_cuenta    = c.id_cuenta;

-- Vista de geografía — estados EE.UU.
CREATE OR REPLACE VIEW vw_us_estados AS
SELECT e.*, s.nombre_subcuenta, c.nombre_cuenta, c.id_cuenta
FROM hechos_us_estados e
JOIN dim_subcuenta s ON e.id_subcuenta = s.id_subcuenta
JOIN dim_cuenta    c ON s.id_cuenta    = c.id_cuenta;

-- Vista de términos de búsqueda
CREATE OR REPLACE VIEW vw_search_terms AS
SELECT t.*, s.nombre_subcuenta, c.nombre_cuenta, c.id_cuenta
FROM hechos_search_terms t
JOIN dim_subcuenta s ON t.id_subcuenta = s.id_subcuenta
JOIN dim_cuenta    c ON s.id_cuenta    = c.id_cuenta;

-- Vista de top videos
CREATE OR REPLACE VIEW vw_top_videos AS
SELECT v.*, s.nombre_subcuenta, c.nombre_cuenta, c.id_cuenta
FROM hechos_top_videos v
JOIN dim_subcuenta s ON v.id_subcuenta = s.id_subcuenta
JOIN dim_cuenta    c ON s.id_cuenta    = c.id_cuenta;

-- ============================================================
-- DATOS MAESTROS — edita según tu catálogo real
-- ============================================================

INSERT INTO dim_cuenta (nombre_cuenta) VALUES
    ('TELEMUNDO'),
    ('SONY'),
    ('FM ENTRETENIMIENTO'),
    ('YO CAMILO'),
    ('VICKY DAVILA'),
    ('JAIME GABALDONI'),
    ('ANDREA NAVARRO'),
	('JN MUSIC');

-- Usuarios (password en texto plano — reemplazar por hash en producción)
INSERT INTO dim_usuario (nombre_usuario, password_hash, id_cuenta, rol) VALUES
    ('telemundo',     '3816', 1, 'viewer'),
    ('sony',          '2358', 2, 'viewer'),
    ('fment',         '1688', 3, 'viewer'),
    ('yocamilo',      '7865', 4, 'viewer'),
    ('vickydavila',   '9834', 5, 'viewer'),
    ('jaimegab',      '8963', 6, 'viewer'),
    ('andreanro',     '9437', 7, 'viewer'),
	('jnmusic',       '4532', 8,'viewer');

-- Subcuentas SONY
INSERT INTO dim_subcuenta (id_cuenta, nombre_subcuenta) VALUES
    (2, 'Zorro: La Espada y la Rosa'),
    (2, 'The Good Doctor en Español'),
    (2, 'The Good Doctor em Portugues'),
    (2, 'The Blacklist en Español'),
    (2, 'The Blacklist em Portugues'),
    (2, 'Sony Novelas'),
    (2, 'Sitcoms Argentinos'),
    (2, 'SharkTank Mexico'),
    (2, 'SharkTank Colombia'),
    (2, 'SharkTank Brasil'),
    (2, 'SharkTank Latam'),
    (2, 'S.W.A.T. en español'),
    (2, 'S.W.A.T. em portugues'),
    (2, 'Rosario Tijeras'),
    (2, 'Retro Comedy Classics'),
    (2, 'Hollywood Clips en Español'),
    (2, 'Hollywood Clips em Portugues'),
    (2, 'Escape Perfecto'),
    (2, 'Doña Barbara'),
    (2, 'Clinica X'),
    (2, 'Casados Con Hijos Mexico'),
    (2, 'Casados Con Hijos Colombia'),
    (2, 'Casados Con Hijos Chile'),
    (2, 'Casados Con Hijos Argentina'),
    (2, 'Breaking Bad en Español'),
    (2, 'Breaking Bad em Portugues'),
    (2, 'Aventura Gastronomica');

-- Subcuentas TELEMUNDO
INSERT INTO dim_subcuenta (id_cuenta, nombre_subcuenta) VALUES
    (1, 'Al Rojo Vivo'),
    (1, 'Caso Cerrado'),
    (1, 'Decisiones'),
    (1, 'El Señor De Los Cielos'),
    (1, 'En Casa Con Telemundo'),
    (1, 'Hoy Dia'),
    (1, 'La Mesa Caliente'),
    (1, 'Noticias Telemundo'),
    (1, 'Sala De Parejas'),
    (1, 'Telemundo Deportes'),
    (1, 'Telemundo English'),
    (1, 'Telemundo Entretenimiento'),
    (1, 'Telemundo'),
    (1, 'Universo'),
    (1, 'Telemundo Series'),
    (1, 'Buzz');

-- Subcuentas otras cuentas
INSERT INTO dim_subcuenta (id_cuenta, nombre_subcuenta) VALUES
    (3, 'FM entretenimiento'),
    (4, 'Yo Camilo'),
    (5, 'Vicky Dávila'),
    (6, 'Jaime Gabaldoni'),
    (7, 'Andrea Navarro'),
	(8, 'JN Music');

-- Proyecciones ejemplo TELEMUNDO 2025
INSERT INTO proyecciones_mensuales (id_cuenta, anio, mes, views_proyectadas, watchtime_proyectado, revenue_proyectado) VALUES
    (1, 2025,  1, 430237349, 64613984, 628264),
    (1, 2025,  2, 385593577, 59810122, 717045),
    (1, 2025,  3, 410026896, 65126298, 747928),
    (1, 2025,  4, 384427800, 63878678, 683860),
    (1, 2025,  5, 386562358, 65305288, 655469),
    (1, 2025,  6, 420888751, 68136317, 764017),
    (1, 2025,  7, 417528354, 69153609, 780623),
    (1, 2025,  8, 366716050, 64923733, 701367),
    (1, 2025,  9, 361246133, 63368092, 771566),
    (1, 2025, 10, 395072510, 65617786, 852941),
    (1, 2025, 11, 395829978, 64993391, 730308),
    (1, 2025, 12, 395829978, 64993391, 730308);

CREATE TABLE hechos_uspr_periodo (
    id               SERIAL PRIMARY KEY,
    id_subcuenta     INT           NOT NULL,
    fecha_inicio     DATE          NOT NULL,
    fecha_fin        DATE          NOT NULL,
    views_total      BIGINT        DEFAULT 0,
    watch_time_total NUMERIC(16,2) DEFAULT 0,
    revenue_total    NUMERIC(12,2) DEFAULT 0,
    likes_total      BIGINT        DEFAULT 0,
    suscriptores_net BIGINT        DEFAULT 0,
    views_search          BIGINT  DEFAULT 0,
    views_suggested       BIGINT  DEFAULT 0,
    views_external        BIGINT  DEFAULT 0,
    views_browse          BIGINT  DEFAULT 0,
    views_playlist        BIGINT  DEFAULT 0,
    views_short_feed      BIGINT  DEFAULT 0,
    views_directortunknown BIGINT DEFAULT 0,
    views_mobile   BIGINT        DEFAULT 0,
    views_tv       BIGINT        DEFAULT 0,
    views_computer BIGINT        DEFAULT 0,
    views_tablet   BIGINT        DEFAULT 0,
    watchtime_mobile   NUMERIC(16,2) DEFAULT 0,
    watchtime_tv       NUMERIC(16,2) DEFAULT 0,
    watchtime_computer NUMERIC(16,2) DEFAULT 0,
    watchtime_tablet   NUMERIC(16,2) DEFAULT 0,
    CONSTRAINT fk_uspr_sub FOREIGN KEY (id_subcuenta)
        REFERENCES dim_subcuenta(id_subcuenta) ON DELETE CASCADE,
    CONSTRAINT uq_uspr_periodo UNIQUE (id_subcuenta, fecha_inicio, fecha_fin)
);

SELECT 'Schema creado exitosamente ✅' AS status;

