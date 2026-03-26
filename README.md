# 📊 YouTube Analytics Dashboard

Pipeline automatizado + Dashboard Streamlit para métricas de YouTube Analytics.

---

## Estructura del proyecto

```
yt_analytics/
├── config/
│   └── settings.py          ← ⚙️  TODA la configuración está aquí
├── db/
│   ├── schema.sql            ← Crear la BD desde cero (ejecutar una vez)
│   └── connection.py         ← Motor SQLAlchemy
├── pipeline/
│   ├── extractor.py          ← Extracción API + carga en PostgreSQL
│   └── run.py                ← Punto de entrada CLI
├── dashboard/
│   ├── app.py                ← Aplicación Streamlit (ejecutar con streamlit run)
│   ├── queries.py            ← Todas las consultas SQL
│   └── components.py         ← Componentes gráficos reutilizables
├── llm/                      ← Módulo chatbot (opcional)
│   ├── pdf_loader.py         ← Carga PDFs por cuenta
│   ├── rag_pipeline.py       ← Construye índice FAISS para búsqueda semántica
│   └── hybrid_agent.py       ← Agente que combina BD + PDFs + GPT
├── tokens/                   ← Carpeta con los .pickle de cada canal
├── pdfs/                     ← PDFs por cuenta: pdfs/{id_cuenta}/*.pdf
├── client_secret.json        ← Credenciales OAuth de Google Cloud
├── .env                      ← OPENAI_API_KEY (no subir a git)
├── requirements.txt
└── README.md
```

---

## 1. Instalación

```bash
# Crear entorno virtual
python -m venv env
source env/bin/activate        # Windows: env\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

---

## 2. Crear la base de datos

```bash
# En PostgreSQL, crear la base de datos vacía
psql -U postgres -c "CREATE DATABASE yt_analytics;"

# Ejecutar el schema (crea tablas, vistas y datos maestros)
psql -U soporte -d yt_analytics -f db/schema.sql
```

> El schema ya incluye cuentas, usuarios, subcuentas y proyecciones de ejemplo.
> Edita `db/schema.sql` al final para agregar o modificar los datos maestros.

---

## 3. Configuración

Edita **`config/settings.py`** con tus credenciales de BD y rutas:

```python
DB_NAME     = "yt_analytics"
DB_USER     = "soporte"
DB_PASSWORD = "soporte"
DB_HOST     = "localhost"
DB_PORT     = "5433"

TOKENS_DIR         = "tokens"
CLIENT_SECRET_FILE = "client_secret.json"

# Cuentas que tendrán análisis WW + US+PR
# Agrega aquí el nombre_cuenta en MAYÚSCULAS
CUENTAS_CON_USPR = {"TELEMUNDO"}
```

---

## 4. Tokens de YouTube

Coloca los archivos `.pickle` en la carpeta `tokens/`.

**Formato del nombre del archivo:**
```
token_{nombre_subcuenta}_{UCxxxxxxxxxxxxxxxxxxxxxxxx}.pickle
```
Ejemplo:
```
token_Telemundo Deportes_UCjZ7QPKb89R-4SxzBoceyOg.pickle
```

El nombre de la subcuenta debe coincidir **exactamente** con el registrado en `dim_subcuenta`.

---

## 5. Ejecutar el pipeline

```bash
# Todos los tokens de todas las subcarpetas (comportamiento actual)
python -m pipeline.run

# Solo una subcarpeta específica
python -m pipeline.run --grupo sony
python -m pipeline.run --grupo telemundo
python -m pipeline.run --grupo restantes

# Combinar grupo + fecha
python -m pipeline.run --grupo sony --start 2025-01-01 --end 2025-03-31

# Subcuenta específica (igual que antes)
python -m pipeline.run --subcuenta "Al Rojo Vivo"
```

El pipeline:
- Refresca tokens vencidos automáticamente
- Re-autentica si el token no se puede refrescar (abre el navegador)
- Es **idempotente**: puedes correrlo varias veces sin duplicar datos
- Genera un log en `pipeline.log`

---

## 6. Ejecutar el dashboard

```bash
streamlit run dashboard/app.py
```

Abre `http://localhost:8501` en el navegador.

**Credenciales de ejemplo:**

| Usuario       | Contraseña | Cuenta        |
|---------------|-----------|---------------|
| telemundo     | 3816      | TELEMUNDO     |
| sony          | 2358      | SONY          |
| magic         | 1688      | MAGIC         |
| andreslpz     | 7865      | ANDRES LOPEZ  |
| amigosasueldo | 9834      | AMIGOS A SUELDO |
| lauracuna     | 8963      | LAURA ACUÑA   |
| andrnvrro     | 9437      | ANDREA NAVARRO |

> ⚠️ Las contraseñas están en texto plano para facilitar el desarrollo.
> En producción, usar bcrypt u otro hash.

---

## 7. Agregar una nueva cuenta con análisis US+PR

Solo edita una línea en `config/settings.py`:

```python
CUENTAS_CON_USPR = {"TELEMUNDO", "NUEVA_CUENTA"}
```

---

## 8. Agregar proyecciones mensuales

```sql
INSERT INTO proyecciones_mensuales (id_cuenta, anio, mes, views_proyectadas, watchtime_proyectado, revenue_proyectado)
VALUES (2, 2025, 1, 100000000, 5000000, 50000);
```

---

## Flujo de datos

```
YouTube Analytics API
        ↓
  pipeline/extractor.py    (extrae y transforma)
        ↓
  PostgreSQL (yt_analytics)
        ↓
  dashboard/app.py         (Streamlit → navegador)
```

---

## 9. Módulo de Chatbot (opcional)

El chatbot combina dos fuentes de conocimiento para responder preguntas:

| Fuente | Qué contiene |
|--------|-------------|
| **Base de datos** | Métricas reales: views, revenue, top videos, demografía, etc. |
| **PDFs** | Reportes, estrategias, contratos, contexto interno de la cuenta |

### Activar el chatbot

1. Asegúrate de tener `LLM_ENABLED = True` en `config/settings.py`
2. Crea un archivo `.env` en la raíz del proyecto:

```
OPENAI_API_KEY=sk-...
```

3. Instala las dependencias del módulo LLM:

```bash
pip install openai langchain langchain-openai langchain-community faiss-cpu pypdf
```

### Agregar PDFs a una cuenta

Crea la carpeta `pdfs/{id_cuenta}/` y coloca ahí los PDFs:

```
pdfs/
  1/                        ← id_cuenta = 1 (TELEMUNDO)
    reporte_q1_2025.pdf
    estrategia_contenido.pdf
  2/                        ← id_cuenta = 2 (SONY)
    plan_anual.pdf
```

> Si una cuenta no tiene PDFs, el chatbot igual funciona usando solo los datos de la BD.

### Cambiar el modelo

En `config/settings.py`:

```python
LLM_MODEL = "gpt-4o"        # más inteligente
LLM_MODEL = "gpt-4o-mini"   # equilibrio costo/calidad (por defecto)
LLM_MODEL = "gpt-3.5-turbo" # más económico
```

### Deshabilitar el chatbot completamente

```python
LLM_ENABLED = False   # en config/settings.py
```

El dashboard funciona perfectamente sin el chatbot activado.
