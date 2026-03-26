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
├──.env.local                 ← Entorno local, donde se almacena credenciales de base de datos local
├──.env.production            ← Entorno produccion, donde se almacena credenciales de base de datos en produccion
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

#Para subir a producción datos:
$env:ENV="production"; python -m pipeline.run --grupo sony --start 2025-01-01 --end 2025-03-31
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
| telemundo     | ****      | TELEMUNDO     |
| sony          | ****      | SONY          |
| magic         | ****      | MAGIC         |
| andreslpz     | ****      | ANDRES LOPEZ  |
| amigosasueldo | ****      | AMIGOS A SUELDO |
| lauracuna     | ****      | LAURA ACUÑA   |
| andrnvrro     | ****      | ANDREA NAVARRO |

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

##  10. Comandos para súbir a Github

```
#Siempre trabajar en dev
git checkot dev
# Haces los cambios en los archivos (components.py, app.py, queries.py...) y Pruebas en local
streamlit run dashboard/app.py
# Verifica que todo se ve bien en http://localhost:8501

# Cuando está listo
git add .
git commit -m "Mejora diseño top videos"
git push origin dev

# Subir a producción
git checkout main
git merge dev
git push origin main        # Railway redeploya automático en ~2 min
git checkout dev            # vuelves a dev

#Para conocer en que rama nos encontramps
git branch #siempre apunta a la rama con el *
```

### A. Para hacer descargar en base de datos local

```
python -m pipeline.run --grupo sony --start 2025-01-01 --end 2025-01-31

```


### B. Para cargar datos a producción

```
$env:ENV="production"; python -m pipeline.run --grupo sony --start 2025-01-01 --end 2025-03-31

```
Esto hace que connection.py cargue .env.production que apunta a Railway.

### C. Flujo recomendado para cargar datos:
```
# 1. Primero prueba contra BD local
python -m pipeline.run --grupo sony --start 2025-01-01 --end 2025-01-31

# 2. Si funciona bien, carga a producción
ENV=production python -m pipeline.run --grupo sony --start 2025-01-01 --end 2025-03-31

```