## Proyecto de Análisis de Datos — Corporación Favorita
**Pipeline de datos con Apache Airflow, Polars, PostgreSQL y Power BI**

---

## Descripción general

Este proyecto implementa un pipeline completo análisis de datos sobre el dataset **Store Sales – Time Series Forecasting** de Kaggle, correspondiente a la cadena de supermercados **Corporación Favorita** (Ecuador). El objetivo es transformar datos crudos para el análisis de negocio, orquestando todo el proceso mediante Apache Airflow y exponiendo los resultados en un dashboard interactivo de Power BI conectado en tiempo real a PostgreSQL.

## Objetivos del proyecto

- Automatizar la carga, limpieza y consolidación de múltiples fuentes de datos.
- Responder preguntas de negocio clave: estacionalidad, impacto de feriados, efecto de promociones, correlación con el precio del petróleo y comportamiento de transacciones.
- Persistir los resultados en una base de datos relacional (PostgreSQL) para su consumo analítico.
- Visualizar los hallazgos mediante un dashboard de Power BI conectado en tiempo real.

## 🗂️ Fuente de datos

**Store Sales – Time Series Forecasting** — Kaggle
🔗 https://www.kaggle.com/competitions/store-sales-time-series-forecasting

Datasets utilizados:
| Archivo | Descripción |
|---|---|
| `train.csv` | Registro histórico de ventas por tienda, producto y fecha |
| `stores.csv` | Metadata de las tiendas (ciudad, estado, tipo, clúster) |
| `transactions.csv` | Número de transacciones diarias por tienda |
| `oil.csv` | Precio diario del petróleo (WTI) |
| `holidays_events.csv` | Feriados y eventos especiales en Ecuador |

##  Arquitectura del pipeline

```
CSV crudos (data/)
      │
      ▼
[Tarea 1] Carga inicial (modo Lazy con Polars)
      │
      ▼
[Tarea 2] EDA inicial — diagnóstico de calidad (nulos, duplicados, rangos)
      │
      ▼
[Tarea 3] Limpieza e imputación
      │   • Eliminación de duplicados
      │   • Interpolación lineal de precios de petróleo
      │   • Imputación de transacciones nulas con mediana
      ▼
[Tarea 4] Consolidación — joins secuenciales en dataset único
      │
      ▼
[Tarea 5] EDA profundo — análisis de negocio completo
      │
      ▼
[Tarea 6] Exportación a PostgreSQL (vía COPY)
      │
      ▼
Power BI (conexión en tiempo real / DirectQuery)
```

Todo el flujo es orquestado mediante un **DAG de Apache Airflow**, que ejecuta las tareas de forma secuencial con manejo de dependencias y reintentos.

## Preguntas de negocio abordadas (EDA profundo)

1. **Ventas generales**: distribución por familia de producto, ranking de tiendas (top 10 mayores/menores), ventas por ciudad/provincia, evolución mensual y anual (2013–2017).
2. **Estacionalidad y feriados**: comparación de ventas en días feriados vs. normales, sensibilidad por familia de producto ante feriados.
3. **Promociones**: impacto de estar en promoción sobre las ventas, segmentado por familia de producto.
4. **Petróleo y economía**: correlación entre el precio del petróleo y las ventas, análisis de la caída del crudo 2015–2016 y su efecto por ciudad.
5. **Transacciones**: relación transacciones-ventas y cálculo de ticket promedio por tienda.

##  Stack tecnológico

| Componente | Tecnología |
|---|---|
| Orquestación | Apache Airflow 2.9.2 |
| Procesamiento de datos | Polars (modo Lazy) + Pandas (interpolación) |
| Base de datos | PostgreSQL |
| Visualización | Power BI (conexión DirectQuery) |
| Lenguaje | Python 3.11 |
| Entorno | Ubuntu (WSL2) |

## 📁 Estructura del repositorio

```
ProyectoFavorita/
├── airflow/                  # Configuración de Airflow (DAGs, cfg)
│   └── dags/                 # DAG de orquestación del pipeline
├── scripts/
│   ├── carga_y_eda_inicial.py    # Tareas 1-2: carga + diagnóstico de calidad
│   ├── limpiar_datos.py          # Tarea 3: limpieza e imputación
│   ├── consolidar.py             # Tarea 4: consolidación en dataset único
│   ├── Eda_profundo.py           # Tarea 5: análisis de negocio
│   └── exportar_postgres.py      # Tarea 6: exportación a PostgreSQL
├── data/                     # CSV crudos y procesados (no versionado)
├── reports/                  # Reportes de calidad (JSON)
├── .gitignore
├── requirements.txt
└── README.md
```

> **Nota:** la carpeta `data/` no se versiona en Git por el peso de los archivos (dataset de +3M de registros). Cada integrante debe descargar los CSV originales y colocarlos localmente en `data/` antes de ejecutar el pipeline.

## ⚙️ Instalación y configuración del entorno

```bash
# 1. Clonar el repositorio
git clone https://github.com/Amy-Mishell06/ProyectoFavorita.git
cd ProyectoFavorita

# 2. Crear entorno virtual (Python 3.11)
python3.11 -m venv venv
source venv/bin/activate

# 3. Instalar dependencias
pip install --upgrade pip
pip install -r requirements.txt

# 4. Configurar AIRFLOW_HOME
export AIRFLOW_HOME=~/ProyectoFavorita/airflow

# 5. Inicializar base de datos de Airflow y crear usuario admin
airflow db migrate
airflow users create --username admin --firstname [Nombre] --lastname [Apellido] --role Admin --email admin@example.com --password admin123

# 6. Colocar los CSV originales en data/ (ver sección Fuente de datos)
```

## Ejecución del pipeline

**Orden de ejecución de los scripts:**
```bash
python scripts/carga_y_eda_inicial.py
python scripts/limpiar_datos.py
python scripts/consolidar.py
python scripts/Eda_profundo.py
python scripts/exportar_postgres.py
```

**O mediante Airflow (orquestación automática):**
```bash
# Terminal 1
airflow webserver

# Terminal 2
airflow scheduler
```
Luego acceder a `http://localhost:8080` y activar el DAG del proyecto.

## Configuración de PostgreSQL

```bash
sudo apt install postgresql postgresql-contrib -y
sudo service postgresql start
sudo -u postgres psql -c "CREATE DATABASE favorita_db;"
```

Para habilitar la conexión remota desde Power BI, se configuró:
- `postgresql.conf` → `listen_addresses = '*'`
- `pg_hba.conf` → `host all all 0.0.0.0/0 scram-sha-256`

## Conexión con Power BI

1. Abrir Power BI Desktop → **Obtener datos** → **PostgreSQL database**.
2. Servidor: `localhost:5432` (o IP de WSL2 si aplica).
3. Base de datos: `favorita_db`.
4. Modo de conectividad: **DirectQuery** (tiempo real).
5. Seleccionar las tablas `consolidado_final` y las tablas `eda_*` generadas por el análisis profundo.

##  Equipo

- | Integrante |
- | AMY DÍAZ  |
- | Jonathan Caiza |
- | Anthony Ledesma |

## Estado del proyecto

- [x] Configuración del entorno (Airflow + PostgreSQL + Polars)
- [x] Carga y diagnóstico inicial de calidad de datos
- [x] Limpieza e imputación de valores nulos
- [x] Consolidación de datasets
- [x] EDA profundo con preguntas de negocio
- [x] Exportación a PostgreSQL
- [ ] DAG de Airflow completamente orquestado
- [ ] Dashboard final en Power BI
- [ ] Documentación técnica (PDF)
