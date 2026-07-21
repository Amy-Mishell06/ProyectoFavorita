# ProyectoFavorita

## Proyecto de Análisis de Datos — Corporación Favorita
**Pipeline de datos con Apache Airflow, Polars, PostgreSQL y Power BI**

## Equipo
- AMY DÍAZ
- JONATHAN CAIZA
- ANTHONY LEDESMA

---

## 1. Descripción del proyecto

Este proyecto implementa un pipeline completo de análisis de datos sobre el dataset **Store Sales – Time Series Forecasting** de Kaggle, correspondiente a la cadena de supermercados **Corporación Favorita**. El objetivo es transformar datos crudos de ventas, transacciones, precios de petróleo y feriados en información lista para el análisis de negocio, orquestando todo el proceso mediante Apache Airflow y exponiendo los resultados en un dashboard interactivo de Power BI conectado en tiempo real a PostgreSQL.

**Objetivos específicos:**
- Automatizar la carga, limpieza y consolidación de múltiples fuentes de datos de ventas.
- Responder preguntas de negocio clave: estacionalidad, impacto de feriados, efecto de promociones, correlación con el precio del petróleo y comportamiento de transacciones.
- Persistir los resultados en una base de datos relacional (PostgreSQL) para su consumo analítico.
- Visualizar los hallazgos mediante un dashboard de Power BI conectado en tiempo real.

---

## 2. Descripción de los archivos del dataset y su rol en el pipeline

**Fuente:** Store Sales – Time Series Forecasting — Kaggle
🔗 https://www.kaggle.com/competitions/store-sales-time-series-forecasting

| Archivo | Rol en el pipeline |
|---|---|
| `train.csv` | Dataset principal. Ventas diarias por tienda, familia de producto y promoción (+3M registros). Base de todo el análisis. |
| `stores.csv` | Metadata de las 54 tiendas (ciudad, provincia, tipo, clúster). Se une con `train` para análisis geográfico. |
| `transactions.csv` | Número de transacciones diarias por tienda. Permite calcular ticket promedio y relación transacciones-ventas. |
| `oil.csv` | Precio diario del petróleo (WTI), con nulos en fines de semana/feriados. Usado para analizar correlación con ventas. |
| `holidays_events.csv` | Feriados nacionales, regionales y locales de Ecuador. Usado para medir el impacto de feriados en ventas. |
| `test.csv` | No se utiliza en este proyecto (pertenece a la competencia original de Kaggle para forecasting). |

---

## 3. Diagrama de arquitectura de la solución

<img width="332" height="1031" alt="image" src="https://github.com/user-attachments/assets/1481bcdd-60b5-4ae5-86b6-4390a9c6ef2d" />


**Explicación del diagrama:**

El pipeline opera sobre una máquina virtual Ubuntu (WSL2). Los archivos del dataset residen localmente en la carpeta `data/` de la máquina. El repositorio de GitHub contiene únicamente los scripts de Polars, el DAG de Airflow y el archivo de control `manifest.json` — los datos crudos nunca se suben al repositorio. El pipeline se ejecuta de forma orquestada desde la interfaz de Airflow (o vía `airflow dags test` desde terminal). El resultado final se persiste en PostgreSQL, y Power BI se conecta a esa base de datos en tiempo real (DirectQuery) para alimentar el dashboard.

**Componentes:**
- **VM Ubuntu (WSL2):** ambiente de ejecución de Airflow y PostgreSQL.
- **Apache Airflow** (instalado con pip en entorno virtual): orquestador del pipeline.
- **Python + Polars:** motor de carga, limpieza, transformación y análisis.
- **PostgreSQL:** base de datos intermedia que almacena los datos limpios y consolidados.
- **Power BI:** capa de visualización conectada en tiempo real a PostgreSQL.
- **GitHub:** repositorio de scripts, DAG y archivo de control `manifest.json`.

**Flujo de datos:**
```
Carpeta local de datos (CSV) → Airflow dispara el DAG → Scripts Polars ejecutan
carga, limpieza y consolidación → Datos limpios se escriben en PostgreSQL →
Power BI consume PostgreSQL en tiempo real.
```

---

## 4. Descripción del DAG: tareas, dependencias y configuración

**DAG ID:** `pipeline_favorita`

El DAG orquesta el pipeline completo en 5 tareas secuenciales, donde cada tarea depende del éxito de la anterior (si una falla, las siguientes no se ejecutan):

| Tarea | Script | Descripción |
|---|---|---|
| `carga_y_eda_inicial` | `carga_y_eda_inicial.py` | Carga los 5 CSV en modo Lazy con Polars y genera el diagnóstico de calidad inicial (nulos, duplicados, tipos, rangos de fecha), guardado en `reports/eda_inicial.json` |
| `limpiar_datos` | `limpiar_datos.py` | Elimina duplicados, imputa el precio del petróleo con interpolación lineal, corrige tipos de datos |
| `consolidar_datasets` | `consolidar.py` | Une los 5 datasets mediante joins secuenciales en un único DataFrame consolidado |
| `eda_profundo` | `Eda_profundo.py` | Ejecuta el análisis estadístico completo: ventas por familia/tienda/ciudad, estacionalidad, feriados, promociones, correlación con petróleo, transacciones |
| `exportar_postgres` | `exportar_postgres.py` | Exporta el dataset consolidado y las tablas de estadísticos del EDA a PostgreSQL vía `COPY` |

**Dependencias (orden de ejecución):**
```
carga_y_eda_inicial >> limpiar_datos >> consolidar_datasets >> eda_profundo >> exportar_postgres
```

**Configuración del DAG:**
- `schedule_interval`: `None` (ejecución manual mediante Trigger DAG desde la UI de Airflow)
- `start_date`: 2026-07-01
- `catchup`: `False`
- `retries`: 1 por tarea
- `retry_delay`: 2 minutos
- `owner`: equipo_favorita
- Operador utilizado: `BashOperator`, ejecutando cada script con el intérprete Python del entorno virtual del proyecto
- Rutas del proyecto calculadas dinámicamente a partir de la ubicación del propio archivo del DAG, evitando rutas fijas dependientes de cada máquina

---

## 5. Proceso del pipeline: descripción de cada etapa con capturas de Airflow

**Etapa 1-2 — Carga y EDA inicial:** se cargan los 5 CSV en modo Lazy y se genera el diagnóstico de calidad (`reports/eda_inicial.json`), detectando por ejemplo 43 valores nulos (3.53%) en `oil.csv`.

**Etapa 3 — Limpieza:** se eliminan duplicados y se imputan los nulos del precio del petróleo mediante interpolación lineal.

**Etapa 4 — Consolidación:** se unen los 5 datasets mediante joins secuenciales (`train` → `stores` → `transactions` → `oil` → `holidays`), generando un dataset unificado de **3,000,888 registros**.

**Etapa 5 — EDA profundo:** se calculan los estadísticos de negocio (ventas por familia, estacionalidad, feriados, promociones, correlación con petróleo, transacciones).

**Etapa 6 — Exportación:** el dataset consolidado y las tablas de estadísticos se exportan a PostgreSQL (`favorita_db`).

<img width="1918" height="1056" alt="image" src="https://github.com/user-attachments/assets/15f075ae-62f2-4606-b824-fac6507113e2" />
<img width="1918" height="1045" alt="image" src="https://github.com/user-attachments/assets/73ec2658-9c9a-4343-8876-0fd6f620b906" />
<img width="1918" height="1050" alt="image" src="https://github.com/user-attachments/assets/0c5edba3-3050-4d7f-9008-75e36dfeea8f" />




---

## 6. Métricas del pipeline

| Métrica | Valor |
|---|---|
| Registros en `train.csv` (dataset principal) | 3,000,888 |
| Registros en `stores.csv` | 54 |
| Registros en `transactions.csv` | 83,488 |
| Registros en `oil.csv` | 1,218 |
| Registros en `holidays_events.csv` | 350 |
| Valores nulos detectados en `oil.csv` | 43 (3.53%) — imputados con interpolación lineal |
| Filas duplicadas detectadas (todos los archivos) | 0 |
| Registros en el dataset consolidado final | 3,000,888 |
| Registros exportados a PostgreSQL | 3,000,888 |
| Tiempo total de ejecución del pipeline (test local) | ~4 segundos (ejecución con datos ya en caché de Polars) |



---

## 7. Capturas del dashboard de Power BI

_(Insertar aquí las capturas del dashboard: ventas por familia, evolución mensual, mapa por ciudad, impacto de feriados, correlación con petróleo, comparativo de promociones, ranking de tiendas)_

---

## 8. Despliegue: instrucciones para reproducir el ambiente

### Instalación y configuración del entorno

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

# 6. Colocar los CSV originales en data/ (ver sección 2 - Fuente de datos)
```

### Ejecución del pipeline

**Manual, script por script:**
```bash
python scripts/carga_y_eda_inicial.py
python scripts/limpiar_datos.py
python scripts/consolidar.py
python scripts/Eda_profundo.py
python scripts/exportar_postgres.py
```

**Orquestado con Airflow:**
```bash
# Terminal 1
airflow webserver

# Terminal 2
airflow scheduler
```
Acceder a `http://localhost:8080`, activar el DAG `pipeline_favorita` y disparar su ejecución (Trigger DAG).

**Prueba rápida desde terminal (sin necesidad del webserver):**
```bash
airflow dags test pipeline_favorita 2026-07-20
```

### Configuración de PostgreSQL

```bash
sudo apt install postgresql postgresql-contrib -y
sudo service postgresql start
sudo -u postgres psql -c "CREATE DATABASE favorita_db;"
```

Para habilitar la conexión remota desde Power BI, se configuró:
- `postgresql.conf` → `listen_addresses = '*'`
- `pg_hba.conf` → `host all all 0.0.0.0/0 scram-sha-256`

### Conexión con Power BI

1. Abrir Power BI Desktop → **Obtener datos** → **Base de datos** → **PostgreSQL database**.
2. Servidor: `localhost:5432`.
3. Base de datos: `favorita_db`.
4. Modo de conectividad: **DirectQuery** (tiempo real — requisito obligatorio del proyecto).
5. Autenticación: pestaña "Base de datos", usuario `postgres`, contraseña configurada.
6. Seleccionar las tablas `consolidado_final` y las tablas `eda_*` generadas por el análisis profundo.

---

## 9. Conclusiones y recomendaciones

**Conclusiones:**

- Trabajar con Polars en modo Lazy resultó clave para manejar el dataset de más de 3 millones de registros sin saturar la memoria de los equipos locales, algo que con Pandas hubiera sido considerablemente más lento.
- La limpieza de datos, especialmente la imputación del precio del petróleo con interpolación lineal, mostró la importancia de entender el contexto de cada dato antes de aplicar una técnica: no todos los nulos se resuelven de la misma forma.
- Consolidar cinco fuentes distintas mediante joins secuenciales dejó claro que el orden de las uniones importa: un join mal ubicado puede duplicar filas o perder registros sin que sea evidente a simple vista.
- La orquestación con Airflow permitió automatizar un proceso que antes se ejecutaba manualmente script por script, reduciendo el margen de error humano de olvidar un paso o correrlo en el orden incorrecto.
- PostgreSQL como capa intermedia conectó todo el trabajo de limpieza con la visualización final en Power BI, evidenciando el valor de una base de datos bien estructurada como punto de entrega entre etapas del pipeline.
- Los errores más frecuentes durante el desarrollo (contraseñas de PostgreSQL, rutas de archivos, procesos de Airflow duplicados) no fueron fallos del diseño del pipeline en sí, sino de configuración del entorno, lo que reforzó la importancia de una buena gestión del ambiente de trabajo.

**Recomendaciones**

- Definir la estructura de carpetas y el flujo de datos del proyecto desde el primer día, antes de escribir código, para evitar reorganizar todo a mitad de camino.
- Probar cada script de forma aislada antes de integrarlo al DAG, de modo que los errores se detecten rápido y no se acumulen en etapas posteriores.
- Validar los datos después de cada etapa del pipeline (no solo al final), para detectar errores de limpieza o consolidación lo antes posible.
- Guardar evidencia (capturas, logs) de cada etapa a medida que se completa, en lugar de reconstruirla después, lo cual consume tiempo valioso cerca de la entrega.
- Establecer desde el inicio un canal claro de comunicación sobre avances diarios entre el equipo, para detectar a tiempo bloqueos técnicos o tareas atrasadas.


---

## Preguntas de negocio abordadas (EDA profundo)

1. **Ventas generales**: distribución por familia de producto, ranking de tiendas (top 10 mayores/menores), ventas por ciudad/provincia, evolución mensual y anual (2013–2017).
2. **Estacionalidad y feriados**: comparación de ventas en días feriados vs. normales, sensibilidad por familia de producto ante feriados.
3. **Promociones**: impacto de estar en promoción sobre las ventas, segmentado por familia de producto.
4. **Petróleo y economía**: correlación entre el precio del petróleo y las ventas, análisis de la caída del crudo 2015–2016 y su efecto por ciudad.
5. **Transacciones**: relación transacciones-ventas y cálculo de ticket promedio por tienda.

## Estructura del repositorio

```
ProyectoFavorita/
├── dags/
│   └── pipeline_favorita_dag.py
├── scripts/
│   ├── carga_y_eda_inicial.py
│   ├── limpiar_datos.py
│   ├── consolidar.py
│   ├── Eda_profundo.py
│   └── exportar_postgres.py
├── manifest.json
├── .gitignore
├── requirements.txt
└── README.md
```

> **Nota:** la carpeta `data/` no se versiona en Git por el peso de los archivos (dataset de +3M de registros). Cada integrante debe descargar los CSV originales y colocarlos localmente en `data/` antes de ejecutar el pipeline.

## Estado del proyecto

- [x] Configuración del entorno (Airflow + PostgreSQL + Polars)
- [x] Carga y diagnóstico inicial de calidad de datos
- [x] Limpieza e imputación de valores nulos
- [x] Consolidación de datasets
- [x] EDA profundo con preguntas de negocio
- [x] Exportación a PostgreSQL
- [x] DAG de Airflow orquestado y probado
- [ ] Dashboard final en Power BI
- [ ] Capturas insertadas en el README (arquitectura, Airflow, Power BI)
