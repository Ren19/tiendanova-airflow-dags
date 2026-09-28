# TiendaNova — Solución completa Clase 6 + Clase 7

Repositorio **completo y resuelto** que junta la Clase 6 (DAGs + Testing + CI/CD)
y la Clase 7 (Dbt + Snowflake + Cosmos) en un solo proyecto, listo para levantar
con Docker de punta a punta. La guía detallada, clic a clic, está en
`Guia_Completa_Docker_Testing_CICD_Dbt.pdf` (entregada junto a este repositorio).

## Estructura

```
tiendanova-airflow-dags/
├── Dockerfile                      # apache/airflow:3.0.0 + dbt-snowflake + cosmos
├── docker-compose.yaml             # Compose oficial + build propio + volumenes extra
├── .env.example                    # copiar a .env y completar
├── requirements.txt                # produccion (Clase 6) + Dbt/Snowflake/Cosmos (Clase 7)
├── requirements-dev.txt            # pytest, pytest-mock, ruff
├── setup_snowflake.sql             # warehouse, db, schemas, rol y datos de prueba
├── dags/
│   ├── ingesta_sucursales_v2.py    # TaskGroups + Dynamic Task Mapping + Sensor (Clase 5)
│   ├── reporte_consolidado.py      # DAG downstream (Clase 5)
│   ├── dag_transformacion_ventas.py# Cosmos + dbt contra Snowflake (Clase 7)
│   ├── utils/transformaciones.py   # Logica de negocio pura, testeable sin Airflow
│   └── dbt/tiendanova/             # Proyecto dbt completo
│       ├── dbt_project.yml
│       ├── profiles.yml            # credenciales via env_var(), nunca hardcodeadas
│       ├── packages.yml            # dbt_utils (para el test accepted_range)
│       └── models/
│           ├── staging/
│           │   ├── sources.yml     # declara TIENDANOVA_DB.RAW.VENTAS
│           │   ├── stg_ventas.sql
│           │   └── schema.yml      # tests not_null / unique
│           └── marts/
│               ├── marts_ventas_netas.sql
│               └── schema.yml      # test de rango (ventas_netas >= 0)
├── tests/test_dags.py              # tests estructurales + unitarios + mock (Clase 6)
└── .github/workflows/{ci.yml,deploy.yml}
```

## Cómo levantarlo (resumen — la guía en PDF trae cada paso a detalle)

```powershell
# 0. Variables de entorno
copy .env.example .env
# edita .env: SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD

# 1. Construir e inicializar
docker compose build
docker compose up airflow-init

# 2. Levantar todo
docker compose up -d
docker compose ps        # espera a que todo diga "healthy"

# 3. Entrar a la UI
# http://localhost:8080  (usuario/clave: airflow / airflow)
```

## Testing (Clase 6)

```powershell
# Local, en un venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-dev.txt
ruff check dags/ tests/
pytest tests/ -v

# Dentro del contenedor (mismas versiones que producción)
docker compose exec airflow-scheduler pytest /opt/airflow/tests/ -v
```

8 tests en verde: 5 estructurales (DagBag), 2 unitarios de `calcular_filas_limpias`,
1 con mock de `os.path.exists`.

## CI/CD (Clase 6)

1. Sube este repositorio a GitHub.
2. Cualquier Pull Request contra `main` dispara `ci.yml` (lint + tests).
3. Activa branch protection en `main` exigiendo el check `lint-and-test`.
4. Cada push a `main` dispara `deploy.yml`, que re-corre CI y sincroniza `dags/`
   a producción vía git-sync, usando el secret `PRODUCCION_SSH_KEY` y el
   Environment `produccion`.

## Dbt + Snowflake + Cosmos (Clase 7)

```powershell
# 1. Crear warehouse/db/schemas/rol en Snowflake (una sola vez)
#    -> corre setup_snowflake.sql en Snowsight

# 2. Crear la Connection snowflake_default en la UI de Airflow
#    (Admin -> Connections; ver la guía en PDF para los campos exactos)

# 3. Verificar la conexión de dbt desde dentro del contenedor
docker compose exec airflow-scheduler bash -lc \
  "cd /opt/airflow/dags/dbt/tiendanova && dbt deps && dbt debug"

# 4. Correr el proyecto manualmente, antes de meterlo en el DAG
docker compose exec airflow-scheduler bash -lc \
  "cd /opt/airflow/dags/dbt/tiendanova && dbt build"

# 5. Correrlo orquestado por Airflow
#    -> DAG "dag_transformacion_ventas" en la UI, botón Trigger DAG
```

`marts_ventas_netas` en Snowflake queda como la única fuente de verdad de
"ventas netas" por sucursal y día — esto es justo lo que resuelve el
incidente de la Clase 7 (Ana y Luis con dos cifras distintas).

## Apagar el ambiente

```powershell
docker compose down       # conserva el historial de corridas
docker compose down -v    # borra también el historial (empezar 100% de cero)
```

---
PEDE/9 — Apache Airflow · Clase 6 + Clase 7 · Profesor Renato Arrascue
