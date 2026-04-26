# Calidad del Aire - Pipeline Airflow

Proyecto de Ingenieria de Datos orientado a automatizar la ingesta, transformacion, integracion y monitoreo de datos ambientales de la Ciudad de Buenos Aires usando Apache Airflow.

El pipeline combina datos de calidad de aire y temperatura, genera datasets intermedios y un producto final integrado, y envia notificaciones por correo para informar ejecuciones correctas, reintentos, fallos y alertas por contaminantes criticos.

## Que demuestra este proyecto

- Orquestacion de procesos ETL con Apache Airflow.
- Ejecucion reproducible en entorno Docker.
- Separacion modular de tareas Python para facilitar mantenimiento.
- Paralelismo entre transformaciones independientes.
- Integracion de fuentes heterogeneas mediante claves temporales.
- Validaciones de correctitud sobre dimensiones, nulos, fechas y joins.
- Alertas automaticas por percentiles dinamicos de contaminacion.
- Envio de correos de alerta y resumen operacional del pipeline.

## Arquitectura del DAG

El DAG principal coordina siete modulos:

- `download_data`: descarga de archivos crudos.
- `transformation_1`: limpieza y normalizacion de temperatura.
- `transformation_2`: limpieza, agregacion y preparacion de calidad de aire.
- `air_quality_alert_email`: evaluacion de contaminantes contra umbrales P95.
- `combina_data`: union de datasets ambientales.
- `email_operator`: envio de notificaciones.
- `email_logging`: resumen de ejecucion y trazabilidad.

La ejecucion esta programada diariamente a las 08:00 UTC:

```text
schedule: 0 8 * * *
catchup: False
retries: 1
retry_delay: 5 minutos
```

## Flujo ETL

### 1. Ingesta

El pipeline descarga automaticamente dos fuentes principales en la carpeta `data`:

- `temperature_raw.csv`
- `air_quality_raw.csv`

El dataset de temperatura contiene 354 filas y 5 columnas con frecuencia mensual para el periodo 1991-2020.

El dataset de calidad de aire contiene 139.068 filas y 14 columnas con mediciones desde 2009 hasta 2025, provenientes de estaciones como Centenario, Cordoba, La Boca y Palermo.

### 2. Transformaciones en paralelo

Airflow ejecuta transformaciones independientes en paralelo:

- Temperatura: lectura con separador `;`, normalizacion de columnas, estandarizacion de tipos y clave natural `anio-mes`.
- Calidad de aire: limpieza de columnas nulas, reemplazo de valores `s/d`, parseo robusto de fechas, ordenamiento temporal y agregacion mensual.

Esta separacion reduce acoplamiento entre fuentes y permite monitorear cada rama del DAG desde la UI de Airflow.

### 3. Alertas de calidad de aire

El pipeline calcula umbrales dinamicos usando el percentil 95 por contaminante y estacion. Luego evalua el ultimo dia procesado contra esos umbrales.

Cuando se detecta un exceso, se envia un correo automatico con el contaminante, estacion, valor observado, umbral P95 y porcentaje de exceso.

Ejemplos detectados:

- `CO CENTENARIO`: 24.06 ppm vs P95 = 2.58, exceso 833.7%.
- `NO2 CORDOBA`: 145.00 ug/m3 vs P95 = 50.15, exceso 189.1%.
- `PM10 LA BOCA`: 62.37 ug/m3 vs P95 = 42.34, exceso 47.3%.
- `CO PALERMO`: 1.36 ppm vs P95 = 0.15, exceso 805.9%.

### 4. Integracion de datos

La tarea `combina_data` realiza un `LEFT JOIN` entre los agregados mensuales de calidad de aire y la serie mensual de temperatura usando la clave compuesta `anio-mes`.

El producto final queda disponible como:

```text
/opt/airflow/data/producto/buenos_aires_environmental_data.csv
```

## Artefactos generados

- `air_quality_daily.csv`: dataset diario para evaluacion operacional.
- `air_quality_monthly.csv`: agregacion mensual de contaminantes.
- `buenos_aires_environmental_data.csv`: producto final integrado.
- Reportes auxiliares de estadisticas de ciudad, alertas historicas P95 y resumen de procesamiento.
- Correos automaticos de alerta, fallo, reintento y finalizacion correcta.

## Correctitud y monitoreo

La correctitud del flujo se valida con:

- Secuencia completa del DAG en estado exitoso.
- Logs de Airflow por tarea.
- Politica de reintento ante fallos.
- Correos automaticos ante fallo o retry.
- Shapes coherentes entre datasets intermedios y dataset final.
- Validacion de fechas y claves de join.
- Control de contaminantes contra umbrales P95.

## Docker

El proyecto esta pensado para ejecutarse en un entorno Dockerizado de Airflow. Docker permite encapsular:

- Version de Airflow.
- Dependencias Python.
- Rutas de entrada y salida.
- Configuracion SMTP para correos.
- Volumenes de datos y logs.

Esto hace que el pipeline sea reproducible y mas facil de transportar entre maquinas o ambientes.

## Resultados

El pipeline completo permite automatizar la adquisicion y analisis diario de datos ambientales, integrando fuentes heterogeneas bajo un flujo auditable. La arquitectura Airflow permite observar la ejecucion, aislar errores por tarea, paralelizar transformaciones y comunicar eventos importantes por correo.

El sistema queda como base para futuras extensiones: dashboards interactivos, modelos predictivos de contaminacion, enriquecimiento meteorologico y monitoreo continuo de calidad de datos.

## Informe

El archivo `airflow_ingenieria.pdf` documenta el flujo, la arquitectura del DAG, la ejecucion exitosa, las notificaciones por correo y los resultados del pipeline.
