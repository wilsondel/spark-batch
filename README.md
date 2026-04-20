# Spark Batch — Payment & Rating Analytics

Job de **Spark Batch** escrito en **Scala** (programación funcional sobre DataFrames) que lee el histórico completo de `topic.payment` y `topic.rating` desde Kafka, genera reportes de agregación y guarda los resultados en archivos JSON.

A diferencia del job de streaming (`spark_streaming_alerts`), este job **arranca, procesa todo el histórico y termina**. No es un proceso siempre activo.

---

## Diferencia con Spark Streaming

| | spark_streaming_alerts | spark_batch |
|---|---|---|
| API Spark | `readStream` / `writeStream` | `read` / `write` |
| Corre | Siempre activo | Arranca y termina |
| Lee | Desde offset actual en adelante | Todo el histórico (earliest → latest) |
| Output | `topic.alert` en Kafka | JSON en `./output/` + consola |
| Deploy | Servicio en ECS / Kubernetes | Job en EMR / cron / Airflow |
| `restart` Docker | `unless-stopped` | `"no"` |

---

## Estructura

```
spark_batch/
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── src/
│   ├── main/scala/com/ridesim/batch/
│   │   ├── Main.scala                    # orquesta los 2 jobs
│   │   ├── config/AppConfig.scala        # env vars → case class
│   │   ├── domain/Schemas.scala          # schemas de payment y rating
│   │   ├── jobs/
│   │   │   ├── PaymentBatchJob.scala     # 4 agregaciones de pagos
│   │   │   └── RatingBatchJob.scala      # 4 agregaciones de ratings
│   │   └── writer/ResultWriter.scala     # guarda JSON + muestra en consola
│   └── test/scala/com/ridesim/batch/
│       ├── jobs/PaymentBatchJobSpec.scala
│       └── jobs/RatingBatchJobSpec.scala
├── Dockerfile
├── docker-compose.yml
└── output/                               # generado al correr (gitignored)
```

---

## Qué procesa cada job

### PaymentBatchJob — lee `topic.payment`

| Reporte | Descripción |
|---|---|
| `payment_summary` | Totales globales: revenue, usuarios únicos, conductores únicos |
| `payment_by_method` | CASH vs CARD vs WALLET con totales, promedios, min, max |
| `payment_by_driver` | Ranking de conductores por ganancias totales |
| `payment_by_user` | Ranking de usuarios por gasto total |

### RatingBatchJob — lee `topic.rating`

| Reporte | Descripción |
|---|---|
| `rating_summary` | Promedio global de estrellas, trips calificados, calificadores únicos |
| `rating_driver_ranking` | Conductores ordenados por promedio de estrellas recibidas |
| `rating_user_ranking` | Usuarios ordenados por promedio de estrellas recibidas |
| `rating_star_dist` | Distribución: cuántos 1★, 2★, 3★, 4★, 5★ en total |

---

## Configuración (env vars)

| Variable        | Default             | Descripción                        |
|-----------------|---------------------|------------------------------------|
| `KAFKA_BROKERS` | `localhost:9092`    | Brokers Kafka                      |
| `TOPIC_PAYMENT` | `topic.payment`     | Tópico de pagos                    |
| `TOPIC_RATING`  | `topic.rating`      | Tópico de calificaciones           |
| `OUTPUT_DIR`    | `/opt/output`       | Directorio de salida               |
| `OUTPUT_FORMAT` | `json`              | Formato de salida (`json` o `csv`) |
| `SPARK_MASTER`  | `local[*]`          | Master de Spark                    |

---

## Cómo ejecutar

### Docker (requiere rides_simulator corriendo)

```bash
# asegúrate que rides_simulator esté activo
cd ../rides_simulator && docker-compose up -d

# corre el batch
cd ../spark_batch
docker-compose up --build
```

### Local con sbt

```bash
export KAFKA_BROKERS=localhost:29092
export OUTPUT_DIR=/tmp/spark-batch-output

sbt run
```

### Tests

```bash
sbt test
```

---

## Output real de ejecución

A continuación el output de consola de una ejecución real con **432 pagos** y **854 ratings** acumulados desde el simulador de viajes.

### PAYMENT — Revenue por método de pago

```
========== REVENUE BY PAYMENT METHOD ==========
+------+-----------+-------------+------------------+----------+----------+
|method|total_rides|total_revenue|avg_amount        |min_amount|max_amount|
+------+-----------+-------------+------------------+----------+----------+
|WALLET|137        |3726.75      |27.20             |4.05      |44.77     |
|CARD  |151        |3617.09      |23.95             |3.59      |44.72     |
|CASH  |144        |3546.73      |24.63             |3.62      |44.59     |
+------+-----------+-------------+------------------+----------+----------+
```

**Observación**: WALLET lidera en revenue total a pesar de tener menos rides que CARD, por su mayor precio promedio ($27.20 vs $23.95). Revenue total: **$10,890.57** en 432 viajes.

---

### PAYMENT — Top 10 conductores por ganancias

```
========== TOP DRIVERS BY EARNINGS ==========
+-------------+---------------+------------+------------+
|driver_id    |rides_completed|total_earned|avg_per_ride|
+-------------+---------------+------------+------------+
|driver_070786|2              |48.56       |24.28       |
|driver_087278|1              |44.77       |44.77       |
|driver_073223|1              |44.72       |44.72       |
|driver_011534|1              |44.68       |44.68       |
|driver_079634|1              |44.59       |44.59       |
|driver_060442|1              |44.54       |44.54       |
|driver_017345|1              |44.53       |44.53       |
|driver_087000|1              |44.51       |44.51       |
|driver_035804|1              |44.39       |44.39       |
|driver_029054|1              |44.1        |44.1        |
+-------------+---------------+------------+------------+
```

**Observación**: `driver_070786` es el único conductor con 2 viajes completados en este período, acumulando $48.56. La mayoría de conductores tienen solo 1 viaje — refleja el pool de 100,000 conductores del simulador, donde la probabilidad de repetición es baja.

---

### PAYMENT — Top 10 usuarios por gasto

```
========== TOP USERS BY SPENDING ==========
+-----------+-----------+-----------------+------------------+
|user_id    |rides_taken|total_spent      |avg_per_ride      |
+-----------+-----------+-----------------+------------------+
|user_009411|2          |72.20            |36.10             |
|user_045688|1          |44.77            |44.77             |
|user_044971|1          |44.72            |44.72             |
|user_039322|1          |44.68            |44.68             |
|user_025282|1          |44.59            |44.59             |
|user_041836|1          |44.54            |44.54             |
|user_023713|1          |44.53            |44.53             |
|user_000138|1          |44.51            |44.51             |
|user_013697|1          |44.39            |44.39             |
|user_002256|1          |44.2             |44.2              |
+-----------+-----------+-----------------+------------------+
```

**Observación**: `user_009411` es el usuario más activo con 2 viajes y $72.20 gastados, con un promedio de $36.10 por viaje — ambos viajes fueron de precio alto.

---

### Archivos generados en `./output/`

```
output/
├── payment_summary/         part-00000-*.json   ← totales globales
├── payment_by_method/       part-00000-*.json   ← por CASH/CARD/WALLET
├── payment_by_driver/       part-00000-*.json   ← ranking conductores
├── payment_by_user/         part-00000-*.json   ← ranking usuarios
├── rating_summary/          part-00000-*.json   ← promedio global
├── rating_driver_ranking/   part-00000-*.json   ← top conductores
├── rating_user_ranking/     part-00000-*.json   ← top usuarios
└── rating_star_dist/        part-00000-*.json   ← distribución 1★-5★
```

Ejemplo de contenido de `payment_summary`:

```json
{
  "total_transactions": 432,
  "total_revenue": 10890.57,
  "avg_ride_price": 25.21,
  "min_ride_price": 3.59,
  "max_ride_price": 44.77,
  "unique_users": 428,
  "unique_drivers": 430,
  "unique_trips": 432
}
```

---

## Cómo se relaciona con el resto del sistema

```
rides_simulator (Go)
    │
    ├──▶ topic.payment  ──▶ spark_batch  ──▶ output/payment_*/
    │                        (batch)
    └──▶ topic.rating   ──▶ spark_batch  ──▶ output/rating_*/

    ├──▶ topic.geolocation ──▶ spark_streaming_alerts ──▶ topic.alert
    └──▶ topic.lifecycle
```

El batch lee **todo el histórico** acumulado desde que el simulador empezó. Cada vez que se lanza reemplaza los resultados anteriores (`mode = overwrite`).

---

## Próximos pasos

- Programar la ejecución con **AWS EventBridge** (cron `0 */1 * * *` → cada hora)
- Guardar output en **S3 Parquet** en lugar de JSON local (cambiar `OUTPUT_FORMAT=parquet` y `OUTPUT_DIR=s3://bucket/reports/`)
- Alimentar un modelo de **SageMaker** con el ranking de conductores para predicción de demanda
- Agregar ventanas temporales (`window("timestamp_utc", "1 hour")`) para reportes por hora del día
