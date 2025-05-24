# Sistema de analisis de datos BIG DATA

Este proyecto es un analisis de la información de taxis de nueva york, los cuales estan separados en meses y años, además de un streaming de datos el cual será actualizado en tiempo real.
El proyecto completo será desarrollado en **Google Cloud Plataform (GCP)** siguiendo una arquitectura previamente descrita.

---

# Comandos para iniciar el lab

1. Definimos la region y el ID del proyecto:

Abrimos la consola en el projecto que no es resource  
y ejecutamos lo siguiente:

```bash
PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
```

2. creamos una nueva cuenta de servicio llamada cf-streaming-sa, para usarla en nuestro cloud function

```bash
gcloud iam service-accounts create cf-streaming-sa --display-name "Cloud Function Streaming SA"
```

3. creamos una nueva cuenta de servicio llamada pubsub-sa, para usarla en nuestro Pub/Sub

```bash
gcloud iam service-accounts create pubsub-sa --display-name "PubSub Service SA"
```

4. creamos una nueva cuenta de servicio llamada dataflow-sa, para usarla en nuestro DataFlow

```bash
gcloud iam service-accounts create dataflow-sa --display-name "Dataflow Worker SA"
```

5. creamos una nueva cuenta de servicio llamada dataproc-sa, para usarla en nuestro DataProc

```bash
gcloud iam service-accounts create dataproc-sa --display-name "Dataproc Cluster SA"
```

---

# Creamos el bucket que va a guardar los datos de los taxis

1. Creamos el bucket llamada {id}-taxis-data

```bash
gsutil mb -p $PROJECT_ID -l $REGION gs://$PROJECT_ID-taxis-data
```

3. Creamos el bucket para el codigo del data proc:

```bash
gsutil mb -l $REGION gs://$PROJECT_ID-code
```

4. Activar Uniform Bucket-Level Access haciendo que solo sea accesible por medio de nuestro usuario IAM de servicio:

```bash
gsutil uniformbucketlevelaccess set on gs://$PROJECT_ID-taxis-data
```

---

# Damos los permisos a nuestras cuentas de servicio

1. Le damos los permisos a la cuenta del **Data Proc** para administrar el bucket:

```bash
gsutil iam ch serviceAccount:dataproc-sa@$PROJECT_ID.iam.gserviceaccount.com:objectAdmin gs://$PROJECT_ID-taxis-data
```

2. Le damos los permisos a la cuenta del **Data Proc** para escribir en **BigQuery**:

```bash
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:dataproc-sa@$PROJECT_ID.iam.gserviceaccount.com --role=roles/bigquery.dataEditor
```

3. Le damos los permisos a la cuenta del **Cloud Function** para subir archivos al bucket (Logs):

```bash
gsutil iam ch serviceAccount:cf-streaming-sa@$PROJECT_ID.iam.gserviceaccount.com:objectCreator gs://$PROJECT_ID-taxis-data
```

---

# Creamos el Script para descargar los parquets

1. Definimos las variables para el script:

```bash
BUCKET_DATA="gs://${PROJECT_ID}-taxis-data"
mkdir -p ~/nyc-taxi
cd ~/nyc-taxi
```

2. Copiamos y pegamos el script en la consola de GCP:

```bash
for y in 2022 2023 2024; do
  for m in $(seq -w 1 12); do
    file="yellow_tripdata_${y}-${m}.parquet"
    url="https://d37ci6vzurychx.cloudfront.net/trip-data/$file"
    echo "➤ Descargando $file ..."
    curl -L --silent "$url" -o "$file"
    echo "   Subiendo a $BUCKET_DATA/taxis/$y/$m/"
    gsutil cp "$file" "$BUCKET_DATA/taxis/$y/$m/"
    rm "$file"
  done
done
```

> [!IMPORTANT] 
> Este comando el multi linea asi que cuidado con copiar y pegar.

---

# Utilizamos DataProc para fusionar los parquets en el bucket

1. Damos los permisos faltantes a nuestro usuario:

```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:dataproc-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/dataproc.worker"
```

2. Creamos nuestro cluster de **DataProc** para fusionar la data

```bash
gcloud dataproc clusters create taxi-clean-cluster \
  --region=$REGION \
  --single-node \
  --enable-component-gateway \
  --service-account dataproc-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform \
  --properties=spark:spark.sql.adaptive.enabled=true \
  --max-idle=10m
```

> [!TIP]  
> Si este comando falla borra el cluster de dataproc y intenta nuevamente

3. Creamos nuestro script en **python** para fusionar la data:

Subimos manualmente el codigo al bucket llamado PROJECTID-code.

4. Creamos el data set en bigquery

```bash
bq --location=US mk taxi
```

5. Script para el proceso de limpieza y carga a bigquery:
Este script ademas de fusionar la data añade 2 columnas nuevas que son **Mes** y **Año** (year y month)

> [!WARNING]  
> Este script necesita que se ingresen manualmente datos como  
> \<TU_BUCKET>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType

# 1. Crear la sesión de Spark
spark = SparkSession.builder.appName("FusionParquets").getOrCreate()

bucket = "<TU_BUCKET>"
base_path = f"gs://{bucket}-taxis-data/taxis/"

# 2. Generar rutas
years = [2022, 2023, 2024]
months = [f"{m:02d}" for m in range(1, 13)]
files = [
    (f"{base_path}{year}/{month}/yellow_tripdata_{year}-{month}.parquet", year, int(month))
    for year in years
    for month in months
]

# 3. Leer, normalizar y agregar columnas de año/mes
dataframes = []
for file_path, year, month in files:
    try:
        df = spark.read.parquet(file_path)

        # Normalización de columnas
        if "VendorID" in df.columns:
            df = df.withColumn("VendorID", col("VendorID").cast(IntegerType()))

        # Agregar columnas year y month
        df = df.withColumn("year", lit(year))
        df = df.withColumn("month", lit(month))

        dataframes.append(df)
    except Exception as e:
        print(f"Error leyendo {file_path}: {e}")

# 4. Unión de todos los DataFrames
df_final = dataframes[0]
for df in dataframes[1:]:
    df_final = df_final.unionByName(df, allowMissingColumns=True)

# 5. Guardar como Parquet
df_final.write.mode("overwrite").parquet(f"gs://{bucket}-taxis-data/taxis/final_data/yellow_tripdata_2022-2024.parquet")
```

6. Ejecutamos nuestro script de PySpark con **DataProc**

```bash
gcloud dataproc jobs submit pyspark \
  gs://$PROJECT_ID-code/fusion_parquet.py \
  --cluster=taxi-clean-cluster --region=$REGION
```

7. Cuando termine de pasar toda la informacion detemos el Cluster

```bash
gcloud dataproc clusters delete taxi-clean-cluster --region=$REGION --quiet
```

---
# Tratamiento de la data

1. Subimos nuestra data "sucia" a bigquery
  1. Publicamos la data momentaniamente para evitar confusion con los permisos
  ```bash
  gsutil iam ch allUsers:objectViewer gs://$PROJECT_ID-taxis-data
  ```
  Volvemos a hacer privada la data
  ```bash
  gsutil iam ch -d allUsers gs://$PROJECT_ID-taxis-data
  ```
  > [!IMPORTANT] 
  > Esto solo se hace al ser un ejercicio de practica **NO ES APLICABLE A LA VIDA REAL**


  2. Subimos la data a **BigQuey**

  ```bash
  bq load \
    --source_format=PARQUET \
    --autodetect \
    --replace \
    --project_id=$PROJECT_ID \
    taxi.yellow_tripdata_2022_2024 \
    "gs://$PROJECT_ID-taxis-data/taxis/final_data/*.parquet"
  ```

2. Limpiamos la data siguiendo el siguiente proceso:
    1. Eliminamos las siguientes columnas ya que no aportan informacion vital para los **ETL'S**.
        1. **store_and_fwd_flag**  
        (Indica si el registro fue almacenado en la memoria del vehículo antes de enviarse al proveedor)
        2. **PULocationID**  
        (Zona TLC donde se activó el taxímetro.)
        3. **DOLocationID**  
        (Zona TLC donde se desactivó el taxímetro.)
    2. Reemplazamos los **nulos**.
        1. En la columna **RatecodeID** los reemplazamos por 99.
        2. En el resto de columnas reemplazamos por la **media**.
      
3. Ejecutamos el proceso anterior en Alteryx (dataprep) y lo dejamos en un una nueva tabla llamada taxis_limpia.

---

# Mostramos los KPI's en looker studio

- Pregunta 1 (Cantidad de viajes por mes):
```query
SELECT
  EXTRACT(YEAR FROM year) AS anio,
  month,
  COUNT(*) AS total_viajes
FROM
  `qwiklabs-gcp-02-4b407eea4480.taxi.taxis_etl`
GROUP BY anio, month
ORDER BY anio, month;
```

- Pregunta 2 (Duración promedio del viaje (en minutos))
```query
SELECT
  AVG(TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE)) AS duracion_promedio_min
FROM
  `qwiklabs-gcp-02-4b407eea4480.taxi.taxis_etl`
WHERE
  tpep_dropoff_datetime IS NOT NULL
  AND tpep_pickup_datetime IS NOT NULL
```

- Pregunta 3 (Promedio de distancia recorrida por viaje (en millas))
```query
SELECT
  AVG(trip_distance) AS distancia_promedio_millas
FROM
  `qwiklabs-gcp-02-4b407eea4480.taxi.taxis_etl`
WHERE
  trip_distance IS NOT NULL
```

- Pregunta 4 (Zonas con mayor número de viajes (Top 5 por pickup))
```query
SELECT
  PULocationID
  COUNT(*) AS total_viajes
FROM
  `qwiklabs-gcp-02-4b407eea4480.taxi.taxis_etl`
GROUP BY PULocationID
ORDER BY total_viajes DESC
LIMIT 5;
```

- Pregunta 5 (Porcentaje de viajes con pago en efectivo vs tarjeta)
```query
SELECT
  CASE
    WHEN payment_type = 1 THEN 'Tarjeta'
    WHEN payment_type = 2 THEN 'Efectivo'
    ELSE 'Otro'
  END AS medio_pago,
  COUNT(*) AS cantidad,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS porcentaje
FROM
  `qwiklabs-gcp-02-4b407eea4480.taxi.taxis_etl`
GROUP BY medio_pago;
```
