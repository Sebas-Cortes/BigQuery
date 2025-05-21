# Sistema de analisis de datos BIG DATA

Este proyecto es un analisis de la información de taxis de nueva york, los cuales estan separados en meses y años, además de un streaming de datos el cual será actualizado en tiempo real.
El proyecto completo será desarrollado en **Google Cloud Plataform (GCP)** siguiendo una arquitectura previamente descrita.

---

# Comandos para iniciar el lab

1. Definimos la region y el ID del proyecto:

```bash
gcloud projects list
gcloud config set project YOUR_REAL_PROJECT_ID
```
Copiamos el proyect id por ejemplo:
```bash
gcloud config set project qwiklabs-gcp-00-6614d8b67483
```

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

2. Creamos un bucket temporal para pasar la tabla final a BigQuery

```bash
gsutil mb -l $REGION gs://$PROJECT_ID-bqtemp
gsutil iam ch serviceAccount:dataproc-sa@$PROJECT_ID.iam.gserviceaccount.com:objectAdmin gs://$PROJECT_ID-bqtemp
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

# Utilizamos DataProc para limpiar la data con los parquets en el bucket

1. Damos los permisos faltantes a nuestro usuario:

```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:dataproc-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/dataproc.worker"
```

2. Creamos nuestro cluster de **DataProc** para limpiar la data

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

3. Creamos nuestro script en **python** para limpiar la data:

```bash
gsutil cp clean_taxi.py gs://$PROJECT_ID-code/spark/
```

4. Creamos el data set en bigquery

```bash
bq --location=US mk taxi
```

5. Limpiamos la data siguiendo el siguiente proceso:
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
    3. Fusionamos los **parquets** en una unica tabla con 2 columnas nuevas:
        1. **Year**  
        (Año)
        2. **Month**  
        (Mes)

6. Script para el proceso de limpieza y carga a bigquery:  

> [!WARNING]  
> Este script necesita que se ingresen manualmente datos como  
> \<TU_BUCKET>

```python
#!/usr/bin/env python
# clean_taxi.py  v2  (salida BigQuery)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, regexp_extract, col, avg
)

# ───────── CONFIGURA ESTOS TRES VALORES ─────────
BUCKET      = "<TU_BUCKET>"          # ej: qwiklabs-gcp-6614d8b67483-data
BQ_DATASET  = "taxi"                      # crea con: bq mk taxi
BQ_TABLE    = "taxis_ny_final"             # nombre de tabla final
# ────────────────────────────────────────────────

PATH_IN   = f"gs://{BUCKET}-taxis-data/taxis/*/*/*.parquet"
TEMP_GCS  = f"gs://{BUCKET}-bqtemp"       # bucket temporal para el conector

spark = (
    SparkSession.builder
    .appName("taxi-cleaning")
    .getOrCreate()
)

# 1. Lectura
df = spark.read.parquet(PATH_IN)

# 2. Añade Year / Month
df = (
    df.withColumn("_filename", input_file_name())
      .withColumn("Year",  regexp_extract("_filename", r"/(\d{4})/", 1).cast("int"))
      .withColumn("Month", regexp_extract("_filename", r"/\d{4}/(\d{2})/", 1).cast("int"))
      .drop("_filename")
)

# 3. Elimina columnas poco útiles
df = df.drop("store_and_fwd_flag", "PULocationID", "DOLocationID")

# 4. Rellena nulos
df = df.fillna({"RatecodeID": 99})
num_cols = [c for (c, t) in df.dtypes if t in ("double", "int", "bigint", "float", "long") and c != "RatecodeID"]
means = df.select([avg(c).alias(c) for c in num_cols]).first().asDict()
df = df.fillna(means)

# 5. Escribe en BigQuery
(
  df.write.format("bigquery")
    .option("table",           f"{BQ_DATASET}.{BQ_TABLE}")
    .option("temporaryGcsBucket", TEMP_GCS)
    .option("writeMethod",     "direct")      # requiere el conector ≥ 0.31
    .mode("overwrite")
    .save()
)

print("✔ Datos limpios cargados en BigQuery:", f"{BQ_DATASET}.{BQ_TABLE}")
spark.stop()
```

7. Ejecutamos nuestro script de PySpark con **DataProc**

```bash
gcloud dataproc jobs submit pyspark \
  gs://$PROJECT_ID-code/spark/clean_taxi.py \
  --cluster=taxi-clean-cluster \
  --region=$REGION \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar
```

8. Cuando termine de pasar toda la informacion detemos el Cluster

```bash
gcloud dataproc clusters delete taxi-clean-cluster --region=$REGION --quiet
```
