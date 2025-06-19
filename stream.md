
# Proceso de **Big data** para los datos de streaming

Para empezar vamos a recordar la arquitectura que se va a llevar a cabo para este proyecto.

El proceso para el control de la información llegada por el streaming sera así.

| Fuente | Google Cloud | | | | |
|-----------------------|----------------|---------|----------|----------|---------------|
|  **Streaming API URL**  | Cloud Function | Pub/Sub | Dataflow | BigQuery | Looker studio |

## 1. Creamos el tópico **Pub/Sub**.

Para comenzar creamos nuestro tópico para después conectarlo al endpoint del api.

```bash
gcloud pubsub topics create topic-stream
```

> Este código crea el tópico con el nombre "topic.stream", si se quiere cambiar solo debe cambiar ese parámetro.

## 2. Definimos la función de **Cloud Function**. 
> [!NOTE]
> Para definir la función debemos usar el código a continuación con su **requeriments.txt**

Este código recibe los datos y los sube al tópico **Pub/Sub**.  
Para crear este codigo se debe crear localmente en **Cloud Console** con:
```bash
nano main.py
```
```python
import json
from google.cloud import pubsub_v1

PROJECT_ID = "qwiklabs-gcp-01-a661d525d46b"
TOPIC_ID = "topic-stream"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

columnas_obligatorias = [
    "id_cliente", "cliente", "genero", "id_producto", "producto",
    "precio", "cantidad", "monto", "forma_pago", "fecreg"
]

def recibir_datos(request):
    if request.method != 'POST':
        return ("Método no permitido", 405)

    try:
        data = request.get_json()
        if data is None:
            return ("No se recibió JSON válido", 400)

        # Validar y completar con None si falta alguna columna
        datos_limpios = {col: data.get(col, None) for col in columnas_obligatorias}

        # Publicar en Pub/Sub
        mensaje = json.dumps(datos_limpios).encode("utf-8")
        publisher.publish(topic_path, mensaje)

        return ("Datos limpiados y enviados a Pub/Sub", 200)

    except Exception as e:
        print(f"Error: {e}")
        return (f"Error al procesar datos: {str(e)}", 500)
```
> [!CAUTION]
> REEMPLAZAR PROJECT_ID

**requeriments.txt**
```bash
nano requeriments.txt
```
```requeriments
google-cloud-pubsub
```
---
Para el despliegue de la función usamos este comando en **Cloud Console**.  

Permisos para usar **Cloud Function Gen2**:
```bash
gcloud services enable run.googleapis.com
```
Comando:
```bash
gcloud functions deploy recibir_datos \
  --gen2 \
  --runtime=python310 \
  --region=us-central1 \
  --entry-point=recibir_datos \
  --trigger-http \
  --allow-unauthenticated
```
> [!IMPORTANT]
> Este comando nos devuelve la URL para registrar.

Para ver los datos publicados ocupamos estos comandos.
```bash
gcloud pubsub subscriptions create ver-mensajes --topic=datos-endpoint
gcloud pubsub subscriptions pull ver-mensajes --limit=10 --auto-ack
```

# 3. Pub/Sub → DataFlow → BigQuery
* Creamos un bucket de **Cloud Storage** para nuestro proceso de limpieza en **python**.  
	* Desbloqueamos los servicios por si acaso:
		```bash
		gcloud services enable dataflow.googleapis.com
		gcloud services enable pubsub.googleapis.com
		gcloud services enable storage.googleapis.com
		```
	* Creamos el bucket con el nombre "bucket-stream":
		```bash
		gcloud storage buckets create gs://bucket-stream --location=us-central1
		```
* Creamos una suscripción en Pub/Sub para Dataflow.
	```bash
	gcloud pubsub subscriptions create subs-dataflow \
	  --topic=topic-stream
	```
* Creamos el dataset de BigQuery donde van a llegar los datos limpios:
	```bash
	bq mk streaming
	```
	```bash
	bq mk --table \
	  "$(gcloud config get-value project):streaming.ventas_limpias" \
	  id_cliente:STRING,cliente:STRING,genero:STRING,id_producto:STRING,producto:STRING,precio:FLOAT,cantidad:INTEGER,monto:FLOAT,forma_pago:STRING,fecreg:TIMESTAMP
	```
	Verificamos que se creó correctamente:
	```bash
	bq show streaming.ventas_limpias
	```
* Creamos el script de limpieza en **Cloud Console**:
	```bash
	nano limpiar_pubsub.py
	```
	```python
	import json
	import apache_beam as beam
	from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
	import google.auth
	from datetime import datetime

	# Obtener el Project ID dinámicamente
	try:
	    _, PROJECT_ID = google.auth.default()
	except Exception:
	    PROJECT_ID = 'qwiklabs-gcp-01-a661d525d46b'

	BUCKET = 'bucket-david-stream'
	TOPIC_ID = 'topic-stream'
	BQ_TABLE = 'streaming.ventas_limpias'  # Dataset.Tabla

	# Columnas obligatorias
	COLUMNAS = [
	    "id_cliente", "cliente", "genero", "id_producto", "producto",
	    "precio", "cantidad", "monto", "forma_pago", "fecreg"
	]

	def limpiar_json(mensaje):
	    try:
	        datos = json.loads(mensaje.decode('utf-8'))

	        limpio = {}
	        for col in COLUMNAS:
	            valor = datos.get(col, None)
	            if col in ["precio", "monto"]:
	                try:
	                    limpio[col] = float(valor)
	                except:
	                    limpio[col] = None
	            elif col == "cantidad":
	                try:
	                    limpio[col] = int(valor)
	                except:
	                    limpio[col] = None
	            elif col == "fecreg":
	                # Verificar formato de timestamp
	                try:
	                    limpio[col] = str(valor)
	                except:
	                    limpio[col] = None
	            else:
	                limpio[col] = valor
	        return limpio
	    except Exception:
	        return None

	def run():
	    options = PipelineOptions(
	        runner='DataflowRunner',
	        project=PROJECT_ID,
	        region='us-central1',
	        temp_location=f'gs://{BUCKET}/temp',
	        staging_location=f'gs://{BUCKET}/staging',
	        streaming=True
	    )
	    options.view_as(StandardOptions).streaming = True

	    with beam.Pipeline(options=options) as p:
	        raw_data = (
	            p
	            | 'Leer de Pub/Sub' >> beam.io.ReadFromPubSub(topic=f'projects/{PROJECT_ID}/topics/{TOPIC_ID}')
	        )

	        # 💾 Guardar mensajes crudos
	        _ = (
	            raw_data
	            | 'Decodificar crudo' >> beam.Map(lambda x: x.decode('utf-8'))
	            | 'Guardar en bruto' >> beam.io.WriteToText(
	                file_path_prefix=f'gs://{BUCKET}/raw/mensajes_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}',
	                file_name_suffix='.jsonl'
	            )
	        )

	        # 🧹 Limpiar y validar
	        datos_limpios = (
	            raw_data
	            | 'Limpiar datos' >> beam.Map(limpiar_json)
	            | 'Filtrar nulos' >> beam.Filter(lambda x: x is not None)
	        )

	        # 🧾 Insertar en BigQuery
	        datos_limpios | 'Guardar en BigQuery' >> beam.io.WriteToBigQuery(
	            table=f'{PROJECT_ID}:{BQ_TABLE}',
	            schema={
	                'fields': [
	                    {'name': 'id_cliente', 'type': 'STRING'},
	                    {'name': 'cliente', 'type': 'STRING'},
	                    {'name': 'genero', 'type': 'STRING'},
	                    {'name': 'id_producto', 'type': 'STRING'},
	                    {'name': 'producto', 'type': 'STRING'},
	                    {'name': 'precio', 'type': 'FLOAT'},
	                    {'name': 'cantidad', 'type': 'INTEGER'},
	                    {'name': 'monto', 'type': 'FLOAT'},
	                    {'name': 'forma_pago', 'type': 'STRING'},
	                    {'name': 'fecreg', 'type': 'TIMESTAMP'}
	                ]
	            },
	            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
	            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER  # la creaste tú
	        )

	if __name__ == '__main__':
	    run()
	```
	> [!CAUTION]
	> Actualizar el project_id al crear
	>
	
	Para ejecutar el script ingresamos este comando en **Cloud Storage**.
	```bash
	python limpiar_pubsub.py
	```
