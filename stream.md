
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

# Reemplaza esto por tu ID real de proyecto
PROJECT_ID = "tu-id-de-proyecto"
TOPIC_ID = "topic-stream"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def recibir_datos(request):
    if request.method != 'POST':
        return ("Método no permitido", 405)

    try:
        datos = request.get_json()
        if datos is None:
            return ("No se recibieron datos JSON", 400)

        mensaje = json.dumps(datos).encode("utf-8")
        publisher.publish(topic_path, mensaje)

        return ("Mensaje publicado en Pub/Sub", 200)

    except Exception as e:
        print(f"Error: {e}")
        return (f"Error al publicar: {str(e)}", 500)
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
