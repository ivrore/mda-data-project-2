# **DATA PROJECT 2** 
## EDEM Master Data Analitycs 22/23
---
### TMS (Trace Mobility Systems)
---


<img src="./img/tms_logo.jpg"  width="200" height="200">

|Nombre|Github|
|:-----:|:-----------:|
|Iván Pla|__[ivplagar/](https://github.com/ivplagar)__|
|Lara Peiró|__[larapeiro/](https://github.com/larapeiro)__|
|Rafa Cuquerella|__[RafaelCuquerella/](https://github.com/RafaelCuquerella)__|
|Carlos Torres|__[CarlosTorresAracil/](https://github.com/CarlosTorresAracil)__|
|Iván Rodríguez|__[ivrore/](https://github.com/ivrore)__|
---
## Instrucciones
---  
1. Crear topics en Pub/Sub de GCP:

+ *Admin output* - Este topic recibe los datos del sensor rfid + status 
+ *Alert output* - Este topic es el que debemos indicar en cloud function. Solo muestra alertas de temperatura.
+ *rfid_input* - Este topic recibe toda la información del sensor rfid.

2. Clonar el repositorio en la shell de GCP:
```
 git clone <REPO_NAME>
```
3. Ejecutar desde la shell el generador **generador.py**
```
cd /Generator/Iotsensor
python generator.py \
    --project_id <PROJECT_ID> \
    --topic_name <INPUT_PUBSUB_TOPIC>
```
4. Ejecutar desde la shell el pipeline **dataflow.py**
```
python dataflow.py \
    --project_id <PROJECT_ID> \
    --input_subscription <INPUT_PUBSUB_SUBSCRIPTION> \
    --output_topic <OUTPUT_PUBSUB_TOPIC> \
    --output_bigquery <DATASET>.<TABLE> \
    --runner DataflowRunner \
    --job_name <YOUR_DATAFLOW_JOB> \
    --region <GCP_REGION> \
    --temp_location gs://<BUCKET_NAME>/tmp \
    --staging_location gs://<BUCKET_NAME>/stg
```
5. Crear una **cloud function** con el código de google_cloud_function.py
6. Asignar variables de entorno en cloud functions.
   
   - PROJECT_ID: Nombre del proyecto
   - ALERT_TOPIC_OUTPUT: Nombre del topic para las alertas

<img src="./img/env_cloud.png"  width="450" height="150">