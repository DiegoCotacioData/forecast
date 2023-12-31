# Microservicio de Pronostico de Precios Agricolas.

## Descripción del microservicio:

El servicio de forecasting de precios de productos agricolas es un esfuerzo integrado a la estrategia de pricing adelantada por la celula de Pricing e Inventario. El objetivo principal de esta herramienta es permitir a los equipos de procurement y sales optimizar estrategias de precios, permitiendoles preveer las tendencias de precios para cerrar mejores acuerdos de compra-venta con usuarios de la cadena de suminitro agricola.

El alcance inicial de este proyecto es la generación de pronostico de precios para los siguientes 5 dias.

## Características Principales

### Funcionamiento: 
El servicio cuenta con 2 grupos de pipelines: ETL Pipelines y Forecast Pipelines:

* ETL Pipelines:  Son los responsables de la extracción de datos crudos desde las fuentes de datos de origen y las disponibilizan en fuentes de datos de Rurall (Temporalmente soportadas en Google Sheets).Estos pipelines se ejecutan bajo un cron de Github Actions de forma diaria y semanal.

* Forecast Pipelines: Son los responsables de ejecutar el servicio de pronostico, desde la extracción, entrenamiento, inferencia y almacenamiento de resultados. Estos pipelines se orquestan y se ejecutan de forma diaria bajo un cron empleando #Captus.

### Datos:
* Inputs: Las features empleadas actualmente son oficiales para colombia, siendo estas: SIPSA y DANE desde las cuales se extraen datos de precios, volumenes movidos en centrales de abasto y exportación de productos agricolas.

* Outputs: El servicio tiene 4 salidas principales:
  * Forecast bajo enfoque Day-Product-Model. 
  * Forecast bajo enfoque Product-Model.
  * Cross  validation training metrics
  * Training Features.


### Modelos: 
La primer version del microservicio realiza una implementación de 3 modelos globales basados en arquitecturas de redes neuronales, siendo estos: 

* Temporal Fusion Transformer (TFT)
*  DeepAR
*  N-Hits.
* Adicionalmente se usa como baseline el calculo de Rolling Window.

 Encontrandose las configuraciones de estas arquitecturas en src/service_settings/models_settings.py.


## Arquitectura

El diseño del servicio se realiza siguiendo practicas de diseño de soluciones de ML en MLOps. Estructurandose este a traves de 3 componentes o pipelines con funciones especificas:

- **Feature pipeline**: Extrae, preprocesa y realiza feature engineering disponibilizando las features de entrenamiento.
- **Training pipeline**: Crea los artefactos de entrenamiento, entrena bajo cross validation y evalua los modelos entrenados.
- **Inference pipeline**: Crea dataframe de inferencia, genera predicciones, realiza seleccion de forecast y carga resultados.

```bash
.
├── .github/workflows/           # github actions:
│       ├── prices_etl.yml      # schedule del pipeline de precios
│       └── run_forecast_service_.yml         # ejecuta manualmente el servicio de forecast
│
├── src/
│   │
│   ├── etl_pipelines/
│   │   ├── sipsa_prices_pipeline.py         #
│   │   ├── abasto_volumnes_pipeline.py
│   │   └── expo_volumnes_pipeline.py
│   │
│   ├── main.py  # orquesta y ejecuta los forecast_pipelines
│   │   
│   ├── forecast_pipelines/
│   │   ├── feature/ 
│   │   │     ├── feature_pipeline.py      # pipeline
│   │   │     └── feature_pipeline_steps/  # modules
│   │   │
│   │   ├── training/ 
│   │   │     ├── training_pipeline.py      # ...
│   │   │     └── training_pipeline_steps/  # ...
│   │   │
│   │   ├── inference/ 
│   │   │      ├── inference_pipeline.py      # ...
│   │   │      └── inference_pipeline_steps/  # ...
│   │   │
│   │   └── training_checkpoints/  # almacena checkpoints de entrenamiento
│   │
│   ├── service_settings/
│   │   ├── infra_settings.py         # define accesos a databases
│   │   ├── forecast_settings.py      # define principales variables del servicio
│   │   ├── models_settings.py        # define la arquitectura de los modelos
│   │   └── etl_settings.py           # define principales variables y accesos de los etl pipelines
│   │
│   ├── utils/
│       ├── gsheets_connector.py      
│       ├── api_requestor.py 
│       └── credentials.json
│   
├── config.yaml
├── requirements.txt
├── etl_equirements.txt
└── README.md 
```


## Licencia

Este proyecto está bajo la licencia MIT.

---
© 2023 Rurall