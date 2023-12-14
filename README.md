# Microservicio de Forecasting de Demanda

## Descripción del microservicio:

Este microservicio proporciona capacidades de forecasting de demanda para apoyar la planificación y gestión de inventario. Utiliza algoritmos avanzados para prever la demanda futura en base a datos históricos y otros factores relevantes.

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


## Instalación y Configuración

### Requisitos Previos

- [Listar los requisitos previos, como un entorno de ejecución específico, bibliotecas, etc.]

### Configuración

1. Clone este repositorio: `git clone https://github.com/tuusuario/tumicroservicio.git`
2. Navegue al directorio del microservicio: `cd tumicroservicio`
3. Configure los archivos de configuración según sea necesario.

### Ejecución

1. Ejecute el servicio de forecasting: `comando de ejecución`
2. Acceda a la API a través del endpoint proporcionado.



Si desea contribuir a este proyecto, siga los siguientes pasos:

1. Fork del repositorio.
2. Cree una nueva rama para su función: `git checkout -b feature/nueva-funcion`.
3. Realice los cambios y confirme: `git commit -m "Agregada nueva función"`.
4. Envíe un Pull Request.

## Problemas Conocidos

[Listar cualquier problema conocido y posibles soluciones o mitigaciones.]

## Licencia

Este proyecto está bajo la licencia [especificar la licencia].

---
© [Año] [Tu Nombre o Nombre de tu Empresa]
