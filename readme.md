# Incluia-Diominicana

Este código contiene el ETL para el procesamiento y transfomación de datos para el mapeo 
de demanda y oferte de la extensión del módulo de cuidados SUPERATE. El objetivo final de
este código es generar tres archivos finales.
1. `mepyd_neighborhoods_20210930` : Este archivo contiene la información agregada a nivel de barrio del procesamiento y cruzamientos de datos. El archivo nos permite identificar las diferentes métricas y variables a nivel de barrio. Este archivo deberá ser subido a la carpeta del proyecto en  [Google Drive](https://drive.google.com/drive/folders/10tj4CPTOMF7Or9GH0pq4rLhr7h-irYyT?usp=sharing). El nombre del archivo deberá ser conservado y siempre hacer una substitución del archivo existente para permitir que Carto haga la actulización en Google Drive. 
2.  `mepyd_municipality_20210930` : Este archivo contiene la información agregada a nivel de barrio del procesamiento y cruzamientos de datos. El archivo nos permite identificar las diferentes métricas y variables a nivel de barrio. Este archivo deberá ser subido a la carpeta del proyecto en  [Google Drive](https://drive.google.com/drive/folders/10tj4CPTOMF7Or9GH0pq4rLhr7h-irYyT?usp=sharing). El nombre del archivo deberá ser conservado y siempre hacer una substitución del archivo existente para permitir que Carto haga la actulización en Google Drive. 
3.  `mepyd_centers_20210930` : Este archivo contiene la información agregada a nivel de barrio del procesamiento y cruzamientos de datos. El archivo nos permite identificar las diferentes métricas y variables a nivel de barrio. Este archivo deberá ser subido a la carpeta del proyecto en  [Google Drive](https://drive.google.com/drive/folders/10tj4CPTOMF7Or9GH0pq4rLhr7h-irYyT?usp=sharing). El nombre del archivo deberá ser conservado y siempre hacer una substitución del archivo existente para permitir que Carto haga la actulización en Google Drive. 
4.  `mepyd_areas_20210930` : Este archivo contiene la información agregada a nivel de barrio del procesamiento y cruzamientos de datos. El archivo nos permite identificar las diferentes métricas y variables a nivel de barrio. Este archivo deberá ser subido a la carpeta del proyecto en  [Google Drive](https://drive.google.com/drive/folders/10tj4CPTOMF7Or9GH0pq4rLhr7h-irYyT?usp=sharing). El nombre del archivo deberá ser conservado y siempre hacer una substitución del archivo existente para permitir que Carto haga la actulización en Google Drive. 

## `data` directory
Una carpeta que contiene la información en crudo que se utilizará en este proyecto. 
## `src` directory
Used for `project-etl` library, containing configuration files and local modules. It is installed in editable mode via `pip` in the `environment.yml` file


## User guide
1. Open `environment.yml` and change the name of the enviroment. We recommend to call it "criteria-<ISO 3166-1 alpha-2 country code in lowercase>", e.g. "criteria-br", "criteria-uy", "citeria-sv". ISO 3166-1 alpha-2 country codes are available [here](https://www.iso.org/obp/ui/#search)

