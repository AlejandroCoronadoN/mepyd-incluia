# Incluia-Diominicana

## `Instalación` 
1. `Clonar repositorio:` Para empezar a utiliza este codigo es necesario seguir los siguientes pasos. En primero lugar se debe descargar el presente [repositorio](https://github.com/AlejandroCoronadoN/mepyd-incluia) e instalarlo de manera local utilizando el siguiente comando.
```
git clone https://github.com/AlejandroCoronadoN/mepyd-incluia.git
```

1. `Descargar datos:` Una vez clonado el reporsitorio es necesario descargar la carpeta comprimida con los datos necesario para correr el codigo.  El archivo comprimido con la información puede ser encontrada dentro de la carptea del proyecto ´mepyd´ compartida con el equipo mepyd en [Google Drive](https://drive.google.com/drive/folders/1svr7c0bSfSUC5y3drHoMfRkDWKj1U5zu). El codigo del proyecto también puede ser descargado directamente de esta carpeta pero se recomienda clonar el repositorio para poder recibir las actualizaciónes de la rama master.

2. `Copiar datos:` Una vez que se haya descargado el archivo comprimido puedes proceder a descomprimir el archivo. Esto generara una nueva carpeta con el nombre de data. Si haces doble click en la carpeta podrás enontrar un subfolder con el mismo nombre de ´data´. Este folder tendrá que ser copiado a la raiz del repositorio clonado. Es decir deberá esta en el mismo path que las carpetas (admindiv, admindiv_aggroupation, centers, eda, export ...)

3. `Crear ambiente del proyecto - Instalar anaconda:` Este código fue escrito y desarrollado usando anaconda3. Para asegurar que el còdigo corra de manera correcta es necesario haber instalado anaconda3 en la computadora local. Para instalar anaconda3 deberá seguir las siguientes instrucciones en el siguiente [Link](https://www.anaconda.com/products/individual).  Dependiendo de sistema operativo que este utilizando será necesario configurar su variables de ambientes para que se reconozca anaconda3 para ejecutar python. En windows es neceario agregar las siguiente rutas en el Environment Variables de su computadora. Puede agregar estas variables buscando ´Edit the system environmental variables´ en su explorador de windows dentro de la pestaña Advances.  Puede hacer click en ´Environmental variables...´ y editar Path para agregar las siguientes rutas:
   * C:\Users\aleja\anaconda3\Library\bin
   * C:\Users\aleja\anaconda3\Scripts
   * C:\Users\aleja\anaconda3\
   
4. `Crear ambiente del proyecto - Crear ambiente:` Al instalar anaconda3 ahora podremos crear el ambiente criteria-do utilizando nuestro archivo yml. Para hacer estos ejecute el siguiente comando en su terminal:
```
conda env create --file environment.yml
```
5. `Crear ambiente del proyecto - Activar ambiente:` Una vez instalado el ambiente podremos activarlo simplemete escribiendo en la terminal:
 ```
conda activate criteria-do
```
5. `Correr el código:` Al haber terminado la instalación del ambiente y después de haberlo activado podemos correr el código. Para esto podemos colocarnos dentro de la carpeta del repositorio y abrir una nueva terminal. Ahora solo tenemos que abrir python (se recomienda utilizar ipython para visualizar con más detalle la ejecución del código y debuggear) y correr el comando run main. Esto empezará a correr el archivo main ubicado en la carpeta raiz del proyecto y esto llamará al resto de módulos para iniciar el procesamiento de datos. 

## Objetivo
Este código contiene el ETL para el procesamiento y transfomación de datos para el mapeo 
de demanda y oferte de la extensión del módulo de cuidados SUPERATE. El objetivo final de
este código es generar tres archivos finales.
1. `mepyd_neighborhoods_20210930` : Este archivo contiene la información agregada a nivel de barrio del procesamiento y cruzamientos de datos. El archivo nos permite identificar las diferentes métricas y variables a nivel de barrio. Este archivo deberá ser subido a la carpeta del proyecto en  [Google Drive](https://drive.google.com/drive/folders/1SoJajYOa1tqLRMcHjMPy5UofHeRZlPjF). El nombre del archivo deberá ser conservado y siempre hacer una substitución del archivo existente para permitir que Carto haga la actulización en Google Drive. 
2.  `mepyd_municipality_20210930` : Este archivo contiene la información agregada a nivel de municipio del procesamiento y cruzamientos de datos. El archivo nos permite identificar las diferentes métricas y variables a nivel de municipio. Este archivo deberá ser subido a la carpeta del proyecto en  [Google Drive](https://drive.google.com/drive/folders/1SoJajYOa1tqLRMcHjMPy5UofHeRZlPjF). El nombre del archivo deberá ser conservado y siempre hacer una substitución del archivo existente para permitir que Carto haga la actulización en Google Drive. 
3.  `mepy_areas_20210930` : Este archivo contiene la información de las areas de influencia de los centros. Este archivo e. Este archivo deberá ser subido a la carpeta del proyecto en  [Google Drive](https://drive.google.com/drive/folders/1SoJajYOa1tqLRMcHjMPy5UofHeRZlPjF). El nombre del archivo deberá ser conservado y siempre hacer una substitución del archivo existente para permitir que Carto haga la actulización en Google Drive. 
4.  `mepyd_centers_20210930` : Este archivo contiene la información de los centros procesados por el etl incluia'dominicana. El archivo nos permite identificar las capacidades y la posición geográfica de los centros. Este archivo deberá ser subido a la carpeta del proyecto en  [Google Drive](https://drive.google.com/drive/folders/1SoJajYOa1tqLRMcHjMPy5UofHeRZlPjF). El nombre del archivo deberá ser conservado y siempre hacer una substitución del archivo existente para permitir que Carto haga la actulización en Google Drive. 

## `Filesystem` Folder de datos para el procesamiento de información
Si completaste los pasos anteriores recordarás que hay una carpeta con la información para correr el código de incluia. Esta carpeta con el nombre de datos nos provee toda la infomación que será procesada, cruzada y condensada en los archivos de salida que se mencionaron en la sección anterior. Esta carpeta contiene dos subcarpetas que complen con diferentes funciones:
* **raw:** Esta carpeta contiene toda la información que será procesada por el pipeline. Todo los archivos crudos (sin procesar) se encuentran en esta carpeta.
* **process:** En esta carpeta se encuentran los archivos intermedios en el procesaminento de nuestro ETL. Cada Stage dentro del codigo main.py produce un archivo de salida que es almacenado dentro de esta carpeta.
* **export:** En este folder se ecnuentran los archivos de salida mencionados en la sección 2 que deberán ser guardados en en la carpeta de visualización de Google Drive (para que los mapas puedan ser actualizdos y publicados)

## `Ejecución:` Testing and Production
Al momento de ejcutar el código hat dos modalidades de 
* **raw:** Esta carpeta contiene toda la información que será procesada por el pipeline. Todo los archivos crudos (sin procesar) se encuentran en esta carpeta.
* **process:** En esta carpeta se encuentran los archivos intermedios en el procesaminento de nuestro ETL. Cada Stage dentro del codigo main.py produce un archivo de salida que es almacenado dentro de esta carpeta.
* **export:** En este folder se ecnuentran los archivos de salida mencionados en la sección 2 que deberán ser guardados en en la carpeta de visualización de Google Drive (para que los mapas puedan ser actualizdos y publicados)



