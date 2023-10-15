# Introduction 

Breve ejemplo de uso de Prefect como sistema de orquestación de procesos. Prefect es una biblioteca de Python
que facilita la orquestación, planificación y monitorización de flujos de trabajo (workflows) de datos.

A lo largo de este ejemplo, tiene lugar el siguiente procedimiento:

- Carga de datos (estudio_ipc): llamada a la API del INE y descarga de la información correspondiente (serie de IPC)
  - La información descargada es guardada en un fichero csv en el escritorio (pretende simular una escritura en BBDD)
- Modelo Arima (prediccion_ipc): lectura del dataset y entrenamiento y predicción de la serie de tiempo 
para n horizontes (usuario)
  - Tanto la predicción como la serie histórica es guardada en un fichero csv (de nuevo, simula la escritura en BBDD)

# Execution

Para el correcto funcionamiento del proceso es necesario que se ejecute los módulos *cargar_datos.py* 
y *modelo_arima.py* como se indica a continuación:

**LLamada a la API para cargar los datos**

```
cargar_datos.py
```

```
prefect deployment run 'INE Datos/estudios_ipc'
```

**Uso de ARIMA para modelizar la serie de tiempo**

```
modelo_arima.py
```

```
prefect deployment run 'INE Datos/prediccion_ipc'
```

**Acceso al servidor**

```
prefect server start
```

Se puede acceder a la UI al ejecutar cada uno de los procesos.

# Dependencies

El proyecto utiliza las siguientes librerías:

```
prefect==2.13.7
numpy==1.26.0
pandas==2.1.1
matplotlib==3.7.2
statsmodels==0.14.0
jupyterlab==4.0.5
```

Para instalar los requerimientos del proyecto:

```
pip install -r requirements.txt
```

# Other Considerations

(C) psancabrera@gmail.com - 2023