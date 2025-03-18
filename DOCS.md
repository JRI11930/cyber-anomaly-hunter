# 🛡️ Detección de Anomalías en Redes con IA  

## 1. ***Business Understanding***
   a. Determinar los objetivos del negocio  
   b. Evaluar la situación actual  
   c. Determinar los objetivos del proyecto  
   d. Producir un plan de proyecto  

## 2. ***Data Understanding***
   a. Recolectar los datos iniciales  
   b. Describir los datos  
   c. Explorar los datos  
   d. Verificar la calidad de los datos  

## 3. ***Data Preparation***
   a. Seleccionar los datos  
   b. Limpiar los datos  
   c. Construir los datos  
   d. Integrar los datos  
   e. Formatear los datos  

## 4. ***Modeling***
   a. Seleccionar la técnica de modelado  
   b. Diseñar la prueba  
   c. Construir el modelo  
   d. Evaluar el modelo  

## 5. ***Evaluation***
   a. Evaluar los resultados del proyecto  
   b. Recuento del proceso  
   c. Determinar los pasos siguientes  


Las características con las que cuenta el dataset son:

| Característica     | Descripción                                                                 |
|--------------|-----------------------------------------------------------------------------|
| Dur      | Duración de la conexión en segundos.                                         |
| Proto    | Protocolo de comunicación utilizado (ej. TCP, UDP, ICMP).                   |
| Dir      | Dirección del flujo de tráfico (ej. → si es de origen a destino, o ← si es de destino a origen). |
| SrcAddr  | Dirección IP de origen (dirección del dispositivo que envía el tráfico). |
| Sport    | Puerto de origen utilizado para la comunicación. |
| DstAddr  | Dirección IP de destino (dirección del dispositivo receptor). |
| DstPort  | Puerto de destino utilizado para la comunicación.|
| State    | Estado de la conexión (ej. CON para conexiones establecidas, INT para interrumpidas). |
| sTos	  | Tipo de servicio (ToS) del paquete enviado por el origen (campo en el encabezado IP). |
| dTos	  | Tipo de servicio (ToS) del paquete enviado por el destino. |
| TotPkts  | Número total de paquetes transmitidos durante el flujo. |
| TotBytes | Número total de bytes transmitidos en ambos sentidos. |
| SrcBytes | Número de bytes enviados desde el origen al destino. |
| Label | Etiqueta que describe la naturaleza del flujo (Background, From-Botnet, etc.). |
---

# ***DASK, Libreria de Python***

Dask es una libreria de Python de codigo abierto, que sirve para realizar computo paralelo, esta libreria permite el uso optimo del CPU asi como la administracion de memoria eficiente. En particular se usa para procesar conjuntos de datos muy grandes donde el tiempo de procesamiento implica deficiencias.

### Algunas caracteristicas de Dask son:
- Computo concurrente: Permite el manejo de diferentes que se pueden llevar a cabo de manera paralela para reducir el tiempo de procesamiento, Dask hace uso de los nucleos que el CPU permita asignar tareas para poder reducir la carga de trabajo.
- Tareas dinamicas usando grafos: Dask usa una representacion de un grafo aciclico, es decir, cada nodo representa una de las operaciones en las que se dividio la tarea y cada enlace representa la dependencia entre ellas. Este grafo se construye de manera dinamica lo cual ayuda a optimizar las operaciones que se llevan en paralelo

# ***Instalacion de Dask***

Para la instalacion debemos usar el instalador de paquetes de Python (pip) e introducir el siguiente comando:

```python
pip install dask
```
## ***Creando un DataFrame Grande***

Vamos a generar un conjunto de datos grande utilizando pandas y convertirlo en un DataFrame de Dask:
```python
# Crear un DataFrame grande con pandas
num_rows = 10**7
num_cols = 5
data = {f"col_{i}": np.random.rand(num_rows) for i in range(num_cols)}
df = pd.DataFrame(data)
```
## ***Cálculo Secuencial con pandas***

Primero, realizaremos un cálculo secuencial utilizando pandas para calcular la media de los datos:
```python
# Cálculo secuencial usando pandas
start_time = time.time()
mean_sequential = df.mean()
sequential_time = time.time() - start_time

print(f"Media secuencial:\n{mean_sequential}\nTiempo: {sequential_time} segundos")
```
## ***Cálculo Paralelo con Dask***

Ahora, utilizaremos Dask para dividir el conjunto de datos en partes (particiones) y realizar el cálculo de manera paralela:
```python
# Convertir el DataFrame de pandas a un DataFrame de Dask
ddf = dd.from_pandas(df, npartitions=4)

# Cálculo paralelo usando Dask
start_time = time.time()
mean_parallel = ddf.mean().compute()
parallel_time = time.time() - start_time

print(f"Media paralela:\n{mean_parallel}\nTiempo: {parallel_time} segundos")
```
## ***Comparación de Rendimiento***

```
Media secuencial (Usando pandas)
col_0    0.499924
col_1    0.499979
col_2    0.500116
col_3    0.500160
col_4    0.500008
dtype: float64
Tiempo: 0.25031352043151855 segundos
Media paralela (Usando dask)
col_0    0.499924
col_1    0.499979
col_2    0.500116
col_3     0.50016
col_4    0.500008
dtype: Float64
Tiempo: 0.13022160530090332 segundos
```

Dask puede mejorar significativamente el rendimiento en grandes conjuntos de datos, especialmente en sistemas con varios núcleos de CPU. En este ejemplo, podemos comparar los tiempos de ejecución y observar la ganancia obtenida al paralelizar la operación.

Si el dataset es aún más grande o tiene una arquitectura de CPU con más núcleos, puede experimentar una mayor mejora en el rendimiento.
