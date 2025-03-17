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
| dur      | Duración de la conexión en segundos.                                         |
| proto    | Protocolo de comunicación utilizado (ej. TCP, UDP, ICMP).                   |
| dir      | Dirección del flujo de tráfico (ej. → si es de origen a destino, o ← si es de destino a origen). |
| state    | Estado de la conexión (ej. CON para conexiones establecidas, INT para interrumpidas). |
| stos / dtos | Tipo de servicio (ToS) del tráfico enviado y recibido. Son valores que indican la prioridad del paquete en la red. |
| tot_pkts | Número total de paquetes enviados en la conexión.                           |
| tot_bytes| Número total de bytes transferidos.                                         |
| src_bytes| Cantidad de bytes enviados desde la IP de origen.                            |
| label    | Etiqueta que indica si el tráfico es normal o pertenece a una botnet (tráfico malicioso). |
| Family   | Especie de botnet detectada (ej. Neris, Rbot, Virut, Murlo, etc.).           |

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


