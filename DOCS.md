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