from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from io import StringIO  # Importando StringIO desde io

# Define los escenarios
escenarios = [48, 49, 50, 51, 53, 54]
lista_escenarios = {48 : 'Sogou', 49 : 'Murlo', 50 : 'Neris', 51 : 'RBot', 53 : 'NsisAy', 54 :'Virut'}

# Función para extraer el enlace .binetflow de una URL
def extraer_enlace_binetflow(escenario):
    url = f'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-{escenario}/detailed-bidirectional-flow-labels/'
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Buscar todos los enlaces en la página
        for enlace in soup.find_all('a'):
            href = enlace.get('href')
            if href and href.endswith('.binetflow'):
                # Construir URL completa
                enlace_completo = url + href if not href.startswith('http') else href
                return enlace_completo
                
        print(f"No se encontró archivo .binetflow en el escenario {escenario}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error al acceder al escenario {escenario}: {e}")
        return None

# Función para descargar y procesar un archivo .binetflow
def procesar_binetflow(url, escenario):
    try:
        print(f"Descargando datos del escenario {escenario}...")
        response = requests.get(url)
        response.raise_for_status()
        
        try:
            df = pd.read_csv(StringIO(response.text), sep=',', low_memory=False)
            # Agregar la etiqueta del tipo de botnet
            if escenario in lista_escenarios:
                df['BOTNET_NAME'] = lista_escenarios[escenario]
            df.to_csv(f"./data/escenario_{escenario}_procesado.csv", index=False)
            print(f"Datos procesados: {df.shape[0]} filas, {df.shape[1]} columnas")
            return df
        except Exception as e:
            print(f"Error al procesar como CSV: {e}")
            print("El archivo fue guardado pero no pudo ser procesado como DataFrame")
            return None
            
    except Exception as e:
        print(f"Error al procesar el escenario {escenario}: {e}")
        return None

# Lista para almacenar todos los DataFrames
dataframes = []

# Procesar cada escenario
for escenario in escenarios:
    enlace = extraer_enlace_binetflow(escenario)
    
    if enlace:
        print(f"Encontrado archivo .binetflow para escenario {escenario}: {enlace}")
        df = procesar_binetflow(enlace, escenario)
        if df is not None:
            dataframes.append(df)
    else:
        print(f"No se encontró archivo .binetflow para el escenario {escenario}")

# Combinar todos los DataFrames (si hay más de uno)
if dataframes:
    try:
        df_final = pd.concat(dataframes, ignore_index=True)
        print(f"Dataset final combinado: {df_final.shape[0]} filas, {df_final.shape[1]} columnas")
        
        # Guardar el resultado en un archivo CSV
        df_final.to_csv('./data/total.csv', index=False)
        print("Datos combinados guardados en 'todos_escenarios_combinados.csv'")
    except Exception as e:
        print(f"Error al combinar datasets: {e}")
        print("Revisa los archivos individuales de cada escenario")
else:
    print("No se pudieron extraer datos de ningún escenario")

