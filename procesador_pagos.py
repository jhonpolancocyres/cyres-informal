import pandas as pd
import os
import glob

def consolidar_pagos():
    ruta_origen = r'C:\Dashboard\data\pagos_diarios'
    ruta_destino = r'C:\Dashboard\data\PagosConsolidado.csv'
    
    # 1. Buscar todos los archivos Excel en la carpeta de pagos
    archivos_csv = glob.glob(os.path.join(ruta_origen, "*.csv"))
    
    if not archivos_csv:
        return "No se encontraron archivos en la carpeta de pagos diarios."

    print(f"Encontrados {len(archivos_csv)} archivos. Iniciando consolidación...")

    # 2. Leer y concatenar todos los archivos
    lista_df = []
    for archivo in archivos_csv:
        try:
            df = pd.read_csv(archivo, sep=';', encoding='latin1')
            # Agregamos una columna opcional para saber de qué archivo vino el dato
            df['ARCHIVO_ORIGEN'] = os.path.basename(archivo)
            lista_df.append(df)
        except Exception as e:
            print(f"Error leyendo {archivo}: {e}")

    # 3. Unir todo en un solo DataFrame
    consolidado = pd.concat(lista_df, ignore_index=True)

    # 4. Guardar el resultado final (Sobrescribe el anterior)
    consolidado.to_csv(ruta_destino, index=False, sep=';', encoding='latin1')
    
    return f"Éxito: Se consolidaron {len(archivos_csv)} archivos en PagosConsolidado.csv"


if __name__ == "__main__":
    resultado = consolidar_pagos()
    print(resultado)