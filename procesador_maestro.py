import pandas as pd
import os
import glob
from datetime import datetime

# --- FUNCIONES DE APOYO PARA LAS FRANJAS ---
def obtener_franja_cyres(dias):
    if dias < -1: return "0- Corriente"
    if dias == -1: return "1- Vence mañana"
    if dias == 0: return "2- Vence hoy"
    if dias == 1: return "3- Venció ayer"
    if 2 <= dias <= 4: return "4- 2 a 4"
    if 5 <= dias <= 7: return "5- 5 a 7"
    if 8 <= dias <= 14: return "6- 8 a 14"
    if 15 <= dias <= 21: return "7- 15 a 21"
    if 22 <= dias <= 30: return "8- 22 a 30"
    return "9- Mayor a 30"

def obtener_franja_coca(dias):
    if dias <= 0: return "0- Corriente"
    if 1 <= dias <= 7: return "1- 1 a 7"
    if 8 <= dias <= 14: return "2- 8 a 14"
    if 15 <= dias <= 21: return "3- 15 a 21"
    if 22 <= dias <= 30: return "4- 22 a 30"
    return "5- Mayor a 30"

def procesar_todo():
    ruta_proy = r'C:\Dashboard\data\proyectados'
    ruta_maestro = r'C:\Dashboard\data\Proyectadoconsolidado.xlsx'
    hoy = pd.to_datetime(datetime.now().date())

    # 1. CONSOLIDACIÓN INICIAL
    archivos = glob.glob(os.path.join(ruta_proy, "*.xlsx"))
    if not archivos:
        return "No se encontraron archivos para procesar."

    lista_df = []
    for archivo in archivos:
        df_temp = pd.read_excel(archivo)
        # COL 1: ID_S (Generado para cada registro en cada archivo)
        df_temp['ID_S'] = (df_temp['COD. CLIENTE'].astype(str) + df_temp['Referencia'].astype(str) + 
                           df_temp['FECHA DOC'].astype(str) + df_temp['Fecha_Vencimiento'].astype(str) + 
                           df_temp['TOTAL CARTERA'].astype(str))
        # Guardamos la fecha del archivo para saber cuándo apareció
        df_temp['FECHA_ORIGEN_ARCHIVO'] = pd.to_datetime(datetime.fromtimestamp(os.path.getmtime(archivo)).date())
        lista_df.append(df_temp)

    # Unimos todo y eliminamos duplicados (dejamos la primera aparición para la fecha)
    df_maestro = pd.concat(lista_df, ignore_index=True)
    df_maestro = df_maestro.sort_values(by='FECHA_ORIGEN_ARCHIVO')
    
    # 2. COL 3: PRIMERA_APARICION (Antes de quitar duplicados, capturamos la fecha mínima)
    df_maestro['PRIMERA_APARICION'] = df_maestro.groupby('ID_S')['FECHA_ORIGEN_ARCHIVO'].transform('min')
    
    # Ahora sí, dejamos un solo registro por ID_S (el más reciente para los datos actuales)
    df_maestro = df_maestro.drop_duplicates(subset=['ID_S'], keep='last')

    # 3. DETERMINAR ESTADOS (Basado en el ÚLTIMO archivo cargado)
    ultimo_archivo_path = max(archivos, key=os.path.getmtime)
    df_ultimo = pd.read_excel(ultimo_archivo_path)
    ids_en_ultimo = set((df_ultimo['COD. CLIENTE'].astype(str) + df_ultimo['Referencia'].astype(str) + 
                         df_ultimo['FECHA DOC'].astype(str) + df_ultimo['Fecha_Vencimiento'].astype(str) + 
                         df_ultimo['TOTAL CARTERA'].astype(str)).unique())

    # COL 2: ESTADO
    df_maestro['ESTADO'] = df_maestro['ID_S'].apply(lambda x: 'PENDIENTE' if x in ids_en_ultimo else 'RECUPERADA')

    # COL 4: RECUPERACION (Fecha del archivo más reciente donde apareció antes de desaparecer)
    # Si está PENDIENTE, no tiene fecha de recuperación. Si está RECUPERADA, usamos su última fecha de origen.
    df_maestro['RECUPERACION'] = df_maestro.apply(
        lambda row: row['FECHA_ORIGEN_ARCHIVO'] if row['ESTADO'] == 'RECUPERADA' else pd.NaT, axis=1
    )

    # COL 5: REVERSO (Lógica: Si el archivo de origen es más reciente que su "supuesta" recuperación previa)
    # En una consolidación total, el reverso se detecta si el estado vuelve a ser PENDIENTE tras haber sido RECUPERADA
    # Por ahora marcamos NO por defecto, ya que la consolidación lo maneja al marcarlo PENDIENTE de nuevo
    df_maestro['REVERSO'] = 'NO' 

    # COL 6: VTO_DT
    df_maestro['VTO_DT'] = pd.to_datetime(df_maestro['Fecha_Vencimiento'])

    # COL 7: DIAS_MORA
    def calc_mora(row):
        if row['ESTADO'] == 'PENDIENTE':
            return (hoy - row['VTO_DT']).days
        else:
            return (row['RECUPERACION'] - row['VTO_DT']).days
    
    df_maestro['DIAS_MORA'] = df_maestro.apply(calc_mora, axis=1)

    # COL 8 Y 9: FRANJAS
    df_maestro['Franja Mora Cyres'] = df_maestro['DIAS_MORA'].apply(obtener_franja_cyres)
    df_maestro['Franja de Mora Coca-Cola'] = df_maestro['DIAS_MORA'].apply(obtener_franja_coca)

    # COL 10: MAX_MORA (Solo de los PENDIENTES)
    mora_activa = df_maestro[df_maestro['ESTADO'] == 'PENDIENTE'].groupby('COD. CLIENTE')['DIAS_MORA'].max()
    df_maestro['MAX_MORA'] = df_maestro['COD. CLIENTE'].map(mora_activa).fillna(0)

    # COL 11: FRANJA TOP GENERAL
    df_maestro['Franja Top General'] = df_maestro['MAX_MORA'].apply(obtener_franja_coca)

    # Limpieza final de columnas técnicas y guardado
    columnas_finales = [col for col in df_maestro.columns if col != 'FECHA_ORIGEN_ARCHIVO']
    df_maestro[columnas_finales].to_excel(ruta_maestro, index=False)
    
    return f"Consolidación exitosa. Archivo maestro actualizado con {len(df_maestro)} registros únicos."

if __name__ == "__main__":
    print(procesar_todo())