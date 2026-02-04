import pandas as pd

def obtener_tabla_recuperacion_franjas(ruta_cartera):
    try:
        # 1. Cargamos el CSV limpio (sin filtros previos)
        df = pd.read_csv(ruta_cartera, sep=';', encoding='latin1')
        df.columns = df.columns.str.strip()

        # 2. Filtramos SOLO las RECUPERADAS
        df_recu = df[df['ESTADO'].str.strip() == 'RECUPERADA'].copy()
        
        if df_recu.empty:
            return [], 0

        # 3. Lógica de clasificación de días
        def clasificar_rango(dias):
            if dias <= -2: return "0- Corriente"
            if dias == -1: return "1- Vence mañana"
            if dias == 0:  return "2- Vence hoy"
            if dias == 1:  return "3- Venció ayer"
            if 2 <= dias <= 4:   return "4- 2 a 4"
            if 5 <= dias <= 7:   return "5- 5 a 7"
            if 8 <= dias <= 14:  return "6- 8 a 14"
            if 15 <= dias <= 21: return "7- 15 a 21"
            if 22 <= dias <= 30: return "8- 22 a 30"
            return "9- Mayor a 30"

        # Aseguramos que DIAS_MORA sea numérico
        df_recu['DIAS_MORA'] = pd.to_numeric(df_recu['DIAS_MORA'], errors='coerce').fillna(0)
        df_recu['Rango_Nuevo'] = df_recu['DIAS_MORA'].apply(clasificar_rango)

        # 4. Sumamos por rango
        resumen = df_recu.groupby('Rango_Nuevo')['TOTAL CARTERA'].sum()
        
        orden = [
            "0- Corriente", "1- Vence mañana", "2- Vence hoy", "3- Venció ayer",
            "4- 2 a 4", "5- 5 a 7", "6- 8 a 14", "7- 15 a 21", "8- 22 a 30", "9- Mayor a 30"
        ]

        tabla_final = []
        total_general = float(resumen.sum())
        
        for r in orden:
            valor = float(resumen.get(r, 0))
            # Calculamos el porcentaje (si el total es 0, ponemos 0)
            porcentaje = (valor / total_general * 100) if total_general > 0 else 0
            
            tabla_final.append({
                'rango': r, 
                'valor': valor,
                'porcentaje': round(porcentaje, 1) # Guardamos el % con un decimal
            })

        return tabla_final, total_general

    except Exception as e:
        print(f"Error en procesador_recuperacion: {e}")
        return [], 0