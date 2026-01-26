import pandas as pd
import os
from datetime import datetime

def calcular_gestion(ruta_cartera, ruta_gestion, analista_seleccionado='Todos'):
    try:
        # 1. Cargar Archivos
        df_car = pd.read_csv(ruta_cartera, sep=';', encoding='latin1')
        df_ges = pd.read_csv(ruta_gestion, sep=';', encoding='latin1')

        # 2. Limpiar columnas
        df_car.columns = df_car.columns.str.strip()
        df_ges.columns = df_ges.columns.str.strip()

        # 3. FILTRO DE ANALISTA
        if analista_seleccionado != 'Todos':
            if 'USUARIO_GESTION' in df_ges.columns:
                df_ges = df_ges[df_ges['USUARIO_GESTION'] == analista_seleccionado]
            if 'USUARIO_GESTION' in df_car.columns:
                df_car = df_car[df_car['USUARIO_GESTION'] == analista_seleccionado]

        # 4. Filtro de Pendientes
        if 'ESTADO' in df_car.columns:
            df_car = df_car[df_car['ESTADO'].str.strip() == 'PENDIENTE'].copy()

        col_car_id = 'COD. CLIENTE'
        col_ges_id = 'CODIGO_CLIENTE'
        col_franja = 'Franja Mora Cyres'
        col_nom = 'RAZÓN SOCIAL'
        col_sal = 'TOTAL CARTERA'
        col_user = 'USUARIO_GESTION'

        df_car[col_car_id] = df_car[col_car_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df_ges[col_ges_id] = df_ges[col_ges_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # Procesar fechas
        df_ges['FECHA_DT'] = pd.to_datetime(df_ges['FECHA_GESTION'], errors='coerce')
        inicio_mes = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        gestiones_mes = df_ges[df_ges['FECHA_DT'] >= inicio_mes].copy()

        # --- LÓGICA DE ANALISTAS MEJORADA (Sin Jhon Polanco) ---
        df_ana = gestiones_mes.copy()
        df_ana = df_ana[df_ana[col_user] != 'Jhon Polanco']
        df_ana['SOLO_FECHA'] = df_ana['FECHA_DT'].dt.date

        # A. Lógica de "Mejor Gestión" por día:
        df_ana['ORDEN_CONTACTO'] = df_ana['CONTACTO'].map({'EFECTIVO': 1, 'NO EFECTIVO': 2}).fillna(3)
        df_mejor_gestion = df_ana.sort_values([col_user, 'SOLO_FECHA', col_ges_id, 'ORDEN_CONTACTO']).drop_duplicates(subset=[col_user, 'SOLO_FECHA', col_ges_id])

        # B. Ranking y Métricas:
        res_analistas = df_mejor_gestion.groupby(col_user).agg(
            Clientes_Unicos_Dia=(col_ges_id, 'count'),
            Efectivos=(col_ges_id, lambda x: (df_mejor_gestion.loc[x.index, 'CONTACTO'] == 'EFECTIVO').sum())
        ).reset_index()

        total_gestiones_raw = df_ana.groupby(col_user).size()
        res_analistas['Intensidad'] = (res_analistas[col_user].map(total_gestiones_raw) / res_analistas['Clientes_Unicos_Dia']).round(1)
        res_analistas['Efec_Porc'] = ((res_analistas['Efectivos'] / res_analistas['Clientes_Unicos_Dia']) * 100).round(1).fillna(0)
        res_analistas = res_analistas.sort_values(by='Efec_Porc', ascending=False).reset_index(drop=True)

        # C. Datos para Gráfica Combinada
        df_timeline = df_mejor_gestion.groupby('SOLO_FECHA').agg(
            Gestionados=(col_ges_id, 'count'),
            Efectivos=(col_ges_id, lambda x: (df_mejor_gestion.loc[x.index, 'CONTACTO'] == 'EFECTIVO').sum())
        ).reset_index()
        
        df_timeline['Efec_P'] = (df_timeline['Efectivos'] / df_timeline['Gestionados'] * 100).round(1)
        df_timeline['No_Efec_P'] = (100 - df_timeline['Efec_P']).round(1)
        df_timeline['FECHA_STR'] = df_timeline['SOLO_FECHA'].apply(lambda x: x.strftime('%d-%m'))

        # --- CONTINUACIÓN LÓGICA ORIGINAL ---
        ultima_gest = gestiones_mes.sort_values('FECHA_DT').groupby(col_ges_id).last()
        df_master = pd.merge(df_car, ultima_gest[['CONTACTO', 'FECHA_DT']], left_on=col_car_id, right_index=True, how='left')

        hoy = datetime.now()
        def clasificar_antiguedad(fecha):
            if pd.isnull(fecha): return "1. Sin Gestión"
            dias = (hoy - fecha).days
            if dias == 0: return "2. Gestión Hoy"
            if dias == 1: return "3. Gestión Ayer"
            if 2 <= dias <= 5: return "4. Sin gestión (2-5 días)"
            if 6 <= dias <= 10: return "5. Sin gestión (6-10 días)"
            if 11 <= dias <= 15: return "6. Sin gestión (11-15 días)"
            return "7. Sin gestión (+15 días)"

        df_master['RANGO_GESTION'] = df_master['FECHA_DT'].apply(clasificar_antiguedad)

        df_uni_matriz = df_master.drop_duplicates(subset=[col_car_id])
        matriz = pd.crosstab(df_uni_matriz['RANGO_GESTION'], df_uni_matriz[col_franja])
        matriz['TOTAL'] = matriz.sum(axis=1)
        tot_v = matriz.sum(axis=0).to_dict()
        tot_v['RANGO_GESTION'] = 'TOTAL GENERAL'
        filas_m = matriz.reset_index().to_dict(orient='records')
        filas_m.append(tot_v)

        total_clientes = len(df_uni_matriz)
        cant_gest = len(df_uni_matriz[df_uni_matriz['CONTACTO'].notnull()])
        cant_efec = len(df_uni_matriz[df_uni_matriz['CONTACTO'] == 'EFECTIVO'])

        res_franja = df_uni_matriz.groupby([col_franja]).agg(Total=(col_car_id, 'size'), Gestionados=('CONTACTO', 'count')).reset_index()
        efec_f = df_uni_matriz[df_uni_matriz['CONTACTO'] == 'EFECTIVO'].groupby(col_franja).size()
        res_franja['Sin_Gestion'] = res_franja['Total'] - res_franja['Gestionados']
        res_franja['Efectivo'] = res_franja[col_franja].map(efec_f).fillna(0).astype(int)
        
        lista_det = []
        for _, fila in df_master.iterrows():
            # 1. Limpiamos el valor de contacto para que el filtro de EFECTIVO funcione
            contacto_original = fila.get('CONTACTO')
            if pd.isnull(contacto_original) or str(contacto_original).strip() == "":
                contacto_limpio = 'PTE / SIN CONTACTO'
            else:
                # Esto elimina espacios y lo pasa a MAYÚSCULAS
                contacto_limpio = str(contacto_original).strip().upper()
            
            # 2. Formateamos la fecha
            fecha_gest = "—"
            if pd.notnull(fila.get('FECHA_DT')):
                fecha_gest = fila['FECHA_DT'].strftime('%d/%m/%Y')

            lista_det.append({
                'ID': fila.get(col_car_id, ''), 
                'NOMBRE': fila.get(col_nom, ''), 
                'FRANJA': fila.get(col_franja, ''),
                'ESTADO': 'GESTIONADO' if pd.notnull(fila.get('CONTACTO')) else 'SIN GESTIÓN',
                'CONTACTO': contacto_limpio, # <--- DATA LIMPIA PARA EL FILTRO
                'SALDO': fila.get(col_sal, 0), 
                'RANGO_INACTIVIDAD': fila.get('RANGO_GESTION', '1. Sin Gestión'),
                'FECHA_ULTIMA': fecha_gest
            })

        # --- LÓGICA PARA RANKING DEL DÍA ---
        hoy_fecha = datetime.now().strftime('%Y-%m-%d')
        gestiones_hoy = df_ges[df_ges['FECHA_DT'].dt.strftime('%Y-%m-%d') == hoy_fecha].copy()
        ranking_dia_final = []

        if not gestiones_hoy.empty:
            ranking_dia_df = gestiones_hoy.groupby('USUARIO_GESTION').agg(
                clientes_gestionados=(col_ges_id, 'nunique'),
                gestiones_realizadas=(col_ges_id, 'count')
            ).reset_index()
            efec_hoy = gestiones_hoy[gestiones_hoy['CONTACTO'] == 'EFECTIVO'].groupby('USUARIO_GESTION').size()
            ranking_dia_df['efectivos'] = ranking_dia_df['USUARIO_GESTION'].map(efec_hoy).fillna(0).astype(int)
            ranking_dia_df['porc_efec'] = ((ranking_dia_df['efectivos'] / ranking_dia_df['gestiones_realizadas']) * 100).round(1).fillna(0)
            ranking_dia_df = ranking_dia_df.sort_values('clientes_gestionados', ascending=False)
            ranking_dia_final = ranking_dia_df.to_dict(orient='records')

        # --- LÓGICA DE RECAUDO (REPARADA PARA FILTRAR POR ANALISTA) ---
        try:
            ruta_pagos = ruta_gestion.replace('gestion.csv', 'PagosConsolidado.csv')
            df_pagos = pd.read_csv(ruta_pagos, sep=';', encoding='latin1')
            df_pagos.columns = df_pagos.columns.str.strip()
            
            col_pag_id = 'COD. CLIENTE'
            df_pagos[col_pag_id] = df_pagos[col_pag_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            df_ana[col_ges_id] = df_ana[col_ges_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            
            df_pagos['FECHA_PAGO_DT'] = pd.to_datetime(df_pagos['FECHA PAGO'], dayfirst=True, errors='coerce')
            df_pagos['FECHA_REF'] = df_pagos['FECHA_PAGO_DT'].dt.strftime('%Y-%m-%d')
            df_pagos['VALOR_PAGADO'] = pd.to_numeric(df_pagos['VALOR PAGADO'], errors='coerce').fillna(0)
            
            df_ana['FECHA_REF'] = df_ana['FECHA_DT'].dt.strftime('%Y-%m-%d')
            
            gest_prio = df_ana[df_ana[col_user] != 'Jhon Polanco'].copy()
            gest_prio['PRIO'] = gest_prio['CONTACTO'].apply(lambda x: 1 if x == 'EFECTIVO' else 2)
            gest_prio = gest_prio.sort_values([col_ges_id, 'FECHA_REF', 'PRIO']).drop_duplicates(subset=[col_ges_id, 'FECHA_REF'], keep='first')

            # Unimos pagos con gestiones para saber de quién es cada pago
            df_atribucion = pd.merge(df_pagos, gest_prio[[col_ges_id, 'FECHA_REF', col_user]], 
                                    left_on=[col_pag_id, 'FECHA_REF'], right_on=[col_ges_id, 'FECHA_REF'], how='left')

            df_atribucion[col_user] = df_atribucion[col_user].fillna('Sin Gestión / No Atribuible')
            
            # --- AQUÍ ESTÁ EL CAMBIO CLAVE: FILTRAR LA ATRIBUCIÓN ---
            df_rec_final = df_atribucion.copy()
            if analista_seleccionado != 'Todos':
                df_rec_final = df_atribucion[df_atribucion[col_user] == analista_seleccionado].copy()

            total_recaudo_gen = df_atribucion['VALOR_PAGADO'].sum()
            
            rank_recaudo = df_atribucion.groupby(col_user).agg(Total_Recaudado=('VALOR_PAGADO', 'sum')).reset_index()
            rank_recaudo['Porc_Part'] = ((rank_recaudo['Total_Recaudado'] / total_recaudo_gen * 100) if total_recaudo_gen > 0 else 0).round(1)
            
            # Agrupar usando el dataframe que ya tiene aplicado el filtro (df_rec_final)
            df_time_rec = df_rec_final.groupby('FECHA_PAGO_DT').agg(monto=('VALOR_PAGADO', 'sum')).reset_index().sort_values('FECHA_PAGO_DT')
            
            recaudo_stats_final = {
                'labels': df_time_rec['FECHA_PAGO_DT'].dt.strftime('%d-%m').tolist(),
                'valores': df_time_rec['monto'].tolist(),
                'ranking': rank_recaudo.sort_values('Total_Recaudado', ascending=False).to_dict(orient='records')
            }
        except Exception as e:
            print(f"Error en recaudo: {e}")
            recaudo_stats_final = {'labels': [], 'valores': [], 'ranking': []}

        # --- RETURN ÚNICO Y PROTEGIDO ---
        return {
            'total_clientes': int(total_clientes), 
            'total_documentos': int(len(df_car)),
            'promedio_doc': round(len(df_car)/total_clientes, 1) if total_clientes > 0 else 0,
            'cant_gestionados': int(cant_gest), 
            'cant_efectivos': int(cant_efec),
            'cant_sin_gestion': int(total_clientes - cant_gest),
            'porc_barrido': round((cant_gest/total_clientes*100),1) if total_clientes > 0 else 0,
            'porc_contactado': round((cant_efec/total_clientes*100),1) if total_clientes > 0 else 0,
            'porc_no_contactado': round(((cant_gest - cant_efec)/total_clientes*100),1) if total_clientes > 0 else 0,
            'porc_sin_gestion': round(((total_clientes-cant_gest)/total_clientes*100),1) if total_clientes > 0 else 0,
            'resumen_franjas': res_franja.to_dict(orient='records'),
            'detalle_maestro': lista_det,
            'matriz_antiguedad': {'columnas': matriz.columns.tolist(), 'filas': filas_m},
            'resumen_analistas': res_analistas.to_dict(orient='records'),
            'dona_efectividad': [int(res_analistas['Efectivos'].sum()), int(res_analistas['Clientes_Unicos_Dia'].sum() - res_analistas['Efectivos'].sum())] if not res_analistas.empty else [0,0],
            'timeline_datos': {
                'labels': df_timeline['FECHA_STR'].tolist(),
                'gestionados': df_timeline['Gestionados'].tolist(),
                'efectividad': df_timeline['Efec_P'].tolist(),
                'no_efectividad': df_timeline['No_Efec_P'].tolist()
            },
            'ranking_dia': ranking_dia_final,
            'recaudo_stats': recaudo_stats_final
        }

    except Exception as e:
        print(f"Error crítico en Python: {e}")
        return {'total_clientes': 0, 'recaudo_stats': {'labels': [], 'valores': [], 'ranking': []}}