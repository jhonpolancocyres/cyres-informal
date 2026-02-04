import pandas as pd
import os
from datetime import datetime
import calendar

def calcular_gestion(ruta_cartera, ruta_gestion, ruta_pagos=None, analista_seleccionado='Todos', fecha_inicio=None, fecha_fin=None, mes_actual=2, anio_actual=2026):
    ahora = datetime.now()

    # LÓGICA DE MÁQUINA DEL TIEMPO
    if mes_actual == ahora.month and anio_actual == ahora.year:
        # Si es el mes vivo, la referencia es hoy
        fecha_referencia = ahora
    else:
        # Si es un histórico, la referencia es el último día de ese mes
        ultimo_dia = calendar.monthrange(anio_actual, mes_actual)[1]
        fecha_referencia = datetime(anio_actual, mes_actual, ultimo_dia, 23, 59, 59)
    try:

        # --- CARGA INTELIGENTE DE CARTERA ---
        if ruta_cartera.endswith('.zip'):
            df_car = pd.read_csv(ruta_cartera, sep=';', encoding='latin1', compression='zip')
        else:
            df_car = pd.read_csv(ruta_cartera, sep=';', encoding='latin1')

        # --- CARGA INTELIGENTE DE GESTIÓN ---
        if ruta_gestion.endswith('.zip'):
            df_ges = pd.read_csv(ruta_gestion, sep=';', encoding='latin1', compression='zip')
        else:
            df_ges = pd.read_csv(ruta_gestion, sep=';', encoding='latin1')

        # --- CARGA DE PAGOS DINÁMICA ---
        if ruta_pagos and os.path.exists(ruta_pagos):
            df_pag = pd.read_csv(ruta_pagos, sep=';', encoding='latin1')
        else:
            df_pag = pd.DataFrame(columns=['FECHA PAGO', 'VALOR PAGADO', 'COD. CLIENTE'])

        # Limpieza de columnas (Asegúrate de que df_pag esté aquí)
        df_car.columns = df_car.columns.str.strip()
        df_ges.columns = df_ges.columns.str.strip()
        df_pag.columns = df_pag.columns.str.strip()

        # CREAMOS LA COPIA MAESTRA ANTES DE FILTRAR PARA EL RANKING
        # --- CARGA DE GESTIÓN (FORMATO 12/02/2026) ---
        df_ges_maestra = df_ges.copy()
        df_ges_maestra['FECHA_DT'] = pd.to_datetime(
            df_ges_maestra['FECHA_GESTION'].astype(str), 
            format='%d/%m/%Y', 
            errors='coerce'
        )

        # 3. FILTRO DE ANALISTA (Este ya lo tienes, déjalo igual)
        if analista_seleccionado != 'Todos':
            if 'USUARIO_GESTION' in df_ges.columns:
                df_ges = df_ges[df_ges['USUARIO_GESTION'] == analista_seleccionado]

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

        # --- SOLUCIÓN AL ERROR DE FECHA (COHERENCIA) ---
        # Forzamos el formato Día/Mes/Año para que 10/01 no sea 01/10
        df_ges['FECHA_DT'] = pd.to_datetime(df_ges['FECHA_GESTION'], format='%d/%m/%Y', errors='coerce')
        
        # Si el formato anterior falla (por cambios en el CSV), usamos dayfirst=True como respaldo
        if df_ges['FECHA_DT'].isnull().all():
            df_ges['FECHA_DT'] = pd.to_datetime(df_ges['FECHA_GESTION'], dayfirst=True, errors='coerce')

        gestiones_mes = df_ges.copy()

        # --- FILTRO DE RANGO DE FECHAS ---
        if fecha_inicio and fecha_fin:
            try:
                # Convertimos strings del form (YYYY-MM-DD) a datetime
                f_ini = pd.to_datetime(fecha_inicio)
                f_fin = pd.to_datetime(fecha_fin)
                
                # IMPORTANTE: Para que el filtro incluya todo el día final, 
                # nos aseguramos que f_fin llegue hasta las 23:59:59
                f_fin = f_fin.replace(hour=23, minute=59, second=59)
                
                # Filtramos el DataFrame principal de gestiones
                df_ges = df_ges[(df_ges['FECHA_DT'] >= f_ini) & (df_ges['FECHA_DT'] <= f_fin)].copy()
                
                # También filtramos la copia de 'gestiones_mes' que usas más adelante
                gestiones_mes = df_ges.copy()
            except Exception as e:
                print(f"Error aplicando filtro de fechas: {e}")

        # --- LÓGICA DE ANALISTAS MEJORADA ---
        df_ana = gestiones_mes.copy()
        df_ana = df_ana[df_ana[col_user] != 'Jhon Polanco']
        df_ana['SOLO_FECHA'] = df_ana['FECHA_DT'].dt.date

        df_ana['ORDEN_CONTACTO'] = df_ana['CONTACTO'].map({'EFECTIVO': 1, 'NO EFECTIVO': 2}).fillna(3)
        df_mejor_gestion = df_ana.sort_values([col_user, 'SOLO_FECHA', col_ges_id, 'ORDEN_CONTACTO']).drop_duplicates(subset=[col_user, 'SOLO_FECHA', col_ges_id])

        res_analistas = df_mejor_gestion.groupby(col_user).agg(
            Clientes_Unicos_Dia=(col_ges_id, 'count'),
            Efectivos=(col_ges_id, lambda x: (df_mejor_gestion.loc[x.index, 'CONTACTO'] == 'EFECTIVO').sum())
        ).reset_index()

        total_gestiones_raw = df_ana.groupby(col_user).size()
        res_analistas['Intensidad'] = (res_analistas[col_user].map(total_gestiones_raw) / res_analistas['Clientes_Unicos_Dia']).round(1)
        res_analistas['Efec_Porc'] = ((res_analistas['Efectivos'] / res_analistas['Clientes_Unicos_Dia']) * 100).round(1).fillna(0)
        res_analistas = res_analistas.sort_values(by='Efec_Porc', ascending=False).reset_index(drop=True)

        df_timeline = df_mejor_gestion.groupby('SOLO_FECHA').agg(
            Gestionados=(col_ges_id, 'count'),
            Efectivos=(col_ges_id, lambda x: (df_mejor_gestion.loc[x.index, 'CONTACTO'] == 'EFECTIVO').sum())
        ).reset_index()
        
        df_timeline['Efec_P'] = (df_timeline['Efectivos'] / df_timeline['Gestionados'] * 100).round(1).fillna(0)
        df_timeline['No_Efec_P'] = (100 - df_timeline['Efec_P']).round(1)
        df_timeline['FECHA_STR'] = df_timeline['SOLO_FECHA'].apply(lambda x: x.strftime('%d-%m') if pd.notnull(x) else "")

        # --- CONTINUACIÓN LÓGICA ORIGINAL ---
        ultima_gest = gestiones_mes.sort_values('FECHA_DT').groupby(col_ges_id).last()
        df_master = pd.merge(df_car, ultima_gest[['CONTACTO', 'FECHA_DT']], left_on=col_car_id, right_index=True, how='left')

        # --- CLASIFICACIÓN DE ANTIGÜEDAD (MÁQUINA DEL TIEMPO) ---
        def clasificar_antiguedad(fecha):
            if pd.isnull(fecha): return "1. Sin Gestión"
            # Comparamos contra el cierre de mes o contra hoy
            dias = (fecha_referencia - fecha).days
            
            if dias <= 0: return "2. Gestión Hoy"
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
            contacto_original = fila.get('CONTACTO')
            contacto_limpio = str(contacto_original).strip().upper() if pd.notnull(contacto_original) and str(contacto_original).strip() != "" else 'PTE / SIN CONTACTO'
            
            fecha_gest = "—"
            if pd.notnull(fila.get('FECHA_DT')):
                fecha_gest = fila['FECHA_DT'].strftime('%d/%m/%Y')

            lista_det.append({
                'ID': fila.get(col_car_id, ''), 
                'NOMBRE': fila.get(col_nom, ''), 
                'FRANJA': fila.get(col_franja, ''),
                'ESTADO': 'GESTIONADO' if pd.notnull(fila.get('CONTACTO')) else 'SIN GESTIÓN',
                'CONTACTO': contacto_limpio, 
                'SALDO': fila.get(col_sal, 0), 
                'RANGO_INACTIVIDAD': fila.get('RANGO_GESTION', '1. Sin Gestión'),
                'FECHA_ULTIMA': fecha_gest
            })

        # --- CLASIFICACIÓN DE ANTIGÜEDAD (MÁQUINA DEL TIEMPO) ---
        def clasificar_antiguedad(fecha):
            if pd.isnull(fecha): return "1. Sin Gestión"
            # Comparamos contra el cierre de mes o contra hoy
            dias = (fecha_referencia - fecha).days
            
            if dias <= 0: return "2. Gestión Hoy"
            if dias == 1: return "3. Gestión Ayer"
            if 2 <= dias <= 5: return "4. Sin gestión (2-5 días)"
            if 6 <= dias <= 10: return "5. Sin gestión (6-10 días)"
            if 11 <= dias <= 15: return "6. Sin gestión (11-15 días)"
            return "7. Sin gestión (+15 días)"

        df_master['RANGO_GESTION'] = df_master['FECHA_DT'].apply(clasificar_antiguedad)

        # ... (Mantén tu código intermedio de matrices y listas igual) ...

        # --- RANKING DEL DÍA (CORREGIDO PARA HISTÓRICOS) ---
        # Si es Enero, hoy_dt será 31/01. Si es Febrero actual, será hoy.
        hoy_dt = fecha_referencia.date() 
        gestiones_hoy = df_ges[df_ges['FECHA_DT'].dt.date == hoy_dt].copy()
        ranking_dia_final = []

        if not gestiones_hoy.empty:
            ranking_dia_df = gestiones_hoy.groupby(col_user).agg(
                clientes_unicos=(col_ges_id, 'nunique'),
                gestiones_totales=(col_ges_id, 'count')
            ).reset_index()

            efec_hoy = gestiones_hoy[gestiones_hoy['CONTACTO'] == 'EFECTIVO'].groupby(col_user).size()
            ranking_dia_df['efectivos'] = ranking_dia_df[col_user].map(efec_hoy).fillna(0).astype(int)
            ranking_dia_df['porc_efec'] = ((ranking_dia_df['efectivos'] / ranking_dia_df['gestiones_totales']) * 100).round(1).fillna(0)
            
            ranking_dia_df = ranking_dia_df[ranking_dia_df[col_user] != 'Jhon Polanco']
            ranking_dia_final = ranking_dia_df.sort_values(by='porc_efec', ascending=False).to_dict(orient='records')

        # --- LÓGICA DE CURVA DE RECUPERACIÓN (CIRUGÍA PRECISA) ---
        curva_rec_data = {'labels': [], 'porcentajes': []}
        try:
            col_vence = 'Fecha_Vencimiento' # Nombre exacto de tu columna
            # Creamos una copia temporal para no dañar las fechas originales del df_car
            df_temp_rec = df_car.copy()
            df_temp_rec[col_vence] = pd.to_datetime(df_temp_rec[col_vence], dayfirst=True, errors='coerce')
            
            # Agrupamos por día de vencimiento
            rec_diaria = df_temp_rec.groupby(df_temp_rec[col_vence].dt.date).agg(
                Total_Vencido=(col_sal, 'sum'),
                Total_Recuperado=(col_sal, lambda x: df_temp_rec.loc[x.index, 'ESTADO'].str.strip().eq('RECUPERADA').multiply(df_temp_rec.loc[x.index, col_sal]).sum())
            ).reset_index()

            # Ordenar y calcular %
            rec_diaria = rec_diaria.dropna(subset=[col_vence]).sort_values(col_vence)
            rec_diaria['Porc_Rec'] = (rec_diaria['Total_Recuperado'] / rec_diaria['Total_Vencido'] * 100).round(1).fillna(0)
            
            curva_rec_data = {
                'labels': rec_diaria[col_vence].apply(lambda x: x.strftime('%d-%m')).tolist(),
                'porcentajes': rec_diaria['Porc_Rec'].tolist()
            }
        except Exception as e:
            print(f"Error en curva de recuperación: {e}")

        # --- LÓGICA DE RECAUDO MODIFICADA ---
        recaudo_stats_final = {'labels': [], 'valores': [], 'ranking': []}
        try:
            
            
            # 9. INTEGRACIÓN CON PAGOS (RECAUDO REAL)
            if not df_pag.empty:
                df_pagos = df_pag.copy() # Usamos la copia del que cargamos arriba
                df_pagos.columns = df_pagos.columns.str.strip()
                
                # 1. Limpieza de código cliente (Mantenlo siempre)
                col_pag_id = 'COD. CLIENTE'
                df_pagos[col_pag_id] = df_pagos[col_pag_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                
                # 2. CARGA DE FECHAS (FORMATO MULTIPLE)
                # Intentamos barras
                df_pagos['FECHA_PAGO_DT'] = pd.to_datetime(df_pagos['FECHA PAGO'].astype(str), format='%d/%m/%Y', errors='coerce')
                
                # Si falló, intentamos puntos (para febrero)
                mask_nan = df_pagos['FECHA_PAGO_DT'].isna()
                if mask_nan.any():
                    df_pagos.loc[mask_nan, 'FECHA_PAGO_DT'] = pd.to_datetime(df_pagos.loc[mask_nan, 'FECHA PAGO'].astype(str), format='%d.%m.%Y', errors='coerce')

                # 3. FILTRO DE FECHAS SEGÚN EL SELECTOR O CALENDARIO
                if fecha_inicio and fecha_fin:
                    f_ini_p = pd.to_datetime(fecha_inicio)
                    f_fin_p = pd.to_datetime(fecha_fin).replace(hour=23, minute=59, second=59)
                    df_pagos = df_pagos[(df_pagos['FECHA_PAGO_DT'] >= f_ini_p) & (df_pagos['FECHA_PAGO_DT'] <= f_fin_p)].copy()
                else:
                    # SI NO HAY CALENDARIO, USAMOS EL MES QUE ELEGISTE EN EL NUEVO SELECTOR
                    df_pagos = df_pagos[
                        (df_pagos['FECHA_PAGO_DT'].dt.month == mes_actual) & 
                        (df_pagos['FECHA_PAGO_DT'].dt.year == anio_actual)
                    ].copy()

                # --- LAS LÍNEAS CLAVE QUE FALTABAN ---
                df_pagos['FECHA_REF'] = df_pagos['FECHA_PAGO_DT'].dt.strftime('%Y-%m-%d')
                df_pagos['VALOR_PAGADO'] = pd.to_numeric(df_pagos['VALOR PAGADO'], errors='coerce').fillna(0)
                df_pagos = df_pagos[df_pagos['VALOR_PAGADO'] > 0].copy()

                # --- ATRIBUCIÓN ESTÁTICA (Usa la maestra) ---
                df_atrib_base = df_ges_maestra.copy() 
                df_atrib_base[col_ges_id] = df_atrib_base[col_ges_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                df_atrib_base['FECHA_REF'] = df_atrib_base['FECHA_DT'].dt.strftime('%Y-%m-%d')
                
                gest_prio = df_atrib_base[df_atrib_base[col_user] != 'Jhon Polanco'].copy()
                gest_prio['PRIO'] = gest_prio['CONTACTO'].apply(lambda x: 1 if x == 'EFECTIVO' else 2)
                gest_prio = gest_prio.sort_values(['PRIO']).drop_duplicates(subset=[col_ges_id, 'FECHA_REF'], keep='first')

                mapa_resp = dict(zip(gest_prio[col_ges_id] + gest_prio['FECHA_REF'], gest_prio[col_user]))
                df_pagos['ANALISTA_REAL'] = (df_pagos[col_pag_id] + df_pagos['FECHA_REF']).map(mapa_resp).fillna('Sin Gestión')

                # 1. Ranking: Siempre todos los analistas
                total_global = float(df_pagos['VALOR_PAGADO'].sum())
                
                # Agrupamos
                rank_completo = df_pagos.groupby('ANALISTA_REAL')['VALOR_PAGADO'].sum().reset_index()
                rank_completo.columns = [col_user, 'Total_Recaudado']
                
                # Convertimos a float puro y redondeamos para evitar decimales basura
                rank_completo['Total_Recaudado'] = rank_completo['Total_Recaudado'].astype(float).round(0)
                
                # Filtramos a Jhon Polanco antes de ordenar
                rank_completo = rank_completo[rank_completo[col_user] != 'Jhon Polanco'].copy()
                
                # Porcentaje
                rank_completo['Porc_Part'] = (rank_completo['Total_Recaudado'] / total_global * 100).round(1) if total_global > 0 else 0
                
                # --- ORDENAMIENTO CRÍTICO ---
                rank_completo = rank_completo.sort_values(by='Total_Recaudado', ascending=False)
                # ----------------------------

                # 2. Gráfico
                df_time_data = df_pagos[df_pagos['ANALISTA_REAL'] == analista_seleccionado] if analista_seleccionado != 'Todos' else df_pagos
                df_time_rec = df_time_data.groupby('FECHA_PAGO_DT')['VALOR_PAGADO'].sum().reset_index().sort_values('FECHA_PAGO_DT')

                recaudo_stats_final = {
                    'labels': df_time_rec['FECHA_PAGO_DT'].dt.strftime('%d-%m').tolist(),
                    'valores': df_time_rec['VALOR_PAGADO'].tolist(),
                    'ranking': rank_completo.to_dict(orient='records')
                }
               
                
        except:
            pass # Mantiene el dict vacío inicial si falla algo
                 
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
            'dona_efectividad': [int(cant_efec), int(cant_gest - cant_efec)],
            'timeline_datos': {
                'labels': df_timeline['FECHA_STR'].tolist(),
                'gestionados': df_timeline['Gestionados'].tolist(),
                'efectividad': df_timeline['Efec_P'].tolist(),
                'no_efectividad': df_timeline['No_Efec_P'].tolist()
            },
            'ranking_dia': ranking_dia_final,
            'recaudo_stats': recaudo_stats_final,
            'curva_recuperacion': curva_rec_data,
            
        }
        
        
    except Exception as e:
        print(f"Error crítico: {e}")
        return {'total_clientes': 0, 'recaudo_stats': {'labels': [], 'valores': [], 'ranking': []}}
    
