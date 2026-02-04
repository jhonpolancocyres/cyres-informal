import os
import glob
import pandas as pd
import calendar
from flask import Flask, render_template, request
import datetime as dt
from datetime import datetime
import pytz


from procesador_maestro import procesar_todo  # Esto importa tu lógica de las 11 columnas
from procesador_recuperacion import obtener_tabla_recuperacion_franjas

app = Flask(__name__)

# --- RUTAS DE ARCHIVOS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_CARTERA = os.path.join(BASE_DIR, 'data', 'Proyectadoconsolidado.csv')
RUTA_PAGOS = os.path.join(BASE_DIR, 'data', 'PagosConsolidado.csv')

def obtener_fecha_archivo(ruta):
    try:
        if os.path.exists(ruta):
            # Obtiene la fecha de modificación del archivo
            mtime = os.path.getmtime(ruta)
            fecha_dt = datetime.fromtimestamp(mtime)
            # Formato: 19/01/2026 03:30 PM
            return fecha_dt.strftime("%d/%m/%Y %I:%M %p")
        return "Archivo no encontrado"
    except Exception as e: 
        # Es bueno imprimir el error en consola por si algo falla internamente
        print(f"Error leyendo fecha: {e}")
        return "Error al obtener fecha"

def procesar_informacion(tipo_vista, ciudad_filtro=None):
    try:
        # --- 1. Cargar datos ---
        df_cartera = pd.read_csv(RUTA_CARTERA, sep=';', encoding='latin1')

        # LIMPIEZA TOTAL DE COLUMNAS
        df_cartera.columns = df_cartera.columns.str.strip()
        
        # Estandarización de la columna Administrador
        col_admin_real = 'ADMINISTRADO POR'
        if col_admin_real not in df_cartera.columns:
            posibles = [c for c in df_cartera.columns if 'ADMINISTRADO' in c.upper()]
            if posibles:
                df_cartera = df_cartera.rename(columns={posibles[0]: col_admin_real})
            else:
                df_cartera[col_admin_real] = 'NO ASIGNADO'

        # 2. Lista de ciudades para el filtro
        ciudades = sorted(df_cartera['CIUDAD'].dropna().unique().tolist())
        
        # 3. Limpieza de columnas numéricas
        columnas_dinero = ['TOTAL CARTERA', '00- Corriente', '05- 1 a 4', '06- 5 a 14', 
                           '07- 15 a 21', '08- 22 a 30', '09- Mayor a 30', 'DIAS_MORA']
        for col in columnas_dinero:
            if col in df_cartera.columns:
                df_cartera[col] = pd.to_numeric(df_cartera[col], errors='coerce').fillna(0)

        # 4. Primero filtramos solo PENDIENTES
        df_pendientes = df_cartera[df_cartera['ESTADO'].astype(str).str.upper() == 'PENDIENTE'].copy()

        # --- LIMPIEZA DE NIT (Evitar el .0) ---
        if 'NIT' in df_pendientes.columns:
            df_pendientes['NIT'] = df_pendientes['NIT'].fillna(0).astype(float).astype(int).astype(str)
            df_pendientes['NIT'] = df_pendientes['NIT'].replace('0', '')

        # LIMPIEZA DEL COD. CLIENTE
        if 'COD. CLIENTE' in df_pendientes.columns:
            df_pendientes['COD. CLIENTE'] = df_pendientes['COD. CLIENTE'].fillna(0).astype(float).astype(int).astype(str)

        # 5. Aplicar Filtro de Ciudad
        if ciudad_filtro and ciudad_filtro != "Todas":
            df_pendientes = df_pendientes[df_pendientes['CIUDAD'] == ciudad_filtro]

        # 6. Selección de columna de mora según vista
        columna_franja = 'Franja de Mora Coca-Cola' if tipo_vista == 'coca-cola' else 'Franja Mora Cyres'
        
        # Validación de existencia de columna de franja para evitar errores en el pivot
        if columna_franja not in df_pendientes.columns:
            df_pendientes[columna_franja] = 'Sin Clasificar'

        resumen_grafico = df_pendientes.groupby(columna_franja)['TOTAL CARTERA'].sum().to_dict()

        # 7. Procesar Recaudo (Pagos)
        total_recaudo = 0
        if os.path.exists(RUTA_PAGOS):
            try:
                df_pagos = pd.read_csv(RUTA_PAGOS, sep=';', encoding='latin1')
                df_pagos.columns = df_pagos.columns.str.strip()
                if 'VALOR PAGADO' in df_pagos.columns:
                    total_recaudo = pd.to_numeric(df_pagos['VALOR PAGADO'], errors='coerce').sum()
            except:
                total_recaudo = 0

        # Cálculo de mora real fila por fila
        df_pendientes['SALDO_ES_VENCIDO'] = df_pendientes.apply(
            lambda x: x['TOTAL CARTERA'] if x['DIAS_MORA'] >= 1 else 0, axis=1
        )

        # --- 8. GRÁFICO 1: PARTICIPACIÓN TOTAL POR CIUDAD ---
        df_ciudades = df_pendientes.groupby('CIUDAD')['TOTAL CARTERA'].sum().sort_values(ascending=False).reset_index()
        if len(df_ciudades) > 10:
            top_10 = df_ciudades.head(10).copy()
            otros_valor = df_ciudades.iloc[10:]['TOTAL CARTERA'].sum()
            fila_otros = pd.DataFrame({'CIUDAD': ['Otras'], 'TOTAL CARTERA': [otros_valor]})
            df_final_ciudades = pd.concat([top_10, fila_otros], ignore_index=True)
        else:
            df_final_ciudades = df_ciudades

        # --- 9. GRÁFICO 2: RANKING MORA ---
        df_mora = df_pendientes.groupby('CIUDAD')['SALDO_ES_VENCIDO'].sum().sort_values(ascending=False).reset_index()
        if len(df_mora) > 10:
            top_10_mora = df_mora.head(10).copy()
            otros_mora = df_mora.iloc[10:]['SALDO_ES_VENCIDO'].sum()
            fila_otros_mora = pd.DataFrame({'CIUDAD': ['Otras'], 'SALDO_ES_VENCIDO': [otros_mora]})
            df_final_mora = pd.concat([top_10_mora, fila_otros_mora], ignore_index=True)
        else:
            df_final_mora = df_mora

        # --- 10. TABLA DE COMPOSICIÓN (CIUDADES) ---
        tabla_comp = df_pendientes.pivot_table(
            index='CIUDAD', 
            columns=columna_franja, 
            values='TOTAL CARTERA', 
            aggfunc='sum'
        ).fillna(0)

        resumen_totales = df_pendientes.groupby('CIUDAD').agg({
            'TOTAL CARTERA': 'sum',
            'SALDO_ES_VENCIDO': 'sum'
        })

        tabla_comp = tabla_comp.merge(resumen_totales, on='CIUDAD')
        tabla_comp = tabla_comp.rename(columns={'TOTAL CARTERA': 'TOTAL_CARTERA', 'SALDO_ES_VENCIDO': 'TOTAL_VENCIDO'})
        tabla_comp['PORCENTAJE_VENCIDO'] = (tabla_comp['TOTAL_VENCIDO'] / tabla_comp['TOTAL_CARTERA'] * 100).fillna(0)
        
        lista_composicion = tabla_comp.reset_index().sort_values(by='TOTAL_CARTERA', ascending=False).to_dict(orient='records')
        columnas_franjas = [c for c in tabla_comp.columns if c not in ['TOTAL_CARTERA', 'TOTAL_VENCIDO', 'PORCENTAJE_VENCIDO', 'CIUDAD']]

        # --- 11. TABLA POR ADMINISTRADOR ---
        tabla_admin_df = df_pendientes.pivot_table(
            index=col_admin_real, 
            columns=columna_franja, 
            values='TOTAL CARTERA', 
            aggfunc='sum'
        ).fillna(0)

        resumen_admin = df_pendientes.groupby(col_admin_real).agg({
            'TOTAL CARTERA': 'sum',
            'SALDO_ES_VENCIDO': 'sum'
        })

        tabla_admin_df = tabla_admin_df.merge(resumen_admin, on=col_admin_real)
        tabla_admin_df = tabla_admin_df.rename(columns={'TOTAL CARTERA': 'TOTAL_CARTERA', 'SALDO_ES_VENCIDO': 'TOTAL_VENCIDO'})
        
        # CORRECCIÓN AQUÍ: Usamos tabla_admin_df, no tabla_cliente_df
        tabla_admin_df['PORCENTAJE_VENCIDO'] = (tabla_admin_df['TOTAL_VENCIDO'] / tabla_admin_df['TOTAL_CARTERA'] * 100).fillna(0)

        lista_admin = tabla_admin_df.reset_index().sort_values(by='TOTAL_CARTERA', ascending=False).to_dict(orient='records')

        # --- 12. TABLA POR CLIENTE ---
        # 1. Creamos la tabla pivote y el resumen
        tabla_cliente_df = df_pendientes.pivot_table(
            index=['COD. CLIENTE', 'NIT', 'RAZÓN SOCIAL'], 
            columns=columna_franja, 
            values='TOTAL CARTERA', 
            aggfunc='sum'
        ).fillna(0)

        resumen_cliente = df_pendientes.groupby(['COD. CLIENTE', 'NIT', 'RAZÓN SOCIAL']).agg({
            'TOTAL CARTERA': 'sum',
            'SALDO_ES_VENCIDO': 'sum'
        })

        # 2. Unir, renombrar y calcular porcentaje
        tabla_cliente_df = tabla_cliente_df.merge(resumen_cliente, on=['COD. CLIENTE', 'NIT', 'RAZÓN SOCIAL'])
        tabla_cliente_df = tabla_cliente_df.rename(columns={'TOTAL CARTERA': 'TOTAL_CARTERA', 'SALDO_ES_VENCIDO': 'TOTAL_VENCIDO'})
        tabla_cliente_df['PORCENTAJE_VENCIDO'] = (tabla_cliente_df['TOTAL_VENCIDO'] / tabla_cliente_df['TOTAL_CARTERA'] * 100).fillna(0)

        # 3. Ordenar y convertir a lista de diccionarios
        lista_clientes = tabla_cliente_df.reset_index() \
                                         .sort_values(by='TOTAL_CARTERA', ascending=False) \
                                         .to_dict(orient='records')

        # --- 13. Retorno final ---
        total_cartera_final = df_pendientes['TOTAL CARTERA'].sum()
        total_vencido_final = df_pendientes['SALDO_ES_VENCIDO'].sum()

        return {
            'ciudades': ciudades,
            'kpis': {
                'total_cartera': total_cartera_final,
                'vencida': total_vencido_final,
                'morosidad': (total_vencido_final / total_cartera_final * 100) if total_cartera_final > 0 else 0,
                'recaudo': total_recaudo,
                'clientes_total': df_pendientes['NIT'].nunique()
            },
            'graficos': {
                'dona_labels': list(resumen_grafico.keys()),
                'dona_valores': list(resumen_grafico.values()),
                'ciudades_labels': df_final_ciudades['CIUDAD'].tolist(),
                'ciudades_valores': df_final_ciudades['TOTAL CARTERA'].tolist(),
                'mora_ciudades_labels': df_final_mora['CIUDAD'].tolist(),
                'mora_ciudades_valores': df_final_mora['SALDO_ES_VENCIDO'].tolist(),
            },
            'tabla_composicion': lista_composicion,
            'tabla_admin': lista_admin,
            'tabla_clientes': lista_clientes, # <--- Ahora esta variable sí existe arriba
            'columnas_franjas': columnas_franjas,
            'detalles': []
        }

    except Exception as e:
        print(f"❌ Error en procesar_informacion: {str(e)}")
        return None

@app.route('/')
def index():
    global RUTA_CARTERA, RUTA_PAGOS
    vista = request.args.get('vista', 'cyres')
    ciudad = request.args.get('ciudad', 'Todas')

    ahora = datetime.now()
    mes_actual_real = ahora.month
    anio_actual_real = ahora.year

    # Capturamos mes y año del selector (si no hay, usa el actual)
    mes_actual = int(request.args.get('mes', mes_actual_real))
    anio_actual = int(request.args.get('anio', anio_actual_real))

    # --- DEFINIR RUTAS SEGÚN EL MES SELECCIONADO ---
    if mes_actual == mes_actual_real and anio_actual == anio_actual_real:
        path_proyectado = os.path.join(BASE_DIR, 'data', 'Proyectadoconsolidado.csv')
        path_pagos = os.path.join(BASE_DIR, 'data', 'PagosConsolidado.csv')
        
    else:
        path_proyectado = os.path.join(BASE_DIR, 'data', 'historico', f'Proyectadoconsolidado_{anio_actual}_{mes_actual:02d}.csv')
        path_pagos = os.path.join(BASE_DIR, 'data', 'historico', f'PagosConsolidado_{anio_actual}_{mes_actual:02d}.csv')
    global RUTA_CARTERA, RUTA_PAGOS
    RUTA_CARTERA = path_proyectado
    RUTA_PAGOS = path_pagos

    # Para que no te de error de "folder_data", definimos esta variable que usas más adelante
    folder_data = os.path.join(BASE_DIR, 'data')

    # Actualizamos las fechas de los archivos para la interfaz
    fecha_act_cartera = obtener_fecha_archivo(path_proyectado)
    fecha_act_pagos = obtener_fecha_archivo(path_pagos)
    
    # --- TUS VARIABLES DE SIEMPRE (Mantenlas tal cual están en tu código) ---
    kpis_calculados = {
        'ingresos': 0, 'presupuesto': 0, 'desviacion': 0, 
        'efectividad': 0, 'presupuesto_actual': 0, 'ingresos_actual': 0,
        'desviacion_actual': 0, 'ejecucion_actual': 0, 'otro': 0
    }
    
    grafico_lineas = {
        'labels': [], 'presupuesto': [], 'ingresos': [],
        'presupuesto_acc': [], 'ingresos_acc': []
    }

    operaciones_tabla = []
    detalle_clientes_grafica = {} 
    detalle_presupuesto_grafica = {}

    
    if vista == 'detalle_analisis':
        try:
            
            ultimo_dia = calendar.monthrange(anio_actual, mes_actual)[1]

            # --- LÓGICA DE CORTE PARA EL PRESUPUESTO ACTUAL ---
            ahora_real = datetime.now()
            # Si el mes es Enero (pasado), el corte es el último día (31)
            if anio_actual < ahora_real.year or (anio_actual == ahora_real.year and mes_actual < ahora_real.month):
                dia_corte = ultimo_dia
            # Si es el mes actual (Febrero), el corte es ayer
            elif mes_actual == ahora_real.month and anio_actual == ahora_real.year:
                dia_corte = ahora_real.day - 1 if ahora_real.day > 1 else 1
            else:
                dia_corte = 1

            # A.1. Leer Presupuesto (CORREGIDO)
            df_filtrado = pd.DataFrame()
            if os.path.exists(path_proyectado):
                df_proy = pd.read_csv(path_proyectado, sep=';', encoding='latin1')
                
                # --- AJUSTE PARA FORMATO CON PUNTOS (10.02.2026) ---
                # Convertimos la columna a fecha especificando el formato de puntos
                df_proy['Fecha_Vencimiento'] = pd.to_datetime(
                    df_proy['Fecha_Vencimiento'].astype(str), 
                    format='%d.%m.%Y', 
                    errors='coerce'
                )
                
                # Si por alguna razón hay fechas con guiones o barras, esto las rescata:
                mask_nan = df_proy['Fecha_Vencimiento'].isna()
                if mask_nan.any():
                    df_proy.loc[mask_nan, 'Fecha_Vencimiento'] = pd.to_datetime(
                        df_proy.loc[mask_nan, 'Fecha_Vencimiento'], 
                        errors='coerce'
                    )
                # ---------------------------------------------------

                filtro = (df_proy['Fecha_Vencimiento'].dt.month == mes_actual) & \
                         (df_proy['Fecha_Vencimiento'].dt.year == anio_actual)
                
                df_filtrado = df_proy[filtro].copy()
                
                # Filtrar y continuar con el resto del código...

                def limpiar_monto(serie):
                    return pd.to_numeric(serie.astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0)
                
                # Cálculo del KPI Presupuesto Total del Mes
                kpis_calculados['presupuesto'] = pd.to_numeric(df_filtrado['TOTAL CARTERA'], errors='coerce').fillna(0).sum()

                if df_filtrado.empty:
                    print(f"ADVERTENCIA: No hay datos PENDIENTES para {mes_actual}/{anio_actual}")

            # A.2. Leer Ingresos
            if os.path.exists(path_pagos):
                df_pagos = pd.read_csv(path_pagos, sep=';', encoding='latin1')
                df_pagos.columns = df_pagos.columns.str.strip()
                
                if 'VALOR PAGADO' in df_pagos.columns:
                    kpis_calculados['ingresos'] = pd.to_numeric(df_pagos['VALOR PAGADO'], errors='coerce').fillna(0).sum()
                
                # A.3. Lógica para la Gráfica Diaria General
                dias_mes = pd.date_range(start=f"{anio_actual}-{mes_actual}-01", end=f"{anio_actual}-{mes_actual}-{ultimo_dia}")
                df_diario = pd.DataFrame({'Fecha': dias_mes})

                df_proy_dia = df_filtrado.groupby(df_filtrado['Fecha_Vencimiento'].dt.date)['TOTAL CARTERA'].sum().reset_index() if not df_filtrado.empty else pd.DataFrame(columns=['Fecha', 'TOTAL CARTERA'])
                df_proy_dia.columns = ['Fecha', 'Presupuesto_Dia']
                df_proy_dia['Fecha'] = pd.to_datetime(df_proy_dia['Fecha'])

                if 'FECHA PAGO' in df_pagos.columns:
                    df_pagos['FECHA_PAGO_DT'] = pd.to_datetime(df_pagos['FECHA PAGO'], dayfirst=True, errors='coerce')
                    df_pagos_dia = df_pagos.groupby(df_pagos['FECHA_PAGO_DT'].dt.date)['VALOR PAGADO'].sum().reset_index()
                    df_pagos_dia.columns = ['Fecha', 'Ingreso_Dia']
                    df_pagos_dia['Fecha'] = pd.to_datetime(df_pagos_dia['Fecha'])

                    df_final = pd.merge(df_diario, df_proy_dia, on='Fecha', how='left')
                    df_final = pd.merge(df_final, df_pagos_dia, on='Fecha', how='left').fillna(0)

                    df_final['Presupuesto_Acc'] = df_final['Presupuesto_Dia'].cumsum()
                    df_final['Ingreso_Acc'] = df_final['Ingreso_Dia'].cumsum()

                    grafico_lineas = {
                        'labels': [d.strftime('%d') for d in df_final['Fecha']],
                        'presupuesto': df_final['Presupuesto_Dia'].tolist(),
                        'ingresos': df_final['Ingreso_Dia'].tolist(),
                        'presupuesto_acc': df_final['Presupuesto_Acc'].tolist(),
                        'ingresos_acc': df_final['Ingreso_Acc'].tolist()
                    }
                    # --- CÁLCULO DE CURVA DE RECUPERACIÓN (MEJORADO) ---
                    curva_rec_data = {'labels': [], 'vencido': [], 'recuperado': [], 'porcentajes': [], 'efectividad_mensual': 0}
                    try:
                        if not df_filtrado.empty:
                            df_curva = df_filtrado.copy()
                            # Agrupamos por día
                            rec_diaria = df_curva.groupby(df_curva['Fecha_Vencimiento'].dt.date).agg(
                                Total_Vencido=('TOTAL CARTERA', 'sum'),
                                Total_Recuperado=('TOTAL CARTERA', lambda x: df_curva.loc[x.index, 'ESTADO'].astype(str).str.strip().str.upper().eq('RECUPERADA').multiply(df_curva.loc[x.index, 'TOTAL CARTERA']).sum())
                            ).reset_index()

                            rec_diaria.columns = ['Fecha_Vence', 'Vencido', 'Recuperado']
                            rec_diaria = rec_diaria.sort_values('Fecha_Vence')
                            
                            # Cálculo del % diario
                            rec_diaria['Porc'] = (rec_diaria['Recuperado'] / rec_diaria['Vencido'] * 100).round(1).fillna(0)
                            
                            # CÁLCULO GLOBAL MENSUAL
                            vencido_total = int(rec_diaria['Vencido'].sum())
                            recuperado_total = int(rec_diaria['Recuperado'].sum())
                            variacion_total = recuperado_total - vencido_total
                            efectividad_global = (recuperado_total / vencido_total * 100) if vencido_total > 0 else 0

                            curva_rec_data = {
                                'labels': [d.strftime('%d-%m') for d in rec_diaria['Fecha_Vence']],
                                'vencido': rec_diaria['Vencido'].tolist(),
                                'recuperado': rec_diaria['Recuperado'].tolist(),
                                'porcentajes': rec_diaria['Porc'].tolist(),
                                'vencido_total': vencido_total,
                                'recuperado_total': recuperado_total,
                                'variacion_total': variacion_total,
                                'efectividad_mensual': round(float(efectividad_global), 1)
                            }

                    except Exception as e:
                        print(f"Error en curva: {e}")
                    
                    # Finalmente lo pasamos a los KPIs que recibe el HTML
                                      
                    kpis_calculados['curva_recuperacion'] = curva_rec_data

                    # --- NUEVO: DICCIONARIO DE INGRESOS DIARIOS POR CLIENTE ---
                    if 'COD. CLIENTE' in df_pagos.columns:
                        ingresos_por_cli_dia = df_pagos.groupby(['COD. CLIENTE', df_pagos['FECHA_PAGO_DT'].dt.day])['VALOR PAGADO'].sum().unstack(fill_value=0)
                        
                        for cod, fila in ingresos_por_cli_dia.iterrows():
                            lista_valores = [float(fila.get(d, 0)) for d in range(1, ultimo_dia + 1)]
                            detalle_clientes_grafica[str(cod)] = lista_valores

                    # --- PEGA AQUÍ EL NUEVO BLOQUE DE PRESUPUESTO ---
                    if not df_filtrado.empty and 'COD. CLIENTE' in df_filtrado.columns:
                        ppto_por_cli_dia = df_filtrado.groupby(['COD. CLIENTE', df_filtrado['Fecha_Vencimiento'].dt.day])['TOTAL CARTERA'].sum().unstack(fill_value=0)
                        for cod, fila in ppto_por_cli_dia.iterrows():
                            lista_ppto = [float(fila.get(d, 0)) for d in range(1, ultimo_dia + 1)]
                            detalle_presupuesto_grafica[str(cod)] = lista_ppto

                    # Usamos dia_corte (que será 31 para enero o 1 para febrero)
                    filtro_corte = df_filtrado['Fecha_Vencimiento'].dt.day <= dia_corte
                    kpis_calculados['presupuesto_actual'] = pd.to_numeric(df_filtrado[filtro_corte]['TOTAL CARTERA'], errors='coerce').fillna(0).sum()

                    # DESVIACIÓN ACTUAL: Ingresos totales vs Presupuesto al día de corte
                    kpis_calculados['ingresos_actual'] = kpis_calculados['ingresos']
                    kpis_calculados['desviacion_actual'] = kpis_calculados['ingresos'] - kpis_calculados['presupuesto_actual']
                    
                    if kpis_calculados['presupuesto_actual'] > 0:
                        kpis_calculados['ejecucion_actual'] = (kpis_calculados['ingresos'] / kpis_calculados['presupuesto_actual'] * 100)
                    else:
                        kpis_calculados['ejecucion_actual'] = 0


                    # --- CONSOLIDACIÓN POR CLIENTE PARA LA TABLA ---
                    if not df_filtrado.empty:
                        col_cod = 'COD. CLIENTE'
                        col_razon = next((c for c in df_filtrado.columns if 'RAZON' in c.upper() or 'SOCIAL' in c.upper() or 'CLIENTE' in c.upper() and c != col_cod), df_filtrado.columns[1])

                        df_cli_proy = df_filtrado.groupby([col_cod, col_razon]).agg(Presupuesto_Mensual=('TOTAL CARTERA', 'sum')).reset_index()
                        df_cli_proy.columns = ['COD_CLIENTE', 'RAZON_SOCIAL', 'Presupuesto_Mensual']

                        df_ayer_cli = df_filtrado[df_filtrado['Fecha_Vencimiento'].dt.day <= dia_corte]
                        df_cli_proy_actual = df_ayer_cli.groupby(col_cod)['TOTAL CARTERA'].sum().reset_index() if not df_ayer_cli.empty else pd.DataFrame(columns=[col_cod, 'Presupuesto_Actual'])
                        df_cli_proy_actual.columns = ['COD_CLIENTE', 'Presupuesto_Actual']

                        df_cli_pagos = df_pagos.groupby(col_cod)['VALOR PAGADO'].sum().reset_index() if col_cod in df_pagos.columns else pd.DataFrame(columns=[col_cod, 'Ingresos_Recibidos'])
                        df_cli_pagos.columns = ['COD_CLIENTE', 'Ingresos_Recibidos']

                        tabla_clientes = pd.merge(df_cli_proy, df_cli_proy_actual, on='COD_CLIENTE', how='left')
                        tabla_clientes = pd.merge(tabla_clientes, df_cli_pagos, on='COD_CLIENTE', how='left').fillna(0)

                        tabla_clientes['Desviacion'] = tabla_clientes['Ingresos_Recibidos'] - tabla_clientes['Presupuesto_Actual']
                        tabla_clientes['Efe_Actual'] = (tabla_clientes['Ingresos_Recibidos'] / tabla_clientes['Presupuesto_Actual'] * 100).replace([float('inf')], 0).fillna(0)
                        tabla_clientes['Efe_Mensual'] = (tabla_clientes['Ingresos_Recibidos'] / tabla_clientes['Presupuesto_Mensual'] * 100).replace([float('inf')], 0).fillna(0)

                        # 1. Primero se ordena la tabla (Mantenlo)
                        tabla_clientes = tabla_clientes.sort_values(by='Presupuesto_Mensual', ascending=False)
                        operaciones_tabla = tabla_clientes.to_dict(orient='records')

            # 2. JUSTO DEBAJO (Saliendo de un nivel de sangría), pones las desviaciones:
            kpis_calculados['desviacion'] = kpis_calculados['ingresos'] - kpis_calculados['presupuesto']
            if kpis_calculados['presupuesto'] > 0:
                kpis_calculados['efectividad'] = (kpis_calculados['ingresos'] / kpis_calculados['presupuesto']) * 100
            else:
                kpis_calculados['efectividad'] = 0

        except Exception as e:
            print(f"Error en detalle: {e}")

        try:
            print(f"\n" + "="*40)
            print(f"DEBUG: PROCESANDO RECUPERACIÓN: {path_proyectado}")
            tabla_recu, total_recu = obtener_tabla_recuperacion_franjas(path_proyectado)
            
            # --- NUEVO: ESTO IMPRIMIRÁ EL DESGLOSE EN TU TERMINAL ---
            print("DESGLOSE POR FRANJAS:")
            for fila in tabla_recu:
                # Añadimos el % al print de la terminal
                print(f" - {fila['rango']:<20} : ${fila['valor']:>12,.0f} ({fila['porcentaje']}% )")
            
            print(f"TOTAL RECUPERADO: ${total_recu:>12,.0f}")
            print("="*40 + "\n")
            
        except Exception as e:
            print(f"ERROR EN EL PROCESADOR: {e}")
            tabla_recu, total_recu = [], 0

        # --- LÓGICA PARA COMPARATIVA DIARIA (Enero vs Febrero) ---
        comparativa_diaria = {
            'labels': list(range(1, 32)),  
            'enero': [0.0]*31,
            'febrero': [0.0]*31
        }

        try:
            def procesar_csv_diario(ruta):
                if not os.path.exists(ruta):
                    print(f"DEBUG: No se encontró el archivo en {ruta}")
                    return {}
                
                # Leer el archivo
                df = pd.read_csv(ruta, sep=';', encoding='latin1')
                df.columns = df.columns.str.strip() # Limpiar espacios en nombres de columnas
                
                # Convertir fecha usando el formato 02/02/2026 (día/mes/año)
                # Usamos 'FECHA PAGO' que es el nombre real de tu columna
                df['FECHA_DT'] = pd.to_datetime(df['FECHA PAGO'], format='%d/%m/%Y', errors='coerce')
                
                # Convertir 'VALOR PAGADO' a número
                df['MONTO'] = pd.to_numeric(df['VALOR PAGADO'], errors='coerce').fillna(0)
                
                # Agrupar por día
                return df.groupby(df['FECHA_DT'].dt.day)['MONTO'].sum().to_dict()

            # 1. Procesar Febrero (Actual en \data)
            recaudo_feb_dict = procesar_csv_diario(RUTA_PAGOS)
            for dia, valor in recaudo_feb_dict.items():
                if pd.notna(dia) and 1 <= int(dia) <= 31:
                    comparativa_diaria['febrero'][int(dia)-1] = float(valor)

            # 2. Procesar Enero (Histórico en \data\historico)
            # Nota: Cambié a 'historico' (sin s) y el año 2026 como indicaste
            path_enero = os.path.join(BASE_DIR, 'data', 'historico', 'PagosConsolidado_2026_01.csv')
            recaudo_ene_dict = procesar_csv_diario(path_enero)
            for dia, valor in recaudo_ene_dict.items():
                if pd.notna(dia) and 1 <= int(dia) <= 31:
                    comparativa_diaria['enero'][int(dia)-1] = float(valor)

        except Exception as e:
            print(f"Error procesando comparativa diaria: {e}")

        # --- RUTAS Y TABLA (MANTENER IGUAL) ---
        
        RUTA_CARTERA = path_proyectado
        RUTA_PAGOS = path_pagos
        # --- DETERMINAR QUÉ ARCHIVO DE CARTERA (PROYECTADO) USAR ---
        if mes_actual == 1:
            # Si es Enero, buscamos en la carpeta historico (ajusta la ruta si es necesario)
            path_proyectado_tabla = os.path.join(BASE_DIR, 'data', 'historico', 'Proyectadoconsolidado_2026_01.csv')
        else:
            # Si es el mes actual (Febrero), usamos el archivo de la carpeta data
            path_proyectado_tabla = RUTA_CARTERA 
    
        # --- LLAMADA A LA TABLA CON EL ARCHIVO CORRECTO ---
        try:
            # Usamos path_proyectado porque ya contiene la ruta correcta (Enero, Febrero, etc.)
            tabla_recu, total_recu = obtener_tabla_recuperacion_franjas(path_proyectado)
        except Exception as e:
            print(f"Error en tabla recu: {e}")
            tabla_recu, total_recu = [], 0

        return render_template('detalle.html', 
                            kpis=kpis_calculados,
                            grafico_lineas=grafico_lineas, 
                            detalle_clientes=detalle_clientes_grafica,
                            detalle_presupuesto=detalle_presupuesto_grafica, 
                            vista_actual=vista,
                            operaciones_tabla=operaciones_tabla, 
                            ciudad_actual=ciudad,
                            mes_actual=mes_actual,
                            anio_actual=anio_actual,
                            fecha_proyectado=fecha_act_cartera, 
                            fecha_pagos=fecha_act_pagos,
                            tabla_recu_franjas=tabla_recu,
                            total_recu_monto_tabla=total_recu,
                            comparativa_diaria=comparativa_diaria)

    datos = procesar_informacion(vista, ciudad)
    
    if datos is None: return "<h1>Error en el procesamiento de datos</h1>"

    return render_template('index.html', 
                           **datos, 
                           vista_actual=vista, 
                           ciudad_actual=ciudad,
                           fecha_proyectado=fecha_act_cartera,
                           fecha_pagos=fecha_act_pagos,
                           mes_actual=mes_actual,
                           anio_actual=anio_actual)
                           

def obtener_ultimo_archivo(ruta_carpeta):
    # Si la carpeta no existe, la creamos para evitar errores
    if not os.path.exists(ruta_carpeta):
        os.makedirs(ruta_carpeta)
    
    archivos = glob.glob(os.path.join(ruta_carpeta, "*.csv"))
    if not archivos:
        return "Sin archivos"
    
    # Buscamos el que tenga la fecha de modificación más reciente
    ultimo = max(archivos, key=os.path.getmtime)
    return os.path.basename(ultimo)

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    mensaje = None
    RUTA_PROY = os.path.join(BASE_DIR, 'data', 'proyectados')
    RUTA_PAGOS = os.path.join(BASE_DIR, 'data', 'pagos_diarios')
    
    if request.method == 'POST':
        # --- Lógica para Pagos ---
        if 'file_pagos' in request.files:
            file = request.files['file_pagos']
            if file.filename != '':
                file.save(os.path.join(RUTA_PAGOS, file.filename))
                mensaje = f"Archivo de pagos '{file.filename}' subido con éxito."
                
        # --- Lógica para Proyectado ---
        if 'file_proy' in request.files:
            file = request.files['file_proy']
            if file.filename != '':
                # 1. Guardamos el archivo físico
                path_destino = os.path.join(RUTA_PROY, file.filename)
                file.save(path_destino)
                
                # 2. DISPARAMOS EL PROCESADOR MAESTRO (Las 11 columnas)
                try:
                    res_maestro = procesar_todo()
                    mensaje = f"Archivo proyectado subido y Maestro actualizado: {res_maestro}"
                except Exception as e:
                    mensaje = f"Archivo subido, pero error en cálculos: {str(e)}"

    # Mantenemos tu lógica de mostrar los últimos archivos en la interfaz
    ult_proy = obtener_ultimo_archivo(RUTA_PROY)
    ult_pago = obtener_ultimo_archivo(RUTA_PAGOS)

    return render_template('upload.html', 
                           mensaje=mensaje, 
                           ultimo_proy=ult_proy, 
                           ultimo_pago=ult_pago, 
                           vista_actual='upload')


@app.route('/ejecutar-script', methods=['POST'])
def ejecutar_script():
    # Este es el espacio donde conectaremos el script .py aparte
    return "<h1>El botón funciona. Esperando instrucciones para el script aparte.</h1>"

from procesador_pagos import consolidar_pagos # Importamos la función del nuevo script

@app.route('/ejecutar-pagos', methods=['POST'])
def ejecutar_pagos():
    try:
        resultado = consolidar_pagos()
        # Redirigimos de vuelta al upload con el mensaje de éxito del script
        return render_template('upload.html', mensaje=resultado, vista_actual='upload')
    except Exception as e:
        return render_template('upload.html', mensaje=f"Error: {str(e)}", vista_actual='upload')

@app.route('/ejecutar-maestro', methods=['POST'])
def ejecutar_maestro():
    try:
        # 1. Ejecutar el proceso que une las 11 columnas
        resultado = procesar_todo()
        
        # 2. Definir rutas para refrescar la interfaz
        ruta_p = os.path.join(BASE_DIR, 'data', 'proyectados')
        ruta_pg = os.path.join(BASE_DIR, 'data', 'pagos_diarios')
        
        ult_proy = obtener_ultimo_archivo(ruta_p)
        ult_pago = obtener_ultimo_archivo(ruta_pg)

        return render_template('upload.html', 
                               mensaje=f"✅ {resultado}", 
                               ultimo_proy=ult_proy, 
                               ultimo_pago=ult_pago, 
                               vista_actual='upload')
    except Exception as e:
        return render_template('upload.html', 
                               mensaje=f"❌ Error al procesar: {str(e)}", 
                               vista_actual='upload')
    

from procesador_gestion import calcular_gestion

@app.route('/gestiones')
def gestiones():
    ahora_real = datetime.now()
    
    # 1. CAPTURAR VARIABLES
    mes_actual = int(request.args.get('mes', ahora_real.month))
    anio_actual = int(request.args.get('anio', ahora_real.year))
    vista_activa = request.args.get('vista', 'general') 
    analista_f = request.args.get('analista', 'Todos')
    f_inicio = request.args.get('fecha_inicio') 
    f_fin = request.args.get('fecha_fin')

    # 2. DEFINIR RUTAS DINÁMICAS
    if mes_actual == ahora_real.month and anio_actual == ahora_real.year:
        path_cartera = os.path.join(BASE_DIR, 'data', 'Proyectadoconsolidado.csv') 
        path_gestion = os.path.join(BASE_DIR, 'data', 'gestion.zip')
        path_pagos = os.path.join(BASE_DIR, 'data', 'PagosConsolidado.csv')
    else:
        path_cartera = os.path.join(BASE_DIR, 'data', 'historico', f'Proyectadoconsolidado_{anio_actual}_{mes_actual:02d}.csv')
        path_gestion = os.path.join(BASE_DIR, 'data', 'historico', f'gestion_{anio_actual}_{mes_actual:02d}.zip')
        path_pagos = os.path.join(BASE_DIR, 'data', 'historico', f'PagosConsolidado_{anio_actual}_{mes_actual:02d}.csv')

    # 3. INICIALIZAR VARIABLES PARA EVITAR ERRORES
    indicadores = {}
    pagos_analista_metodo = {}
    metodos_fijos = ['WALLET', 'TRANSFERENCIA', 'CONSIGNACIÓN']

    # 4. LLAMAR AL PROCESADOR Y CALCULAR PAGOS
    try:
        # Procesador principal
        indicadores = calcular_gestion(
            path_cartera, 
            path_gestion,
            ruta_pagos=path_pagos,
            analista_seleccionado=analista_f,
            fecha_inicio=f_inicio, 
            fecha_fin=f_fin,
            mes_actual=mes_actual,
            anio_actual=anio_actual
        )

        # Lógica de Pagos por Analista (Usando path_pagos definido arriba)
        if os.path.exists(path_pagos):
            df_p = pd.read_csv(path_pagos, encoding='latin1', sep=None, engine='python')
            df_p.columns = [c.upper().strip() for c in df_p.columns]
            
            col_v, col_m, col_a = 'VALOR PAGADO', 'MÉTODO DE PAGO', 'ANALISTA'

            if all(c in df_p.columns for c in [col_v, col_m, col_a]):
                df_p[col_v] = pd.to_numeric(df_p[col_v], errors='coerce').fillna(0)
                df_p[col_m] = df_p[col_m].astype(str).str.upper().str.strip()
                df_p[col_a] = df_p[col_a].astype(str).str.strip()

                # Quitamos tildes para agrupar bien
                df_p[col_m] = df_p[col_m].str.replace('Ó', 'O').str.replace('Á', 'A')

                resumen = df_p.groupby([col_a, col_m])[col_v].sum().reset_index()

                for _, row in resumen.iterrows():
                    ana = row[col_a]
                    met = row[col_m]
                    val = row[col_v]

                    nombre_final = None
                    if 'WALLET' in met: nombre_final = 'WALLET'
                    elif 'TRANSFERENCIA' in met: nombre_final = 'TRANSFERENCIA'
                    elif 'CONSIGNACION' in met or 'CONSIGNACIÓN' in met: nombre_final = 'CONSIGNACIÓN'

                    if nombre_final:
                        if ana not in pagos_analista_metodo:
                            pagos_analista_metodo[ana] = {m: 0 for m in metodos_fijos}
                            pagos_analista_metodo[ana]['TOTAL'] = 0
                        
                        pagos_analista_metodo[ana][nombre_final] += val
                        pagos_analista_metodo[ana]['TOTAL'] += val

    except Exception as e:
        print(f"Error general en ruta gestiones: {e}")

    # 5. RETORNO (Corregido: las variables deben tener nombre=valor)
    return render_template('gestiones.html',
                           stats=indicadores,
                           vista_actual='gestiones',
                           vista_detalle=vista_activa,
                           analista_actual=analista_f,
                           mes_actual=mes_actual,
                           anio_actual=anio_actual,
                           pagos_analista_metodo=pagos_analista_metodo,
                           metodos_fijos=metodos_fijos,
                           now=ahora_real)


@app.route('/historicos')
def historicos():

    meses_es = {
        'Jan': 'Ene', 'Feb': 'Feb', 'Mar': 'Mar', 'Apr': 'Abr', 
        'May': 'May', 'Jun': 'Jun', 'Jul': 'Jul', 'Aug': 'Ago', 
        'Sep': 'Sep', 'Oct': 'Oct', 'Nov': 'Nov', 'Dec': 'Dic'
    }
    # 1. DATOS FRÍOS (Jul-Dic 2025)
    datos_finales = {
        datetime(2025, 7, 1): 3416,
        datetime(2025, 8, 1): 2998,
        datetime(2025, 9, 1): 3137,
        datetime(2025, 10, 1): 2911,
        datetime(2025, 11, 1): 2379,
        datetime(2025, 12, 1): 3025
    }

    # 2. RECOLECTOR DE PAGOS (2026+)
    # Buscamos en la carpeta historico y el archivo vivo actual
    ruta_patron_hist = os.path.join(BASE_DIR, 'data', 'historico', 'PagosConsolidado_*.csv')
    archivos_a_procesar = glob.glob(ruta_patron_hist)
    archivos_a_procesar.append(RUTA_PAGOS) 

    for ruta in archivos_a_procesar:
        try:
            if os.path.exists(ruta):
                # Usamos sep=None y engine='python' para detectar si es coma o punto y coma
                df_p = pd.read_csv(ruta, encoding='latin1', sep=None, engine='python')
                
                if 'FECHA PAGO' in df_p.columns and 'VALOR PAGADO' in df_p.columns:
                    # FORZAMOS EL FORMATO: dayfirst=True para que 01/02 sea 1 de Feb
                    df_p['FECHA PAGO'] = pd.to_datetime(df_p['FECHA PAGO'], dayfirst=True, errors='coerce')
                    
                    # Limpiamos filas sin fecha o sin valor
                    df_p = df_p.dropna(subset=['FECHA PAGO', 'VALOR PAGADO'])
                    
                    # FILTRO DE SEGURIDAD: Solo años 2026 en adelante para esta parte
                    df_p = df_p[df_p['FECHA PAGO'].dt.year >= 2026]

                    # Agrupamos por Mes y Año
                    df_p['MES_KEY'] = df_p['FECHA PAGO'].dt.to_period('M')
                    resumen = df_p.groupby('MES_KEY')['VALOR PAGADO'].sum().reset_index()

                    for _, row in resumen.iterrows():
                        fecha_dt = row['MES_KEY'].to_timestamp()
                        valor_m = round(row['VALOR PAGADO'] / 1000000, 0)
                        
                        # Guardar o sumar si ya existe
                        if fecha_dt in datos_finales:
                            datos_finales[fecha_dt] += int(valor_m)
                        else:
                            datos_finales[fecha_dt] = int(valor_m)
        except Exception as e:
            print(f"Error procesando {ruta}: {e}")

    # --- INICIO LÓGICA GRÁFICA 2: MORA (VERSIÓN CORREGIDA) ---
    mora_valor_final = {
        datetime(2025, 7, 1): 763, datetime(2025, 8, 1): 830,
        datetime(2025, 9, 1): 673, datetime(2025, 10, 1): 597,
        datetime(2025, 11, 1): 764, datetime(2025, 12, 1): 391
    }
    
    mora_porcentaje_final = {
        datetime(2025, 7, 1): 46.8, datetime(2025, 8, 1): 51.40,
        datetime(2025, 9, 1): 44.17, datetime(2025, 10, 1): 37.98,
        datetime(2025, 11, 1): 49.30, datetime(2025, 12, 1): 30.90
    }

    # --- LÓGICA GRÁFICA 2: INDICADOR DE MORA (FOTO DE HOY) ---
    try:
        # 1. Definimos los archivos a leer (El consolidado actual y los de meses pasados si existen)
        # Esto es para que la gráfica tenga historia: Ene, Feb...
        rutas_mora = glob.glob(os.path.join(BASE_DIR, 'data', 'historico', 'Proyectadoconsolidado_*.csv'))
        rutas_mora.append(RUTA_CARTERA) 

        for ruta in rutas_mora:
            if os.path.exists(ruta):
                df_m = pd.read_csv(ruta, encoding='latin1', sep=';')
                df_m.columns = [str(c).strip() for c in df_m.columns]

                # Convertimos a números
                df_m['DIAS_MORA'] = pd.to_numeric(df_m['DIAS_MORA'], errors='coerce').fillna(0)
                df_m['TOTAL CARTERA'] = pd.to_numeric(df_m['TOTAL CARTERA'], errors='coerce').fillna(0)

                # --- NUEVA LÓGICA DE FILTROS ---
                # 1. Todo lo que te deben (Denominador)
                mask_todo_pendiente = (df_m['ESTADO'].str.strip() == 'PENDIENTE')
                suma_cartera_pendiente = df_m.loc[mask_todo_pendiente, 'TOTAL CARTERA'].sum()

                # 2. Lo que está en mora (Numerador)
                mask_mora_real = mask_todo_pendiente & (df_m['DIAS_MORA'] >= 1)
                suma_mora_vencida = df_m.loc[mask_mora_real, 'TOTAL CARTERA'].sum()

                # Identificar mes de la foto
                df_m['PRIMERA_APARICION'] = pd.to_datetime(df_m['PRIMERA_APARICION'], errors='coerce')
                fecha_foto = df_m['PRIMERA_APARICION'].dropna().iloc[0] 
                fecha_mes = datetime(fecha_foto.year, fecha_foto.month, 1)

                # Guardar resultados
                mora_valor_final[fecha_mes] = int(round(suma_mora_vencida / 1000000, 0))
                
                if suma_cartera_pendiente > 0:
                    mora_porcentaje_final[fecha_mes] = round((suma_mora_vencida / suma_cartera_pendiente) * 100, 1)
                
                    
    except Exception as e:
        print(f"Error crítico en mora: {str(e)}")

    # Preparar listas para la Gráfica 2 (Mora)
    labels_mora = []
    data_mora_val = []
    data_mora_pct = []
    colores_mora = []  # <--- Creamos esta lista

    fechas_mora_ord = sorted(mora_valor_final.keys())
    ahora = datetime.now()

    for f_m in fechas_mora_ord:
        lbl_en = f_m.strftime('%b-%y')
        m_en_m = lbl_en.split('-')[0]
        a_en_m = lbl_en.split('-')[1]
        labels_mora.append(f"{meses_es.get(m_en_m, m_en_m)}-{a_en_m}")
        data_mora_val.append(mora_valor_final[f_m])
        data_mora_pct.append(mora_porcentaje_final[f_m])

        # Lógica de color: Naranja si es el mes actual, Verde si es pasado
        if f_m.month == ahora.month and f_m.year == ahora.year:
            colores_mora.append('rgba(245, 174, 39, 0.8)')  # Naranja
        else:
            colores_mora.append('rgba(16, 185, 129, 0.6)')  # Verde

    # 3. ORDENAR Y PREPARAR COLORES
    fechas_ordenadas = sorted(datos_finales.keys())
    
    # Obtenemos el mes y año actual para comparar
    ahora = datetime.now()
    mes_actual_str = ahora.strftime('%b-%y')

    labels_recaudo = []
    data_recaudo = []
    colores_barras = []

    for f in fechas_ordenadas:
        # Generamos el nombre en inglés primero
        label_en = f.strftime('%b-%y') # Ej: 'Feb-26'
        
        # Traducimos la parte del mes
        mes_en = label_en.split('-')[0]
        anio_part = label_en.split('-')[1]
        label_es = f"{meses_es.get(mes_en, mes_en)}-{anio_part}"
        
        labels_recaudo.append(label_es)
        data_recaudo.append(datos_finales[f])
        
        # Color: Naranja si es el mes actual, Verde si es pasado
        ahora = datetime.now()
        if f.month == ahora.month and f.year == ahora.year:
            colores_barras.append('rgba(245, 174, 39, 0.8)')
        else:
            colores_barras.append('rgba(16, 185, 129, 0.6)')

    # --- GRÁFICA 3: % PARTICIPACIÓN POR MÉTODO DE PAGO ---
    metodos_fijos = ['WALLET', 'TRANSFERENCIA', 'CONSIGNACIÓN']
    
    # 1. Datos estáticos Jul-Dic 2025
    metodos_pct = {
        'WALLET': {
            datetime(2025, 7, 1): 33.16, datetime(2025, 8, 1): 28.58, datetime(2025, 9, 1): 32.39,
            datetime(2025, 10, 1): 34.31, datetime(2025, 11, 1): 38.47, datetime(2025, 12, 1): 38.07
        },
        'TRANSFERENCIA': {
            datetime(2025, 7, 1): 3.31, datetime(2025, 8, 1): 1.71, datetime(2025, 9, 1): 3.33,
            datetime(2025, 10, 1): 4.60, datetime(2025, 11, 1): 5.23, datetime(2025, 12, 1): 5.03
        },
        'CONSIGNACIÓN': {
            datetime(2025, 7, 1): 63.53, datetime(2025, 8, 1): 69.69, datetime(2025, 9, 1): 64.28,
            datetime(2025, 10, 1): 61.10, datetime(2025, 11, 1): 56.29, datetime(2025, 12, 1): 56.90
        }
    }

    # 2. Procesar archivos de pagos reales para 2026
    for ruta in archivos_a_procesar:
        try:
            if os.path.exists(ruta):
                df_met = pd.read_csv(ruta, encoding='latin1', sep=None, engine='python')
                df_met.columns = [c.upper().strip() for c in df_met.columns]
                
                col_f, col_v, col_m = 'FECHA PAGO', 'VALOR PAGADO', 'MÉTODO DE PAGO'

                if all(col in df_met.columns for col in [col_f, col_v, col_m]):
                    df_met[col_f] = pd.to_datetime(df_met[col_f], dayfirst=True, errors='coerce')
                    df_met = df_met[df_met[col_f].dt.year == 2026].copy()

                    if not df_met.empty:
                        df_met['MES_DT'] = df_met[col_f].dt.to_period('M').dt.to_timestamp()
                        df_met[col_m] = df_met[col_m].astype(str).str.upper().str.strip()
                        df_met[col_m] = df_met[col_m].str.replace('Ó', 'O').str.replace('Á', 'A')

                        total_por_mes = df_met.groupby('MES_DT')[col_v].sum()
                        agrupado = df_met.groupby(['MES_DT', col_m])[col_v].sum().reset_index()

                        for _, row in agrupado.iterrows():
                            m_fecha, m_nombre = row['MES_DT'], row[col_m]
                            nombre_final = None
                            if 'WALLET' in m_nombre: nombre_final = 'WALLET'
                            elif 'TRANSFERENCIA' in m_nombre: nombre_final = 'TRANSFERENCIA'
                            elif 'CONSIGNACION' in m_nombre or 'CONSIGNACIÓN' in m_nombre: nombre_final = 'CONSIGNACIÓN'

                            if nombre_final:
                                total_mes = total_por_mes[m_fecha]
                                if total_mes > 0:
                                    pct = round((row[col_v] / total_mes) * 100, 1)
                                    metodos_pct[nombre_final][m_fecha] = pct
        except Exception as e:
            print(f"Error detectando métodos: {e}")

    # 3. Generar Etiquetas de Tiempo (Jul 2025 a Feb 2026)
    labels_linea_obj = sorted(list(set(f for m in metodos_pct.values() for f in m.keys())))
    
    # 4. Formatear Datasets finales
    labels_linea_es = []
    for f in labels_linea_obj:
        mes_en = f.strftime('%b')
        labels_linea_es.append(f"{meses_es.get(mes_en, mes_en)}-{f.strftime('%y')}")

    datasets_linea = []
    colores_map = {'WALLET': '#3B82F6', 'TRANSFERENCIA': '#10B981', 'CONSIGNACIÓN': '#F59E0B'}

    for nombre_m in metodos_fijos:
        puntos = [float(metodos_pct[nombre_m].get(f, 0)) for f in labels_linea_obj]
        datasets_linea.append({
            'label': nombre_m,
            'data': puntos,
            'borderColor': colores_map.get(nombre_m, '#94a3b8'),
            'backgroundColor': colores_map.get(nombre_m, '#94a3b8'),
            'borderWidth': 3,
            'tension': 0.3,
            'fill': False,
            'pointRadius': 5
        })

    # --- RETURN FINAL (Sin duplicar variables) ---
    return render_template('historicos.html',
                           vista_actual='historicos',
                           labels_recaudo=labels_recaudo,
                           data_recaudo=data_recaudo,
                           colores_recaudo=colores_barras,
                           labels_mora=labels_mora,
                           data_mora_val=data_mora_val,
                           data_mora_pct=data_mora_pct,
                           colores_mora=colores_mora,
                           labels_linea_es=labels_linea_es,
                           datasets_linea=datasets_linea,
                           fecha_proyectado=obtener_fecha_archivo(RUTA_CARTERA),
                           fecha_pagos=obtener_fecha_archivo(RUTA_PAGOS))


if __name__ == '__main__':
    # Esto permite que Render asigne el puerto automáticamente
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)