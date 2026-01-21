import os
import glob
import pandas as pd
import calendar
from flask import Flask, render_template, request
import datetime as dt
from datetime import datetime


from procesador_maestro import procesar_todo  # Esto importa tu lógica de las 11 columnas

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
    vista = request.args.get('vista', 'cyres')
    ciudad = request.args.get('ciudad', 'Todas')

    # --- FECHAS GENERALES ---
    ahora = datetime.now()
    mes_actual = ahora.month
    anio_actual = ahora.year

    # --- LLAMADA A LAS FECHAS ---
    fecha_act_cartera = obtener_fecha_archivo(RUTA_CARTERA)
    fecha_act_pagos = obtener_fecha_archivo(RUTA_PAGOS)
    
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
    detalle_clientes_grafica = {} # Inicializamos el diccionario de clientes
    detalle_presupuesto_grafica = {}

    if vista == 'detalle_analisis':
        try:
            
            folder_data = r'C:\Dashboard\data'
            ahora = datetime.now()
            mes_actual = datetime.now().month
            anio_actual = datetime.now().year
            ultimo_dia = calendar.monthrange(anio_actual, mes_actual)[1]

            # A.1. Leer Presupuesto (CORREGIDO)
            path_proyectado = os.path.join(folder_data, 'Proyectadoconsolidado.csv')
            df_filtrado = pd.DataFrame()
            if os.path.exists(path_proyectado):
                df_proy = pd.read_csv(path_proyectado, sep=';', encoding='latin1')
                df_proy.columns = df_proy.columns.str.strip()
                
                # 1. Intentamos leer la fecha sin forzar formato (Pandas detecta YYYY-MM-DD del CSV)
                df_proy['Fecha_Vencimiento'] = pd.to_datetime(df_proy['Fecha_Vencimiento'], errors='coerce')
                
                # 2. Si falló (NaT), intentamos el formato con puntos que viene del Maestro
                if df_proy['Fecha_Vencimiento'].isna().all():
                    df_proy['Fecha_Vencimiento'] = pd.to_datetime(df_proy['Fecha_Vencimiento'], format='%d.%m.%Y', errors='coerce')
                
                # 3. FILTRO: Mes actual, Año actual Y que esté PENDIENTE
                # Nota: Asegúrate de que anio_actual coincida con el de tus archivos (2026)
                filtro = (df_proy['Fecha_Vencimiento'].dt.month == mes_actual) & \
                         (df_proy['Fecha_Vencimiento'].dt.year == anio_actual)
                
                df_filtrado = df_proy[filtro].copy()

                def limpiar_monto(serie):
                    return pd.to_numeric(serie.astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0)
                
                # Cálculo del KPI Presupuesto Total del Mes
                kpis_calculados['presupuesto'] = pd.to_numeric(df_filtrado['TOTAL CARTERA'], errors='coerce').fillna(0).sum()

                if df_filtrado.empty:
                    print(f"ADVERTENCIA: No hay datos PENDIENTES para {mes_actual}/{anio_actual}")

            # A.2. Leer Ingresos
            path_pagos = os.path.join(folder_data, 'PagosConsolidado.csv')
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

                    # Cálculos "Actual" (Corte ayer)
                    dia_hoy = datetime.now().day
                    dia_ayer = dia_hoy - 1 if dia_hoy > 1 else 1

                    filtro_ayer = df_filtrado['Fecha_Vencimiento'].dt.day <= dia_ayer
                    presupuesto_ayer = pd.to_numeric(df_filtrado[filtro_ayer]['TOTAL CARTERA'], errors='coerce').fillna(0).sum()
                    kpis_calculados['presupuesto_actual'] = presupuesto_ayer

                    # Reemplazo para asegurar que la caja de abajo siempre se actualice con el total
                    filtro_hoy = df_filtrado['Fecha_Vencimiento'].dt.day <= dia_hoy
                    kpis_calculados['ingresos_actual'] = kpis_calculados['ingresos']
                    kpis_calculados['desviacion_actual'] = kpis_calculados['ingresos'] - kpis_calculados['presupuesto_actual']
                    kpis_calculados['ejecucion_actual'] = (kpis_calculados['ingresos'] / kpis_calculados['presupuesto_actual'] * 100) if kpis_calculados['presupuesto_actual'] > 0 else 0


                    # --- CONSOLIDACIÓN POR CLIENTE PARA LA TABLA ---
                    if not df_filtrado.empty:
                        col_cod = 'COD. CLIENTE'
                        col_razon = next((c for c in df_filtrado.columns if 'RAZON' in c.upper() or 'SOCIAL' in c.upper() or 'CLIENTE' in c.upper() and c != col_cod), df_filtrado.columns[1])

                        df_cli_proy = df_filtrado.groupby([col_cod, col_razon]).agg(Presupuesto_Mensual=('TOTAL CARTERA', 'sum')).reset_index()
                        df_cli_proy.columns = ['COD_CLIENTE', 'RAZON_SOCIAL', 'Presupuesto_Mensual']

                        df_ayer_cli = df_filtrado[df_filtrado['Fecha_Vencimiento'].dt.day <= dia_ayer]
                        df_cli_proy_actual = df_ayer_cli.groupby(col_cod)['TOTAL CARTERA'].sum().reset_index() if not df_ayer_cli.empty else pd.DataFrame(columns=[col_cod, 'Presupuesto_Actual'])
                        df_cli_proy_actual.columns = ['COD_CLIENTE', 'Presupuesto_Actual']

                        df_cli_pagos = df_pagos.groupby(col_cod)['VALOR PAGADO'].sum().reset_index() if col_cod in df_pagos.columns else pd.DataFrame(columns=[col_cod, 'Ingresos_Recibidos'])
                        df_cli_pagos.columns = ['COD_CLIENTE', 'Ingresos_Recibidos']

                        tabla_clientes = pd.merge(df_cli_proy, df_cli_proy_actual, on='COD_CLIENTE', how='left')
                        tabla_clientes = pd.merge(tabla_clientes, df_cli_pagos, on='COD_CLIENTE', how='left').fillna(0)

                        tabla_clientes['Desviacion'] = tabla_clientes['Ingresos_Recibidos'] - tabla_clientes['Presupuesto_Actual']
                        tabla_clientes['Efe_Actual'] = (tabla_clientes['Ingresos_Recibidos'] / tabla_clientes['Presupuesto_Actual'] * 100).replace([float('inf')], 0).fillna(0)
                        tabla_clientes['Efe_Mensual'] = (tabla_clientes['Ingresos_Recibidos'] / tabla_clientes['Presupuesto_Mensual'] * 100).replace([float('inf')], 0).fillna(0)

                        # --- AGREGADO: ORDENAR POR PRESUPUESTO MENSUAL MAYOR A MENOR ---
                        tabla_clientes = tabla_clientes.sort_values(by='Presupuesto_Mensual', ascending=False)

                        operaciones_tabla = tabla_clientes.to_dict(orient='records')

            kpis_calculados['desviacion'] = kpis_calculados['ingresos'] - kpis_calculados['presupuesto']
            if kpis_calculados['presupuesto'] > 0:
                kpis_calculados['efectividad'] = (kpis_calculados['ingresos'] / kpis_calculados['presupuesto']) * 100
        
            if os.path.exists(path_proyectado):
                df_proy = pd.read_csv(path_proyectado, sep=';', encoding='latin1')
                print(f"DEBUG: Columnas encontradas: {df_proy.columns.tolist()}")
                # ... resto del código ...
                df_filtrado = df_proy[filtro]
                print(f"DEBUG: Filas encontradas para el mes {mes_actual}: {len(df_filtrado)}")
        
        except Exception as e:
            print(f"Error en detalle: {e}")

        # FUERZA BRUTA: Justo antes de enviar a la página, igualamos
        kpis_calculados['ingresos_actual'] = kpis_calculados['ingresos']
        print(">>> SI VES ESTO, EL CODIGO SI SE ACTUALIZO <<<")

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
                               fecha_pagos=fecha_act_pagos)
    

    datos = procesar_informacion(vista, ciudad)
    if datos is None: return "<h1>Error</h1>"

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
    RUTA_PROY = r'C:\Dashboard\data\proyectados'
    RUTA_PAGOS = r'C:\Dashboard\data\pagos_diarios'
    
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
        ruta_p = r'C:\Dashboard\data\proyectados'
        ruta_pg = r'C:\Dashboard\data\pagos_diarios'
        
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


if __name__ == '__main__':
    # Esto permite que Render asigne el puerto automáticamente
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)