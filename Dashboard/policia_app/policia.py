import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
from google.cloud import firestore
from google.cloud import storage
from google.cloud import bigquery
import requests
import time
import os
from dotenv import load_dotenv
from datetime import datetime
import altair as alt

# ==========================================
# 1. CARGA DE VARIABLES Y CONFIGURACIÓN
# ==========================================
env_path = os.path.join(os.getcwd(), '.env')
load_dotenv(env_path, override=True)

st.set_page_config(page_title="POLICÍA - Centro de Mando", layout="wide", page_icon="🚓")

API_BASE_URL = os.getenv("API_BASE_URL")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
PROJECT_ID = "data-project-streaming-487217" 
BQ_DATASET = "analitical_dataset5" 

if not API_BASE_URL:
    st.error("🔒 ERROR: No se ha encontrado la variable API_BASE_URL en el entorno.")
    st.stop()

# ==========================================
# 2. ESTILO CSS (SANEADO PARA EVITAR COLAPSOS)
# ==========================================
st.markdown("""
    <style>
    /* Suavizado de scroll */
    html, body, [data-testid="stAppViewContainer"], .main .block-container {
        scroll-behavior: smooth;
    }
    
    .stApp { background-color: #F8F9FA; color: #212529; }
    [data-testid="stSidebar"] { background-color: #E9ECEF; border-right: 2px solid #DEE2E6; }
    h1 { color: #003366 !important; font-family: 'Segoe UI', sans-serif; text-transform: uppercase; font-weight: 800; margin: 0; padding: 0; font-size: 2.2rem; }
    div.stButton > button { background-color: #003366; color: white; border: none; font-weight: bold; width: 100%; margin-top: 10px; }
    div.stButton > button:hover { background-color: #004488; }
    
    /* Tarjetas de Alerta */
    .alert-card { background-color: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); border-left: 5px solid #10b981; margin-bottom: 15px; }
    .alert-card.critical { border-left-color: #ef4444; background-color: #fef2f2; }
    .dist-critical { color: #dc2626; font-weight: 800; font-size: 1.2rem; }
    .dist-safe { color: #059669; font-weight: 800; font-size: 1.2rem; }
    .global-status { padding: 15px; border-radius: 8px; background-color: #1e293b; color: white; text-align: center; margin-bottom: 20px; }
    .global-status h3 { margin: 0; font-size: 2rem; }
    
    /* Inteligencia Predictiva */
    .intel-header { border-bottom: 2px solid #003366; padding-bottom: 5px; margin-top: 30px; margin-bottom: 15px; color: #003366; }
    .intel-desc { font-size: 1rem; color: #4b5563; margin-bottom: 15px; background-color: #e2e8f0; padding: 15px; border-left: 5px solid #3b82f6; border-radius: 4px; }
    
    /* Oculta unicamente botones que estén obsoletos para no ver "fantasmas", SIN aplastar la página */
    button[data-stale="true"] {
        display: none !important;
    }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 3. CONEXIONES: FIRESTORE, STORAGE, BQ Y API
# ==========================================
if 'db_fs' not in st.session_state:
    try:
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "credentials.json"
        if os.path.exists(cred_path):
            st.session_state.db_fs = firestore.Client.from_service_account_json(cred_path, database="firestore-database5")
        else:
            st.session_state.db_fs = firestore.Client(database="firestore-database5")
    except Exception as e:
        st.error(f"Error Firestore: {e}")
        st.stop()

@st.cache_resource
def get_bq_client():
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "credentials.json"
    if os.path.exists(cred_path):
        return bigquery.Client.from_service_account_json(cred_path, project=PROJECT_ID)
    else:
        return bigquery.Client(project=PROJECT_ID)

bq_client = get_bq_client()

@st.cache_data(ttl=300)
def query_bigquery(query_string):
    try: return bq_client.query(query_string).to_dataframe()
    except Exception as e: return pd.DataFrame()

def subir_imagen_gcs(file_buffer, ruta_destino):
    if not file_buffer or not GCS_BUCKET_NAME: return "vacio"
    try:
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "credentials.json"
        storage_client = storage.Client.from_service_account_json(cred_path) if os.path.exists(cred_path) else storage.Client()
        blob = storage_client.bucket(GCS_BUCKET_NAME).blob(ruta_destino)
        blob.upload_from_string(file_buffer.getvalue(), content_type=file_buffer.type)
        return blob.public_url
    except Exception as e: return "vacio"

@st.cache_data(ttl=15)
def obtener_datos_api():
    try:
        response = requests.get(f"{API_BASE_URL}/api/policia/dashboard_data")
        response.raise_for_status()
        data = response.json()
        df_victimas = pd.DataFrame(data.get("victimas", []))
        if not df_victimas.empty: df_victimas['nombre_completo'] = df_victimas['nombre_victima'] + " " + df_victimas['apellido_victima']
        df_agresores = pd.DataFrame(data.get("agresores", []))
        if not df_agresores.empty: df_agresores['nombre_completo'] = df_agresores['nombre_agresor'] + " " + df_agresores['apellido_agresor']
        df_safe_places = pd.DataFrame(data.get("safe_places", []))
        if not df_safe_places.empty: df_safe_places.rename(columns={'place_name': 'nombre'}, inplace=True)
        df_rel = pd.DataFrame(data.get("relaciones_agresores", []))
        df_relaciones = pd.DataFrame()
        if not df_rel.empty and not df_victimas.empty and not df_agresores.empty:
            df_relaciones = pd.merge(df_rel, df_victimas[['id_victima', 'nombre_victima', 'apellido_victima']], on='id_victima', how='left')
            df_relaciones = pd.merge(df_relaciones, df_agresores[['id_agresor', 'nombre_agresor', 'apellido_agresor']], on='id_agresor', how='left')
        return df_victimas, df_agresores, df_relaciones, df_safe_places
    except: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

df_victimas, df_agresores, df_relaciones, df_safe_places = obtener_datos_api()

sql_lookup = {}
if isinstance(df_relaciones, pd.DataFrame) and not df_relaciones.empty:
    for _, row in df_relaciones.iterrows():
        key = f"{row.get('id_victima')}_{row.get('id_agresor')}"
        sql_lookup[key] = {
            'vic': f"{row.get('nombre_victima')} {row.get('apellido_victima')}",
            'agr': f"{row.get('nombre_agresor')} {row.get('apellido_agresor')}"
        }
place_lookup = {row['id_place']: row['nombre'] for _, row in df_safe_places.iterrows()} if not df_safe_places.empty else {}

def generar_nuevo_id(df, columna, prefijo):
    if df.empty or columna not in df.columns: return f"{prefijo}001"
    try:
        max_num = int(df[columna].str.extract(r'(\d+)').astype(int).max()[0])
        return f"{prefijo}{max_num + 1:03d}"
    except: return f"{prefijo}001"

def get_coord_from_string(coord_data):
    if not coord_data: return None, None
    if isinstance(coord_data, str) and ',' in coord_data:
        try: return float(coord_data.split(',')[0].strip()), float(coord_data.split(',')[1].strip())
        except: return None, None
    elif isinstance(coord_data, list) and len(coord_data) >= 2:
        try: return float(coord_data[0]), float(coord_data[1])
        except: return None, None
    return None, None

# ==========================================
# 4. HEADER Y TABS
# ==========================================
col_logo, col_titulo = st.columns([3, 9], vertical_alignment="center")
with col_logo:
    if os.path.exists("logo_policia.png"): st.image("logo_policia.png", width=450)
with col_titulo:
    st.markdown("<h1>Centro de Control Policial Integrado</h1>", unsafe_allow_html=True)
    st.markdown("<p style='font-size: 1.2rem; color: #6c757d;'><b>POLICÍA LOCAL DE VALÈNCIA</b> | UNIDAD DE PROTECCIÓN</p>", unsafe_allow_html=True)

tab_monitor, tab_gestion, tab_bbdd, tab_intel = st.tabs(["🗺️ MONITORIZACIÓN GPS", "📝 GESTIÓN (RMS)", "📊 BASE DE DATOS", "🧠 INTELIGENCIA PREDICTIVA"])

# ------------------------------------------------------------------
# TAB 1: MONITORIZACIÓN GPS
# ------------------------------------------------------------------
with tab_monitor:
    st.sidebar.markdown("### 📡 Radar Global")
    activar_streaming = st.sidebar.toggle("🔴 ACTIVAR RASTREO EN VIVO", value=False)
    
    lista_alertas_activas = ["🗺️ VER TODAS LAS ALERTAS"]
    if activar_streaming:
        try:
            docs_previos = st.session_state.db_fs.collection("alertas").where("activa", "==", True).get()
            for d in docs_previos: lista_alertas_activas.append(d.id)
        except: pass
        
    alerta_seleccionada = st.selectbox("🎯 Focalizar en Alerta Específica", lista_alertas_activas)

    if 'ultimo_foco' not in st.session_state: st.session_state.ultimo_foco = None
    if 'map_view' not in st.session_state: st.session_state.map_view = pdk.ViewState(latitude=39.4699, longitude=-0.3763, zoom=12)

    if st.session_state.ultimo_foco != alerta_seleccionada:
        st.session_state.ultimo_foco = alerta_seleccionada
        st.session_state.actualizar_camara = True
    else:
        st.session_state.actualizar_camara = False

    @st.fragment(run_every=2 if activar_streaming else None)
    def renderizar_radar_tiempo_real(alerta_foco):
        col_mapa, col_feed = st.columns([3, 1])
        
        if activar_streaming:
            try:
                if alerta_foco != "🗺️ VER TODAS LAS ALERTAS":
                    doc_unico = st.session_state.db_fs.collection("alertas").document(alerta_foco).get()
                    docs = [doc_unico] if doc_unico.exists and doc_unico.to_dict().get('activa') == True else []
                else:
                    docs = st.session_state.db_fs.collection("alertas").where("activa", "==", True).stream()
                    
                p_agresores, p_objetivos, lineas, textos, feed_alertas = [], [], [], [], []
                alertas_criticas = 0
                
                for doc in docs:
                    data = doc.to_dict()
                    nombres = sql_lookup.get(doc.id, {'vic': 'Desconocido', 'agr': 'Desconocido'})
                    a_lat, a_lon = get_coord_from_string(data.get('coordenadas_agresor'))
                    
                    if data.get('alerta') == "place":
                        t_lat, t_lon = get_coord_from_string(data.get('coordenadas_place'))
                        nombre_obj = place_lookup.get(data.get('id_place'), data.get('nombre_place', 'Zona Segura'))
                        icono, col = "📍", [14, 165, 233, 255]
                    else:
                        t_lat, t_lon = get_coord_from_string(data.get('coordenadas_victima'))
                        nombre_obj, icono, col = nombres['vic'], "👤", [16, 185, 129, 255]

                    if None in (a_lat, a_lon, t_lat, t_lon):
                        feed_alertas.append({'id': doc.id, 'agr': nombres['agr'], 'obj': f"{nombre_obj} (⚠️ GPS NO DISPONIBLE)", 'icono': icono, 'dist': 0, 'nivel': "CRITICO"})
                        alertas_criticas += 1
                        continue

                    try: dist = float(data.get('distancia_metros', 9999))
                    except: dist = 9999
                    
                    nivel = "CRITICO" if data.get('activa') == True else "NORMAL"
                    if nivel == "CRITICO": alertas_criticas += 1
                    
                    p_agresores.append({'lon': a_lon, 'lat': a_lat, 'name': f"Agresor: {nombres['agr']}"})
                    p_objetivos.append({'lon': t_lon, 'lat': t_lat, 'name': f"{icono} {nombre_obj}", 'color': col})
                    lineas.append({'start': [a_lon, a_lat], 'end': [t_lon, t_lat], 'color': [239, 68, 68]})
                    textos.append({'pos': [(t_lon+a_lon)/2, (t_lat+a_lat)/2], 'text': f"{int(dist)}m"})
                    feed_alertas.append({'id': doc.id, 'agr': nombres['agr'], 'obj': nombre_obj, 'icono': icono, 'dist': dist, 'nivel': nivel})

                if st.session_state.actualizar_camara:
                    lat_inicial, lon_inicial, zoom_inicial = 39.4699, -0.3763, 12
                    if alerta_foco != "🗺️ VER TODAS LAS ALERTAS" and p_agresores:
                        lat_inicial, lon_inicial, zoom_inicial = p_agresores[0]['lat'], p_agresores[0]['lon'], 15
                    
                    st.session_state.map_view = pdk.ViewState(latitude=lat_inicial, longitude=lon_inicial, zoom=zoom_inicial)
                    st.session_state.actualizar_camara = False

                with col_mapa:
                    # 🔥 LA CLAVE ANTI-PARPADEO 2.0: El mapa vive dentro de una caja rígida de 600px.
                    # Aunque se esté recargando por dentro, la altura total no cambia, por lo que el scroll NO salta.
                    with st.container(height=600, border=False):
                        st.pydeck_chart(pdk.Deck(
                            map_style="light", 
                            initial_view_state=st.session_state.map_view,
                            layers=[
                                pdk.Layer("LineLayer", data=lineas, get_source_position="start", get_target_position="end", get_color="color", get_width=5),
                                pdk.Layer("ScatterplotLayer", data=p_agresores, get_position="[lon, lat]", get_fill_color=[239, 68, 68], get_radius=1, radius_min_pixels=10),
                                pdk.Layer("ScatterplotLayer", data=p_objetivos, get_position="[lon, lat]", get_fill_color="color", get_radius=1, radius_min_pixels=10),
                                pdk.Layer("TextLayer", data=textos, get_position="pos", get_text="text", get_size=20, get_color=[15, 23, 42, 255])
                            ]
                        ))

                with col_feed:
                    with st.container(height=600, border=False):
                        st.markdown(f"<div class='global-status'><p>Alertas Críticas Mostradas</p><h3>{alertas_criticas}</h3></div>", unsafe_allow_html=True)
                        
                        for a in sorted(feed_alertas, key=lambda x: (x['nivel']!='CRITICO', x['dist'])):
                            cl = "alert-card critical" if a['nivel']=="CRITICO" else "alert-card"
                            st.markdown(f"<div class='{cl}'><b>{a['agr']} ({a['id']})</b><br><small>{a['icono']} {a['obj']}</small><div class='{'dist-critical' if a['nivel']=='CRITICO' else 'dist-safe'}'>{int(a['dist'])} m</div></div>", unsafe_allow_html=True)
                            
                            if a['nivel'] == "CRITICO":
                                if st.button("🚔 RESOLVER", key=f"res_{a['id']}"):
                                    st.session_state.db_fs.collection("alertas").document(a['id']).update({"activa": False})
                                    st.toast("Alerta marcada como resuelta.")
                                    st.rerun() 
                            
            except Exception as e: col_feed.error(f"Error radar: {e}")
        else:
            col_mapa.info("Active el radar en el menú lateral para iniciar la monitorización.")

    renderizar_radar_tiempo_real(alerta_seleccionada)

# ------------------------------------------------------------------
# TAB 2: GESTIÓN (RMS) - CREACIÓN Y EDICIÓN
# ------------------------------------------------------------------
with tab_gestion:
    opciones_gestion = ["Nuevo Agresor", "Editar Agresor", "Nueva Víctima", "Editar Víctima", "Nuevo Safe Place", "Editar Safe Place", "Vincular: Agresor - Víctima", "Vincular: Víctima - Safe Place"]
    tipo_gestion = st.selectbox("OPERACIÓN", opciones_gestion)
    
    if tipo_gestion == "Nuevo Agresor":
        with st.form("f_agr"):
            id_gen = generar_nuevo_id(df_agresores, 'id_agresor', 'agr_')
            n, a = st.text_input("Nombre"), st.text_input("Apellidos")
            foto_upload = st.file_uploader("Fotografía del Agresor", type=["jpg", "png", "jpeg"])
            if st.form_submit_button("GUARDAR REGISTRO"): 
                url_foto = subir_imagen_gcs(foto_upload, f"agresores/{id_gen}.{foto_upload.name.split('.')[-1]}") if foto_upload else "vacio"
                resp = requests.post(f"{API_BASE_URL}/api/policia/nuevo_agresor", json={"id_agresor": id_gen, "nombre_agresor": n, "apellido_agresor": a, "url_foto_agresor": url_foto})
                if resp.status_code == 201: st.success("Registrado."); time.sleep(1); st.rerun()
                
    elif tipo_gestion == "Nueva Víctima":
        with st.form("f_vic"):
            id_gen = generar_nuevo_id(df_victimas, 'id_victima', 'vic_')
            n, a = st.text_input("Nombre"), st.text_input("Apellidos")
            foto_upload = st.file_uploader("Fotografía de la Víctima", type=["jpg", "png", "jpeg"])
            if st.form_submit_button("GUARDAR REGISTRO"): 
                url_foto = subir_imagen_gcs(foto_upload, f"victimas/{id_gen}.{foto_upload.name.split('.')[-1]}") if foto_upload else "vacio"
                resp = requests.post(f"{API_BASE_URL}/api/policia/nueva_victima", json={"id_victima": id_gen, "nombre_victima": n, "apellido_victima": a, "url_foto_victima": url_foto})
                if resp.status_code == 201: st.success("Registrada."); time.sleep(1); st.rerun()
                
    elif tipo_gestion == "Nuevo Safe Place":
        with st.form("f_sp"):
            id_gen = generar_nuevo_id(df_safe_places, 'id_place', 'plc_') 
            n, coord, rad = st.text_input("Nombre del Lugar"), st.text_input("Coordenadas (Lat, Lon)", "39.4699, -0.3763"), st.number_input("Radio de seguridad (metros)", value=500)
            if st.form_submit_button("GUARDAR"): 
                resp = requests.post(f"{API_BASE_URL}/api/policia/nuevo_safe_place", json={"id_place": id_gen, "place_coordinates": coord, "radius": rad, "place_name": n})
                if resp.status_code == 201: st.success("Lugar registrado."); time.sleep(1); st.rerun()
                
    elif tipo_gestion == "Editar Agresor":
        if not df_agresores.empty:
            a_sel = st.selectbox("Selecciona Agresor", df_agresores['id_agresor'] + " - " + df_agresores['nombre_completo'])
            id_edit = a_sel.split(" - ")[0]
            datos_edit = df_agresores[df_agresores['id_agresor'] == id_edit].iloc[0]
            with st.form("f_edit_agr"):
                n, a = st.text_input("Nombre", value=datos_edit['nombre_agresor']), st.text_input("Apellidos", value=datos_edit['apellido_agresor'])
                foto_upload = st.file_uploader("Nueva Fotografía", type=["jpg", "png", "jpeg"])
                if st.form_submit_button("ACTUALIZAR REGISTRO"):
                    url_foto = subir_imagen_gcs(foto_upload, f"agresores/{id_edit}.{foto_upload.name.split('.')[-1]}") if foto_upload else datos_edit['url_foto_agresor']
                    resp = requests.put(f"{API_BASE_URL}/api/policia/editar_agresor", json={"id_agresor": id_edit, "nombre_agresor": n, "apellido_agresor": a, "url_foto_agresor": url_foto})
                    if resp.status_code == 200: st.success("Actualizado."); time.sleep(1); st.rerun()
                    
    elif tipo_gestion == "Editar Víctima":
        if not df_victimas.empty:
            v_sel = st.selectbox("Selecciona Víctima", df_victimas['id_victima'] + " - " + df_victimas['nombre_completo'])
            id_edit = v_sel.split(" - ")[0]
            datos_edit = df_victimas[df_victimas['id_victima'] == id_edit].iloc[0]
            with st.form("f_edit_vic"):
                n, a = st.text_input("Nombre", value=datos_edit['nombre_victima']), st.text_input("Apellidos", value=datos_edit['apellido_victima'])
                foto_upload = st.file_uploader("Nueva Fotografía", type=["jpg", "png", "jpeg"])
                if st.form_submit_button("ACTUALIZAR REGISTRO"):
                    url_foto = subir_imagen_gcs(foto_upload, f"victimas/{id_edit}.{foto_upload.name.split('.')[-1]}") if foto_upload else datos_edit['url_foto_victima']
                    resp = requests.put(f"{API_BASE_URL}/api/policia/editar_victima", json={"id_victima": id_edit, "nombre_victima": n, "apellido_victima": a, "url_foto_victima": url_foto})
                    if resp.status_code == 200: st.success("Actualizada."); time.sleep(1); st.rerun()
                    
    elif tipo_gestion == "Editar Safe Place":
        if not df_safe_places.empty:
            sp_sel = st.selectbox("Selecciona Safe Place", df_safe_places['id_place'] + " - " + df_safe_places['nombre'])
            id_edit = sp_sel.split(" - ")[0]
            datos_edit = df_safe_places[df_safe_places['id_place'] == id_edit].iloc[0]
            with st.form("f_edit_sp"):
                n, coord, rad = st.text_input("Nombre del Lugar", value=datos_edit['nombre']), st.text_input("Coordenadas", value=datos_edit['place_coordinates']), st.number_input("Radio de seguridad", value=int(datos_edit['radius']))
                if st.form_submit_button("ACTUALIZAR REGISTRO"):
                    resp = requests.put(f"{API_BASE_URL}/api/policia/editar_safe_place", json={"id_place": id_edit, "place_coordinates": coord, "radius": rad, "place_name": n})
                    if resp.status_code == 200: st.success("Actualizado."); time.sleep(1); st.rerun()
                    
    elif tipo_gestion == "Vincular: Agresor - Víctima":
        with st.form("f_vinc_agr"):
            if not df_agresores.empty and not df_victimas.empty:
                a_sel = st.selectbox("Agresor", df_agresores['id_agresor'] + " - " + df_agresores['nombre_completo'])
                v_sel = st.selectbox("Víctima a proteger", df_victimas['id_victima'] + " - " + df_victimas['nombre_completo'])
                dist = st.number_input("Distancia de seguridad mínima (m)", value=500)
                if st.form_submit_button("VINCULAR ORDEN"):
                    resp = requests.post(f"{API_BASE_URL}/api/policia/relacion_victima_agresor", json={"id_agresor": a_sel.split(" - ")[0], "id_victima": v_sel.split(" - ")[0], "dist_seguridad": dist})
                    if resp.status_code == 201: st.success("Orden registrada."); time.sleep(1); st.rerun()
                    
    elif tipo_gestion == "Vincular: Víctima - Safe Place":
        with st.form("f_vinc_sp"):
            if not df_victimas.empty and not df_safe_places.empty:
                v_sel = st.selectbox("Víctima Protegida", df_victimas['id_victima'] + " - " + df_victimas['nombre_completo'])
                s_sel = st.selectbox("Lugar Seguro", df_safe_places['id_place'] + " - " + df_safe_places['nombre'])
                if st.form_submit_button("ESTABLECER PERÍMETRO"):
                    resp = requests.post(f"{API_BASE_URL}/api/policia/relacion_victima_safe_place", json={"id_victima": v_sel.split(" - ")[0], "id_place": s_sel.split(" - ")[0]})
                    if resp.status_code == 201: st.success("Perímetro asignado."); time.sleep(1); st.rerun()

# ------------------------------------------------------------------
# TAB 3: BASE DE DATOS
# ------------------------------------------------------------------
with tab_bbdd:
    st.markdown("### 📊 DIRECTORIO DE VÍCTIMAS Y AGRESORES")
    col1, col2 = st.columns(2)
    with col1:
        st.write("VÍCTIMAS")
        if not df_victimas.empty: st.dataframe(df_victimas[['id_victima', 'nombre_completo']], hide_index=True)
    with col2:
        st.write("AGRESORES")
        if not df_agresores.empty: st.dataframe(df_agresores[['id_agresor', 'nombre_completo']], hide_index=True)

# ------------------------------------------------------------------
# TAB 4: INTELIGENCIA PREDICTIVA (BIGQUERY) CON ALTAIR
# ------------------------------------------------------------------
with tab_intel:
    st.markdown("### 🧠 Central de Inteligencia y Predicción de Amenazas")
    st.write("Análisis algorítmico de telemetría GPS conectada directamente al Data Warehouse policial.")

    def buscar_col(df, keywords, tipo=None):
        cols = df.select_dtypes(include=[tipo]).columns if tipo else df.columns
        for c in cols:
            if any(kw in c.lower() for kw in keywords): return c
        return cols[0] if len(cols) > 0 else None

    @st.fragment(run_every=300)
    def renderizar_inteligencia():
        
        # =========================================================
        # MÓDULO: EL RADAR DE MERODEO (Tiempo de Asedio)
        # =========================================================
        st.markdown("<h4 class='intel-header'>Medidor de Intimidación (Tiempo de Asedio / Lurking)</h4>", unsafe_allow_html=True)
        df_asedio = query_bigquery(f"SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.marts_tiempo_asedio` LIMIT 50")
        
        if not df_asedio.empty:
            col_minutos = buscar_col(df_asedio, ['minut', 'tiemp', 'durac', 'asedio'], 'number')
            col_agresor = buscar_col(df_asedio, ['agr', 'id', 'nom'], 'object')
            
            if col_minutos:
                max_min = int(df_asedio[col_minutos].max())
                color_kpi = "normal" if max_min < 15 else "inverse"
                st.metric(label="🚨 ASEDIO MÁXIMO ACTIVO", value=f"{max_min} Minutos", delta="Nivel de Alarma Crítico" if max_min >= 30 else "Merodeo Detectado", delta_color=color_kpi)
                
                if col_agresor:
                    chart = alt.Chart(df_asedio).mark_bar(color='#b91c1c').encode(
                        x=alt.X(f"{col_agresor}:N", title="Sujeto / Ubicación", sort='-y', axis=alt.Axis(labelAngle=0)),
                        y=alt.Y(f"{col_minutos}:Q", title="Duración (Minutos)"),
                        tooltip=[col_agresor, col_minutos]
                    ).properties(height=300)
                    st.altair_chart(chart, use_container_width=True)
        else: st.info("Recopilando telemetría de merodeo...")

        # =========================================================
        # MÓDULO: LA PLANIFICACIÓN TÁCTICA (Patrones de Interceptación)
        # =========================================================
        st.markdown("<h4 class='intel-header'>Predictor de Rutinas (Patrones de Interceptación)</h4>", unsafe_allow_html=True)
        df_patrones = query_bigquery(f"SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.marts_patrones_interceptacion` LIMIT 1000")
        
        if not df_patrones.empty:
            num_cols = df_patrones.select_dtypes(include=['number']).columns.tolist()
            col_hora = None
            col_val = None
            
            for col in num_cols:
                if df_patrones[col].max() <= 24 and df_patrones[col].max() > 0:
                    col_hora = col
                    break
            
            if not col_hora:
                col_hora = buscar_col(df_patrones, ['hora', 'hour', 'franja'])
                
            if num_cols:
                candidatos_val = [c for c in num_cols if c != col_hora]
                if candidatos_val:
                    col_val = candidatos_val[0]
            
            if not col_val:
                col_val = buscar_col(df_patrones, ['alert', 'num', 'count', 'frec', 'total', 'cantidad'], 'number')
            
            if col_hora and col_val:
                df_horas = df_patrones.groupby(col_hora, as_index=False)[col_val].sum()
                
                bar_chart_horas = alt.Chart(df_horas).mark_bar(color='#dc2626').encode(
                    x=alt.X(f"{col_hora}:O", title='Hora del Día (00:00 - 23:00)', axis=alt.Axis(labelAngle=0)),
                    y=alt.Y(f"{col_val}:Q", title='Total de Alertas Detectadas', scale=alt.Scale(zero=True)),
                    tooltip=[alt.Tooltip(f"{col_hora}:O", title="Hora"), alt.Tooltip(f"{col_val}:Q", title="Total Alertas")]
                ).properties(height=300)
                
                st.altair_chart(bar_chart_horas, use_container_width=True)
            else: st.bar_chart(df_patrones) 
        else: st.info("Calculando clústeres horarios...")

        # =========================================================
        # MÓDULO: EL INFORME JUDICIAL (Acecho Premeditado)
        # =========================================================
        st.markdown("<h4 class='intel-header'>Demostrador de Intenciones (Acecho Premeditado)</h4>", unsafe_allow_html=True)
        df_acecho = query_bigquery(f"SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.marts_acecho_premeditado` LIMIT 50")
        
        if not df_acecho.empty:
            num_cols = df_acecho.select_dtypes(include=['number']).columns.tolist()
            cat_cols = df_acecho.select_dtypes(include=['object', 'string']).columns.tolist()
            
            if len(num_cols) >= 2 and cat_cols:
                df_melted = df_acecho.melt(id_vars=cat_cols[0], value_vars=num_cols[:2], var_name='Tipo de Alerta', value_name='Cantidad')
                bar_100 = alt.Chart(df_melted).mark_bar().encode(
                    x=alt.X('sum(Cantidad):Q', stack='normalize', title='Porcentaje (%)', axis=alt.Axis(format='%')),
                    y=alt.Y(f"{cat_cols[0]}:N", title='Agresor'),
                    color=alt.Color('Tipo de Alerta:N', scale=alt.Scale(range=['#d1d5db', '#ef4444'])), 
                    tooltip=[cat_cols[0], 'Tipo de Alerta', 'Cantidad']
                ).properties(height=250)
                st.altair_chart(bar_100, use_container_width=True)
                
                col_bool = df_acecho.select_dtypes(include=['bool']).columns
                if len(col_bool) > 0:
                    st.markdown("**📋 Top Obsesiones (Acecho Confirmado)**")
                    st.dataframe(df_acecho[df_acecho[col_bool[0]] == True], use_container_width=True, hide_index=True)
            else: st.bar_chart(df_acecho)
        else: st.info("Buscando evidencias de targeting...")

        # =========================================================
        # MÓDULO: LA ALERTA TEMPRANA (Escalada de Riesgo)
        # =========================================================
        st.markdown("<h4 class='intel-header'>Detector de Tendencias (Escalada de Riesgo)</h4>", unsafe_allow_html=True)
        df_escalada = query_bigquery(f"SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.marts_escalada_riesgo` LIMIT 50")
        
        if not df_escalada.empty:
            col_tiempo = buscar_col(df_escalada, ['fech', 'date', 'dia', 'sem'])
            col_frec = buscar_col(df_escalada, ['frec', 'cant', 'num', 'alert'], 'number')
            col_dist = buscar_col(df_escalada, ['dist', 'med', 'prox'], 'number')
            
            if col_tiempo and col_frec and col_dist and col_frec != col_dist:
                base = alt.Chart(df_escalada).encode(x=alt.X(f"{col_tiempo}:N", title='Evolución Temporal', axis=alt.Axis(labelAngle=-45)))
                barras = base.mark_bar(opacity=0.6, color='#3b82f6', width=20).encode(y=alt.Y(f"{col_frec}:Q", title='Frecuencia de Alertas', scale=alt.Scale(zero=True)))
                linea = base.mark_line(color='#ef4444', strokeWidth=3, point=True).encode(y=alt.Y(f"{col_dist}:Q", title='Distancia Media'))
                st.altair_chart(alt.layer(barras, linea).resolve_scale(y='independent').properties(height=350), use_container_width=True)
            else: st.line_chart(df_escalada)
        else: st.info("Analizando desviaciones estándar de comportamiento...")

    renderizar_inteligencia()