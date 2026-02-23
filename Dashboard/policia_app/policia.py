import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
from google.cloud import firestore
from google.cloud import storage  # LIBRERÍA PARA EL BUCKET
import requests
import time
import os
from dotenv import load_dotenv
from datetime import datetime

# ==========================================
# 1. CARGA DE VARIABLES Y CONFIGURACIÓN
# ==========================================
env_path = os.path.join(os.getcwd(), '.env')
load_dotenv(env_path, override=True)

st.set_page_config(page_title="POLICÍA - Centro de Mando", layout="wide", page_icon="🚓")

API_BASE_URL = os.getenv("API_BASE_URL")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

if not API_BASE_URL:
    st.error("🔒 ERROR: No se ha encontrado la variable API_BASE_URL en el entorno.")
    st.stop()

# ESTILO CSS
st.markdown("""
    <style>
    .stApp { background-color: #F8F9FA; color: #212529; }
    [data-testid="stSidebar"] { background-color: #E9ECEF; border-right: 2px solid #DEE2E6; }
    h1 { color: #003366 !important; font-family: 'Segoe UI', sans-serif; text-transform: uppercase; font-weight: 800; margin: 0; padding: 0; font-size: 2.2rem; }
    div.stButton > button { background-color: #003366; color: white; border: none; font-weight: bold; width: 100%; margin-top: 10px; }
    div.stButton > button:hover { background-color: #004488; }
    .alert-feed { max-height: 600px; overflow-y: auto; padding-right: 10px; }
    .alert-card { background-color: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); border-left: 5px solid #10b981; margin-bottom: 15px; }
    .alert-card.critical { border-left-color: #ef4444; background-color: #fef2f2; }
    .dist-critical { color: #dc2626; font-weight: 800; font-size: 1.2rem; }
    .dist-safe { color: #059669; font-weight: 800; font-size: 1.2rem; }
    .global-status { padding: 15px; border-radius: 8px; background-color: #1e293b; color: white; text-align: center; margin-bottom: 20px; }
    .global-status h3 { margin: 0; font-size: 2rem; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. CONEXIONES: FIRESTORE, STORAGE Y API
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

def subir_imagen_gcs(file_buffer, ruta_destino):
    if not file_buffer or not GCS_BUCKET_NAME:
        return "vacio"
    try:
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "credentials.json"
        if os.path.exists(cred_path):
            storage_client = storage.Client.from_service_account_json(cred_path)
        else:
            storage_client = storage.Client()
            
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(ruta_destino)
        blob.upload_from_string(file_buffer.getvalue(), content_type=file_buffer.type)
        return blob.public_url
    except Exception as e:
        st.error(f"Error subiendo imagen: {e}")
        return "vacio"

@st.cache_data(ttl=15)
def obtener_datos_api():
    try:
        response = requests.get(f"{API_BASE_URL}/api/policia/dashboard_data")
        response.raise_for_status()
        data = response.json()

        df_victimas = pd.DataFrame(data.get("victimas", []))
        if not df_victimas.empty:
            df_victimas['nombre_completo'] = df_victimas['nombre_victima'] + " " + df_victimas['apellido_victima']
        
        df_agresores = pd.DataFrame(data.get("agresores", []))
        if not df_agresores.empty:
            df_agresores['nombre_completo'] = df_agresores['nombre_agresor'] + " " + df_agresores['apellido_agresor']

        df_safe_places = pd.DataFrame(data.get("safe_places", []))
        if not df_safe_places.empty:
            df_safe_places.rename(columns={'place_name': 'nombre'}, inplace=True)

        df_rel = pd.DataFrame(data.get("relaciones_agresores", []))
        df_relaciones = pd.DataFrame()
        if not df_rel.empty and not df_victimas.empty and not df_agresores.empty:
            df_relaciones = pd.merge(df_rel, df_victimas[['id_victima', 'nombre_victima', 'apellido_victima']], on='id_victima', how='left')
            df_relaciones = pd.merge(df_relaciones, df_agresores[['id_agresor', 'nombre_agresor', 'apellido_agresor']], on='id_agresor', how='left')

        return df_victimas, df_agresores, df_relaciones, df_safe_places
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

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
        max_num = df[columna].str.extract(r'(\d+)').astype(int).max()[0]
        return f"{prefijo}{int(max_num + 1):03d}"
    except: return f"{prefijo}001"

# Función mejorada para extraer coordenadas de forma segura
def get_coord_from_string(coord_data):
    if not coord_data: return None, None
    # Si viene como string separado por coma (ej: "39.46,-0.37")
    if isinstance(coord_data, str) and ',' in coord_data:
        parts = coord_data.split(',')
        try:
            return float(parts[0].strip()), float(parts[1].strip())
        except: return None, None
    # Si ya es una lista [lat, lon]
    elif isinstance(coord_data, list) and len(coord_data) >= 2:
        try:
            return float(coord_data[0]), float(coord_data[1])
        except: return None, None
    return None, None

# ==========================================
# 3. HEADER
# ==========================================
col_logo, col_titulo = st.columns([3, 9], vertical_alignment="center")
with col_logo:
    if os.path.exists("logo_policia.png"): st.image("logo_policia.png", width=450)
with col_titulo:
    st.markdown("<h1>Centro de Control Policial Integrado</h1>", unsafe_allow_html=True)
    st.markdown("<p style='font-size: 1.2rem; color: #6c757d;'><b>POLICÍA LOCAL DE VALÈNCIA</b> | UNIDAD DE PROTECCIÓN</p>", unsafe_allow_html=True)

tab_monitor, tab_gestion, tab_bbdd = st.tabs(["🗺️ MONITORIZACIÓN GPS", "📝 GESTIÓN (RMS)", "📊 BASE DE DATOS"])

# -# ------------------------------------------------------------------
# TAB 1: MONITORIZACIÓN GPS (CON SELECTOR DE ALERTA)
# ------------------------------------------------------------------
with tab_monitor:
    st.sidebar.markdown("### 📡 Radar Global")
    activar_streaming = st.sidebar.toggle("🔴 ACTIVAR RASTREO EN VIVO", value=False)
    
    # 1. Obtener la lista de IDs de alertas activas para el desplegable
    lista_alertas_activas = ["🗺️ VER TODAS LAS ALERTAS"]
    if activar_streaming:
        try:
            docs_previos = st.session_state.db_fs.collection("alertas").where("activa", "==", True).get()
            for d in docs_previos:
                lista_alertas_activas.append(d.id)
        except: pass
        
    alerta_seleccionada = st.selectbox("🎯 Focalizar en Alerta Específica", lista_alertas_activas)
    
    col_mapa, col_feed = st.columns([3, 1])
    
    if activar_streaming:
        try:
            # Si el usuario elige una en concreto...
            if alerta_seleccionada != "🗺️ VER TODAS LAS ALERTAS":
                doc_unico = st.session_state.db_fs.collection("alertas").document(alerta_seleccionada).get()
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

                # Si faltan coordenadas, avisamos pero no rompemos el mapa
                if None in (a_lat, a_lon, t_lat, t_lon):
                    feed_alertas.append({
                        'id': doc.id, 'agr': nombres['agr'], 'obj': f"{nombre_obj} (⚠️ GPS NO DISPONIBLE)", 
                        'icono': icono, 'dist': 0, 'nivel': "CRITICO"
                    })
                    alertas_criticas += 1
                    continue

                try:
                    dist = float(data.get('distancia_metros', 9999))
                except: dist = 9999
                
                nivel = "CRITICO" if data.get('activa') == True else "NORMAL"
                if nivel == "CRITICO": alertas_criticas += 1
                
                p_agresores.append({'lon': a_lon, 'lat': a_lat, 'name': f"Agresor: {nombres['agr']}"})
                p_objetivos.append({'lon': t_lon, 'lat': t_lat, 'name': f"{icono} {nombre_obj}", 'color': col})
                lineas.append({'start': [a_lon, a_lat], 'end': [t_lon, t_lat], 'color': [239, 68, 68]})
                textos.append({'pos': [(t_lon+a_lon)/2, (t_lat+a_lat)/2], 'text': f"{int(dist)}m"})
                feed_alertas.append({'id': doc.id, 'agr': nombres['agr'], 'obj': nombre_obj, 'icono': icono, 'dist': dist, 'nivel': nivel})

            # Cámara del mapa
            lat_inicial, lon_inicial, zoom_inicial = 39.4699, -0.3763, 12
            if alerta_seleccionada != "🗺️ VER TODAS LAS ALERTAS" and p_agresores:
                lat_inicial = p_agresores[0]['lat']
                lon_inicial = p_agresores[0]['lon']
                zoom_inicial = 15

            # RENDERIZAMOS MAPA
            with col_mapa:
                st.pydeck_chart(pdk.Deck(map_style="light", initial_view_state=pdk.ViewState(latitude=lat_inicial, longitude=lon_inicial, zoom=zoom_inicial),
                    layers=[pdk.Layer("LineLayer", data=lineas, get_source_position="start", get_target_position="end", get_color="color", get_width=5),
                            pdk.Layer("ScatterplotLayer", data=p_agresores, get_position="[lon, lat]", get_fill_color=[239, 68, 68], get_radius=1, radius_min_pixels=10),
                            pdk.Layer("ScatterplotLayer", data=p_objetivos, get_position="[lon, lat]", get_fill_color="color", get_radius=1, radius_min_pixels=10),
                            pdk.Layer("TextLayer", data=textos, get_position="pos", get_text="text", get_size=20, get_color=[15, 23, 42, 255])]))

            # RENDERIZAMOS PANEL LATERAL Y BOTONES
            with col_feed:
                st.markdown(f"<div class='global-status'><p>Alertas Críticas Mostradas</p><h3>{alertas_criticas}</h3></div>", unsafe_allow_html=True)
                for a in sorted(feed_alertas, key=lambda x: (x['nivel']!='CRITICO', x['dist'])):
                    cl = "alert-card critical" if a['nivel']=="CRITICO" else "alert-card"
                    st.markdown(f"<div class='{cl}'><b>{a['agr']} ({a['id']})</b><br><small>{a['icono']} {a['obj']}</small><div class='{'dist-critical' if a['nivel']=='CRITICO' else 'dist-safe'}'>{int(a['dist'])} m</div></div>", unsafe_allow_html=True)
                    
                    if st.button("🚔 RESOLVER", key=f"res_{a['id']}"):
                        st.session_state.db_fs.collection("alertas").document(a['id']).update({"activa": False})
                        st.toast("Alerta marcada como resuelta.")
                        time.sleep(0.5)
                        st.rerun()
            
            # --- LA CLAVE ESTÁ AQUÍ ---
            # En lugar del while, esperamos 2 segundos y forzamos un reinicio limpio
            time.sleep(2)
            st.rerun()

        except Exception as e:
            col_feed.error(f"Error radar: {e}")
    else:
        col_mapa.info("Active el radar en el menú lateral para iniciar la monitorización.")

# ------------------------------------------------------------------
# TAB 2: GESTIÓN (RMS) - SUBIDA DE IMÁGENES
# ------------------------------------------------------------------
with tab_gestion:
    tipo_gestion = st.selectbox("OPERACIÓN", ["Nuevo Agresor", "Nueva Víctima", "Nuevo Safe Place", "Vincular: Agresor - Víctima", "Vincular: Víctima - Safe Place"])
    
    if tipo_gestion == "Nuevo Agresor":
        with st.form("f_agr"):
            id_gen = generar_nuevo_id(df_agresores, 'id_agresor', 'agr_')
            st.info(f"🆔 Expediente: **{id_gen}**")
            n = st.text_input("Nombre")
            a = st.text_input("Apellidos")
            foto_upload = st.file_uploader("Fotografía del Agresor (Opcional)", type=["jpg", "png", "jpeg"])
            
            if st.form_submit_button("GUARDAR REGISTRO"): 
                with st.spinner("Guardando..."):
                    url_foto = subir_imagen_gcs(foto_upload, f"agresores/{id_gen}.{foto_upload.name.split('.')[-1]}") if foto_upload else "vacio"
                    resp = requests.post(f"{API_BASE_URL}/api/policia/nuevo_agresor", json={"id_agresor": id_gen, "nombre_agresor": n, "apellido_agresor": a, "url_foto_agresor": url_foto})
                    if resp.status_code == 201: st.success("Registrado."); time.sleep(1); st.rerun()

    elif tipo_gestion == "Nueva Víctima":
        with st.form("f_vic"):
            id_gen = generar_nuevo_id(df_victimas, 'id_victima', 'vic_')
            st.info(f"🆔 Expediente: **{id_gen}**")
            n = st.text_input("Nombre")
            a = st.text_input("Apellidos")
            foto_upload = st.file_uploader("Fotografía de la Víctima (Opcional)", type=["jpg", "png", "jpeg"])
            
            if st.form_submit_button("GUARDAR REGISTRO"): 
                with st.spinner("Guardando..."):
                    url_foto = subir_imagen_gcs(foto_upload, f"victimas/{id_gen}.{foto_upload.name.split('.')[-1]}") if foto_upload else "vacio"
                    resp = requests.post(f"{API_BASE_URL}/api/policia/nueva_victima", json={"id_victima": id_gen, "nombre_victima": n, "apellido_victima": a, "url_foto_victima": url_foto})
                    if resp.status_code == 201: st.success("Registrada."); time.sleep(1); st.rerun()

    elif tipo_gestion == "Nuevo Safe Place":
        with st.form("f_sp"):
            id_gen = generar_nuevo_id(df_safe_places, 'id_place', 'plc_') 
            st.info(f"🆔 Identificador de Lugar: **{id_gen}**")
            n = st.text_input("Nombre del Lugar")
            coord = st.text_input("Coordenadas (Lat, Lon)", "39.4699, -0.3763")
            rad = st.number_input("Radio de seguridad (metros)", value=500)
            if st.form_submit_button("GUARDAR"): 
                resp = requests.post(f"{API_BASE_URL}/api/policia/nuevo_safe_place", json={"id_place": id_gen, "place_coordinates": coord, "radius": rad, "place_name": n})
                if resp.status_code == 201: st.success("Lugar registrado."); time.sleep(1); st.rerun()

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
        if not df_victimas.empty: st.dataframe(df_victimas[['id_victima', 'nombre_completo', 'url_foto_victima']], hide_index=True)
    with col2:
        st.write("AGRESORES")
        if not df_agresores.empty: st.dataframe(df_agresores[['id_agresor', 'nombre_completo', 'url_foto_agresor']], hide_index=True)
    st.divider()
    st.markdown("### 🛡️ ÓRDENES DE ALEJAMIENTO ACTIVAS")
    if not df_relaciones.empty: st.dataframe(df_relaciones, use_container_width=True, hide_index=True)