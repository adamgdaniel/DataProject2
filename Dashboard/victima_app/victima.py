import streamlit as st
import pandas as pd
import pydeck as pdk
from google.cloud import firestore
import requests  # <-- Añadido para consumir la API
import time
import os
from dotenv import load_dotenv
from datetime import datetime

# 1. CARGA DE VARIABLES Y CONFIGURACIÓN
env_path = os.path.join(os.getcwd(), '.env')
load_dotenv(env_path, override=True)

st.set_page_config(page_title="🛡️ Sistema de Protección", layout="wide", initial_sidebar_state="expanded")

# --- CSS PROFESIONAL ---
st.markdown("""
    <style>
    /* Fondo general más suave */
    .stApp { background-color: #f4f7f6; }
    
    /* Tipografía y cabeceras */
    h1 { color: #1e293b !important; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; font-weight: 800; padding-bottom: 0; margin-bottom: 0; }
    .sub-header { color: #64748b; font-size: 1.1rem; font-weight: 500; margin-top: 5px; margin-bottom: 20px; }
    
    /* Animación del radar/pulso en vivo */
    .live-indicator {
        display: inline-block; width: 14px; height: 14px; background-color: #10b981; 
        border-radius: 50%; vertical-align: middle; margin-right: 8px;
        box-shadow: 0 0 8px #10b981; animation: pulse 1.5s infinite;
    }
    @keyframes pulse {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 10px rgba(16, 185, 129, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }
    
    /* Tarjetas de Estado (Status Cards) */
    .status-card { background-color: white; padding: 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-bottom: 20px; }
    .card-safe { border-left: 6px solid #10b981; }
    .card-danger { border-left: 6px solid #ef4444; background-color: #fef2f2; }
    
    /* Textos dentro de las tarjetas */
    .label-title { font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 1.2px; font-weight: 700; margin-bottom: 5px; }
    .status-title-safe { color: #059669; font-size: 1.5rem; font-weight: 800; margin-bottom: 15px; }
    .status-title-danger { color: #dc2626; font-size: 1.5rem; font-weight: 800; margin-bottom: 15px; }
    .metric-value { font-size: 3rem; font-weight: 800; color: #0f172a; line-height: 1; margin-bottom: 5px; }
    .metric-value-danger { color: #b91c1c; }
    .timestamp { font-size: 0.8rem; color: #94a3b8; margin-top: 15px; text-align: right; font-style: italic; }
    </style>
""", unsafe_allow_html=True)

# 2. CONEXIONES (API Y FIRESTORE)
API_BASE_URL = os.getenv("API_BASE_URL")

if not API_BASE_URL:
    st.error("🔒 ERROR DE SEGURIDAD: No se ha encontrado la variable API_BASE_URL en el entorno.")
    st.stop()

if 'db_fs' not in st.session_state:
    try:
        # 1. Modo Local (Tu PC): Si el archivo existe, lo usa.
        if os.path.exists("credentials.json"):
            st.session_state.db_fs = firestore.Client.from_service_account_json("credentials.json", database="firestore-database5")
        
        # 2. Modo Producción (Cloud Run): Si no existe, usa la magia automática de Google.
        else:
            st.session_state.db_fs = firestore.Client(database="firestore-database5")
            
    except Exception as e:
        st.error(f"Error Firestore: {e}")

@st.cache_data(ttl=30)
def get_api_data():
    """Obtiene los datos cruzados desde la API conectada a BigQuery."""
    try:
        # Petición a la API centralizada
        response = requests.get(f"{API_BASE_URL}/api/policia/dashboard_data")
        response.raise_for_status()
        data = response.json()

        # Extraer listas a DataFrames
        df_vic = pd.DataFrame(data.get("victimas", []))
        df_agr = pd.DataFrame(data.get("agresores", []))
        df_rel = pd.DataFrame(data.get("relaciones_agresores", []))

        if df_vic.empty or df_agr.empty or df_rel.empty:
            return pd.DataFrame([{'full_vic': 'Esperando datos...', 'full_agr': 'Esperando datos...', 'id_victima': '', 'id_agresor': ''}])

        # Cruzar los datos (Mismo JOIN que se hacía en SQL)
        df_merged = pd.merge(df_rel, df_vic[['id_victima', 'nombre_victima', 'apellido_victima']], on='id_victima', how='left')
        df_merged = pd.merge(df_merged, df_agr[['id_agresor', 'nombre_agresor', 'apellido_agresor']], on='id_agresor', how='left')

        # Crear nombres completos
        df_merged['full_vic'] = df_merged['nombre_victima'] + " " + df_merged['apellido_victima']
        df_merged['full_agr'] = df_merged['nombre_agresor'] + " " + df_merged['apellido_agresor']

        return df_merged

    except Exception as e:
        st.error(f"Error conectando a la API: {e}")
        return pd.DataFrame([{'full_vic': 'Error API', 'full_agr': 'Error API', 'id_victima': '', 'id_agresor': ''}])

# Funciones lógicas para coordenadas
def get_coord(data, field):
    c = data.get(field)
    if isinstance(c, list) and len(c) >= 2: return float(c[0]), float(c[1])
    if isinstance(c, dict) and '0' in c and '1' in c: return float(c['0']), float(c['1'])
    return 39.4699, -0.3763 

def get_coord_from_string(coord_str):
    if isinstance(coord_str, str) and ',' in coord_str:
        parts = coord_str.split(',')
        return float(parts[0].strip()), float(parts[1].strip())
    return 39.4699, -0.3763

# 3. INTERFAZ Y SIDEBAR
st.sidebar.markdown("### 🛡️ Panel de Control")
df_relaciones = get_api_data()

if df_relaciones['id_victima'].iloc[0] == '':
    st.warning("No hay datos de víctimas o relaciones activas en la API.")
    st.stop()

seleccion_vic = st.sidebar.selectbox("Identidad Protegida:", df_relaciones['full_vic'].unique())
user_data = df_relaciones[df_relaciones['full_vic'] == seleccion_vic].iloc[0]

doc_id = f"{user_data['id_victima']}_{user_data['id_agresor']}"

# Cabecera principal
st.markdown(f"<h1>Panel de Seguridad: {user_data['full_vic']}</h1>", unsafe_allow_html=True)
st.markdown(f"<div class='sub-header'><span class='live-indicator'></span>Monitorización Activa | Sujeto: <b>{user_data['full_agr']}</b> | ID: {doc_id}</div>", unsafe_allow_html=True)

col_map, col_stat = st.columns([2.5, 1.2])
placeholder_map = col_map.empty()
placeholder_info = col_stat.empty()

# Historiales
if f'rastro_v_{doc_id}' not in st.session_state: 
    st.session_state[f'rastro_v_{doc_id}'] = []
    st.session_state[f'rastro_a_{doc_id}'] = []

# 4. BUCLE PRINCIPAL (TIEMPO REAL CON FIRESTORE)
while True:
    try:
        doc = st.session_state.db_fs.collection("alertas").document(doc_id).get()
        
        if doc.exists:
            data = doc.to_dict()
            
            tipo_alerta = data.get('alerta', 'fisica') 
            a_lat, a_lon = get_coord(data, 'coordenadas_agresor')
            
            if tipo_alerta == "place":
                p_str = data.get('coordenadas_place', f"{a_lat},{a_lon}")
                t_lat, t_lon = get_coord_from_string(p_str)
                nombre_objetivo = data.get('nombre_place', 'Zona Segura')
                color_objetivo = [14, 165, 233, 255] # Azul vibrante profesional
                icono_label = "📍"
                es_lugar_fijo = True
            else:
                t_lat, t_lon = get_coord(data, 'coordenadas_victima')
                nombre_objetivo = "Posición Física Actual"
                color_objetivo = [16, 185, 129, 255] # Verde esmeralda profesional
                icono_label = "👤"
                es_lugar_fijo = False

            dist = data.get('distancia_metros', 0)
            nivel = data.get('nivel', 'NORMAL')
            
            # Doble validación de seguridad
            if dist < 500:
                nivel = "CRITICO"

            # Rastro
            if not es_lugar_fijo:
                st.session_state[f'rastro_v_{doc_id}'].append([t_lon, t_lat])
                if len(st.session_state[f'rastro_v_{doc_id}']) > 30: st.session_state[f'rastro_v_{doc_id}'].pop(0)
            
            st.session_state[f'rastro_a_{doc_id}'].append([a_lon, a_lat])
            if len(st.session_state[f'rastro_a_{doc_id}']) > 30: st.session_state[f'rastro_a_{doc_id}'].pop(0)

            mid_lon = (t_lon + a_lon) / 2
            mid_lat = (t_lat + a_lat) / 2

            # --- CAPAS PYDECK ---
            capas_mapa = [
                pdk.Layer("PathLayer", data=[{'path': st.session_state[f'rastro_a_{doc_id}']}], get_path="path", get_color=[239, 68, 68, 120], width_min_pixels=3),
                pdk.Layer("LineLayer", data=[{'start': [a_lon, a_lat], 'end': [t_lon, t_lat]}], get_source_position="start", get_target_position="end", get_color=[245, 158, 11, 255], get_width=1, width_min_pixels=4),
                pdk.Layer("TextLayer", data=[{'pos': [mid_lon, mid_lat], 'text': f"{int(dist)} m"}], get_position="pos", get_text="text", get_size=20, get_color=[15, 23, 42, 255], outline_width=3, outline_color=[255, 255, 255, 255], get_alignment_baseline="'center'", get_text_anchor="'middle'"),
                pdk.Layer("ScatterplotLayer", data=[{'lon': a_lon, 'lat': a_lat, 'name': f'Agresor: {user_data["full_agr"]}'}], get_position="[lon, lat]", get_fill_color=[239, 68, 68, 255], get_radius=1, radius_min_pixels=9, radius_max_pixels=14, stroked=True, get_line_color=[255, 255, 255, 255], line_width_min_pixels=2),
                pdk.Layer("ScatterplotLayer", data=[{'lon': t_lon, 'lat': t_lat, 'name': f'{icono_label} {nombre_objetivo}'}], get_position="[lon, lat]", get_fill_color=color_objetivo, get_radius=1, radius_min_pixels=9, radius_max_pixels=14, stroked=True, get_line_color=[255, 255, 255, 255], line_width_min_pixels=2)
            ]
            
            if not es_lugar_fijo:
                capas_mapa.insert(0, pdk.Layer("PathLayer", data=[{'path': st.session_state[f'rastro_v_{doc_id}']}], get_path="path", get_color=[16, 185, 129, 120], width_min_pixels=3))

            # --- RENDERIZAR PANEL LATERAL ---
            with placeholder_info.container():
                if nivel == "CRITICO":
                    card_class = "status-card card-danger"
                    status_title = "<div class='status-title-danger'>⚠️ ALERTA DE PROXIMIDAD</div>"
                    dist_html = f"<div class='metric-value metric-value-danger'>{int(dist)} <span style='font-size: 1.5rem;'>m</span></div>"
                    warning_msg = f"<div style='color: #b91c1c; font-weight: 600; margin-top: 10px;'>Protocolo activo: Vulneración de perímetro detectada.</div>"
                else:
                    card_class = "status-card card-safe"
                    status_title = "<div class='status-title-safe'>✅ PERÍMETRO SEGURO</div>"
                    dist_km = round(dist/1000, 2)
                    dist_html = f"<div class='metric-value'>{dist_km} <span style='font-size: 1.5rem;'>km</span></div>"
                    warning_msg = ""

                html_content = f"""<div class="{card_class}">
<div class="label-title">Estado del Sistema</div>
{status_title}
<div class="label-title" style="margin-top: 20px;">Distancia al Objetivo</div>
{dist_html}
<div class="label-title" style="margin-top: 20px;">Punto Protegido ({icono_label})</div>
<div style="font-size: 1.1rem; color: #334155; font-weight: 500;">{nombre_objetivo}</div>
{warning_msg}
<div class="timestamp">Última lectura del radar: {datetime.now().strftime('%H:%M:%S')}</div>
</div>"""

                st.markdown(html_content, unsafe_allow_html=True)

            # --- RENDERIZAR MAPA ---
            with placeholder_map.container():
                st.pydeck_chart(pdk.Deck(
                    map_style="light",
                    initial_view_state=pdk.ViewState(latitude=t_lat, longitude=t_lon, zoom=16, pitch=45, bearing=0), 
                    layers=capas_mapa,
                    tooltip={"html": "<b>{name}</b>", "style": {"backgroundColor": "steelblue", "color": "white"}}
                ))
        else:
            with placeholder_info.container():
                st.info("📡 Calibrando sistema de posicionamiento global...")
                
    except Exception as e:
        with placeholder_info.container():
            st.error(f"Error de conexión con el satélite: {e}")
            
    time.sleep(1.5)
    