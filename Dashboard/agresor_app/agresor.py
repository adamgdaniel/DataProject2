import streamlit as st
import time
import streamlit.components.v1 as components
from datetime import datetime
import textwrap
import pandas as pd
from google.cloud import firestore
import requests
import os
from dotenv import load_dotenv

# ==========================================
# 1. CARGA DE VARIABLES Y CONFIGURACIÓN
# ==========================================
env_path = os.path.join(os.getcwd(), '.env')
load_dotenv(env_path, override=True)

st.set_page_config(page_title="App Monitorización Agresor", page_icon="📱", layout="centered")

# ==========================================
# 2. CONEXIONES (FIRESTORE Y API REST)
# ==========================================
# Carga la URL de tu API
API_AGRESORES_URL = os.getenv("API_AGRESORES_URL")

# Si no la encuentra, bloquea la app y avisa del error de seguridad
if not API_AGRESORES_URL:
    st.error("🔒 ERROR DE SEGURIDAD: No se ha encontrado la variable API_AGRESORES_URL en el entorno.")
    st.stop()

if 'db_fs' not in st.session_state:
    try:
        # 1. Modo Local (Tu PC)
        if os.path.exists("credentials.json"):
            st.session_state.db_fs = firestore.Client.from_service_account_json("credentials.json", database="firestore-database5")
        # 2. Modo Producción (Cloud Run)
        else:
            st.session_state.db_fs = firestore.Client(database="firestore-database5")
    except Exception as e:
        st.error(f"Error Firestore: {e}")

@st.cache_data(ttl=30)
def obtener_agresores_api():
    try:
        response = requests.get(API_AGRESORES_URL)
        response.raise_for_status()
        payload = response.json()
        
        lista_agresores = payload.get("agresores", [])
        lista_relaciones = payload.get("relaciones_agresores", [])
        
        df_agresores = pd.DataFrame(lista_agresores)
        df_relaciones = pd.DataFrame(lista_relaciones)
        
        if df_agresores.empty:
            return pd.DataFrame()
            
        df_agresores['nombre_completo'] = df_agresores['nombre_agresor'] + " " + df_agresores['apellido_agresor']
        
        if not df_relaciones.empty:
            df_final = pd.merge(df_agresores, df_relaciones[['id_agresor', 'id_victima']], on='id_agresor', how='left')
        else:
            df_final = df_agresores
            df_final['id_victima'] = None 
            
        return df_final
        
    except Exception as e:
        st.error(f"Error conectando a la API: {e}")
        return pd.DataFrame()

# ==========================================
# 3. ESTILOS CSS (CHASIS MÓVIL)
# ==========================================
st.markdown("""
<style>
    #MainMenu, footer, header {visibility: hidden;}
    .stApp { background-color: #1e1e1e; }
    .block-container { padding: 0 !important; max-width: 400px; }
    .iphone-case { width: 350px; height: 720px; background-color: black; border-radius: 45px; border: 12px solid #2d2d2d; position: relative; margin: 20px auto; box-shadow: 0 0 30px rgba(0,0,0,0.7); overflow: hidden; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }
    .notch-container { position: absolute; top: 0; left: 0; right: 0; height: 30px; z-index: 10; display: flex; justify-content: center; }
    .notch { background-color: #2d2d2d; width: 150px; height: 25px; border-bottom-left-radius: 15px; border-bottom-right-radius: 15px; }
    .status-icons { position: absolute; top: 5px; width: 100%; display: flex; justify-content: space-between; color: white; font-size: 12px; font-weight: bold; padding: 0 10px 0 25px; z-index: 9; }
    .screen-alert { background-color: #8a0000; height: 100%; width: 100%; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center; color: white; animation: flash 0.8s infinite alternate; }
    @keyframes flash { from {opacity: 1;} to {opacity: 0.8;} }
    .screen-safe { background-color: #f0f0f0; height: 100%; position: relative; }
    .map-bg { width: 100%; height: 100%; object-fit: cover; opacity: 0.9; }
    .safe-overlay { position: absolute; bottom: 60px; left: 20px; right: 20px; background: white; padding: 20px; border-radius: 20px; box-shadow: 0 10px 25px rgba(0,0,0,0.15); text-align: center; }
    .home-indicator { position: absolute; bottom: 8px; left: 50%; transform: translateX(-50%); width: 120px; height: 5px; background-color: rgba(255,255,255,0.5); border-radius: 10px; z-index: 20; }
</style>
""", unsafe_allow_html=True)

# 4. SONIDO
def play_sound():
    js = """<script>var ctx = new (window.AudioContext || window.webkitAudioContext)(); var o = ctx.createOscillator(); var g = ctx.createGain(); o.connect(g); g.connect(ctx.destination); o.frequency.value = 650; o.type = 'sawtooth'; o.start(); setTimeout(function(){o.stop();}, 300);</script>"""
    components.html(js, height=0)

# ==========================================
# 5. DATOS Y SELECCIÓN EN SIDEBAR
# ==========================================
df_agresores = obtener_agresores_api()

st.sidebar.title("🛠️ Selector de Dispositivo")

if not df_agresores.empty:
    agresor_seleccionado = st.sidebar.selectbox(
        "Dispositivo Agresor a Monitorizar",
        df_agresores['nombre_completo'].tolist()
    )
    datos_actuales = df_agresores[df_agresores['nombre_completo'] == agresor_seleccionado].iloc[0]
    
    if pd.isna(datos_actuales.get('id_victima')):
        doc_id = None
        st.sidebar.warning("⚠️ Este sujeto no tiene ninguna orden de alejamiento vinculada en este momento.")
    else:
        doc_id = f"{datos_actuales['id_victima']}_{datos_actuales['id_agresor']}"
        st.sidebar.success(f"📡 Vinculado al radar de alejamiento activo.")
else:
    st.error("No se pudieron cargar los datos desde la API.")
    st.stop()

st.sidebar.divider()
st.sidebar.info(f"Visualizando terminal de: **{datos_actuales['nombre_completo']}**")

# ==========================================
# 6. LÓGICA PRINCIPAL (RENDERIZADO)
# ==========================================
if 'on' not in st.session_state: st.session_state.on = False

if not st.session_state.on:
    st.markdown("<br><br><h3 style='text-align:center; color:white;'>Sistema Bloqueado</h3>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    if c2.button("INICIAR DISPOSITIVO", use_container_width=True):
        st.session_state.on = True
        st.rerun()
else:
    placeholder = st.empty()
    
    while True:
        es_alerta = False
        distancia = 0
        dir_escape = "ALEJARSE" 
        
        # 7. LEER DATOS REALES DE FIRESTORE
        if doc_id:
            try:
                doc_ref = st.session_state.db_fs.collection("alertas").document(doc_id)
                doc = doc_ref.get()
                
                if doc.exists:
                    data_fs = doc.to_dict()
                    
                    # Para seguir mostrando la distancia en la pantalla (aunque no se use para activar la alerta)
                    try:
                        distancia = float(data_fs.get('distancia_metros', 9999))
                    except (ValueError, TypeError):
                        distancia = 9999
                        
                    dir_escape = data_fs.get('direccion_escape', 'ALEJARSE')
                    
                    # NUEVA LÓGICA: Leemos directamente el booleano 'activa'
                    alerta_activa = data_fs.get('activa', False)
                    
                    if alerta_activa == True:
                        es_alerta = True
                else:
                    st.warning(f"No se encuentra el documento '{doc_id}' en Firestore.")
                    
            except Exception as e:
                st.error(f"Error interno leyendo la alerta: {e}")

        hora = datetime.now().strftime("%H:%M")
        bat = 78
        color_bat = "green" if bat > 20 else "red"
        
        with placeholder.container():
            if es_alerta:
                play_sound()
                html = textwrap.dedent(f"""
                    <div class="iphone-case">
                        <div class="notch-container"><div class="notch"></div></div>
                        <div class="status-icons"><span>{hora}</span><span>LTE {bat}%</span></div>
                        <div class="screen-alert">
                            <div style="font-size: 80px;">⚠️</div>
                            <h1 style="margin:0; font-size:30px;">ALERTA</h1>
                            <p style="font-size:18px;">PERÍMETRO VULNERADO</p>
                            <p style="font-size:14px; color:#ffcccc;">Usuario: {datos_actuales['nombre_completo']}</p>
                            <div style="background:rgba(0,0,0,0.3); padding:15px; margin:20px; border-radius:10px; border:2px solid #ffcc00;">
                                <strong style="color:#ffcc00;">ACCIÓN REQUERIDA</strong><br>
                                Su posición ha sido reportada.<br>Aléjese inmediatamente.<br>
                                <span style="font-size:35px; font-weight:bold;">{dir_escape}</span><br>
                                <small>Distancia al objetivo: {int(distancia)}m</small>
                            </div>
                        </div>
                        <div class="home-indicator"></div>
                    </div>
                """)
            else:
                html = textwrap.dedent(f"""
                    <div class="iphone-case">
                        <div class="notch-container"><div class="notch"></div></div>
                        <div class="status-icons" style="color:black; z-index:99;">
                            <span>{hora}</span>
                            <span style="color:{color_bat}">🔋 {bat}%</span>
                        </div>
                        <div class="screen-safe">
                            <img src="https://upload.wikimedia.org/wikipedia/commons/e/ec/Valencia_Street_Map.png" class="map-bg">
                            <div class="safe-overlay">
                                <h2 style="margin:0; color:#00aa00;">CONECTADO</h2>
                                <p style="color:#666; font-size:14px; margin:5px 0 15px 0;">Monitorizando a: <b>{datos_actuales['nombre_completo']}</b></p>
                                <div style="display:flex; justify-content:space-around; border-top:1px solid #eee; padding-top:10px;">
                                     <span style="color:#333;">📍 Valencia</span>
                                     <span style="color:#333;">📡 ID: {datos_actuales['id_agresor']}</span>
                                </div>
                            </div>
                        </div>
                        <div class="home-indicator" style="background-color:rgba(0,0,0,0.3);"></div>
                    </div>
                """)
            
            st.markdown(html, unsafe_allow_html=True)
            
        time.sleep(2)