import pandas as pd
import folium
from folium.plugins import MarkerCluster
import os
import time
from flask import Flask, render_template, jsonify
import threading
import webbrowser

app = Flask(__name__)
DATA_FILE = 'live_tracking.csv'

def get_latest_data():
    """Lee el CSV y extrae la última posición conocida de cada sujeto."""
    if not os.path.exists(DATA_FILE):
        return []
    
    try:
        df = pd.read_csv(DATA_FILE)
        if df.empty:
            return []
        # Agrupamos por user_id y nos quedamos con el último registro de cada uno
        latest_positions = df.sort_values('timestamp').groupby('user_id').tail(1)
        return latest_positions.to_dict(orient='records')
    except Exception as e:
        print(f"Error al leer datos: {e}")
        return []

@app.route('/')
def index():
    # Centramos el mapa en Valencia
    m = folium.Map(location=[39.4697, -0.3774], zoom_start=13, tiles='cartodbpositron')
    
    # Cluster para manejar múltiples sujetos de forma limpia
    marker_cluster = MarkerCluster().add_to(m)
    
    data = get_latest_data()
    
    for p in data:
        # Lógica de colores según el Rol y el Estado
        color = 'blue'
        icon_type = 'user'
        
        if p['role'] == 'AGRESOR':
            color = 'red'
            icon_type = 'warning'
        else:
            # Si es víctima, el color depende del estrés
            if p['stress'] > 70:
                color = 'orange'
                icon_type = 'exclamation-circle'
            if p['battery'] < 15:
                color = 'black' # Alerta de batería crítica
        
        # Construimos un Popup profesional con HTML
        html = f"""
        <div style="font-family: Arial; width: 180px;">
            <h4>{p['name']} {p['surname']}</h4>
            <hr>
            <b>ID:</b> {p['user_id']}<br>
            <b>Pareja:</b> {p['id_pareja']}<br>
            <b>Modo:</b> {p['transport_mode']}<br>
            <b>Estrés:</b> {p['stress']}%<br>
            <b>Batería:</b> {p['battery']}%<br>
            <b>Calle:</b> {p['street_name']}
        </div>
        """
        
        folium.Marker(
            location=[p['latitude'], p['longitude']],
            popup=folium.Popup(html, max_width=250),
            icon=folium.Icon(color=color, icon=icon_type, prefix='fa'),
            tooltip=f"{p['user_id']} ({p['role']})"
        ).add_to(marker_cluster)

        # Si el agresor está cerca de la víctima, dibujamos un círculo de peligro
        if p['role'] == 'AGRESOR':
            folium.Circle(
                location=[p['latitude'], p['longitude']],
                radius=200, # Radio basado en vuestra lógica de Haversine
                color='red',
                fill=True,
                fill_opacity=0.1
            ).add_to(m)

    return m._repr_html_()

if __name__ == "__main__":
    # Abrir el navegador automáticamente
    threading.Timer(1.5, lambda: webbrowser.open("http://127.0.0.1:5000")).start()
    app.run(debug=False, port=5000)