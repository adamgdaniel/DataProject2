# Usamos una imagen de Python ligera
FROM python:3.10-slim

# Evita que Python genere archivos .pyc y fuerza los logs a la consola
ENV PYTHONUNBUFFERED True

# Copiamos nuestro código al contenedor
WORKDIR /app
COPY requirements.txt .
COPY app_env_only_one_get.py .

# Instalamos las librerías
RUN pip install --no-cache-dir -r requirements.txt

# Cloud Run necesita que arranquemos con un servidor de producción como Gunicorn
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app_env_only_one_get:app