# 1. Forzamos la arquitectura linux/amd64 (vital para Cloud Run y evita fallos en Macs M1/M2)
FROM --platform=linux/amd64 python:3.10-slim

ENV PYTHONUNBUFFERED=True

# 2. Instalamos las herramientas de compilación del sistema operativo
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 3. Copiamos los requisitos e instalamos
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiamos el resto de tu código
COPY . .

# 5. Comando de arranque
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app_env_only_one_get:app