FROM python:3.9-slim
WORKDIR /app

# Copia e instala dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script
COPY init_db.py .

# Define el punto de entrada (los argumentos los pone Terraform)
ENTRYPOINT ["python", "init_db.py"]