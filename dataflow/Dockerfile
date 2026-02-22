# Usamos la imagen oficial de Google para Flex Templates en Python
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base:latest

# Configuramos las variables de entorno que necesita Dataflow
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/dataflow_firestore.py"

# Establecemos el directorio de trabajo
WORKDIR /template

# Copiamos todo tu código (script, requirements, etc.) dentro de la imagen
COPY . /template/

# Instalamos las dependencias del sistema y de Python.
# ¡Aquí está la magia! Al hacer esto nosotros mismos, omitimos el comando "pip check"
# que Google ejecutaba por defecto y que hacía que todo fallara por culpa de google-auth-httplib2.
RUN apt-get update \
    && apt-get install -y libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -U -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE