FROM gcr.io/dataflow-templates-base/python3-template-launcher-base:latest

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/dataflow_firestore.py"

WORKDIR /template

COPY . /template/

RUN apt-get update \
    && apt-get install -y libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -U -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE