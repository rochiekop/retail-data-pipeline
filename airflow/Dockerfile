FROM apache/airflow:3.1.0

COPY --chown=airflow:root pyproject.toml /opt/pipeline/pyproject.toml
COPY --chown=airflow:root src /opt/pipeline/src
RUN python -m venv /home/airflow/pipeline-venv \
 && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir -e /opt/pipeline
