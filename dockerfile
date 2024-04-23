FROM apache/airflow:2.9.0

COPY ./airflow/requirements.txt /opt/airflow/requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /opt/airflow/requirements.txt

