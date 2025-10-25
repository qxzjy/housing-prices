FROM apache/airflow:2.8.4

# Switch user
USER root

COPY secrets/myEC2key.pem /opt/airflow/
# Ensure correct permissions for the .pem file
RUN chmod 444 /opt/airflow/myEC2key.pem \
   && chown airflow /opt/airflow/myEC2key.pem

# Switch back to airflow user
USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
