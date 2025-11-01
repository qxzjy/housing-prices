FROM apache/airflow:2.8.4

# Switch user
USER root

# Copy the PEM key to the docker image
COPY secrets/myEC2key.pem /opt/airflow/

# Ensure correct permissions for the .pem file
RUN chmod 444 /opt/airflow/myEC2key.pem \
   && chown airflow /opt/airflow/myEC2key.pem

# Switch back to airflow user
USER airflow

# Copy the requirements file to the docker image
COPY requirements.txt .

# Install dependencies using the requirements file
RUN pip install --no-cache-dir -r requirements.txt
