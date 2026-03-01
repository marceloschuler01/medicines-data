# Use an official Airflow image as the base
FROM apache/airflow:2.6.1-python3.10

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor

RUN python -m pip install --upgrade pip
RUN pip install pandas==2.2.3
RUN pip install psycopg2-binary==2.9.10
RUN pip install xlrd==2.0.1
RUN pip install blinker==1.7.0
RUN pip install stealth_requests==1.2.1
RUN pip install pymongo==4.16.0
RUN pip install openpyxl==3.1.5

# Copy DAGs into the Airflow DAGs folder
COPY --chown=airflow ./dags /opt/airflow/dags

# Initialize the Airflow DB
RUN airflow db init
RUN airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

# Expose ports (8080 for the webserver)
EXPOSE 8080

# Command to start Airflow webserver and scheduler
CMD ["bash", "-c", "airflow webserver --port 8080 & airflow scheduler"]
