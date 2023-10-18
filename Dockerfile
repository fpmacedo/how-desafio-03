FROM apache/airflow:2.0.2
RUN pip install scrapy
RUN pip install scrapy-user-agents
RUN pip install apache-airflow[aws]