FROM python:2.7

RUN pip install prometheus_client pymongo

ADD prometheus-mongodb-sharding-exporter.py /
EXPOSE 8000
CMD ["python","/prometheus-mongodb-sharding-exporter.py"]
