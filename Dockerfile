FROM python:3.12-slim
WORKDIR /app
COPY upstash_producer.py /app/
RUN pip install python-binance kafka-python
CMD ["python", "/app/upstash_producer.py"]
