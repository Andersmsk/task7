
# Используйте образ Airflow, например, python:3.8-slim-buster
FROM apache/airflow:2.7.0

# Установка зависимостей из файла requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt