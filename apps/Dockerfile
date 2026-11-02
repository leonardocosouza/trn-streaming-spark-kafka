FROM python:3.11.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py .
COPY .streamlit .streamlit

RUN useradd --create-home demo
USER demo
