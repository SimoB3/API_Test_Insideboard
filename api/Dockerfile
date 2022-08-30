FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./app /app
COPY ./data /data

ENV MAX_WORKERS=1
ENV MODULE_NAME=main
ENV VARIABLE_NAME=api
ENV PYTHONPATH=/app

EXPOSE 80
