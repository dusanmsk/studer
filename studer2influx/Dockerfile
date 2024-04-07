FROM python:3.11.9-slim-bookworm

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py /app

CMD [ "python", "/app/studer2influx.py" ]
