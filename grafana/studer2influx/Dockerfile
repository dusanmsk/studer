FROM python:3.11.9-alpine3.19

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py /app

CMD [ "python", "/app/studer2influx.py" ]
