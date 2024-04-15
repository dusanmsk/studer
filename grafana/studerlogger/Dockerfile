FROM python:3.11.9-alpine3.19

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# xcom-protocol issue #6 workaround
RUN mkdir /tmp/xcomproto/; cd /tmp/xcomproto; wget https://github.com/dusanmsk/xcom-protocol/archive/refs/heads/master.zip; unzip master.zip; pip3 install ./xcom-protocol-master/; cd -; rm -rf /tmp/xcomproto/

COPY *.py /app/

CMD [ "python", "/app/studerlogger.py" ]
