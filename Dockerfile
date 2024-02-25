FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN mkdir -p /var/log/cron

RUN apt-get update \
    && apt-get -y install cron netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN echo "*/5 * * * * root /app/run.sh >> /var/log/cron/cron.log 2>&1" > /etc/cron.d/cronjob
RUN chmod 0644 /etc/cron.d/cronjob

COPY run.sh /app/run.sh
COPY runprod.sh /app/runprod.sh
RUN chmod 0744 /app/run.sh /app/runprod.sh

RUN crontab /etc/cron.d/cronjob

RUN touch /var/log/cron.log

RUN pip install --no-cache-dir -r requirements.txt

CMD ./run.sh

