# https://hub.docker.com/_/python
FROM python:3.10-slim
RUN apt-get update
RUN apt-get install default-jdk -y
ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY ./app.py ./app.py
COPY ./functions.py ./functions.py
COPY ./yelp_dataset ./yelp_dataset
COPY ./requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r ./requirements.txt

CMD exec gunicorn --bind :5777 --workers 4 --threads 8 --timeout 0 app:app