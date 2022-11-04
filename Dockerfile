FROM python:3.9.15-slim-buster

RUN apt-get update && \
    apt-get -y install build-essential && \
    rm -rf /var/lib/apt/lists/*

ADD build/ais_decoder/requirements.txt /REQUIREMENTS.txt
RUN pip install -r /REQUIREMENTS.txt

ADD build/ais_decoder/ /usr/local/ais_decoder
#RUN cd /usr/local/data && unzip \*.zip
WORKDIR /usr/local/
RUN export set PYTHONPATH=$PYTHONPATH:.
