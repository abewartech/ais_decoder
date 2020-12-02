FROM python:3.7-buster

ADD db_inserter/requirements.txt /REQUIREMENTS.txt
RUN pip install -r /REQUIREMENTS.txt

ADD db_inserter/ /usr/local/db_inserter 
WORKDIR /usr/local/
RUN export set PYTHONPATH=$PYTHONPATH:.
