---
# Hugo Key Value pairs 
title: AIS Decoding
comments: false
weight: 30
---
<!-- 
# AIS Decoder -->

The goal of this container is to take parsed AIS messages from an AMQP message broker and provide decoded AIS messages to the same, or a different, AMQP broker. 

The parsed data message includes the raw AIS message as well as the metadata attached to the message from the data provider and the metadata created for each incoming data stream. 

## Input Data
The input parsed data is held in a JSON dictionary and fed to RabbitMQ with a routing key. Here's an example of a message: 

```JSON
{"ais": "!AIVDM,1,1,,B,H6<u`S19DEV0V1<QT4pN373<000,2*23\r", 
 "header": null, 
 "server_time": "2022-11-03T10:06:10.904079", 
 "event_time": "2022-11-03T10:06:10.904084", 
 "routing_key": "encoded_ais.aishub.all", 
 "multiline": false}
```
This is a single line message and does not include any header or footer information. Part of the issue with AIS decoding is the existance of multi-line messages. While these almost always follow each other in the message stream there is no guarentee in the protocol that this will be the case. The AIS-i-Mov ingestor stores multiline messages and tries to gather the different parts together before publishing them. The below message contains 2 parts and has header information for each part. 

```JSON
{"ais": ["!AIVDM,2,1,5,A,53m66:800000h4l4000pu8LD000000000000000S2`t666JW0;hQDQiC,0*2B\r", "!AIVDM,2,2,5,A,P00000000000008,2*49\r"], 
"header": [null, null], 
"server_time": "2022-11-03T10:28:08.190386", 
"event_time": "2022-11-03T10:28:08.190391", 
"routing_key": "encoded_ais.aishub.all", 
"multiline": true, 
"msg_id": "5"}
``` 

### Header/Footer Metadata
Header and footer metadata is stripped from the AIS message and included as "header" and "footer" items in the parsed dictionary object. The definition of "header" and "footer" is the string that is before and after the AIS message for each row. In some cases this string is a dictionary of objects (IMIS styled AIS data) and these are parsed as a dict.

Special decoding steps will need to be written to include any metadata in the decoded file structure. 
 
## Routing Keys
RabbitMQ uses routing keys to gather and distribute messages. The routing keys are strings seperated by periods. Each individual string can be replaced by wildcards so, for example, if you wanted a decoder that ingested all AIS data, regardless of source, the following routing key could be provided: "*.encoded_ais.*". This would pull in all streams that had a "encoded_ais" term in them.
The general routing_key convention in the OpenAIS project is to include the provider of the data, go from most generic to most specific, and to label the data as either "encoded_ais" or "ais". The routing keys can be configured to be almost anything in the config file so not too much emphasis is put on this. A good routing key would be:

\<class of data\>.\<source of data\>.\<sub type of data\>.\<sub-sub type of date\>
  - class of data: The generic class of data (encoded_AIS, AIS, VMS, SAR, etc)
  - source of data: The source or provider of the data (IMIS, Spire, AISHub, etc)
  - type of data: Some descriptor that groups the data source together (Coastal vs satellite, port AIS, fishing vessels only etc)
  - Some further subtype for the data: Not really used but can be.

So for example; if coastal AIS data was being received from a local port authority, but only type A receivers were being sent to the data stream the encoded data routing key could look like:

*encoded_ais.port_authority.coastal.class_A*

## Config

The service is configured using an environment variable file. An example can be found in ./config/sample.env. This config file is loaded into the docker container on run (generally with docker-compose).

The config file is split up into several different catagories. Below is a short explanation of them. 

```
#----------------------
#Project
#----------------------
PROJECT_NAME=testing        - The name of the project, this is prepended to all the containers being run
UID=1000                    - UID and GID of the user that will end up owning files created by the containers
GID=1000
 
#----------------------
# Source_RabbitMQ (Just in case you wanna go across servers)
#----------------------
SRC_RABBIT_HOST=rabbit              - The server details for incoming AMQP messages
SRC_RABBIT_MSG_PORT=5672
SRC_RABBITMQ_DEFAULT_USER=rory
SRC_RABBITMQ_DEFAULT_PASS=rory_pw
SRC_RABBIT_EXCHANGE=ais_decoder_test
SRC_KEYS=["rais.tnpa.test"]         - a list of routing keys to accept. Allows combining multiple streams
SRC_QUEUE=ais_source_queue
QUEUE_MAX_LENGTH=100
ON_ERROR_DROP_MSGS=True

#----------------------
# Sink_RabbitMQ (Just in case you wanna go across servers)
#----------------------
SNK_RABBIT_HOST=rabbit              - The server details for publishing AMQP messages
SNK_RABBIT_MSG_PORT=5672
SNK_RABBITMQ_DEFAULT_USER=rory
SNK_RABBITMQ_DEFAULT_PASS=rory_pw
SNK_RABBIT_EXCHANGE=ais_decoder_test
PRODUCE_KEY=sink.key.test           - the routing key to use for processed messages
ON_ERROR_DROP_MSGS=True             - Decide whether or not to drop messages if processing them causing an error

```

## Deployment
The system is generally deployed using docker-compose. There is a compose file stored in the repo that shows how to start the container, mount volumes, use config files in the .env file. The steps to get this running on your system is:
  - Clone/Pull the repo to the target machine
  - Copy the ./config/sample.env to .env and edit it to reflect your environment
  - run "docker-compose up --build -d" 

That will get the service running on your machine but will be pretty useless without any of the other services. There is work being done on a generic deployment that can be found in the deployment project in the OpenAIS namespace.

