#!/bin/bash

cd ../../lookup/docker

docker rm sentimantic_dbpedia_lookup
docker run -d -p 1111:1111 --name sentimantic_dbpedia_lookup -it dbpedia/lookup java -jar /opt/lookup/dbpedia-lookup-3.1-jar-with-dependencies.jar /opt/lookup/2015-10/

