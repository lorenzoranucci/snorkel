#!/bin/bash

cd ../../lookup/docker

docker run -p 1111:1111 -it dbpedia/lookup java -jar /opt/lookup/dbpedia-lookup-3.1-jar-with-dependencies.jar /opt/lookup/2015-10/

