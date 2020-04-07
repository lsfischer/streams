#!/bin/bash

if [ "$#" -eq 0 ]; then 
DATASET=/debs/sample.csv.gz
TIME=60
elif [ "$#" -eq 1 ]; then 
DATASET=$1
TIME=60
else
DATASET=$1
TIME=$2
fi

docker pull nunopreguica/ps1819-publisher

[ ! "$(docker network ls | grep ps-net )" ] && \
	docker network create --driver=bridge --subnet=10.123.0.0/16 ps-net

#docker run --network=ps-net -v $(pwd)/logs:/debs nunopreguica/ps1819-publisher java -cp .:/home/pstr/* debs.Publisher
docker run --network=ps-net -v $(pwd)/logs:/debs nunopreguica/ps1819-publisher java -cp .:/home/pstr/* debs.Publisher $TIME $DATASET
#docker run --network=ps-net -v /Users/nmp/Work/WORK/aulas/PS/2018-19/labs/projeto/proj2/logs:/debs nunopreguica/ps1819-publisher java -cp .:/home/pstr/* debs.Publisher $TIME $DATASET

