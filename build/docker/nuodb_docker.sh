#!/bin/bash
#  Author: Masthaka Team 

CONTAINER='nuodb-cdmt-cont'
IMAGE='nuodb-cdmt-img'

echo "Nuodb Docker Process Starting"
rm -rf nuodb-dev-docker
git clone git://github.com/mgodekere/nuodb-dev-docker.git 

# Temporary fix for bug in the script, it will be removed once below fix is added in github repo for above script.
sed -i 's/PATH=${NUODB_HOME}/PATH ${NUODB_HOME}/' nuodb-dev-docker/Dockerfile
docker build -t $IMAGE ./nuodb-dev-docker/ # building docker image
rm -rf nuodb-dev-docker
echo "NuoDB Image Creation is done.";

# Running new container with the following ports forwarded to the host system.
# Forwared Ports : 28888,2001,28004,2222
docker rm -f $CONTAINER > /dev/null || true
docker run --name=$CONTAINER -d -p 2888:8888 -p 2001:9001 -p 28004:48004 -p 2222:22 $IMAGE > /dev/null
   
STARTED=$(docker inspect --format="{{ .State.StartedAt }}" $CONTAINER)
 
echo "OK - $CONTAINER is running.  StartedAt: $STARTED"

# Docker mounting
 
docker cp $CONTAINER:/opt/nuodb/jar/nuodbjdbc.jar common
chmod 770 common

echo "NuoDB Docker Container Creation Process Is Done";
sleep 40s

curl -u domain:bird -X GET -H "Accept: application/json" http://localhost:2888/api/1/hosts > test.json 

grep -Po '"id":.*?[^\\]",' test.json | sed -e 's/.*\:"//g' | sed -e 's/".*//g' > hostid && hostid=`cat hostid` && curl -u domain:bird -X POST -H "Accept: application/json" -H "Content-type: application/json" -d '{ "name": "test", "username": "test", "password": "test", "template": "Single Host","options": { "commit": "remote:1", "mem": "2g" },"groupOptions": { "SMs": {"journal-max-directory-entries": "500"}, "TEs": {"verbose": "sql-statements"} },"variables": { "HOST": "'$hostid'"} }' http://localhost:2888/api/1/databases

sleep 20s

rm hostid test.json
