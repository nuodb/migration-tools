#!/bin/bash
#  Author: Masthaka Team 


CONTAINER='mysql-cdmt-cont'
IMAGE='mysql-cdmt-img'
BASE_DIR=$(dirname $0)

cp -r core/src/test/resources/mysql  ${BASE_DIR}/docker/docker-mysql/ 
echo "Mysql Docker Process Starting"

docker build -t $IMAGE ${BASE_DIR}/docker/docker-mysql/ # building docker image

echo "MySQL Image Creation is done.";



docker rm -f $CONTAINER > /dev/null || true
docker run --name="mysql-cdmt-cont"  -d  -p 33306:3306 $IMAGE
  
STARTED=$(docker inspect --format="{{ .State.StartedAt }}" $CONTAINER)

echo "OK - $CONTAINER is running.  StartedAt: $STARTED";
echo "MySQL Image Building & Container Creation Process Is Done";
