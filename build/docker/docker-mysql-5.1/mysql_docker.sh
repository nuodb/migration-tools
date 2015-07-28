#!/bin/bash
#  Author: Masthaka Team 


CONTAINER='mysql-cdmt-cont'
IMAGE='mysql-cdmt-img'

cp -r ../core/src/test/resources/mysql  docker/docker-mysql-5.1/ 
echo "Mysql Docker Process Starting"

docker build -t $IMAGE ./docker/docker-mysql-5.1/ # building docker image

echo "MySQL Image Creation is done.";



docker rm -f $CONTAINER > /dev/null || true
docker run --name="mysql-cdmt-cont"  -d  -p 33306:3306 $IMAGE
  
STARTED=$(docker inspect --format="{{ .State.StartedAt }}" $CONTAINER)

echo "OK - $CONTAINER is running.  StartedAt: $STARTED";
echo "MySQL Image Building & Container Creation Process Is Done";
