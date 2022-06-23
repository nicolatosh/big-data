#!/bin/bash

# Colors
GREEN='\033[0;32m'
NC='\033[0m' 

echo -e "${GREEN} == Stopping application stack == ${NC}"


docker-compose -f ./docker-files/docker-compose-mongo.yaml stop 
docker-compose -f ./docker-files/docker-compose-redis.yaml stop 
docker-compose -f ./docker-files/docker-compose-multinode.yaml stop 

echo -e "${GREEN} == Done == ${NC}"