#!/bin/sh
docker network create container_bridge
docker volume create --name=downloads
docker-compose up -d