#!/bin/sh
docker build -t docker_selenium -f Dockerfile-selenium .
docker build -t docker_airflow -f Dockerfile-airflow .
