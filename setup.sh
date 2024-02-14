#!/bin/bash

mkdir -p ./logs
echo -e "AIRFLOW_UID=$(id -u)" > .env
