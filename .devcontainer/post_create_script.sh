#!/bin/bash

docker pull quay.io/minio/minio
docker tag quay.io/minio/minio:latest minio:latest
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin
MINIO_VOLUMES="/mnt/data"
docker run -dt                                  \
  -p 9000:9000 -p 9090:9090                     \
  -v PATH:/mnt/data                             \
  -v /etc/default/minio:/etc/config.env         \
  -e "MINIO_CONFIG_ENV_FILE=/etc/config.env"    \
  --name "minio_local"                          \
  minio server --console-address ":9090"
astro dev start -n