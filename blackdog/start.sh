#!/bin/bash


PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $PROJECT_ROOT_PATH


DW_CREDENTIALS=$(echo "$1" | base64 --decode)


docker ps | grep apache/zeppelin:0.7.3 | grep -v grep | awk '{ print $1 }' | xargs -r docker stop


export DW_HOST="$(echo $DW_CREDENTIALS | jq -r '.host')"
export DW_PORT="$(echo $DW_CREDENTIALS | jq -r '.port')"
export DW_DATABASE="$(echo $DW_CREDENTIALS | jq -r '.database')"
export DW_USER="$(echo $DW_CREDENTIALS | jq -r '.user')"
export DW_PASSWORD="$(echo $DW_CREDENTIALS | jq -r '.password')"


docker-compose up -d
