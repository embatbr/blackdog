#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $PROJECT_ROOT_PATH


DW_CREDENTIALS=$(echo "$1" | base64 --decode)
AWS_CREDENTIALS=$(echo "$2" | base64 --decode)


docker ps | grep apache/zeppelin:0.7.3 | grep -v grep | awk '{ print $1 }' | xargs -r docker stop
# docker system prune --all
# docker network prune


export DW_HOST="$(echo $DW_CREDENTIALS | jq -r '.host')"
export DW_PORT="$(echo $DW_CREDENTIALS | jq -r '.port')"
export DW_DATABASE="$(echo $DW_CREDENTIALS | jq -r '.database')"
export DW_USER="$(echo $DW_CREDENTIALS | jq -r '.user')"
export DW_PASSWORD="$(echo $DW_CREDENTIALS | jq -r '.password')"

export AWS_ACCESS_KEY_ID="$(echo $AWS_CREDENTIALS | jq -r '.AWS_ACCESS_KEY_ID')"
export AWS_SECRET_ACCESS_KEY="$(echo $AWS_CREDENTIALS | jq -r '.AWS_SECRET_ACCESS_KEY')"


docker-compose up -d


CONTAINER_ID="$(docker ps | grep apache/zeppelin:0.7.3 | grep -v grep | awk '{ print $1 }')"

docker exec $CONTAINER_ID pip install pendulum pandas

docker exec $CONTAINER_ID mkdir -p /zeppelin/interpreter/spark/dep
docker cp $PROJECT_ROOT_PATH/resources/interpreter/spark/dep/postgresql-42.1.4.jar $CONTAINER_ID:/zeppelin/interpreter/spark/dep/postgresql-42.1.4.jar

mkdir -p $PROJECT_ROOT_PATH/output/notebook/levantamento-dados
cp $PROJECT_ROOT_PATH/resources/notebook/levantamento-dados/note.json $PROJECT_ROOT_PATH/output/notebook/levantamento-dados/note.json
python3 $PROJECT_ROOT_PATH/deploy/create_notebook.py "levantamento-dados" "global-loads"
python3 $PROJECT_ROOT_PATH/deploy/create_notebook.py "levantamento-dados" "table-loads"
python3 $PROJECT_ROOT_PATH/deploy/create_notebook.py "levantamento-dados" "enum-loads"
python3 $PROJECT_ROOT_PATH/deploy/create_notebook.py "levantamento-dados" "query"

docker exec $CONTAINER_ID rm -Rf /zeppelin/notebook
docker exec $CONTAINER_ID mkdir -p /zeppelin/notebook/levantamento-dados
docker cp $PROJECT_ROOT_PATH/output/notebook/levantamento-dados/note.json $CONTAINER_ID:/zeppelin/notebook/levantamento-dados/note.json

rm -Rf $PROJECT_ROOT_PATH/output

# docker cp $PROJECT_ROOT_PATH/zeppelin/bin/zeppelin-env.sh $CONTAINER_ID:/zeppelin/bin/zeppelin-env.sh
# docker cp $PROJECT_ROOT_PATH/secrets/aws/credentials $CONTAINER_ID:/zeppelin/.aws/credentials
# # docker exec $CONTAINER_ID /bin/bash "./bin/zeppelin.sh"

docker restart $CONTAINER_ID
