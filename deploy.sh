#!/bin/bash


PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $PROJECT_ROOT_PATH

PROJECT_NAME="blackdog"

ENV="$1"


DW_CREDENTIALS=$(cat $PROJECT_ROOT_PATH/secrets/dw_access.json | base64)
AWS_CREDENTIALS=$(cat $PROJECT_ROOT_PATH/secrets/aws_credentials.json | base64)


if [ -z "$ENV" ]; then
    sudo ./start.sh "$DW_CREDENTIALS" "$AWS_CREDENTIALS"
else
    KEY_PATH="$HOME/docs/accesses/keys/aws-raichu-prod.pem"
    REMOTE_USER="ubuntu"
    REMOTE_HOME="/home/ubuntu"
    REMOTE_PATH="$REMOTE_HOME/$PROJECT_NAME"
    INSTANCE_PRIVATE_IP="10.0.1.50"

    sudo ssh -i $KEY_PATH $REMOTE_USER@$INSTANCE_PRIVATE_IP "rm -Rf $PROJECT_NAME"
    sudo ssh -i $KEY_PATH $REMOTE_USER@$INSTANCE_PRIVATE_IP "mkdir $PROJECT_NAME"

    sudo scp -i $KEY_PATH start.sh $REMOTE_USER@$INSTANCE_PRIVATE_IP:$REMOTE_HOME/$PROJECT_NAME/start.sh
    sudo scp -i $KEY_PATH docker-compose.yml $REMOTE_USER@$INSTANCE_PRIVATE_IP:$REMOTE_PATH/docker-compose.yml

    sudo ssh -i $KEY_PATH $REMOTE_USER@$INSTANCE_PRIVATE_IP "sudo ./$PROJECT_NAME/start.sh \"$DW_CREDENTIALS\" \"$AWS_CREDENTIALS\""
fi
