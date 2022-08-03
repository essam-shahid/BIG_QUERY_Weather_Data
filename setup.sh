#!/usr/bin/env sh

info_msg() {
    echo "========================================================================"
    echo "$(date +'%Y-%m-%dT%H:%M:%S%z') $@"
    echo "========================================================================"
}

cd $(dirname $0)
echo $(dirname $0)


info_msg "building containers"
docker-compose build

info_msg "launching application"
docker-compose up

info_msg "Closing containers"
docker-compose down
