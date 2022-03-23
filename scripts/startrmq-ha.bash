#!/bin/bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SCRATCH_DIR="${SCRIPT_DIR}/.tmprstream"

trap "clean" EXIT
function clean() {
    rc=$?
    rm -rf "$SCRATCH_DIR"
    exit $rc
}

mkdir -p "$SCRATCH_DIR"
pushd "$SCRATCH_DIR"

git clone git@github.com:rabbitmq/rabbitmq-stream-go-client.git

# the make command didn't work out of the box, using $HOST in the cert filenames
# make rabbitmq-ha-proxy

pushd rabbitmq-stream-go-client/compose/ha_tls
rm -rf tls-gen
git clone https://github.com/michaelklishin/tls-gen tls-gen

pushd tls-gen/basic
make

# workaround for cert names
if [[ ! -f "./result/server_certificate.pem" ]]; then
    find ./result -name "server_*_certificate.pem" -exec cp {} "./result/server_certificate.pem" \;
fi

if [[ ! -f "./result/server_key.pem" ]]; then
    find ./result -name "server_*_key.pem" -exec cp {} "./result/server_key.pem" \;
    chmod 666 ./result/server_key.pem
fi

popd
docker build -t haproxy-rabbitmq-cluster .
docker-compose down
docker-compose up
