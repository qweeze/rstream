#!/bin/bash

set -e

docker run --rm \
    --env RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
    -p "5552:5552" \
    -p "15672:15672" \
    pivotalrabbitmq/rabbitmq-stream
