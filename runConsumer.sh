#!/usr/bin/env bash

echo compile...

# pass <topicName> <numOfRecsToProduce> as args

mvn -q clean compile exec:java \
 -Dexec.mainClass="com.kong.Main" \
 -Dexec.args="consumer"