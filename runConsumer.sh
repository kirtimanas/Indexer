#!/usr/bin/env bash

echo compile...

mvn -q clean compile exec:java \
 -Dexec.mainClass="com.kong.Main" \
 -Dexec.args="consumer"