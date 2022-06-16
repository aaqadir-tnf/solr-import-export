#!/bin/bash

PARAMS=""

if [ -z "$1" ]
then
  PARAMS="--help"
fi

export JAVA_OPTS=-Dfile.encoding=utf-8

java $JAVA_OPTS -jar target/solr-import-export-0.0.1-SNAPSHOT.jar "$@" $PARAMS
