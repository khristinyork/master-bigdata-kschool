#!/bin/bash


export PATH="$JAVA_HOME/bin:$PATH"
export CLASSPATH="$PATH"

java -cp $CLASSPATH -jar FromKafkaPixyToAvroSpark-0.0-shaded.jar
