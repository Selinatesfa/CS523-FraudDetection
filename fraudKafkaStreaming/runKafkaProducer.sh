#!/bin/bash

setup() {
    echo "*****************************************************************"
    echo "********************** Kafka Streaming *********************"
    echo "*****************************************************************"
}

runSimulator() {
    mvn exec:java -Dexec.mainClass="finalProject.fraudKafkaStreaming.FraudProducer"
}

tearDown()
{
  echo "Shuting down KafkaStreaming"
}

setup;
runSimulator;
tearDown;