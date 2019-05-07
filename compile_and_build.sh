#!/bin/bash
mvn -DskipTests=true clean install
cd storm-dist/binary
mvn -DskipTests=true package
tar xzvf final-package/target/apache-storm-2.0.0.tar.gz
cp storm.yaml apache-storm-2.0.0/conf
storm nimbus &