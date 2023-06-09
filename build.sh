#!/usr/bin/env bash

set -ex

DIR="$(dirname "${BASH_SOURCE[0]}")"
DIR="$(realpath "${DIR}")"

cd "$DIR"

rm -rf target/jars && mkdir -p target/jars

mvn package -pl ./ccf-bdci2022-datalake-contest -am -DskipTests

cp ./ccf-bdci2022-datalake-contest/target/ccf-bdci2022-datalake-contest-1.0.0-SNAPSHOT.jar target/jars/datalake_contest.jar
cp ./ccf-bdci2022-datalake-contest/lakesoul.properties target/jars

rm -f ./submit.zip
zip -r -j ./submit.zip target/jars/*
