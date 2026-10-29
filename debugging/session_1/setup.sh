#!/usr/bin/env bash
# Downloads the jars Consumer.java needs into ./libs (Kafka Streams 3.9.1 + its runtime deps).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p libs

M=https://repo1.maven.org/maven2
JARS=(
  org/apache/kafka/kafka-streams/3.9.1/kafka-streams-3.9.1.jar
  org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar
  org/rocksdb/rocksdbjni/7.9.2/rocksdbjni-7.9.2.jar
  com/github/luben/zstd-jni/1.5.6-4/zstd-jni-1.5.6-4.jar
  org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar
  org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar
  com/fasterxml/jackson/core/jackson-databind/2.16.2/jackson-databind-2.16.2.jar
  com/fasterxml/jackson/core/jackson-core/2.16.2/jackson-core-2.16.2.jar
  com/fasterxml/jackson/core/jackson-annotations/2.16.2/jackson-annotations-2.16.2.jar
  org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
  org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar
)

for jar in "${JARS[@]}"; do
  out="libs/$(basename "$jar")"
  [ -f "$out" ] || { echo "Downloading $(basename "$jar")"; curl -fsSL "$M/$jar" -o "$out"; }
done
echo "Done. Run: java --enable-native-access=ALL-UNNAMED -cp 'libs/*' Consumer.java"
