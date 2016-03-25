#!/bin/bash
set -e
SCALA_VERSION="2.10"

sbt -java-home $(/usr/libexec/java_home -v '1.6*') assembly
TARGET=$(find target/scala-$SCALA_VERSION/ -name *assembly* | head -1)

scp $TARGET chenxm@hadoopjob.omnilab.sjtu.edu.cn:/home/chenxm/workspace/flowmap/