#!/bin/bash

# Figure out where we are.
FWDIR="$(cd `dirname $0`; pwd)"

CLASS=$1
shift

if [[ -z "$SPARK_HOME" ]]; then
  echo "SPARK_HOME is not set, running pipeline locally"
  $FWDIR/run-main.sh $CLASS "$@"
elif [[ -z "$KEYSTONE_HOME" ]]; then
  echo "KEYSTONE_HOME is not set, must be set to run"
else
  # TODO: Figure out a way to pass in either a conf file / flags to spark-submit
  KEYSTONE_MEM=${KEYSTONE_MEM:-1g}
  export KEYSTONE_MEM

  # Set some commonly used config flags on the cluster
  $SPARK_HOME/bin/spark-submit \
    --deploy-mode client \
    --class $CLASS \
    --driver-class-path target/scala-2.10/keystone-app-assembly.jar \
    --driver-library-path $KEYSTONE_HOME/lib \
    --conf spark.executor.extraLibraryPath=$KEYSTONE_HOME/lib \
    --conf spark.executor.extraClassPath=target/scala-2.10/keystone-app-assembly.jar \
    --driver-memory $KEYSTONE_MEM \
    target/scala-2.10/keystone-app-assembly.jar \
    "$@"
fi