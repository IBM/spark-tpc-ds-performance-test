#!/bin/bash 

SPARK_HOME=$1
OUTPUT_DIR=$2
DRIVER_OPTIONS="--driver-memory 4g --driver-java-options -Dlog4j.configuration=file:///${OUTPUT_DIR}/log4j.properties"
EXECUTOR_OPTIONS="--executor-memory 2g --num-executors 1 --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${OUTPUT_DIR}/log4j.properties --conf spark.sql.crossJoin.enabled=true"

cd $SPARK_HOME
for i in `seq 1 99`;
do
  num=`printf "%02d\n" $i`
  bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS}  -database TPCDS -f ${TPCDS_ROOT_DIR}/temp/query${num}.sql > ${TPCDS_ROOT_DIR}/temp/query${num}.res 2>&1 
done  
