#!/bin/bash 

SPARK_HOME=$1
OUTPUT_DIR=$2
DRIVER_OPTIONS="--driver-memory 4g --driver-java-options -Dlog4j.configuration=file:///${OUTPUT_DIR}/log4j.properties"
EXECUTOR_OPTIONS="--executor-memory 2g --num-executors 1 --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${OUTPUT_DIR}/log4j.properties --conf spark.sql.crossJoin.enabled=true"

cd $SPARK_HOME
divider===============================
divider=$divider$divider
header="\n %-10s %11s %15s\n"
format=" %-10s %11.2f %10s %4d\n" 
width=40
printf "$header" "Query" "Time(secs)" "Rows returned" > ${OUTPUT_DIR}/run_summary.txt
printf "%$width.${width}s\n" "$divider" >> ${OUTPUT_DIR}/run_summary.txt
for i in `cat ${OUTPUT_DIR}/runlist.txt`;
do
  num=`printf "%02d\n" $i`
  bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} ${ADDITION_SPARK_OPTIONS} -database TPCDS -f ${OUTPUT_DIR}/query${num}.sql > ${OUTPUT_DIR}/query${num}.res 2>&1 
  lines=`cat ${OUTPUT_DIR}/query${num}.res | grep "Time taken:"`
  echo "$lines" | while read -r line; 
  do
    time=`echo $line | tr -s " " " " | cut -d " " -f3`
    num_rows=`echo $line | tr -s " " " " | cut -d " " -f6`
    printf "$format" \
       query${num} \
       $time \
       "" \
       $num_rows >> ${OUTPUT_DIR}/run_summary.txt 
  done 

done 
touch ${OUTPUT_DIR}/queryfinal.res
