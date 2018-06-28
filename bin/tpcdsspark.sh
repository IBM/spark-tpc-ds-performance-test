#!/bin/bash 

killtree() {
  local parent=$1 child
  for child in $(ps -o ppid= -o pid= | awk "\$1==$parent {print \$2}"); do
    killtree $child
  done
  kill $parent > /dev/null 2>&1
}

handle_shutdown() {
  killtree $1
  cleanup $2
  echo ""
  echo "Script was terminated abnormally. Finished cleaning up.. "
}

handle_shutdown1() {
  echo "Script was terminated abnormally. Finished cleaning up.. "
  exit 1
}

cleanup() {
  if [ -n "$1" ]; then
    rm -rf $1/*.log
    rm -rf $1/*.txt
    rm -rf $1/*.sql
    rm -rf $1/*.properties
    rm -rf $1/*.out
    rm -rf $1/*.res
    rm -rf $1/*.dat
    rm -rf $1/*.rrn
    rm -rf $1/*.tpl
    rm -rf $1/*.lst
    rm -rf $1/README
  fi
}

validate_querynum() {
 re='^[0-9]+$'
 if ! [[ $1 =~ $re ]] ; then
   return 1
 fi
 if [[ $1 -le 0 || $1 -gt 99 ]] ; then
   return 1
 fi
 return 0
}

cleanup_all() {
  cleanup $TPCDS_WORK_DIR
  cleanup $TPCDS_LOG_DIR
  logInfo "Cleanup successful.."
}

check_compile() {
 if [ ! -f $TPCDS_ROOT_DIR/src/toolkit/tools/dsdgen ]; then
   logError "Toolkit has not been compiled. Please complete option 1"
   echo     "before continuing with the currently selected option."
   return 1
 fi

 if [ ! -f $TPCDS_ROOT_DIR/src/toolkit/tools/dsqgen ]; then
   logError "Toolkit has not been compiled. Please complete option 1"
   echo     "before continuing with the currently selected option."
   return 1
 fi
}

check_gendata() {
 num_datafiles=`find $TPCDS_GENDATA_DIR -name *.dat | wc -l`
 if [ "$num_datafiles" -lt 24 ]; then 
  logError "TPC-DS data files have not been generated. Please complete option 2"
  echo     "before continuing with the currently selected option."
  return 1
 fi
}

check_genqueries() {
 num_queries=`find $TPCDS_GENQUERIES_DIR -name *.sql | wc -l`
 if [ "$num_queries" -lt 99 ]; then 
  logError "TPC-DS queries have not been generated. Please complete option 4"
  echo     "before continuing with the currently selected option."
  return 1
 fi
}

check_createtables() {
  result=$?
  if [ "$result" -ne 0 ]; then
    return 1 
  fi
  
  cd $SPARK_HOME
  DRIVER_OPTIONS="--driver-memory 4g --driver-java-options -Dlog4j.configuration=file:///${output_dir}/log4j.properties"
  EXECUTOR_OPTIONS="--executor-memory 2g --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${output_dir}/log4j.properties"
  logInfo "Checking pre-reqs for running TPC-DS queries. May take a few seconds.."
  bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.catalogImplementation=hive -f ${TPCDS_WORK_DIR}/row_counts.sql > ${TPCDS_WORK_DIR}/rowcounts.out 2>&1
  cat ${TPCDS_WORK_DIR}/rowcounts.out | grep -v "Time" | grep -v "SLF4J" >> ${TPCDS_WORK_DIR}/rowcounts.rrn
  file1=${TPCDS_WORK_DIR}/rowcounts.rrn
  file2=${TPCDS_ROOT_DIR}/src/ddl/rowcounts.expected
  if cmp -s "$file1" "$file2"
  then
     logInfo "Checking pre-reqs for running TPC-DS queries is successful."
     return 0 
  else
    logError "The rowcounts for TPC-DS tables are not correct. Please make sure option 1"
    echo     "is run before continuing with currently selected option"
    return 1
  fi
}

check_prereq() {
  option=$1
  case $option in
    "2")
        check_createtables;;
    "3")
        check_createtables;;
  esac
  return $?
}

logInfo() {
  echo "INFO: $1"
}

logError() {
  echo "ERROR: $1"
}

set_environment() {
  bin_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  script_dir="$(dirname "$bin_dir")"
  
  if [ -z "$TPCDS_ROOT_DIR" ]; then
     TPCDS_ROOT_DIR=${script_dir}
  fi  
  if [ -z "$TPCDS_LOG_DIR" ]; then
     TPCDS_LOG_DIR=${script_dir}/log
  fi  
  if [ -z "$TPCDS_DBNAME" ]; then
     TPCDS_DBNAME="TPCDS"
  fi  
  if [ -z "$TPCDS_GENDATA_DIR" ]; then
     TPCDS_GENDATA_DIR=${TPCDS_ROOT_DIR}/src/data
  fi  
  if [ -z "$TPCDS_GEN_QUERIES_DIR" ]; then
     TPCDS_GENQUERIES_DIR=${TPCDS_ROOT_DIR}/src/queries
  fi  
  if [ -z "$TPCDS_WORK_DIR" ]; then
     TPCDS_WORK_DIR=${TPCDS_ROOT_DIR}/work
  fi  
}

check_environment() {
 if [ -z "$SPARK_HOME" ]; then
    logError "SPARK_HOME is not set. Please make sure the following conditions are met."
    logError "1. Set SPARK_HOME in ${TPCDS_ROOT_DIR}/bin/tpcdsenv.sh and make sure"
    logError "   it points to a valid spark installation."
    logError "2. The userid running the script has permission to execute spark shell."
    logError "3. After setting up SPARK_HOME, re-run the script and choose this option."
    exit 1
 fi  
}

template(){
    # usage: template file.tpl
    while read -r line ; do
            line=${line//\"/\\\"}
            line=${line//\`/\\\`}
            line=${line//\$/\\\$}
            line=${line//\\\${/\${}
            eval "echo \"$line\""; 
    done < ${1}
}

function ProgressBar {
  # Process data
    let _progress=(${1}*100/${2}*100)/100
    let _done=(${_progress}*4)/10
    let _left=40-$_done
  # Build progressbar string lengths
    _fill=$(printf "%${_done}s")
    _empty=$(printf "%${_left}s")

  # 1.2 Build progressbar strings and print the ProgressBar line
  # 1.2.1 Output example:
  # 1.2.1.1 Progress : [########################################] 100%
  printf "\rINFO: Progress : [${_fill// /#}${_empty// /-}] ${_progress}%%"
}

function platformCheck {
  local osType="UNKNOWN"
  unameOut="$(uname -s)"
  case "${unameOut}" in
    Linux*)     osType=LINUX;;
    Darwin*)    osType=MACOS;;
    *)          echo "Tool supported only on Linux or Mac systems : OS=UNKNOWN:${unameOut}"
                exit 1
  esac
  echo $osType
}

function run_tpcds_common {
  output_dir=$TPCDS_WORK_DIR
  cp ${TPCDS_GENQUERIES_DIR}/*.sql $TPCDS_WORK_DIR

  ${TPCDS_ROOT_DIR}/bin/runqueries.sh $SPARK_HOME $TPCDS_WORK_DIR  > ${TPCDS_WORK_DIR}/runqueries.out 2>&1 &
  script_pid=$!
  trap 'handle_shutdown $$ $output_dir; exit' SIGHUP SIGQUIT SIGINT SIGTERM
  cont=1
  error_code=0
  while [  $cont -gt 0 ]; do
    progress=`find $TPCDS_WORK_DIR -name "*.res" | wc -l`
    ProgressBar ${progress} ${NUM_QUERIES}

    ps -p $script_pid > /dev/null 
    if [ $? == 1 ]; then
       error_code=1
    fi
    if [ "$error_code" -gt 0 ] || [ "$progress" -ge $NUM_QUERIES ] ; then 
      cont=-1
    fi
    sleep 0.1
  done 
  progress=`find $TPCDS_WORK_DIR -name "*.res" | wc -l`
   
  if [ "$progress" -lt $NUM_QUERIES ] ; then 
    echo ""
    logError "Failed to run TPCDS queries. Please look at ${TPCDS_WORK_DIR}/runqueries.out for error details" 
  else
    echo ""
    logInfo "TPCDS queries ran successfully. Below are the result details"
    logInfo "Individual result files: ${TPCDS_WORK_DIR}/query<number>.res"
    logInfo "Summary file: ${TPCDS_WORK_DIR}/run_summary.txt"
  fi
}

function run_subset_tpcds_queries {
  output_dir=$TPCDS_WORK_DIR
  cleanup $TPCDS_WORK_DIR
  echo "Enter a comma separated list of queries to run (ex: 1, 2), followed by [ENTER]:"
  read run_list
  if [ -z "$run_list" ]; then
    logError "Empty query list is not allowed. Please supply a comma separated query list"
    return 1
  fi  
  touch ${TPCDS_WORK_DIR}/runlist.txt
  for query_no in $(echo $run_list | sed -n 1'p' | tr ',' '\n')
  do
    validate_querynum $query_no
    result=$?
    if [ "$result" -eq 1 ]; then 
      logError "Supplied query numbers are either non-integers or not within valid range of 1-99"
      return 1
    fi 
    echo "$query_no" >> ${TPCDS_WORK_DIR}/runlist.txt
  done
  for i in `ls ${TPCDS_ROOT_DIR}/src/properties/*`
  do
    baseName="$(basename $i)"
    template $i > ${output_dir}/$baseName
  done 
  for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/*.sql`
  do
    baseName="$(basename $i)"
    template $i > ${output_dir}/$baseName
  done 
  check_prereq "2"
  result=$?
  
  NUM_QUERIES=`cat ${TPCDS_WORK_DIR}/runlist.txt | wc -l`
  # 1 added for final result.
  NUM_QUERIES=`expr $NUM_QUERIES + 1` 
  if [ "$result" -ne 1 ]; then 
    logInfo "Running TPCDS queries. Will take a few minutes depending upon the number of queries specified.. "
    run_tpcds_common
  fi 
}

function run_tpcds_queries {
  output_dir=$TPCDS_WORK_DIR
  cleanup $TPCDS_WORK_DIR
  touch ${TPCDS_WORK_DIR}/runlist.txt
  for i in `seq 1 99`
  do
    echo "$i" >> ${TPCDS_WORK_DIR}/runlist.txt
  done
  for i in `ls ${TPCDS_ROOT_DIR}/src/properties/*`
  do
    baseName="$(basename $i)"
    template $i > ${output_dir}/$baseName
  done 
  for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/*.sql`
  do
    baseName="$(basename $i)"
    template $i > ${output_dir}/$baseName
  done 
  # 1 add to 99 queries to signal the end of the run to progress bar
  NUM_QUERIES=100
  check_prereq "3"
  result=$?
  if [ "$result" -ne 1 ]; then 
    logInfo "Running TPCDS queries. Will take a few hours.. "
    run_tpcds_common
  fi
}

function create_spark_tables {
  check_environment
  output_dir=$TPCDS_WORK_DIR
  cleanup $TPCDS_WORK_DIR
  trap 'handle_shutdown $$ $output_dir; exit' SIGHUP SIGQUIT SIGINT SIGTERM
  echo "USE ${TPCDS_DBNAME};" >> ${output_dir}/create_tables_temp.sql
  for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/individual/*.sql`
  do
     cat $i >> ${output_dir}/create_tables_temp.sql
     echo "" >> ${output_dir}/create_tables_temp.sql
  done
  template ${output_dir}/create_tables_temp.sql > ${output_dir}/create_tables_work.sql

  for i in `ls ${TPCDS_ROOT_DIR}/src/ddl/*.sql`
  do
    baseName="$(basename $i)"
    template $i > ${output_dir}/$baseName
  done 
  for i in `ls ${TPCDS_ROOT_DIR}/src/properties/*`
  do
    baseName="$(basename $i)"
    template $i > ${output_dir}/$baseName
  done 
  result=$?
  if [ "$result" -ne 1 ]; then 
    current_dir=`pwd`
    cd $SPARK_HOME
    DRIVER_OPTIONS="--driver-java-options -Dlog4j.configuration=file:///${output_dir}/log4j.properties"
    EXECUTOR_OPTIONS="--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${output_dir}/log4j.properties"
    logInfo "Creating tables. Will take a few minutes ..."
    ProgressBar 2 122
    bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.catalogImplementation=hive -f ${TPCDS_WORK_DIR}/create_database.sql > ${TPCDS_WORK_DIR}/create_database.out 2>&1
    script_pid=$!
    bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.catalogImplementation=hive -f ${TPCDS_WORK_DIR}/create_tables_work.sql > ${TPCDS_WORK_DIR}/create_tables.out 2>&1 &
    script_pid=$!
    cont=1
    error_code=0
    while [  $cont -gt 0 ]; do
      progress=`cat ${TPCDS_WORK_DIR}/create_tables.out | grep -i "time taken" | wc -l`
      progress=`expr $progress + 2`
      ProgressBar ${progress} 122
      if [ -e ${TPCDS_WORK_DIR}/create_tables.out ]; then
        error_code=`cat ${TPCDS_WORK_DIR}/create_tables.out | grep -i "error" | wc -l`
      fi

      ps -p $script_pid > /dev/null 
      if [ $? == 1 ]; then
         error_code=1
      fi
 
      if [ "$error_code" -gt 0 ] || [ "$progress" -gt 121 ] ; then 
        cont=-1
      fi
      sleep 0.1
    done  
    if [ "$error_code" -gt 0 ] ; then 
      logError "Failed to create spark tables. Please review the following logs"
      logError "${TPCDS_WORK_DIR}/create_tables.out"
      logError "${TPCDS_WORK_DIR}/temp/create_database.out"
      logError "${TPCDS_LOG_DIR}/spark-tpcds-log"
    else
      echo ""
      logInfo "Spark tables created successfully.."
    fi
    cd $current_dir
  fi
}

set_env() {
  # read -n1 -s
  TEST_ROOT=`pwd`
  set_environment
  . $TPCDS_ROOT_DIR/bin/tpcdsenv.sh
  echo "SPARK_HOME is " $SPARK_HOME
  set_environment
}

main() {
  set_env
  while :
  do
      clear
      cat<<EOF
==============================================
TPC-DS On Spark Menu
----------------------------------------------
SETUP
(1) Create spark tables
RUN
(2) Run a subset of TPC-DS queries
(3) Run All (99) TPC-DS Queries
CLEANUP
(4) Cleanup
(Q) Quit
----------------------------------------------
EOF
      printf "%s" "Please enter your choice followed by [ENTER]: "
      read option
      printf "%s\n\n" "----------------------------------------------"
      case "$option" in
      "1")  create_spark_tables ;;
      "2")  run_subset_tpcds_queries ;;
      "3")  run_tpcds_queries ;;
      "4")  cleanup_all ;;
      "Q")  exit                      ;;
      "q")  exit                      ;;
       * )  echo "invalid option"     ;;
      esac
      echo "Press any key to continue"
      read -n1 -s
  done
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main
