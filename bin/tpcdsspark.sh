#!/bin/bash 

handle_shutdown() {
  kill $1
  rm -rf $2/*
  echo ""
  echo "Script was terminated abnormally. Finished cleaning up.. "
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
     TPCDS_GENDATA_DIR=${TPCDS_ROOT_DIR}/gendata
  fi  
  if [ -z "$TPCDS_GEN_QUERIES_DIR" ]; then
     TPCDS_GENQUERIES_DIR=${TPCDS_ROOT_DIR}/genqueries
  fi  
  if [ -z "$TPCDS_WORK_DIR" ]; then
     TPCDS_WORK_DIR=${TPCDS_ROOT_DIR}/work
  fi  

  echo "TPCDS_ROOT_DIR = $TPCDS_ROOT_DIR"
  echo "TPCDS_LOG_DIR = $TPCDS_LOG_DIR"
  echo "TPCDS_DBNAME = $TPCDS_DBNAME"
  echo "TPCDS_GENDATA_DIR = $TPCDS_GENDATA_DIR"
  echo "TPCDS_GENQUERIES_DIR = $TPCDS_GENQUERIES_DIR"
  echo "TPCDS_WORK_DIR = $TPCDS_WORK_DIR"
}

#check_environment() {
#}

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
  printf "\rProgress : [${_fill// /#}${_empty// /-}] ${_progress}%%"
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

function cleanup_toolkit {
  cd toolkit/tools
  make clean
  rm -rf temp/*
  rm -rf data/*
  rm -rf log/*
}

function download_and_build {
  platform=$(platformCheck)
  
  if [ "$platform" -eq 'MACOS' ] ; then 
    if type xcode-select >&- && xpath=$( xcode-select --print-path ) &&
     test -d "${xpath}" && test -x "${xpath}" ; then
     echo "INFO - Found xcode"
    else
     echo "Xcode is not installed. Install xcode first"
     exit 1
    fi
  fi
  cd toolkit/tools
  make clean
  echo "make OS=${platform}"
  make OS=${platform}
  echo "Completed building toolkit successfully.."
  cd ../../
}

function gen_data {
  cd toolkit/tools
  rm -rf $1/data/*
  ./dsdgen -dir $TPCDS_GENDATA_DIR -scale $2  -verbose y -terminate n > ${TPCDS_WORK_DIR}/dsgen.out 2>&1 &
  dsgen_pid=$!
  trap 'handle_shutdown $dsgen_pid $1/data; exit' SIGHUP SIGQUIT SIGINT SIGTERM
  echo "Starting to generate data. Will take a few minutes ..."
  cont=1
  error_code=0
  while [  $cont -gt 0 ]; do
    progress=`find ${TPCDS_GENDATA_DIR} -name "*.dat" | wc -l`
    ProgressBar ${progress} 25

    ps -p $dsgen_pid > /dev/null 
    if [ $? == 1 ]; then
       error_code=1
    fi
    if [ "$error_code" -gt 0 ] || [ "$progress" -gt 24 ] ; then 
      cont=-1
    fi
    sleep 0.1
  done 
  progress=`ls ${TPCDS_GENDATA_DIR}/*.dat | wc -l`
   
  if [ "$progress" -lt 25 ] ; then 
    echo ""
    echo "Failed to generate the data. Look at ${TPCDS_WORK_DIR}/dsgen.out for error details."
  else
    echo ""
    echo "TPCDS data is generated successfully at ${TPCDS_GENDATA_DIR}"
  fi
  
  cd $1
}

function generate_queries {
  rm -rf ${TPCDS_WORK_DIR}/*
  cp ${TPCDS_ROOT_DIR}/toolkit/query_templates/* $TPCDS_WORK_DIR
  cp $TPCDS_ROOT_DIR/query-templates/* $TPCDS_WORK_DIR
  templDir=$TPCDS_WORK_DIR
  outDir=${TPCDS_GEN_QUERIES_DIR}
  perl ${TPCDS_ROOT_DIR}/bin/qual.pl
  cd $TPCDS_ROOT_DIR
}

function run_tpcds_queries {
  output_dir=$TPCDS_WORK_DIR
  rm -rf ${TPCDS_WORK_DIR}/*
  cp ${TPCDS_GEN_QUERIES_DIR}/queries/*.sql $TPCDS_WORK_DIR

  for i in `ls ${TPCDS_ROOT_DIR}/src/properties/*`
  do
    baseName="$(basename $i)"
    template $i > ${output_dir}/$baseName
  done 

  echo "Running TPCDS queries. Will take a couple of hours.. "
  ${TPCDS_ROOT_DIR}/bin/runqueries.sh $SPARK_HOME $TPCDS_WORK_DIR  > ${TPCDS_WORK_DIR}/runqueries.out 2>&1 &
  script_pid=$!
  trap 'handle_shutdown $script_pid $output_dir; exit' SIGHUP SIGQUIT SIGINT SIGTERM
  cont=1
  error_code=0
  while [  $cont -gt 0 ]; do
    progress=`find $TPCDS_WORK_DIR -name "*.res" | wc -l`
    ProgressBar ${progress} 99

    ps -p $script_pid > /dev/null 
    if [ $? == 1 ]; then
       error_code=1
    fi
    if [ "$error_code" -gt 0 ] || [ "$progress" -gt 98 ] ; then 
      cont=-1
    fi
    sleep 0.1
  done 
  progress=`find $TPCDS_WORK_DIR -name "*.res" | wc -l`
   
  if [ "$progress" -lt 99 ] ; then 
    echo ""
    echo "Failed to run TPCDS queries. Please look at ${TPCDS_ROOT_DIR}/temp/runqueries.out for error details" 
  else
    echo ""
    echo "TPCDS queries ran successfully"
  fi

}

function create_spark_tables {
  output_dir=$TPCDS_WORK_DIR
  rm -rf $output_dir/* 
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


  current_dir=`pwd`
  cd $SPARK_HOME
  DRIVER_OPTIONS="--driver-java-options -Dlog4j.configuration=file:///${output_dir}/log4j.properties"
  EXECUTOR_OPTIONS="--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${output_dir}/log4j.properties"
  echo "Creating tables. Will take a few minutes ..."
  ProgressBar 2 122
  bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.catalogImplementation=hive -f ${TPCDS_WORK_DIR}/create_database.sql > ${TPCDS_WORK_DIR}/create_database.out 2>&1
  bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.catalogImplementation=hive -f ${TPCDS_WORK_DIR}/create_temp.sql > ${TPCDS_WORK_DIR}/create_temp.out 2>&1 &
  script_pid=$!
  trap 'handle_shutdown $script_pid $output_dir; exit' SIGHUP SIGQUIT SIGINT SIGTERM

  cont=1
  error_code=0
  while [  $cont -gt 0 ]; do
    progress=`cat ${TPCDS_WORK_DIR}/create_temp.out | grep -i "time taken" | wc -l`
    progress=`expr $progress + 2`
    ProgressBar ${progress} 122
    if [ -e ${TPCDS_WORK_DIR}/create_temp.out ]; then
      error_code=`cat ${TPCDS_WORK_DIR}/create_temp.out | grep -i "error" | wc -l`
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
    echo "Failed to create spark tables. Please review the following logs"
    echo "${TPCDS_WORK_DIR}/create_temp.out"
    echo "${TPCDS_WORK_DIR}/temp/create_database.out"
    echo "${TPCDS_LOG_DIR}/spark-tpcds-log"
  else
    echo ""
    echo "Spark tables created successfully.."
  fi

  #Restore to current directory
  cd $current_dir
}

TEST_ROOT=`pwd`
while :
do
    clear
    set_environment
    cat<<EOF
    ==============================
    TPCDS On Spark Menu
    ------------------------------
    Please enter your choice:

    (1) Compile TPCDS toolkit
    (2) Generate TPCDS data with 1GB scale
    (3) Create Spark Tables
    (4) Generate TPCDS queries
    (5) Run TPCDS Queries
    (6) Cleanup toolkit
    (Q)uit
    ------------------------------
EOF
    read -n1 -s
    case "$REPLY" in
    "1")  download_and_build ;;
    "2")  gen_data $TEST_ROOT '1G' ;;
    "3")  create_spark_tables ;;
    "4")  generate_queries ;;
    "5")  run_tpcds_queries ;;
    "6")  cleanup_toolkit ;;
    "Q")  exit                      ;;
    "q")  echo "case sensitive!!"   ;; 
     * )  echo "invalid option"     ;;
    esac
    echo "Press any key to continue"
    read -n1 -s
done
