#!/usr/bin/env bash
CURDIR=`dirname $(realpath $0)`
LOGDIR=$CURDIR/../eventLogs
JARNAME=$1
INSOURCE=hdfs://`hostname`:9000/user/ubuntu/CFSROcsv/vals
OUTDEST=hdfs://`hostname`:9000/user/ubuntu/CFSROparquet
PIECESLOGNAME=$CURDIR/../CSVToParquetConversion-pieces.log
COMBINELOGNAME=$CURDIR/../CSVToParquetConversion-combine.log

convertchunk () {
  PART=$1
  spark-submit --verbose \
    --master yarn \
    --num-executors 29 \
    --driver-memory 180G \
    --executor-memory 180G \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=$LOGDIR \
    --conf spark.driver.maxResultSize=50G \
    --conf spark.task.maxFailures=4 \
    --conf spark.worker.timeout=1200000 \
    --conf spark.network.timeout=1200000 \
    --jars $JARNAME \
    --class org.apache.spark.climate.CSVToParquetPiecewise \
      $JARNAME \
      $INSOURCE $OUTDEST $PART\
      2>&1 | tee $PIECESLOGNAME
}

for PREFIX in 0 1 2 3 4
do
  convertchunk $PREFIX
done

spark-submit --verbose \
  --master yarn \
  --num-executors 29 \
  --driver-memory 180G \
  --executor-memory 180G \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=50G \
  --conf spark.task.maxFailures=4 \
  --conf spark.worker.timeout=1200000 \
  --conf spark.network.timeour=1200000 \
  --jars $JARNAME \
  --class org.apache.spark.climate.ParquetCombiner \
    $JARNAME \
    $OUTDEST \
    2>&1 | tee $COMBINELOGNAME

hdfs dfs -get CFSROparquet/origcolindices0/part-00000 cols0
hdfs dfs -get CFSROparquet/origcolindices1/part-00000 cols1
hdfs dfs -get CFSROparquet/origcolindices2/part-00000 cols2
hdfs dfs -get CFSROparquet/origcolindices3/part-00000 cols3
hdfs dfs -get CFSROparquet/origcolindices4/part-00000 cols4
cat cols0 cols1 cols2 cols3 cols4 > origcolindices
hdfs dfs -put origcolindices CFSROparquet/origcolindices
rm cols0 cols1 cols2 cols3 cols4 origcolindices

