#!/bin/bash -l

DIRNAME=`dirname $0`
SIZE="SMALL"

NCPUS=$1
RANK=$2
SLACK=$3
NITERS=$4
NPARTS=$5
LOGDIR=$6 

(time -p spark-submit --verbose \
  --total-executor-cores $NCPUS\
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --executor-memory 40G \
  --master $SPARKURL \
  --driver-memory 40G \
  --conf spark.driver.maxResultSize=40g \
  --conf spark.task.maxFailures=1 \
  --conf spark.speculation=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  $DIRNAME/../../target/scala-2.10/heromsi-assembly-0.0.1.jar \
  df \
  file:///scratch3/scratchdirs/yyao/safe/cx/data_1_stripe/Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked.mat.df \
  0 0 \
  $LOGDIR/cx-out-$SIZE-$NCPUS-$RANK-$SLACK-$NITERS-$NPARTS.json \
  $RANK $SLACK $NITERS $NPARTS ) >& $LOGDIR/cx-log-$SIZE-$NCPUS-$RANK-$SLACK-$NITERS-$NPARTS-`date +%s`.log
#  csv \
#  file:///scratch3/scratchdirs/yyao/safe/cx/data_1_stripe/Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked.mat.csv \
#  8258911 131048 \
