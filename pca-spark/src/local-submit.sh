#!/bin/bash -l
JARNAME=$1
INSOURCE=$2
NUMROWS=$3
NUMCOLS=$4
PREPROCESS=$5
NUMEOFS=$6
OUTDEST=$7
RANDOMIZEDEQ=$8

if [ $# -lt 8 ]
then
echo  'JARNAME $INSOURCE $NUMROWS $NUMCOLS $PREPROCESS $NUMEOFS $OUTDEST $RADNDOMIZEDEQ'
exit
fi

LOGDIR=$SCRATCH/spark
/usr/common/software/spark/1.5.1/bin/spark-submit --verbose \
  --master local[5] \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --jars $JARNAME \
  --class org.apache.spark.mllib.climate.computeEOFs \
  $JARNAME \
  $INSOURCE $NUMROWS $NUMCOLS $PREPROCESS $NUMEOFS $OUTDEST $RANDOMIZEDEQ \
