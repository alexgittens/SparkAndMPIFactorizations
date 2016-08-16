#!/usr/bin/env bash
# Tests the NMF spark code using CFSR dataset (resized)
# Assumes we're on Cori

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR="$DIR/eventLogs"
OUTDIR="$DIR/data"
JARNAME=$1
NUMNODES=$2

# double precision: this is a 3GB dataset
#INSOURCE=/global/cscratch1/sd/jialin/dayabay/2016/data/one.h5
INSOURCE=/global/cscratch1/sd/aditya08/nmf_single_dset_stripe72/one.h5
VARIABLE=charge
NUMROWS=1099413914
NUMCOLS=192
RANK=10
if [ "$NUMNODES" -lt "100" ]
then
  NUMPARTITIONS=$((NUMNODES*64))
else
  NUMPARTITIONS=$((NUMNODES*32))
fi


JOBNAME="nmf-$VARIABLE-$NUMROWS-$NUMCOLS-$NUMPARTITIONS-$RANK"
OUTDEST="$OUTDIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already done successfully, stopping"; exit 1)

# Each core does a task, so we can take advantage of larger partitions when the
# core count is higher

# On Cori there are 32 cores/node and 128GB/node
NUMEXECUTORS=$NUMNODES
NUMCORES=32
DRIVERMEMORY=120G
EXECUTORMEMORY=120G

spark-submit --verbose \
  --master $SPARKURL \
  --num-executors $NUMEXECUTORS \
  --executor-cores $NUMCORES \
  --driver-memory $DRIVERMEMORY \
  --executor-memory $EXECUTORMEMORY \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=2G \
  --jars $JARNAME \
  --class org.apache.spark.mllib.nmf \
  $JARNAME \
  $INSOURCE $VARIABLE $NUMROWS $NUMCOLS $NUMPARTITIONS $RANK $OUTDEST \
  2>&1 | tee $LOGNAME
