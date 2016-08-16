#!/bin/env bash
#
# Runs the SVD variants:
# - CX, RSVD, tSVD on centered data
# and estimates the frobenius norm reconstruction error
# dumps parameters for the run and the errors
# from each method to OUTDEST
#
# Changes the JOBNAME each time the parameters change 
# to avoid overwriting old runs

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR="$DIR/../eventlogs"
DATADIR="$DIR/data"
JARNAME=$1

INSOURCE=hdfs:///Lewis_Dalisay_Peltatum_100G
NUMROWS=8258911
NUMCOLS=131048

# parameter settings taken from the google doc
RANK=8
SLACK=0
NITERS=2
NPARTS="" #default, 960, 1920

JOBNAME="svdvariants-$NUMROWS-$NUMCOLS-$RANK-$SLACK-$NITERS-$NPARTS"
OUTDEST="$DATADIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

spark-submit --verbose \
  --driver-memory 100G \
  --executor-memory 100G \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=72G \
  --jars $JARNAME \
  --class org.apache.spark.mllib.linalg.distributed.SVDVariants \
  $JARNAME \
  csv $INSOURCE $NUMROWS $NUMCOLS $OUTDEST $RANK $SLACK $NITERS $NPARTS \
  2>&1 | tee $LOGNAME
