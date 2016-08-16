#!/bin/env bash
#
# Runs the PCA variants:
# - CX on uncentered data
# - CX, RPCA, PCA on centered data
# and estimates the frobenius norm reconstruction error
# dumps parameters for the run, the errors, and the 
# matrix decompositions from each methods to OUTDEST
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
RANK=16 #32
SLACK=0
NITERS=5
NPARTS="" #default, 960, 1920

JOBNAME="pcavariants-$NUMROWS-$NUMCOLS-$RANK-$SLACK-$NITERS-$NPARTS"
OUTDEST="$DATADIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

spark-submit --verbose \
  --driver-memory 64G \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=64g \
  --jars $JARNAME \
  --class org.apache.spark.mllib.linalg.distributed.PCAvariants \
  $JARNAME \
  csv $INSOURCE $NUMROWS $NUMCOLS $OUTDEST $RANK $SLACK $NITERS $NPARTS \
  2>&1 | tee $LOGNAME
