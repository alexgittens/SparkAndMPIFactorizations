#!/usr/bin/env bash
# Computes the 3D EOFs using CSFR dataset
# You need to change the memory setting and location of the data for different platforms

DIR="$(cd "`dirname "$0"`"/..; pwd)"
LOGDIR="$DIR/eventLogs"
DATADIR="$DIR/data"
JARNAME=$1
INSOURCE=$2
# for the small dataset
#NUMROWS=104
#NUMCOLS=6349676
#FORMAT=csv
#INSOURCE=hdfs://master:9000/user/ubuntu/smallclimatevals
#MASKSOURCE='notmasked'
#MASKSOURCE=hdfs://master:9000/user/ubuntu/CSFROcsv/mask/part-00000.gz
# for the large dataset
NUMROWS=46715
NUMCOLS=6349676
#INSOURCE=hdfs://`hostname`:9000/user/ubuntu/CFSROparquet
#INSOURCE=hdfs://`hostname`:9000/user/root/CFSROparquet
#INSOURCE=$SCRATCH/CFSROparquet

PREPROCESS="centerOverAllObservations"
NUMEOFS=20

JOBNAME="eofs-$PREPROCESS-$NUMEOFS"
OUTDEST="$DATADIR/$JOBNAME.bin"
LOGNAME="$JOBNAME.log"

[ -e $OUTDEST ] && (echo "Job already run successfully, stopping"; exit 1)

# On EC2 there are 32 cores/node and 244GB/node 
# use 30 executors b/c that's what did for CX (apparently, but I wonder if it helps to increase executors)
# use as much memory as available so can cache the entire 2GB RDD
#--num-executors 30
#--driver-memory 210G
#--executor-memory 210G
#--master "spark://ec2-54-200-88-120.us-west-2.compute.amazonaws.com:7077"  # for example

# On Cori there are 32 cores/node and 128GB/node
# use 30 executors b/c that's what did for CX (apparently, but I wonder if it helps to increase executors)
# can only cache ? < 100 % of the RDD
#--num-executors 30
#--driver-memory 100G
#--executor-memory 100G
#--master $SPARKURL
#LMG 2/13/16
#Crashed out with "Remote RPC client disassociated." with
# --num-executors 29
# --driver-memory 100G \
#  --executor-memory 100G \
#  --total-executor-cores 400 \
#Trying slightly smaller exector memory

spark-submit --verbose \
  --master $SPARKURL \
  --driver-memory 70G \
  --executor-memory 100G \
#  --total-executor-cores 400 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=$LOGDIR \
  --conf spark.driver.maxResultSize=30G \
  --conf spark.task.maxFailures=4 \
  --conf spark.worker.timeout=1200000 \
  --conf spark.network.timeout=1200000 \
  --jars $JARNAME \
  --class org.apache.spark.mllib.climate.computeEOFs \
  $JARNAME \
  $INSOURCE $NUMROWS $NUMCOLS $PREPROCESS $NUMEOFS $OUTDEST exact \
  2>&1 | tee $LOGNAME
