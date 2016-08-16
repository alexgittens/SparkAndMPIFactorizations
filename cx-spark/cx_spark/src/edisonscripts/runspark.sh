#!/bin/bash -l
SPARK_LOCAL_DIRS="/tmp,/tmp,/tmp,/scratch3/scratchdirs/beehive"
start-all.sh

#wait for 100% of all workers to come online, give up after 200 seconds
waitforstartup.sh 100 200
RESULT=$?

if [ "$RESULT" == "0" ]
then

SCRIPTDIR=/global/homes/b/beehive/cx/sc-2015/cx_spark/src/edisonscripts/
TARGETDIR=/scratch1/scratchdirs/beehive/cxedisonruns/$PBS_JOBID
mkdir -p $TARGETDIR

#$SCRIPTDIR/runsmall.sh 960 16 0 5 ""   $TARGETDIR
#$SCRIPTDIR/runsmall.sh 960 16 0 5 960  $TARGETDIR
#$SCRIPTDIR/runsmall.sh 960 16 0 5 1920 $TARGETDIR

#$SCRIPTDIR/runsmall.sh 960 32 0 5 ""   $TARGETDIR
#$SCRIPTDIR/runsmall.sh 960 32 0 5 960  $TARGETDIR
#$SCRIPTDIR/runsmall.sh 960 32 0 5 1920 $TARGETDIR

$SCRIPTDIR/runbig.sh 960 16 0 5 ""   $TARGETDIR
#$SCRIPTDIR/runbig.sh 960 16 0 5 4800 $TARGETDIR
#$SCRIPTDIR/runbig.sh 960 16 0 5 9600 $TARGETDIR

#$SCRIPTDIR/runbig.sh 960 32 0 5 ""   $TARGETDIR
#$SCRIPTDIR/runbig.sh 960 32 0 5 4800 $TARGETDIR
#$SCRIPTDIR/runbig.sh 960 32 0 5 9600 $TARGETDIR

else
  echo "ERROR: Spark didn't start correctly, giving up for now, please try again!"
fi

#stop-all.sh necessary for more complete logs
stop-all.sh
