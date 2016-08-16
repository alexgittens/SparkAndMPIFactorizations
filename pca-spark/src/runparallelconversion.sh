#!/usr/bin/env bash
# Call with runparallelconversion.sh AWS_KEY AWS_SECRET_KEY from the master node as user ubuntu

# if you change these numbers, note that MASTERPROCESSES + numslaves *
# SLAVEPROCESSES must be prime this setting works for 30 machines (29 slaves 1
# master) might need to change numbers afterwards and rerun to deal with files
# skipped b/c there were too many ssh connections (pay attention to the ssh
# outputs as you run this script to see if this is necessary)
SLAVEPROCESSES=7
MASTERPROCESSES=8
NUMSLAVES=$((`wc -l /home/mpich2.hosts | cut -f 1 -d " "` - 1))
NUMPROCESSES=$(($NUMSLAVES * $SLAVEPROCESSES + $MASTERPROCESSES))
JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64

CURDIR=`dirname $(realpath $0)`
LOGDIR=..
CONVERSIONSCRIPT=convertGRIBToCSV.py
MYREMAINDER=0

AWS_KEY=$1
AWS_SECRET_KEY=$2

#TODO: check these don't exist before running
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /user/ubuntu/CFSROcsv/vals
/usr/local/hadoop/bin/hdfs dfs -mkdir /user/ubuntu/CFSROcsv/recordDateMapping

cd $CURDIR
while [ $MYREMAINDER -lt $MASTERPROCESSES ]; do
  echo "Launching conversion process $MYREMAINDER on the master"
  python $CONVERSIONSCRIPT $AWS_KEY $AWS_SECRET_KEY $NUMPROCESSES $MYREMAINDER 2>&1 | tee runlog-master-$MYREMAINDER &
  let MYREMAINDER+=1
done

if [ $SLAVEPROCESSES -gt 0 ]; then
  for HOST in `tail -n +2 /home/mpich2.hosts`; do
    for ITER in `seq 1 $SLAVEPROCESSES`; do
      echo "Launching conversion process $MYREMAINDER on $HOST"
      (ssh $HOST "cd $CURDIR; JAVA_HOME=$JAVA_HOME python $CONVERSIONSCRIPT $AWS_KEY $AWS_SECRET_KEY $NUMPROCESSES $MYREMAINDER 2>&1 | tee runlog-$HOST-$ITER") &
      let MYREMAINDER+=1
    done
  done
fi

wait

# concatenate error_logs and delete intermediate runlogs
cat grib_conversion_error_log* > $LOGDIR/conversion_error_log 
rm grib_conversion_error_log*
rm runlog-*
