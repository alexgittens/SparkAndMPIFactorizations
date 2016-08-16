#!/bin/bash -l
set -o errexit
set -o pipefail
echo "starting job" >&2

remote_exec() {
    host="$1"
    shift
    cmd="$*"
    if [ -z "$host" -o "$host" = "localhost" ]; then
        /bin/bash -c "$cmd"
    else
        ssh "$host" "$cmd"
    fi
}

die() {
    msg="$1"
    echo "$msg" >&2
    exit 1
}

start_perflog() {
    host="$1"
    [ ! -z "$PERFLOG_DIR" ] || die "PERFLOG_DIR not set"
    outpath="$PERFLOG_DIR/collectl-$host"
    echo "starting collectl on $host" >&2
    remote_exec "$host" collectl \
        --daemon \
        --align \
        --filename "$outpath" \
        --flush 0 \
        --interval 2:4 \
        --subsys sbcdfijmnstZ \
        --procopts cw \
        ;
    echo "starting shmtrace on $host" >&2
    outpath="$PERFLOG_DIR/shmtrace-$host"
    remote_exec "$host" "$PWD/shmtrace.sh &> \"$outpath\" &"
}

if [ ! -z "$PERFLOG_DIR" ]; then
    echo "`date`: starting perflogs..." >&2
    start_perflog localhost &
    if [ ! -z "$SPARK_SLAVES" ]; then
        for host in `cat ${SPARK_SLAVES}`; do
            start_perflog "$host" &
        done
    fi
    echo "`date`: waiting for perflogs to start..." >&2
    wait
    echo "`date`: done waiting for perflogs" >&2
else
    echo "skipping perflog" >&2
fi

pyfiles=`find . -name \*.py | paste -sd , -`
echo "pyfiles: $pyfiles" >&2

export SPARK_CONF_DIR=$PWD/spark-conf

echo "`date`: starting spark cluster" >&2
start-all.sh
echo "`date`: submitting job" >&2
#    --conf spark.task.maxFailures=1 \
spark-submit \
    --verbose \
    --conf spark.task.cpus=12 \
    --conf spark.python.worker.memory=4G \
    --conf spark.cleaner.referenceTracking=true \
    --conf spark.cleaner.referenceTracking.blocking=true \
    --conf spark.cleaner.referenceTracking.blocking.shuffle=true \
    --conf spark.ui.showConsoleProgress=false \
    --conf spark.driver.maxResultSize=2G \
    --conf spark.akka.frameSize=256 \
    --conf pbs.jobId="$PBS_JOBID" \
    --conf spark.eventLog.enabled=true  \
    --conf spark.eventLog.dir="$SPARK_EVENTLOG_DIR"  \
    --master "$SPARKURL"  \
    --executor-memory 48G  \
    --driver-memory 48G \
    --py-files $pyfiles \
    $* \
    1>&2 \
    ;
stop-all.sh
