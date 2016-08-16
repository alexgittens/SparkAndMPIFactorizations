#!/usr/bin/env bash
# Generates a 200 by 55 random matrix as input to test the PCA variants
# you may need to change the name of the spark assembly jar to whatever it is on your machine
# Note that the CWD when this is run by sbt is cx_spark
export DATADIR="$(cd "`dirname "$0"`"/..; pwd)"/data
export SPARKJAR=$SPARKHOME/lib/spark-assembly-1.5.0-SNAPSHOT-hadoop2.4.0.jar

$DATADIR/genrandomlowrank.py 20000 55 10 $DATADIR/input.csv /dev/null --csv
spark-submit --master local[2] --verbose --class org.apache.spark.mllib.linalg.distributed.SVDVariants $1 csv file://$DATADIR/input.csv 55 20000 $DATADIR/output.bin 10 2 5
#scala -cp $1:$SPARKJAR org.apache.spark.mllib.linalg.distributed.ConvertDump $DATADIR/output.bin $DATADIR

