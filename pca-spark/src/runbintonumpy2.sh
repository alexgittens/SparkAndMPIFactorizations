#!/usr/bin/env bash
# Converts the binary dump of the EOFs and the data on row and column indices into a nice numpy dataset
# TODO: get the colindices and row indices from S3

CURDIR=`dirname $(readlink -f $0)`
WORKINGDIR=$CURDIR/..
JARFILE=$1
SPARKHOME=/opt/Spark
#SPARKHOME=/root/spark

#INBIN=$WORKINGDIR/data/eofs-centerOverAllObservations-20.bin
#INBIN=$WORKINGDIR/data/eofs-standardize-20.bin
INBIN=$WORKINGDIR/data/eofs-cosLat+centerOverAllObservations-100.bin
OUTCSV=$WORKINGDIR/data
OUTNUMPY=$OUTCSV

spark-submit is a BAD idea (unnecessary), try to just get the right classpath and use scala?
spark-submit --master $SPARKURL --verbose \
   --driver-memory 40G \
   --class org.apache.spark.mllib.linalg.distributed.ConvertDump \
   $JARFILE $INBIN $OUTCSV

python $CURDIR/convertEofsToNetCDF.py
