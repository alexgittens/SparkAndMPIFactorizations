#!/usr/bin/env bash
# Computes the 3D EOFs using multiple settings on the CSFR dataset:
# different ranks, exact and randomized PCAs
#
# Note: you will need to set the spark master url in runOneJob.sh manually for EC2

CURDIR="$(cd "`dirname "$0"`"; pwd)"

PLATFORM="CORI"
#PLATFORM="EC2"

#$CURDIR/runOneJob.sh $1 $PLATFORM 10 exact
#$CURDIR/runOneJob.sh $1 $PLATFORM 10 randomized

$CURDIR/runOneJob.sh $1 $PLATFORM 20 exact
#$CURDIR/runOneJob.sh $1 $PLATFORM 20 randomized

#$CURDIR/runOneJob.sh $1 $PLATFORM 40 exact
#$CURDIR/runOneJob.sh $1 $PLATFORM 40 randomized
