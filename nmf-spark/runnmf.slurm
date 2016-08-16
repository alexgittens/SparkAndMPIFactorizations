#!/bin/bash -l

#SBATCH -N 2
#SBATCH -p debug
#SBATCH -t 4
#SBATCH --ccm
#SBATCH --qos=premium

module load spark/1.5.1 sbt
export SPARK_LOCAL_DIRS="/tmp,/dev/shm"
start-all.sh
sbt submit
