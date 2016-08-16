#!/bin/bash -l

#SBATCH -N 1
#SBATCH -p debug
#SBATCH -t 5
#SBATCH --ccm
##SBATCH --volume="/global/cscratch1/sd/gittens/nmf-spark/temp_fs_dir/nmftest:/tmp:perNodeCache=size=200G"

module load spark/1.5.1 sbt #collectl
export SPARK_LOCAL_DIRS="/tmp,/dev/shm"
#start-collectl.sh
start-all.sh
sbt runTest
stop-all.sh
#stop-collectl.sh

rm /global/cscratch1/sd/gittens/nmf-spark/temp_fs_dir/nmftest*
