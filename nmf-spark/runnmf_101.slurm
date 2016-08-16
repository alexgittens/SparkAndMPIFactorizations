#!/bin/bash -l

#SBATCH -N 101
#SBATCH -p regular
#SBATCH -t 30
#SBATCH --ccm
#SBATCH --reservation=INC0082890
#SBATCH -A dasrepo
#SBATCH --volume="/global/cscratch1/sd/gittens/nmf-spark/temp_fs_dir/nmf101:/tmp:perNodeCache=size=200G"

module load spark/1.5.1 sbt # collectl
export SPARK_LOCAL_DIRS="/tmp"
# start-collectl.sh
start-all.sh
sbt nodes101
stop-all.sh
# stop-collectl.sh

rm /global/cscratch1/sd/gittens/nmf-spark/temp_fs_dir/nmf101*
