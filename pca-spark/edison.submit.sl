#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 100
#SBATCH -t 5:00:00
#SBATCH --qos=premium 
#SBATCH --ccm
#SBATCH -o edison-spark-%j.out
#SBATCH -e edison-spark-%j.err

module load spark/1.6.0

start-all.sh
#module load collectl
#start-collectl.sh
sleep 300

cd /project/projectdirs/paralleldb/spark/benchmarks/pca_climate/large-scale-climate
src/nersc.computeEOFs.sh /global/project/projectdirs/paralleldb/spark/benchmarks/pca_climate/large-scale-climate/target/scala-2.10/large-scale-climate-assembly-0.0.1.jar

stop-all.sh

#stop-collectl.sh

