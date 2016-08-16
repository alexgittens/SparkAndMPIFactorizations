#!/bin/bash -l

#SBATCH -p debug
#SBATCH -A m1523
#SBATCH -N 50
#SBATCH -t 00:05:00  
#SBATCH -J 50nodes_scaling
#SBATCH -o 50nodes_scaling

srun -n 1600 --cpu-freq=2300000 -u ./pca /global/cscratch1/sd/jialin/climate/oceanTemps.hdf5 temperatures 6349676 46715 20 output.hdf5
