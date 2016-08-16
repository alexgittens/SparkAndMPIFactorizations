#!/bin/bash -l

#SBATCH -p regular
#SBATCH -A dasrepo
#SBATCH -N 1200
#SBATCH -t 00:15:00  
#SBATCH -J 1200nodes_scaling
#SBATCH -o 1200nodes_scaling
#SBATCH --reservation=INC0082890

srun -n 38400 --cpu-freq=2300000 -u ./pca /global/cscratch1/sd/gittens/large-climate-dataset/data/production/Q.h5 rows 26542080 81600 20 output.hdf5
