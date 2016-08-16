#!/bin/bash -l

#SBATCH -p debug     
#SBATCH -A dasrepo
#SBATCH -N 2       
#SBATCH -t 00:02:00  
#SBATCH -J 1node    

srun -N 2 -n 64 --cpu-freq=21000000 ./bench_nmf 1 /global/cscratch1/sd/aditya08/daya-bay/single_630000.h5 inputs 3149874 192 8
