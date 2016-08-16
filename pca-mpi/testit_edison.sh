#!/bin/bash -l
module load cray-hdf5-parallel
cc pca.c -I$HDF5_INCLUDE_OPTS -static -lhdf5 -larpack -L. -L$CRAY_LD_LIBRARY_PATH
srun -u -n 5 ./a.out test.hdf5 temperatures 10 4 1
