#SHELL="/bin/bash"
.SHELLARGS="-l -c"
FNAMEIN=/global/cscratch1/sd/gittens/conversion-code/ocean_conversion/testOutputs/ocean.h5
VARNAME=rows
LATFNAME=/global/cscratch1/sd/gittens/conversion-code/ocean_conversion/testOutputs/observedLatitudes.csv
NUMROWS=6177583
NUMCOLS=8096
RANK=100
OUTFNAME=eofs.h5

all: cori

cori: pca.c
	module load cray-hdf5-parallel; \
	cc -g -o pca pca.c -larpack -L.

edison: pca.c
	module load cray-hdf5-parallel; \
	cc -o pca pca.c -I$$HDF5_INCLUDE_OPTS -static -lhdf5 -larpack -L. -L$$CRAY_LD_LIBRARY_PATH

coritest: cori
	module load cray-hdf5-parallel; \
	srun -u -n 5 ./pca test2.hdf5 temperatures rowWeights.csv 10 4 3 out.hdf5

coriproduction: cori
	module load cray-hdf5-parallel; \
	srun -u -c 1 -n 187 ./pca ${FNAMEIN} ${VARNAME} ${LATFNAME} ${NUMROWS} ${NUMCOLS} ${RANK} ${OUTFNAME}
