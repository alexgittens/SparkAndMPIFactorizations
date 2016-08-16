#ifndef __READHDF5_H__
#define __READHDF5_H__

#include "mpi.h"
#include "hdf5.h"

#define ALIGN 16
void readHdf5(const char *fname, const char *dsname, double *dA, MPI_Comm, MPI_Info);

#endif
