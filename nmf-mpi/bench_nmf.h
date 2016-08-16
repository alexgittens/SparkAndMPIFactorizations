#ifndef __BENCH_NMF_H__
#define __BENCH_NMF_H__

#include "mpi.h"
#include "omp.h"
#include "mkl.h"
#include "TAU.h"
#include "CANDMC.h"
#include "readHdf5.h"


#define ALIGN 16
extern "C" {
	#include "nnls.h"
}

void xray(double *A, int r);

#endif
