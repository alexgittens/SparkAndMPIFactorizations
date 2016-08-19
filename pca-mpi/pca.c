#include "hdf5.h"
#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include "cblas.h"
#include "lapacke.h"
#include <math.h>

// NB: CBLAS has nonconstant overhead, because after operations, it stores the output in row major
// TODO : read the dimensions from the input dataset

extern void dsaupd_(int * ido, char * bmat, int * n, char * which,
                    int * nev, double * tol, double * resid, 
                    int * ncv, double * v, int * ldv,
                    int * iparam, int * ipntr, double * workd,
                    double * workl, int * lworkl, int * info);

extern void dseupd_(int *rvec, char *HowMny, int *select,
                    double *d, double *Z, int *ldz,
                    double *sigma, char *bmat, int *n,
                    char *which, int *nev, double *tol,
                    double *resid, int *ncv, double *V,
                    int *ldv, int *iparam, int *ipntr,
                    double *workd, double *workl,
                    int *lworkl, int *info);

lapack_int LAPACKE_dgesdd(int matrix_layout, char jobz, lapack_int m,
    lapack_int n, double * a, lapack_int lda, double * s, double * u,
    lapack_int ldu, double * vt, lapack_int ldvt);

// future optimizations: use memory-aligned mallocs

void printvec(char * label, double * v, int numentries);
void printmat(char * label, double * mat, int height, int width);
void mattrans(const double * A, long m, long n, double * B);
void flipcolslr(double * A, long m, long n); 

#define DEBUGATAFLAG 0
#define DEBUG_DISTMATVEC_FLAG 0
#define DISPLAY_FLAG 0
#define MAX_ITERS 100000

// Computes C = A'*(A*Omega)
// Scratch should have the size of (A*Omega)
void multiplyGramianChunk(double A[], double Omega[], double C[], double Scratch[], int rowsA, int colsA, int colsOmega); 

// computes a distributed matrix vector product against v, and updates v: v <- A'*A*v
void distributedGramianVecProd(double v[]);

// computes A*Omega and stores in C, Scratch should have dimensions of C
void multiplyAChunk(double A[], double Omega[], double C[], int rowsA, int colsA, int colsOmega);

// computes A*mat and stores result on the rank 0 process in matProd
void distributedMatMatProd(double mat[], double matProd[]);

// computes means of the rows of A, subtracts them from A, and returns them  in meanVec
void computeRowMeans(double meanVec[]);

// rescales the rows of A by the given weights
void rescaleRows(double weights[]);

// loads the latitudes of each row from the given file, and returns the corresponding vector of row weights in rowWeights
void loadRowWeights(char * weightsFname, double * rowWeights); 

/* Local variables */
int mpi_size, mpi_rank;
MPI_Comm comm;
MPI_Info info;
double * Alocal; // contains the batch of rows for this processor
double * Scratch, * Scratch2, * Scratch3; //Scratch and Scratch2 are used in multiplyGramianChunk, Scratch3 in distributedMatMatProd
double * AV;
double * meanVec; // contains the vector of row means
double * rowWeights; // contains the weights to multiply each row of A by
int numcols, numrows, numeigs; // number of columns and rows in A, PCs desired
int localrows, startingrow; // number of rows on this processor, index of the first row on this processor (0-based)
int * elementcounts, * elementoffsets; // for an n-by-k matrix, contains the number of matrix elements on each processor, indices of the first element on each processor (only on rank 0)
int * rowcounts, * rowoffsets; // for an n-by-k matrix, contains the number of rows on each processor, indices of the first row on each processor (only on rank 0)


int main(int argc, char **argv) {

    char * infilename, *weightsFname, * datasetname, * outfname;
	double elapstr, elapstp;
    /* HDF5 API definitions */
    hid_t plist_id, daccess_id, file_id, dataset_id, filespace, memspace; 
    herr_t status; 
    hsize_t offset[2], count[2], offset_out[2];

    /* MPI variables */
    comm = MPI_COMM_WORLD;
    info = MPI_INFO_NULL;

    /* Initialize MPI */
    MPI_Init(&argc, &argv);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);
	elapstr = MPI_Wtime();
    
    infilename = argv[1];
    datasetname = argv[2];
    weightsFname = argv[3];
    numrows = atoi(argv[4]);
    numcols = atoi(argv[5]);
    numeigs = atoi(argv[6]);
    outfname = argv[7];

    if (mpi_rank == 0) {
        printf("Arguments:\n");
        printf("\tInput filename: %s\n", infilename);
        printf("\tInput dataset: %s\n", datasetname);
        printf("\tRow to latitude mapping file: %s\n", weightsFname);
        printf("\tRow count: %d\n", numrows);
        printf("\tColumn count: %d\n", numcols);
        printf("\tTarget rank: %d\n", numeigs);
        printf("\tOutput filename: %s\n", outfname);
    }

    /* Allocate the correct portion of the input to each processor */
    double rdmtxstr = MPI_Wtime();
	
	int littlePartitionSize = numrows/mpi_size;
    int bigPartitionSize = littlePartitionSize + 1;
    int numLittlePartitions = mpi_size - numrows % mpi_size;
    int numBigPartitions = numrows % mpi_size;

    if (mpi_rank < numBigPartitions) {
        localrows = bigPartitionSize;
        startingrow = bigPartitionSize*mpi_rank;
    } else {
        localrows = littlePartitionSize;
        startingrow = bigPartitionSize*numBigPartitions + 
                      littlePartitionSize*(mpi_rank - numBigPartitions);
    }
    //printf("Rank %d: assigned %d rows, %d--%d\n", mpi_rank, localrows, startingrow, startingrow + localrows - 1);

    // assuming double inputs, check that the chunks aren't too big to be read by HDF5 in parallel mode
    if (bigPartitionSize*numcols >= 268435456 && mpi_rank == 0) {
        printf("Error: MPIIO-based HDF5 is limited to reading 2GiB at most in each call to read; try increasing the number of processors\n");
        exit(-1);
    }

    // compute some counts and offsets needed for MPI gatherv-type operations
    if (mpi_rank == 0) {
        elementcounts = (int *) malloc( mpi_size * sizeof(int));
        elementoffsets = (int *) malloc( mpi_size * sizeof(int));
        rowcounts = (int *) malloc( mpi_size * sizeof(int));
        rowoffsets = (int *) malloc( mpi_size * sizeof(int));
        int idx;
        for(idx = 0; idx < numBigPartitions; idx = idx + 1) {
            elementcounts[idx] = bigPartitionSize * numeigs;
            elementoffsets[idx] = bigPartitionSize * numeigs * idx;

            rowcounts[idx] = bigPartitionSize;
            rowoffsets[idx] = bigPartitionSize * idx;
        }
        for(idx = numBigPartitions; idx < mpi_size; idx = idx + 1) {
            elementcounts[idx] = littlePartitionSize * numeigs;
            elementoffsets[idx] = bigPartitionSize * numeigs * numBigPartitions + 
                              littlePartitionSize * numeigs * (idx - numBigPartitions);

            rowcounts[idx] = littlePartitionSize;
            rowoffsets[idx] = bigPartitionSize * numBigPartitions + littlePartitionSize * (idx - numBigPartitions);
        }
    }

    /* Load my portion of the data, compute row means, and center the rows */

    plist_id = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(plist_id, comm, info);

    file_id = H5Fopen(infilename, H5F_ACC_RDONLY, plist_id);
    dataset_id = H5Dopen(file_id, datasetname, H5P_DEFAULT);

    count[0] = localrows;
    count[1] = numcols;
    offset[0] = mpi_rank < numBigPartitions ? ( mpi_rank * bigPartitionSize ) : 
                (numBigPartitions * bigPartitionSize + (mpi_rank - numBigPartitions) * littlePartitionSize );
    offset[1] = 0;

    filespace = H5Dget_space(dataset_id);
    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, count, NULL);
    if (status < 0) {
        printf("Error selecting input file hyperslab in process %d\n", mpi_rank);
        exit(-1);
    }

    memspace = H5Screate_simple(2, count, NULL);
    offset_out[0] = 0;
    offset_out[1] = 0;
    Alocal = (double *) malloc( localrows * numcols *sizeof(double));
    status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, NULL, count, NULL);
    daccess_id = H5Pcreate(H5P_DATASET_XFER);
    if (status < 0) {
        printf("Error selecting memory hyperslab in process %d\n", mpi_rank);
        exit(-1);
    }
    if (Alocal == NULL) {
        printf("Out of memory in process %d\n", mpi_rank);
        exit(-1);
    }
    // collective io seems slow for this
    H5Pset_dxpl_mpio(daccess_id, H5FD_MPIO_INDEPENDENT);

    //MPI_Barrier(comm);
    if (mpi_rank == 0) {
        printf("Loading matrix\n");
    }
    status = H5Dread(dataset_id, H5T_NATIVE_DOUBLE, memspace, filespace, daccess_id, Alocal);
    if (status < 0) {
        printf("Error reading from file (after hyperslab selections) in process %d\n", mpi_rank);
        exit(-1);
    }

    H5Pclose(daccess_id);
    H5Dclose(dataset_id);
    H5Sclose(memspace);
    H5Sclose(filespace);
    H5Pclose(plist_id);
    H5Fclose(file_id);

	double rdmtxstp = MPI_Wtime();
	if(mpi_rank == 0) {
		printf("Time to readHDF5 matrix: %f\n", rdmtxstp - rdmtxstr); 
    }
	
    double pdmtxstr = MPI_Wtime();
    if (mpi_rank == 0) {
        meanVec = malloc(numrows * sizeof(double));
        rowWeights = malloc(numrows * sizeof(double));
        loadRowWeights(weightsFname, rowWeights);
    }
    computeRowMeans(meanVec);
    rescaleRows(rowWeights);
    double pdmtxstp = MPI_Wtime();
    if (mpi_rank == 0) {
        printf("Time to preprocess the data (center rows and weight rows by sqrt(cos(lat))): %f\n", pdmtxstp - pdmtxstr);
    }

    double * vector = (double *) malloc( numcols * sizeof(double));
    Scratch = (double *) malloc( localrows * sizeof(double));
    Scratch2 = (double *) malloc( numcols * sizeof(double));
    Scratch3 = (double *) malloc( localrows * numeigs * sizeof(double));
    double * singVals = (double *) malloc( numeigs * sizeof(double));
    double * rightSingVecs = (double *) malloc( numeigs * numcols * sizeof(double));

    if (vector == NULL || Scratch == NULL || Scratch2 == NULL || Scratch3 == NULL || singVals == NULL || rightSingVecs == NULL) {
        printf("Out of memory on process %d\n", mpi_rank);
        exit(-1);
    }

    // initial call to arpack
    int ido = 0;
    int ncv = 2*numeigs > numcols ? numcols : 2*numeigs; // ncv > nev and ncv < n (but ncv >= 2*nev recommended)
    int maxiter = 30;
    double tol = 1e-13;
    double * resid = (double *) malloc( numcols * sizeof(double));
    double * v = (double *) malloc(numcols * ncv *sizeof(double));
    int iparam[11] = {1, 0, 30, 1, 0, 0, 1, 0, 0, 0, 0};
    iparam[2] = maxiter;
    int ipntr[11];
    double * workd = (double *) malloc(3*numcols*sizeof(double));
    int lworkl = ncv*(ncv + 8);
    double * workl = (double *) malloc(lworkl*sizeof(double));
    int arpack_info = 0;

    if ( resid == NULL || v == NULL || workd == NULL || workl == NULL) {
        printf("Out of memory on process %d\n", mpi_rank);
        exit(-1);
    }

    char bmat = 'I';
    char which[3] = "LM";

    // initialize ARPACK
    if (mpi_rank == 0) {
        printf("Computing the EVD of the Gram matrix\n");
        dsaupd_(&ido, &bmat, &numcols, which,
                &numeigs, &tol, resid,
                &ncv, v, &numcols,
                iparam, ipntr, workd,
                workl, &lworkl, &arpack_info);
        //printf("Info : %d\n", arpack_info);
        cblas_dcopy(numcols, workd + ipntr[0] - 1, 1, vector, 1); 
        //printf("ipntr[0] - 1 = %d, should be less than %d\n", ipntr[0] - 1, 3*numcols); 
    }
    MPI_Bcast(&ido, 1, MPI_INTEGER, 0, comm);
    MPI_Bcast(vector, numcols, MPI_DOUBLE, 0, comm);

    // keep calling ARPACK until done
    int niters = 0;
	double tgrammv = 0., grammvstr, grammvstp;
	double tarpk = 0., arpkstr, arpkstp;
	while(niters < MAX_ITERS) {
            /*if (mpi_rank == 0) {
                printf("Return code %d\n", ido);
            }*/
            if (ido == 1 || ido == -1) {
				grammvstr = MPI_Wtime();
                distributedGramianVecProd(vector);
				grammvstp = MPI_Wtime();
				tgrammv += grammvstp - grammvstr;
				arpkstr = MPI_Wtime();
                if (mpi_rank == 0) {
                    //printf("ipntr[1] - 1 = %d, should be less than %d\n", ipntr[1] - 1, 3*numcols); 
					cblas_dcopy(numcols, vector, 1, workd + ipntr[1] - 1, 1); // y = A x
                    if (DEBUG_DISTMATVEC_FLAG) { 
                        printvec("Input vector: ", workd + ipntr[0] - 1, numcols); 
                        printvec("Output vector: ", workd + ipntr[1] - 1, numcols);
                    }
                    dsaupd_(&ido, &bmat, &numcols, which,
                            &numeigs, &tol, resid,
                            &ncv, v, &numcols,
                            iparam, ipntr, workd,
                            workl, &lworkl, &arpack_info);
                    cblas_dcopy(numcols, workd + ipntr[0] - 1, 1, vector, 1);
                }
                MPI_Bcast(vector, numcols, MPI_DOUBLE, 0, comm); 
            }
            MPI_Bcast(&ido, 1, MPI_INTEGER, 0, comm);
			arpkstp = MPI_Wtime();
			tarpk += arpkstp - arpkstr;
    	niters++;
	}

    if (mpi_rank == 0) {
        if (niters == MAX_ITERS) {
            printf("Terminated eval decomposition due to reaching max_iters mat-vec products: %d\n", MAX_ITERS);
        } else {
            printf("Completed eval decomposition after %d mat-vec products\n", niters);
        }
    }

	if(mpi_rank == 0){
		printf("Time to perform distributed Gram matrix-vectors: %f\n", tgrammv); 
		printf("Time spend in arpack: %f\n", tarpk); 
	}


    if (mpi_rank == 0) {
        int num_iters = iparam[8];
        int num_evals = iparam[4];
        double trtzstr = MPI_Wtime();
		printf("Used %d matrix-vector products to converge to %d eigenvalue\n", num_iters, num_evals);
        printf("Extracting the right singular vectors\n");
        //printf("Return value: %d\n", arpack_info);

        int rvec = 1; // compute Ritz vectors
        char HowMny = 'A';
		//printf("Changed select dim from numeigs to ncv\n");
        int * select = (int * ) malloc(ncv * sizeof(int));
        double sigma = 0;
    	
        // eigenvalues and eigenvectors are returned in ascending order
        // eigenvectors are returned in column major form
        double * svtranspose = (double *) malloc( numeigs * numcols * sizeof(double));
        //printf("Calling dseupd_ from rank 0\n");
		if(svtranspose == NULL || select == NULL)
        	printf("Uh Oh, svtranspose is NULL\n");
			
		dseupd_(&rvec, &HowMny, select,
                singVals, svtranspose, &numcols,
                &sigma, &bmat, &numcols,
                which, &numeigs, &tol, 
                resid, &ncv, v,
                &numcols, iparam, ipntr,
                workd, workl, 
                &lworkl, &arpack_info);

        //printf("Calling mattrans from rank 0\n");
        mattrans(svtranspose, numeigs, numcols, rightSingVecs);
        //printf("Calling flipcolslr from rank 0\n");
        flipcolslr(rightSingVecs, numcols, numeigs);

        //printmat("right singular vectors (in descending order left to right)\n", rightSingVecs, numcols, numeigs);
		double trtzstp = MPI_Wtime();
		printf("Time to compute Ritz vectors: %f\n", trtzstp - trtzstr);
        free(svtranspose); 
        free(select);
    }

    if (mpi_rank ==0) {
        printf("Computing AV\n");
    }
    double tcompavstr, tcompavstp;
	tcompavstr = MPI_Wtime();
	MPI_Bcast(rightSingVecs, numeigs*numcols, MPI_DOUBLE, 0, comm);
    double * AV = (double *) malloc( numrows * numeigs * sizeof(double));
    distributedMatMatProd(rightSingVecs, AV);
	tcompavstp = MPI_Wtime();
    if (mpi_rank == 0) {
		printf("Time to compute AV: %f\n", tcompavstp - tcompavstr);
        printmat("best low-rank approximation of A\n", AV, numrows, numeigs);
    }


    // dgesdd returns its singular values in descending order
    // note that the right singular vectors of AV should by definition be the identity 
    if (mpi_rank == 0) {

        printf("Computing the SVD of AV\n");
        double tsddstr, tsddstp;
		tsddstr = MPI_Wtime();
		double * U = (double *) malloc( numrows * numeigs * sizeof(double));
        double * VT = (double *) malloc( numeigs * numeigs * sizeof(double));
        double * V = (double *) malloc( numeigs * numeigs * sizeof(double));
        double * finalV = (double *) malloc( numcols * numeigs * sizeof(double));
        double * singvals = (double *) malloc( numeigs * sizeof(double));
        LAPACKE_dgesdd(LAPACK_ROW_MAJOR, 'S', numrows, numeigs, AV, numeigs, singvals, U, numeigs, VT, numeigs);
        mattrans(VT, numeigs, numeigs, V);
        cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, numcols, numeigs, numeigs, 1.0, rightSingVecs, numeigs, V, numeigs, 0.0, finalV, numeigs);
        tsddstp = MPI_Wtime();
		printf("Time to compute SVD of AV: %f\n", tsddstp - tsddstr);
        printvec("top singular values of A\n", singvals, numeigs);
        printmat("top left singular vectors of A\n", U, numrows, numeigs);
        printmat("top right singular vectors of A\n", finalV, numcols, numeigs);

        // Write the output
        printf("Writing the output of the SVD to file\n");
        file_id = H5Fcreate(outfname, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
        hsize_t dims[2];
        hid_t dataspace_id;
        plist_id = H5Pcreate(H5P_DATASET_XFER);

        dims[0] = numrows;
        dims[1] = numeigs;
        dataspace_id = H5Screate_simple(2, dims, NULL);
        dataset_id = H5Dcreate2(file_id, "/U", H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, dataspace_id, plist_id, U);
        H5Dclose(dataset_id);
        H5Sclose(dataspace_id);

        dims[0] = numcols;
        dims[1] = numeigs;
        dataspace_id = H5Screate_simple(2, dims, NULL);
        dataset_id = H5Dcreate2(file_id, "/V", H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, dataspace_id, plist_id, finalV);
        H5Dclose(dataset_id);
        H5Sclose(dataspace_id);

        dims[0] = numeigs;
        dataspace_id = H5Screate_simple(1, dims, NULL);
        dataset_id = H5Dcreate2(file_id, "/S", H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, dataspace_id, plist_id, singvals);
        H5Dclose(dataset_id);
        H5Sclose(dataspace_id);

        dims[0] = numrows;
        dataspace_id = H5Screate_simple(1, dims, NULL);
        dataset_id = H5Dcreate2(file_id, "/rowMeans", H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, dataspace_id, plist_id, meanVec);
        H5Dclose(dataset_id);
        H5Sclose(dataspace_id);

        dims[0] = numrows;
        dataspace_id = H5Screate_simple(1, dims, NULL);
        dataset_id = H5Dcreate2(file_id, "/rowWeights", H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, dataspace_id, plist_id, rowWeights);
        H5Dclose(dataset_id);
        H5Sclose(dataspace_id);

        H5Pclose(plist_id);
        H5Fclose(file_id);

        free(U);
        free(VT);
        free(finalV);
        free(singvals);
    }
    free(Alocal);
    free(Scratch);
    free(Scratch2);
    free(vector);
    free(resid);
    free(v);
    free(workd);
    free(workl);
    if(mpi_rank == 0){
		free(elementcounts);
    	free(elementoffsets);
        free(rowcounts);
        free(rowoffsets);
        free(meanVec);
        free(rowWeights);
    }
	elapstp = MPI_Wtime();
	if(mpi_rank == 0)
		printf("Total PCA elapsed time: %f\n", elapstp - elapstr);
	MPI_Finalize();
    return 0;
}

// computes means of the rows of A, subtracts them from A, and returns them in meanVec on the root process
// assumes memory has already been allocated
void computeRowMeans(double meanVec[]) {
    double * onesVec = (double *) malloc( numcols * sizeof(double));
    double * localMeanVec = (double *) malloc( localrows * sizeof(double));
    cblas_dgemv(CblasRowMajor, CblasNoTrans, localrows, numcols, 1.0/((double)numcols), Alocal, numcols, onesVec, 1, 0, localMeanVec, 1);
    cblas_dger(CblasRowMajor, localrows, numcols, -1.0, localMeanVec, 1, onesVec, 1, Alocal, numcols);
    if (mpi_rank != 0) {
        MPI_Gatherv(localMeanVec, localrows, MPI_DOUBLE, NULL, NULL, NULL, MPI_DOUBLE, 0, comm);
    } else {
        MPI_Gatherv(localMeanVec, localrows, MPI_DOUBLE, meanVec, rowcounts, rowoffsets, MPI_DOUBLE, 0, comm);
    }
    free(onesVec);
    free(localMeanVec);
}

// rescales the rows of A by the given weights
// weights only needs to be defined on the root process
void rescaleRows(double weights[]) {
    double * localweights = (double *) malloc( localrows * sizeof(double));
    if(mpi_rank != 0) {
        MPI_Scatterv(NULL, rowcounts, rowoffsets, MPI_DOUBLE, localweights, localrows, MPI_DOUBLE, 0, comm);
    } else {
        MPI_Scatterv(weights, rowcounts, rowoffsets, MPI_DOUBLE, localweights, localrows, MPI_DOUBLE, 0, comm);
    }
    int rowIdx;
    for(rowIdx = 0; rowIdx < localrows; rowIdx = rowIdx + 1)
        cblas_dscal(numcols, localweights[rowIdx], Alocal + (rowIdx*numcols), 1);
    free(localweights);
}

// computes A^T*A*v and stores back in v
void distributedGramianVecProd(double v[]) {
    multiplyGramianChunk(Alocal, v, v, Scratch, localrows, numcols, 1); // TODO: write an appropriate mat-vec function instead of using the mat-mat function
    MPI_Allreduce(v, Scratch2, numcols, MPI_DOUBLE, MPI_SUM, comm);
    cblas_dcopy(numcols, Scratch2, 1, v, 1);
}

// computes A*mat and stores result on the rank 0 process in matProd (assumes the memory has already been allocated)
void distributedMatMatProd(double mat[], double matProd[]) {
    multiplyAChunk(Alocal, mat, Scratch3, localrows, numcols, numeigs);
    if (mpi_rank != 0) {
        MPI_Gatherv(Scratch3, localrows*numeigs, MPI_DOUBLE, NULL, NULL, NULL, MPI_DOUBLE, 0, comm);
    } else {
        MPI_Gatherv(Scratch3, localrows*numeigs, MPI_DOUBLE, matProd, elementcounts, elementoffsets, MPI_DOUBLE, 0, comm);
    }
}

// computes C = A*Omega 
void multiplyAChunk(double A[], double Omega[], double C[], int rowsA, int colsA, int colsOmega) {
    cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, rowsA, colsOmega, colsA, 1.0, A, colsA, Omega, colsOmega, 0.0, C, colsOmega);
}

/* computes A'*(A*Omega) = A*S , so Scratch must have size rowsA*colsOmega */
void multiplyGramianChunk(double A[], double Omega[], double C[], double Scratch[], int rowsA, int colsA, int colsOmega) {
    //printf("A should have size %d by %d\n", rowsA, colsA);
    //printf("Omega should have size %d by %d\n", colsA, colsOmega);
    //printf("Scratch = A*Omega should have size %d by %d\n", rowsA, colsOmega);
    cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, rowsA, colsOmega, colsA, 1.0, A, colsA, Omega, colsOmega, 0.0, Scratch, colsOmega);
    //printf("after dgemm 1");
    cblas_dgemm(CblasRowMajor, CblasTrans, CblasNoTrans, colsA, colsOmega, rowsA, 1.0, A, colsA, Scratch, colsOmega, 0.0, C, colsOmega);
    //printf("after dgemm 2");
    /*
    double * C2 = (double *) malloc( sizeof(double) * colsA * colsOmega );
    double * A2 = (double *) malloc( sizeof(double) * rowsA * colsA );
    double * ScratchMe = (double *) malloc( sizeof(double) * rowsA * colsOmega);
    if (C2 == NULL || A2 == NULL || ScratchMe == NULL) {
        printf("Out of memory on process %d\n", mpi_rank);
        exit(-1);
    }
    cblas_dgemm(CblasRowMajor, CblasTrans, CblasNoTrans, colsA, colsOmega, rowsA, 1.0, A2, colsA, ScratchMe, colsOmega, 0.0, C, colsOmega);
    */
}

void printvec(char * label, double * v, int length) {
    if (!DISPLAY_FLAG) {
        return;
    }
    char buffer[2000];
    int nextpos = 0;
    nextpos = sprintf(buffer, label);
    int idx;
    int practical_length = length < 20 ? length : 20;
    for(idx = 0; idx < practical_length; idx = idx + 1) {
        nextpos = nextpos + sprintf(buffer + nextpos, "%f, ", v[idx]);
    }
    sprintf(buffer + nextpos - 2, "\n");
    printf(buffer);
}

// prints a matrix stored in row major format
void printmat(char * label, double * mat, int height, int width) {
    if (!DISPLAY_FLAG) {
        return;
    }
    char buffer[2000];
    int nextpos = 0;
    nextpos = sprintf(buffer, label);
    int rowidx, colidx;
    for( rowidx = 0; rowidx < height; rowidx = rowidx + 1) {
        for(colidx = 0; colidx < width; colidx = colidx + 1) {
            nextpos = nextpos + sprintf(buffer + nextpos, "%f, ", mat[rowidx*width + colidx]);
        }
        sprintf(buffer + nextpos - 2, "\n");
        nextpos = nextpos - 1;
    }
    printf(buffer); 
}

// copies matrix A
void dgecopy(const double * A, long m, long n, long incRowA, long incColA, double * B, long incRowB, long incColB)
{
    int i, j;
    for (j=0; j<n; ++j) {
        for (i=0; i<m; ++i) {
            B[i*incRowB+j*incColB] = A[i*incRowA+j*incColA];
        }
    }
}

// stores the transpose of matrix A in matrix B
void mattrans(const double * A, long m, long n, double * B) {
    dgecopy(A, m, n, n, 1, B, 1, m); 
}

// flips the left-right ordering of the columns of a matrix stored in rowmajor format
void flipcolslr(double * A, long m, long n) {
    int idx = 0;
    for(idx = 0; idx < n/2; idx = idx + 1) {
        cblas_dswap(m, A + idx, n, A + (n - 1 - idx), n);
		//printf("Performed flipcolslr idx: %d/%d. m: %d. n: %d\n", idx, n/2, m , n);
    }
}

// loads the latitudes of each row from the given file, and returns the corresponding vector of row weights in rowWeights
// expects each line of the file to be in the form ^idx,latitude$
// returns sqrt(cos(lat)) as the weights vector
void loadRowWeights(char * weightsFname, double * rowWeights) {
    int rowIdx;
    double latVal;
    FILE * fin = fopen(weightsFname, "r");

    if( fin == NULL ) {
        fprintf(stderr, "Can't open latitude csv file %s!\n", weightsFname);
        exit(1);
    }
    while(fscanf(fin, "%d,%lf", &rowIdx, &latVal) != EOF) {
        rowWeights[rowIdx] = sqrt(cos(latVal));
        //printf("Read %d: %lf\n", rowIdx, latVal);
    }
    fclose(fin);
}
