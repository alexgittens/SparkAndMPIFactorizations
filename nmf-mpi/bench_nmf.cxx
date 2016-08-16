/* Copyright (c) Aditya Devarakonda 2016, all rights reserved.*/
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string>
#include <fstream>

#include "bench_nmf.h"
//#define PROFILE

using namespace std;

void xray(double *A, int m, int n, int* index, int r){
	int count = 0;
	double *Res, *C, *anrms, *resnrms, *scores, *H, *asub;
	char transa = 'T', transb = 'N';
	double alp = 1., bet = 0., max_val = -1.0;
	int inc = 1;
	//omp_set_num_threads(32);
	//cout << "Inside XRAY function" << endl;

	assert(0==posix_memalign((void**)&Res, ALIGN, sizeof(double)*m*n));
	assert(0==posix_memalign((void**)&C, ALIGN, sizeof(double)*m*n));
	assert(0==posix_memalign((void**)&H, ALIGN, sizeof(double)*r*n));
	assert(0==posix_memalign((void**)&asub, ALIGN, sizeof(double)*m*r));
	assert(0==posix_memalign((void**)&anrms, ALIGN, sizeof(double)*n));
	assert(0==posix_memalign((void**)&resnrms, ALIGN, sizeof(double)*n));
	assert(0==posix_memalign((void**)&scores, ALIGN, sizeof(double)*n));
	lda_cpy(m, n, m, m, A, Res);
	
	//Compute col2norm of A
	//double tsta = MPI_Wtime();
	#pragma omp parallel for shared(anrms)
	for(int i = 0; i < n; ++i){
		anrms[i] = dnrm2(&m, &A[i*m], &inc);
	}
	//double tstp = MPI_Wtime();
	//cout << "col2norm(A) Elapsed time: " << tstp - tsta << endl;

	//tsta = MPI_Wtime();
	while(count < r){
		//Compute C = Res**T * A
		//tsta = MPI_Wtime();
		cdgemm(transa, transb, n, n, m, alp, Res, m, A, m, bet, C, n);
		//tstp = MPI_Wtime();
		//cout << "R**T * X Elapsed time: " << tstp - tsta << endl;
		//Compute col2norm of C
		//tsta = MPI_Wtime();
		#pragma omp parallel for shared(resnrms, scores, C)
		for(int i = 0; i < n; ++i){
			resnrms[i] = dnrm2(&n, &C[i*n], &inc);
			scores[i] = resnrms[i]/anrms[i];
		}
		//Set norms of best cols so far to -1 (don't want to select them again).
		//#pragma omp parallel for shared(scores,index)
		for(int i = 0; i < count; ++i){
			scores[index[i]] = -1;
		}
		//Find largest norm column and store its index
		#pragma omp parallel
		{
			int index_local = -1;
			double max_local = -1.0;
			#pragma omp for nowait
			for(int i = 0; i < n; ++i){
				if(scores[i] > max_local){
					max_local = scores[i];
					index_local = i;
				}
			}
			#pragma omp critical
			{
				if(max_local > max_val){
					index[count] = index_local;
					max_val = max_local;
				}
			}
		}
		//tstp = MPI_Wtime();
		//cout << "find max norm column Elapsed time: " << tstp - tsta << endl;
		//cout << "max norm column: " << index[count] << endl;
		lda_cpy(m, 1, m, m, &A[index[count]*m], &asub[count*m]);
		count++;
		max_val = -1.;

		//In parallel (TODO: Decide OpenMP or MPI) compute current estimate of H
		/* OpenMP Version */
		#pragma omp parallel
		{
			double **a = (double **) malloc(sizeof(double*)*r);
			double *b = (double *) malloc(sizeof(double)*m);
			double rnorm;
			for(int i = 0; i < count; ++i){
				a[i] = (double *) malloc(sizeof(double)*m);
				lda_cpy(m, 1, m, m, &asub[i*m], a[i]);
			}
			#pragma omp for
			for(int i = 0; i < n; ++i){
				lda_cpy(m, 1, m, m, &A[i*m], b);
				nnls(a, m, count, b, &H[i*count], &rnorm, NULL, NULL, NULL);
				for(int i = 0; i < count; ++i){
					lda_cpy(m, 1, m, m, &asub[i*m], a[i]);
				}
			}
			
			for(int i = 0; i < count; ++i){
				free(a[i]);
			}
			free(a), free(b);
		}

		lda_cpy(m, n, m, m, A, Res);
		cdgemm('N', 'N', m, n, count, -1., asub, m, H, count, 1., Res, n);
	}
	//tstp = MPI_Wtime();
	//cout << "finding r cols Elapsed time: " << tstp - tsta << endl;
	/*
	cout << "Resulting H" << endl;
	for(int i = 0; i < r; ++i){
		for(int j = 0; j < n; ++j){
			cout << H[i + j*r] << " ";
		}
		cout << endl;
	}
	*/
	free(Res); free(C); free(scores); free(asub); free(anrms); free(resnrms);
	free(H);
}

int main(int argc, char** argv){
	#ifdef PROFILE
	TAU_PROFILE_TIMER(tautimer, "int main(int, char**)", " ", TAU_DEFAULT);
	TAU_INIT(&argc, &argv);
	#endif
	int rank, npes;
	double *A, *R, *tree_tau;
	double tstart, tstop, btsqrstr, btsqrstp, xraystr, xraystp;
	int m = atoi(argv[4]); 
	int n = atoi(argv[5]);
	int i;
	int req_id = 0;
	int r = atoi(argv[6]);
	int root = 0;
	int *index;
	CommData_t cdt;

	INIT_COMM(npes, rank, 1, cdt);
	omp_set_num_threads(atoi(argv[1]));
	#ifdef PROFILE
	TAU_PROFILE_SET_NODE(rank);
	TAU_PROFILE_START(tautimer);
	#endif
	assert(0==posix_memalign((void**)&R, ALIGN, sizeof(double)*n*n));
	assert(0==posix_memalign((void**)&tree_tau, ALIGN, sizeof(double)*n));
	assert(0==posix_memalign((void**)&index, ALIGN, sizeof(int)*r));
	
	if(rank == 0)
		tstart = MPI_Wtime();

	int mod = m%npes;
	int nrows = (rank < mod) ? m/npes + 1 : m/npes;

	assert(0==posix_memalign((void**)&A, ALIGN, sizeof(double)*nrows*n));
	if(rank == 0)
		cout << "Parallel HDF5 read of " << argv[2] << endl;
	double hdf5str = MPI_Wtime();
	readHdf5(argv[2], argv[3], A, MPI_COMM_WORLD, MPI_INFO_NULL);
	double hdf5stp = MPI_Wtime();
	if(rank == 0)
		cout << "HDF5 Elapsed time " << hdf5stp - hdf5str << " seconds" << endl;

	m = nrows;
	/*
	cout << "Processor " << rank << " nrows " << m << " ncols " << n << endl;
	for(int j = 0; j < 15; ++j){
		cout << A[j] << " ";
	}
	cout << "Processor " << rank  << endl;
	*/
	//double *dA = (double *) malloc(sizeof(double)*m*n);
	//memcpy(dA, A, sizeof(double)*m*n);
	
	if(rank == 0)
		btsqrstr = MPI_Wtime();
	bitree_tsqr(A, m, R, tree_tau, m, n, rank, npes, root, req_id, cdt, 0);
	if(rank == 0 ){
		btsqrstp = MPI_Wtime();
		cout << "TSQR Elapsed time " << btsqrstp - btsqrstr << " seconds" << endl;
	}
	//cout << "Processor " << rank << " Broadcasting R" << endl;
	//MPI_Bcast(R, n*n, MPI_DOUBLE, root, MPI_COMM_WORLD);
	
	/* Perform column subset selection (redundantly) on all processors */
	if(rank == 0){
		//cout << "Processor " << rank << " calling XRAY" << endl;
		xraystr = MPI_Wtime();
		xray(R, n, n, index, r);
		xraystp = MPI_Wtime();
		cout << "XRAY Elapsed time " << xraystp - xraystr << " seconds" << endl;
	}

	/*Perform n NNLS in parallel to get H. Columns of H are distributed across processors.*/
	//nnls();
	if(rank == 0)
		tstop = MPI_Wtime();

	//cout << "Processor " << rank << " has final R." << endl;
	
	//MPI_Finalize();
	if(rank == 0){
		cout << "Total Elapsed time " << tstop - tstart << " seconds" << endl;
		if(n <= 16){
			for(i = 0; i < n; ++i){
				for(int j = 0; j < n; ++j){
					cout << R[j*n + i] << " ";
				}
				cout << endl;
			}
		}
	}
	free(A); 
	//free(dA);
	free(index);
	free(R); free(tree_tau);
	//MPI_Barrier(MPI_COMM_WORLD);
	//MPI_Finalize();
	COMM_EXIT;
	#ifdef PROFILE
	TAU_PROFILE_STOP(tautimer);
	#endif
}
