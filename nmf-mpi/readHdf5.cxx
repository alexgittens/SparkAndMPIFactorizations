#include "stdlib.h"

#include "readHdf5.h"

void readHdf5(const char *fname, const char *dsname, double* dA,  MPI_Comm comm, MPI_Info info){
	int rank, drank, npes;
	int nrows;
	float *A;
	
	MPI_Comm_size(comm, &npes);
	MPI_Comm_rank(comm, &rank);

	hsize_t offset[2], count[2], stride[2], block[2];

	hid_t	file_id, dset_id;
	hid_t	plist_id;
	herr_t	status;
	
	double tst = MPI_Wtime();
	//printf("Proc %d started up!\n",rank);

	plist_id = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_fapl_mpio(plist_id, comm, info);
	
	file_id = H5Fopen(fname, H5F_ACC_RDONLY, plist_id);
	H5Pclose(plist_id);

	dset_id = H5Dopen(file_id, dsname, H5P_DEFAULT);
	
	hid_t dataspace, memspace;
	hsize_t dimsm[1];
	hsize_t dims_out[2];


	dataspace = H5Dget_space(dset_id);
	drank = H5Sget_simple_extent_ndims(dataspace);
	status = H5Sget_simple_extent_dims(dataspace, dims_out, NULL);

	if(rank == 0)
		printf("Rank: %d\nDimensions: %lu x %lu \n", drank, (unsigned long) (dims_out[0]), (unsigned long) (dims_out[1]));
	//MPI_Barrier(MPI_COMM_WORLD);
	
	int mod = dims_out[0]%npes;
	nrows = (rank < mod) ? (dims_out[0]/npes + 1) : (dims_out[0]/npes);
	//printf("Proc %d will read %d rows\n",rank, nrows);
	
	stride[0] = 1; stride[1] = 1;
	block[0] = 1; block[1] = 1;
	count[0] = nrows; count[1] = 192;
	offset[0] = (rank < mod) ? (nrows*rank) : (((nrows + 1)*mod) + (nrows*(rank - mod)));
	offset[1] = 0;

	status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, offset, stride, count, block);
	int mrank = 1;
	hsize_t offset_out[1], stride_out[1], count_out[1], block_out[1];
	offset_out[0] = 0;
	stride_out[0] = 1;
	count_out[0] = nrows*192;
	block_out[0] = 1;
	dimsm[0] = nrows*192;
	
	memspace = H5Screate_simple(mrank, dimsm, NULL);
	status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, offset_out, stride_out, count_out, block_out);
	
	plist_id = H5Pcreate(H5P_DATASET_XFER);
	H5Pset_dxpl_mpio(plist_id, H5FD_MPIO_INDEPENDENT);
	
	posix_memalign((void **)&A, ALIGN, sizeof(float)*nrows*192);

	status = H5Dread(dset_id, H5T_IEEE_F32LE, memspace, dataspace, plist_id, A);
	
	double tstp = MPI_Wtime();
	//MPI_Barrier(MPI_COMM_WORLD);
	if(rank == 0)
		printf("Time to read matrix from HDF5: %f seconds\n", tstp - tst);
		int i;
		for(i = 0; i < nrows*192; ++i){
			dA[i] = (double) A[i];
			if(A[i] < 0.){
				printf("Proc %d found element less than 0\n", rank);
				break;
			}
		}
	//MPI_Barrier(MPI_COMM_WORLD);
	
	status = H5Pclose(plist_id);
	status = H5Dclose(dset_id);
	status = H5Fclose(file_id);
	free(A);


}

/*
int main(int argc, char** argv){

	int rank, npes;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &npes);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	
	double *A = readHdf5(argv[1], argv[2], MPI_COMM_WORLD, MPI_INFO_NULL);


	MPI_Finalize();
	free(A);
//	file_id = H5Fopen(FILE, H5F_ACC_RDWR, H5P_DEFAULT);
	return 0;
}
*/
