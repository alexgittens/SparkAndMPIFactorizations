#include  <stdio.h>
#include <stdlib.h>

extern void dsaupd_(int * ido, char * bmat, int * n, char * which,
                    int * nev, double * tol, double * resid, 
                    int * ncv, double * v, int * ldv,
                    int * iparam, int * ipntr, double * workd,
                    double * workl, int * lworkl, int * info);

int main(int argc, char ** argv) {

    int numcols = 10;
    int numeigs = 1;
    double * vector = (double *) malloc(numcols * sizeof(double));

    // initial call to arpack
    int ido = 0;
    int ncv = 2*numeigs;
    int maxiter = 30;
    double tol = 1e-13;
    double * v = (double *) malloc(numcols * ncv *sizeof(double));
    int iparam[11] = {1, 0, 30, 1, 0, 0, 1, 0, 0, 0, 0};
    iparam[2] = maxiter;
    int ipntr[11];
    double * workd = (double *) malloc(3*numcols*sizeof(double));
    int lworkl = ncv*(ncv + 8);
    double * workl = (double *) malloc(lworkl*sizeof(double));
    int arpack_info = 0;

    char bmat[2] = "I";
    char which[3] = "LM";

    printf("Here!\n");
    dsaupd_(&ido, bmat, &numcols, which,
            &numeigs, &tol, vector, 
            &ncv, v, &numcols,
            iparam, ipntr, workd, 
            workl, &lworkl, &arpack_info);
    printf("Here!\n");
}
