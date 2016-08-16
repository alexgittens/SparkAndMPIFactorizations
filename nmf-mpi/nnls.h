/******************************************************************************
*     2   Copyright (c) 2002-2013 by Turku PET Centre
*         3 
*             4   nnls.h
*                 5   
*                     6   Version:
*                         7   2002-08-19 Vesa Oikonen
*                             8   2003-05-08 Kaisa Sederholm & VO
*                                 9   2003-05-12 KS
*                                    10   2007-05-17 VO
*                                       11   2009-04-27 VO
*                                          12   2013-06-22 VO
*                                             13     Removed global variable NNLS_TEST.
*                                                14 
*                                                   15 ******************************************************************************/
#ifndef _NNLS_H
#define _NNLS_H
/*****************************************************************************/
extern int nnls(
 double **a, int m, int n, double *b, double *x,
 double *rnorm, double *w, double *zz, int *index
);
extern int nnlsWght(
 int N, int M, double **A, double *b, double *weight
);
extern int nnlsWghtSquared(
  int N, int M, double **A, double *b, double *sweight
);
/*****************************************************************************/
#endif

