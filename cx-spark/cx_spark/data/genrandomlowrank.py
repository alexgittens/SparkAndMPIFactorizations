#!/usr/bin/env python
# generates a low-rank dataset A, consisting of n rows of 
# p-dimensional observations, according to
#
# A = standard_i.i.d_gaussians_{n by p} * C^{1/2} + 1*mu^T
#
# where mu is the mean vector in R^p
# C is the rank r covariance matrix
# n is the number of samples
#
# and writes A to file, as well as 
# cov(A)*ones(p, 32)
# for comparison with the testMain function's output in the 
# Scala RPCA code

import argparse
import numpy as np

def writeMatrixMarket(mat, fname):
    """ writeMatrixMarket(mat, fname) writes a numpy matrix out to fname in Matrix Market format """
    r,c = mat.shape
    with open(fname, "w") as fout:
        print >> fout, "%%MatrixMarket matrix coordinate real general"
        print >> fout, "{0} {1} {2}".format(r, c, r*c)
        for rowidx in range(0, r):
            for colidx in range(0, c):
                print >> fout, "{0} {1} {2}".format(rowidx+1, colidx+1, mat[rowidx, colidx])

def writeCSV(mat, fname):
    """ writeCSV(mat, fname) writes a numpy matrix out to fname in csv format """
    r,c = mat.shape
    with open(fname, "w") as fout:
        for rowidx in range(0, r):
            for colidx in range(0, c):
                print >> fout, "{0},{1},{2}".format(rowidx, colidx, mat[rowidx, colidx])

parser = argparse.ArgumentParser()
parser.add_argument("n", help="number of observations", type=int)
parser.add_argument("p", help="number of features in an observation", type=int)
parser.add_argument("r", help="effective rank of covariance matrix", type=int)
parser.add_argument("matfname", help="where to output Matrix Market version of matrix", type=str)
parser.add_argument("resultfname", help="where to output Matrix Market version of covariance matrix times 32 column matrix of all ones", type=str)
parser.add_argument("--csv", help="outputs matrix as CSV instead of Matrix Market", action="store_true")

args = parser.parse_args()

C = 10*np.diag(np.append(np.random.randn(args.r,), np.zeros(args.p - args.r,)))
U, _, _ = np.linalg.svd(np.random.randn(args.p, args.p))
mean = np.random.rand(args.p,1)
A = np.random.randn(args.n, args.p).dot(C).dot(U) + np.outer(np.ones(args.n), mean)

empMean = 1.0/args.n*np.sum(A, 0)
centeredA = A - np.outer(np.ones(args.n), empMean)
empCov = 1.0/args.n*centeredA.transpose().dot(centeredA)
X = empCov.dot(np.ones((args.p, 32)))

# Store A and X
if args.csv:
    writeCSV(A, args.matfname)
else:
    writeMatrixMarket(A, args.matfname)
writeMatrixMarket(X, args.resultfname)

# store the eigen decomposition
U, s, _ = np.linalg.svd(empCov)
writeMatrixMarket(U, args.matfname + ".U")
writeMatrixMarket(s.reshape((1, s.shape[0])), args.matfname + ".s")
