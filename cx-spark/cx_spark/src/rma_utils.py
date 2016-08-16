import numpy.linalg as npl
import numpy as np
from scipy.sparse import csr_matrix

from utils import *

def to_sparse(A):
    sA = csr_matrix(A)
    return [(i, ( sA.indices[range(sA.indptr[i],sA.indptr[i+1])], sA.data[range(sA.indptr[i],sA.indptr[i+1])])) for i in range(A.shape[0])]

def convert_rdd(rdd):
    row = rdd.first()
    if isinstance(row, unicode):
        rdd = rdd.map(lambda row: np.array([float(x) for x in row.split(' ')]))
    else:
        rdd = rdd.map(lambda row: np.array(row))

    return rdd

def parse_data(data, feats):
    if feats:
        return data[:,feats[0]:feats[1]+1]
    else:
        return data

def form_csr_matrix(data,m,n):
    return coo_matrix((data['val'], (data['row'], data['col'])), shape=(m, n)).tocsr()

def comp_l2_obj(Ab_rdd, x):
    # x is a np array
    return np.sqrt( Ab_rdd.map( lambda (key,row): (np.dot(row[:-1],x) - row[-1])**2 ).reduce(add) )

def add_index(rdd): 
    starts = [0] 
    nums = rdd.mapPartitions(lambda it: [sum(1 for i in it)]).collect() 
    for i in range(len(nums) - 1): 
        starts.append(starts[-1] + nums[i]) 

    def func(k, it): 
        for i, v in enumerate(it, starts[k]): 
            #yield v, i
            yield i, v

    return rdd.mapPartitionsWithIndex(func)

def get_x(pa,return_N=False):
    A = pa[:,:-1]
    b = pa[:,-1]
    m = A.shape[0]

    [U, s, V] = npl.svd(A, 0)
    N = V.transpose()/s

    if return_N:
        return (N, np.dot(N, np.dot(U.T,b)))
    else:
        return np.dot(N, np.dot(U.T,b))

def get_N(pa,feats=None):
    pa = parse_data(pa, feats)
    [U, s, V] = npl.svd(pa, 0)
    N = V.transpose()/s
    return N

def unif_sample_solve(rows,m,k,s):
    return rows.flatMap(lambda row:unif_sample(parse(row),k,m,s)).groupByKey().map(lambda sa: get_x(sa[1])).collect()

def unif_sample(x,k,m,s):
    x = np.array(x)
    np.random.seed()
    for i in range(k):
        p = s/m
        if np.random.rand() < p:
            yield (i,x.tolist())

def sumIteratorOuter(iterator):
    yield sum(np.outer(x, y) for x, y in iterator)

def compLevExact(A, k, axis):
    """ This function computes the column or row leverage scores of the input matrix.
         
        :param A: n-by-d matrix
        :param k: rank parameter, k <= min(n,d)
        :param axis: 0: compute row leverage scores; 1: compute column leverage scores.
        
        :returns: 1D array of leverage scores. If axis = 0, the length of lev is n.  otherwise, the length of lev is d.
    """

    U, D, V = np.linalg.svd(A, full_matrices=False)

    if axis == 0:
        lev = np.sum(U[:,:k]**2,axis=1)
    else:
        lev = np.sum(V[:k,:]**2,axis=0)

    p = lev/k

    return lev, p
