import logging

import numpy as np
from pyspark.storagelevel import StorageLevel 
from rma_utils import add_index
from rma_utils import convert_rdd 
from rma_utils import form_csr_matrix
from utils import BlockMapper, add
from utils import prepare_matrix


logger = logging.getLogger(__name__)

class SparseRowMatrix(object):
    """
    A sparse row matrix class
    Each record is of the format: (row_idx, column_id, val)
    """

    def __init__(self, raw_rdd, name, m, n, cache=False):
        self.rdd = raw_rdd#prepare_matrix(raw_rdd)
        self.name = name
        self.m = m
        self.n = n

        if cache:
            self.rdd.cache()

    def gaussian_projection(self, c):
        """
        compute G*A with G is Gaussian matrix with size r by m
        """
        direct_sum = False # option to use a direct sum() function or not

        gaussian_projection_mapper = GaussianProjectionMapper(direct_sum)
        n = self.n

        if direct_sum:
            print "using direct sum"
            gp = self.rdd.mapPartitions(lambda records: gaussian_projection_mapper(records,n=n,c=c)).filter(lambda x: x is not None).sum()
        else:
            print "not using direct sum"
            gp_dict = self.rdd.mapPartitions(lambda records: gaussian_projection_mapper(records,n=n,c=c)).filter(lambda x: x is not None).reduceByKey(add).collectAsMap()
            logger.info('In gaussian_projection, finish collecting results!')

            order = sorted(gp_dict.keys())
            gp = []
            for i in order:
                gp.append( gp_dict[i] )

            gp = np.hstack(gp)
            logger.info('In gaussian_projection, finish sorting and forming the prodcut!')

        return gp

    def atamat(self,mat):
        """
        compute A.T*A*B
        """
        # TO-DO: check dimension compatibility
        if mat.ndim == 1:
            mat = mat.reshape((len(mat),1))

        direct_sum = False # option to use a direct sum() function or not

        [n,c] = mat.shape

        if n*c > 1e4: # the size of mat is too large to broadcast
            b = []
            #mini_batch_sz = 1e8/n # make sure that each mini batch has less than 1e8 elements
            mini_batch_sz = 4
            start_idx = np.arange(0, c, mini_batch_sz)
            end_idx = np.append(np.arange(mini_batch_sz, c, mini_batch_sz), c)

            for j in range(len(start_idx)):
                logger.info("processing mini batch {0}".format(j))
                b.append(self.__atamat_sub(mat[:,start_idx[j]:end_idx[j]],direct_sum))
            
            return np.hstack(b)

        else:
            return self.__atamat_sub(mat,direct_sum)

    def __atamat_sub(self,mat,direct_sum):
        n = self.n

        atamat_mapper = MatrixAtABMapper(direct_sum)
        if direct_sum:
            print "using direct sum"
            b = self.rdd.mapPartitions(lambda records: atamat_mapper(records,mat=mat,n=n) ).filter(lambda x: x is not None).sum()
        else:
            print "not using direct sum"
            b_dict = self.rdd.mapPartitions(lambda records: atamat_mapper(records,mat=mat,n=n) ).filter(lambda x: x is not None).reduceByKey(add).collectAsMap()
            logger.info('In atamat, finish collecting results!')

            order = sorted(b_dict.keys())
            b = []
            for i in order:
                b.append( b_dict[i] )

            b = np.vstack(b)
            logger.info('In atamat, finish sorting and forming the prodcut!')

        #mat.unpersist()

        return b

    def transpose(self):
        pass

class GaussianProjectionMapper(BlockMapper):

    def __init__(self,direct_sum=False):
        BlockMapper.__init__(self, 128)
        self.gp = None
        self.data = {'row':[],'col':[],'val':[]}
        self.direct_sum = direct_sum

    def parse(self, r):
        self.keys.append(r[0])
        self.data['row'] += [self.sz]*len(r[1][0])
        self.data['col'] += r[1][0].tolist()
        self.data['val'] += r[1][1].tolist()

    def process(self, n, c):
        sz = len(self.keys)

        if self.gp is not None:
            self.gp += (form_csr_matrix(self.data,sz,n).T.dot(np.random.randn(sz,c))).T
        else:
            self.gp = (form_csr_matrix(self.data,sz,n).T.dot(np.random.randn(sz,c))).T

        return iter([])

    def close(self):
        for r in emit_results(self.gp, self.direct_sum, axis=1):
            yield r

class MatrixAtABMapper(BlockMapper):

    def __init__(self,direct_sum=False):
        BlockMapper.__init__(self, 128)
        self.atamat = None
        self.data = {'row':[],'col':[],'val':[]}
        self.direct_sum = direct_sum

    def parse(self, r):
        self.keys.append(r[0])
        self.data['row'] += [self.sz]*len(r[1][0])
        self.data['col'] += r[1][0].tolist()
        self.data['val'] += r[1][1].tolist()

    def process(self, mat, n):
        data = form_csr_matrix(self.data,len(self.keys),n)
        if self.atamat is not None:
            self.atamat += data.T.dot( data.dot(mat) )
        else:
            self.atamat = data.T.dot( data.dot(mat) )
        return iter([])

    def close(self):
        for r in emit_results(self.atamat, self.direct_sum, axis=0):
            yield r

def emit_results(b,direct_sum,axis,block_sz=1e3):
    # axis (=0 or 1) determines which dimension to partition along
    if b is None:
        yield None
    else:
        if direct_sum:
            yield b
        else:
            m = b.shape[axis]
            start_idx = np.arange(0, m, block_sz)
            end_idx = np.append(np.arange(block_sz, m, block_sz), m)
            if axis == 0:
                for j in range(len(start_idx)):
                    yield j, b[start_idx[j]:end_idx[j],:]
            else:
                for j in range(len(start_idx)):
                    yield j, b[:,start_idx[j]:end_idx[j]]
