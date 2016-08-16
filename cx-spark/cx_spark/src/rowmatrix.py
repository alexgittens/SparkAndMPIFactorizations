import logging

import numpy as np


from rma_utils import add_index
from rma_utils import convert_rdd
from rma_utils import parse_data
from utils import add
from utils import BlockMapper


logger = logging.getLogger(__name__)

class RowMatrix(object):
    '''
    A row matrix class
    rdd: RDD object that stores the matrix row wise,
        the matrix can be A, [A b], or [b A].
    name: name of the matrix
    m, n: actual size of the matrix
    feats: indices of columns of A
    repnum: number of times to replicate the matrix vertically
    '''

    def __init__(self, rdd, name, m=None, n=None, cache=False, stack_type=1, repnum=1):
        self.rdd_original = rdd
        self.stack_type = stack_type
        self.repnum = repnum

        if m is None:
            self.m_original, self.n = self.get_dimensions()
        else:
            self.m_original = m
            self.n = n

        self.rdd_original = convert_rdd(self.rdd_original)

        if repnum>1:
            self.name = name + '_stack' + str(stack_type) + '_rep' + str(self.repnum)
            if stack_type == 1:
                self.rdd = self.rdd_original.flatMap(lambda row:[row for i in range(repnum)])
                self.m = self.m_original*repnum
            elif stack_type == 2:
                n = self.n
                self.rdd = add_index(self.rdd_original).flatMap(lambda row: [row[0] for i in range(repnum)] if row[1]<self.m_original-n/2 else [row[0]])
                self.m = (self.m_original-self.n/2)*repnum + self.n/2
        else:
            self.name = name
            self.rdd = self.rdd_original
            self.m = m

        self.rdd_original = add_index(self.rdd_original)
        self.rdd = add_index(self.rdd) # sortedByKey?

        if cache:
            self.rdd.cache()
            logger.info('number of rows: {0}'.format( self.rdd.count() )) # materialize the matrix

    def atamat(self,mat,feats=None):
        # TO-DO: check dimension compatibility
        if mat.ndim == 1:
            mat = mat.reshape((len(mat),1))

        atamat_mapper = MatrixAtABMapper()
        b_dict = self.rdd.mapPartitions(lambda records: atamat_mapper(records,mat=mat,feats=feats) ).reduceByKey(add).collectAsMap()

        order = sorted(b_dict.keys())
        b = []
        for i in order:
            b.append( b_dict[i] )

        b = np.vstack(b)

        mat.unpersist()

        return b

    def rtimes(self,mat,feats=None,return_rdd=False):
        # TO-DO: check dimension compatibility
        if mat.ndim == 1:
            mat = mat.reshape((len(mat),1))

        matrix_rtimes_mapper = MatrixRtimesMapper()
        a = self.rdd.mapPartitions(lambda records: matrix_rtimes_mapper(records,mat=mat,feats=feats) )

        if not return_rdd:
            a_dict = a.collectAsMap()
            order = sorted(a_dict.keys())

            b = []
            for i in order:
                b.append( a_dict[i] )

            b = np.vstack(b)

        mat.unpersist()

        return b

    def ltimes(self,mat,feats=None):
        # TO-DO: check dimension compatibility

        if mat.ndim == 1:
            mat = mat.reshape((1,len(mat)))

        matrix_ltimes_mapper = MatrixLtimesMapper()
        b_dict = self.rdd.mapPartitions(lambda records: matrix_ltimes_mapper(records,mat=mat,feats=feats) ).reduceByKey(add).collectAsMap()

        order = sorted(b_dict.keys())
        b = []
        for i in order:
            b.append( b_dict[i] )

        b = np.hstack(b)

        mat.unpersist()

        return b

    def get_dimensions(self):
        m = self.matrix.count()
        try:
            n = len(self.matrix.first())
        except:
            n = 1
        return m, n
              
    def take(self, num_rows):
        return self.rdd.take(num_rows)

    def top(self):
        return self.rdd.first()

    def collect(self):
        return self.rdd.collect()


class MatrixRtimesMapper(BlockMapper):

    def process(self, mat, feats):
        yield self.keys[0], np.dot( parse_data( np.vstack(self.data), feats ), mat )

class MatrixLtimesMapper(BlockMapper):

    def __init__(self):
        BlockMapper.__init__(self)
        self.ba = None

    def process(self, mat, feats):
        if self.ba:
            self.ba += np.dot( mat[:,self.keys[0]:(self.keys[-1]+1)], parse_data( np.vstack(self.data), feats ) )
        else:
            self.ba = np.dot( mat[:,self.keys[0]:(self.keys[-1]+1)], parse_data( np.vstack(self.data), feats ) )

        return iter([])
    #def process(self, mat, feats):
    #    yield np.dot( mat[:,self.keys[0]:(self.keys[-1]+1)], parse_data( np.vstack(self.data), feats ) )

    def close(self):
        #yield self.atamat

        block_sz = 50
        n = self.ba.shape[1]
        start_idx = np.arange(0, n, block_sz)
        end_idx = np.append(np.arange(block_sz, n, block_sz), n)

        for j in range(len(start_idx)):
            yield j, self.ba[:,start_idx[j]:end_idx[j]]

class MatrixAtABMapper(BlockMapper):

    def __init__(self):
        BlockMapper.__init__(self)
        self.atamat = None

    def process(self, mat, feats):
        data = parse_data( np.vstack(self.data), feats )
        if self.atamat:
            self.atamat += np.dot( data.T, np.dot( data, mat ) )
        else:
            self.atamat = np.dot( data.T, np.dot( data, mat ) )

        return iter([])

        #yield np.dot( data.T, np.dot( data, mat ) )

    def close(self):
        #yield self.atamat

        block_sz = 50
        m = self.atamat.shape[0]
        start_idx = np.arange(0, m, block_sz)
        end_idx = np.append(np.arange(block_sz, m, block_sz), m)

        for j in range(len(start_idx)):
            yield j, self.atamat[start_idx[j]:end_idx[j],:]
