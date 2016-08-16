from pyspark import SparkContext
from pyspark import SparkConf
from cx import CX
from rowmatrix import RowMatrix
from sparse_row_matrix import SparseRowMatrix
from rma_utils import to_sparse
import time
import sys
import argparse
import scipy.stats
import numpy as np
import logging.config

def usage():
    print sys.exit(__doc__)

def print_params(args, logger):
    logger.info('--------------------------------------------------------')
    logger.info('---------------Computing CX Decomposition---------------')
    logger.info('--------------------------------------------------------')
    logger.info('dataset: {0}'.format( args.dataset ) )
    logger.info('size: {0} by {1}'.format( args.dims[0], args.dims[1] ) )
    logger.info('loading file from {0}'.format( args.file_source ) )
    if args.sparse:
        logger.info('sparse format!')
    if args.nrepetitions>1:
        logger.info('number of repetitions: {0}'.format( args.nrepetitions ))
    logger.info('number of partitions: {0}'.format( args.npartitions ))
    if args.axis == 0:
        logger.info('compute row leverage scores!')
    else:
        logger.info('compute column leverage scores!')
    logger.info('rank: {0}'.format( args.k ))
    logger.info('number of rows to select: {0}'.format( args.r ))
    logger.info('number of iterations to run: {0}'.format( args.q ))
    logger.info('scheme to use: {0}'.format( args.scheme ))
    logger.info('stages to run: {0}'.format( args.stage ))
    logger.info('--------------------------------------------------------')
    if args.test:
        logger.info('Compute accuracies!')
    if args.save_logs:
        logger.info('Logs will be saved!')
    logger.info('--------------------------------------------------------')

class ArgumentError(Exception):
    pass

class OptionError(Exception):
    pass

def main(argv):
    logging.config.fileConfig('logging.conf',disable_existing_loggers=False)
    logger = logging.getLogger('') #using root

    parser = argparse.ArgumentParser(description='Getting parameters.',prog='run_cx.sh')

    parser.add_argument('dataset', type=str, help='dataset.txt stores the input matrix to run CX on; \
           dataset_U.txt stores left-singular vectors of the input matrix (only needed for -t); \
           dataset_D.txt stores singular values of the input matrix (only needed for -t)')
    parser.add_argument('--dims', metavar=('m','n'), type=int, nargs=2, required=True, help='size of the input matrix')
    parser.add_argument('--sparse', dest='sparse', action='store_true', help='whether the data is sparse')
    parser.add_argument('--hdfs', dest='file_source', default='local', action='store_const', const='hdfs', help='load dataset from HDFS')
    parser.add_argument('-k', '--rank', metavar='targetRank', dest='k', default=5, type=int, help='target rank parameter in the definition of leverage scores, this value shoud not be greater than m or n')
    parser.add_argument('-r', metavar='numRowsToSelect', default=20, type=int, help='number of rows to select in CX')
    parser.add_argument('-q', '--niters', metavar='numIters', dest='q', default=2, type=int, help='number of iterations to run in approximation of leverage scores')
    parser.add_argument('--deterministic', dest='scheme', default='randomized', action='store_const', const='deterministic', help='use deterministic scheme instead of randomized when selecting rows')
    parser.add_argument('-c', '--cache', action='store_true', help='cache the dataset in Spark')
    parser.add_argument('-t', '--test', action='store_true', help='compute accuracies of the returned solutions')
    parser.add_argument('-s', '--save_logs', action='store_true', help='save Spark logs')
    parser.add_argument('--nrepetitions', metavar='numRepetitions', default=1, type=int, help='number of times to stack matrix vertically in order to generate large matrices')
    parser.add_argument('--npartitions', metavar='numPartitions', default=280, type=int, help='number of partitions in Spark')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--row', dest='axis', default=0, action='store_const', const=0, help='compute row leverage scores')
    group.add_argument('--column', dest='axis', default=0, action='store_const', const=1, help='compute column leverage scores')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--leverage-scores-only', dest='stage', default='full', action='store_const', const='leverage', help='return approximate leverage scores only')
    group.add_argument('--indices-only', dest='stage', default='full', action='store_const', const='indices', help='return approximate leverage scores and selected row indices only')
    
    if len(argv)>0 and argv[0]=='print_help':
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    (m,n) = args.dims

    # validating
    if args.k > m or args.k > n:
        raise ValueError('Rank parameter({0}) should not be greater than m({1}) or n({2})'.format(args.k,m,n))

    if args.npartitions > m or args.npartitions > n:
        args.npartitions = min(m,n)

    if args.test and args.nrepetitions>1:
        raise OptionError('Do not use the test mode(-t) on replicated data(numRepetitions>1)!')

    if args.axis == 0:
        raise OptionError('Need to implement transpose first!')

    if args.sparse and args.file_source=='hdfs':
        raise OptionError('Not yet!')

    # print parameters
    print_params(args, logger)

    # TO-DO: put these to a configuration file
    dire = '../data/'
    hdfs_dire = 'data/'
    logs_dire = 'file:///home/jiyan/cx_logs'

    # instantializing a Spark instance
    if args.save_logs:
        conf = SparkConf().set('spark.eventLog.enabled','true').set('spark.eventLog.dir',logs_dire)
    else:
        conf = SparkConf()
    sc = SparkContext(appName="cx_exp",conf=conf)

    # loading data
    if args.file_source=='hdfs':
        A_rdd = sc.textFile(hdfs_dire+args.dataset+'.txt',args.npartitions) #loading dataset from HDFS
    else:
        A = np.loadtxt(dire+args.dataset+'.txt') #loading dataset from local disc
        if args.sparse:
            sA = to_sparse(A)
            A_rdd = sc.parallelize(sA,args.npartitions)
        else:
            A_rdd = sc.parallelize(A.tolist(),args.npartitions)

    if args.axis == 0:
        pass # get rdd from the transpose of A

    t = time.time()
    if args.sparse:
        matrix_A = SparseRowMatrix(A_rdd,args.dataset,m,n,args.cache) # creating a SparseRowMatrix instance
    else:
        matrix_A = RowMatrix(A_rdd,args.dataset,m,n,args.cache,repnum=args.nrepetitions) # creating a RowMatrix instance
        
    cx = CX(matrix_A)

    lev, p = cx.get_lev(args.k, q=args.q) # getting the approximate row leverage scores. it has the same size as the number of rows 

    if args.test:
        if args.file_source != 'local':
            A = np.loadtxt(dire+args.dataset+'.txt')
        U, D, V = np.linalg.svd(A,0)

        if args.axis == 0:
            lev_exact = np.sum(U[:,:args.k]**2,axis=1)
        else:
            lev_exact = np.sum(V.T[:,:args.k]**2,axis=1)
        p_exact = lev_exact/args.k
        logger.info('KL divergence between the estimation of leverage scores and the exact one is {0}'.format( scipy.stats.entropy(p_exact,p) ))
    logger.info('finished stage 1')
    logger.info('----------------------------------------------')

    if args.stage=='indices' or args.stage=='full':
        idx = cx.comp_idx(args.scheme,args.r) # choosing rows based on the leverage scores
        # maybe to store the indices to file
        logger.info('finished stage 2')
        logger.info('----------------------------------------------')

    if args.stage=='full':
        rows = cx.get_rows() # getting back the selected rows based on the idx computed above (this might give you different results if you rerun the above)

        if args.test:
            diff = cx.comp_err() # computing the relative error
            logger.info('relative error ||A-CX||/||A|| is {0}'.format( diff/np.linalg.norm(A,'fro') ))
            logger.info('raltive error of the best rank-{0} approximation is {1}'.format( args.k, np.sqrt(np.sum(D[args.k:]**2))/np.sqrt(np.sum(D**2)) ))
        logger.info('finished stage 3')

    rtime = time.time() - t
    logger.info('time elapsed: {0} second'.format( rtime ))
    
if __name__ == "__main__":
    main(sys.argv[1:])



