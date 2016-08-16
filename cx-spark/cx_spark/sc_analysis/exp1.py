from pyspark import SparkContext
from pyspark import SparkConf
from cx import *
from rowmatrix import *
from utils import *
import sys
import argparse
import scipy.stats
import numpy as np

import time
def main(kwargs):
 
    (m,n) = kwargs['dims']

    # validating
    if kwargs['k'] > m or kwargs['k'] > n:
        raise ValueError('Rank parameter({0}) should not be greater than m({1}) or n({2})'.format(args.k,m,n))

    #if m < n:
    #    raise ValueError('Number of rows({0}) should be greater than number of columns({1})').format(m,n)

    # print parameters
    #print_params(args)

    # TO-DO: put these to a configuration file
    results = {}
    #dire = '../data/'
    hdfs_dire = 'data/'
    logs_dire = '/global/u2/m/msingh/sc_paper/new_version/sc-2015/logs'

    
    sc = kwargs['sc']
    # loading data
    if kwargs['file_source']=='hdfs':
        A_rdd = sc.textFile(hdfs_dire+kwargs['dataset']+'.txt',kwargs['npartitions']) #loading dataset from HDFS
    else:
        A = np.loadtxt(kwargs['dataset']+'.txt') #loading dataset from local disc
        A_rdd = sc.parallelize(A.tolist(),kwargs['npartitions'])
        #A_rdd = sc.textFile(A.tolist(),args.npartitions)

    if kwargs['test']:  #only need to load these in the test mode
        A = np.loadtxt(kwargs['dataset']+'.txt')
        D = np.loadtxt(kwargs['dataset']+'_D.txt')
        U = np.loadtxt(kwargs['dataset']+'_U.txt')
    
    t = time.time()
    matrix_A = RowMatrix(A_rdd,kwargs['dataset'],m,n,kwargs['cache'])

    cx = CX(matrix_A, sc=sc)
    k = kwargs['k']
    q = kwargs['q']
    lev, p = cx.get_lev(k,axis=0, q=q) # getting the approximate row leverage scores. it has the same size as the number of rows 
    #lev, p = cx.get_lev(n, load_N, save_N, projection_type='gaussian',c=1e3) #target rank is n

    if kwargs['test']:
        lev_exact = np.sum(U[:,:k]**2,axis=1)
        p_exact = lev_exact/k
        print 'KL divergence between the estimation of leverage scores and the exact one is {0}'.format( scipy.stats.entropy(p_exact,p) )
        results['kl_diverge'] = scipy.stats.entropy(p_exact,p)
    print 'finished stage 1'
    print '----------------------------------------------'
    r = kwargs['r']
    if kwargs['stage']=='leverage' or kwargs['stage']=='full':
        idx = cx.comp_idx(kwargs['scheme'],r) # choosing rows based on the leverage scores
        # maybe to store the indices to file
        print 'finished stage 2'
        print '----------------------------------------------'

    if kwargs['stage']=='full':
        rows = cx.get_rows() # getting back the selected rows based on the idx computed above (this might give you different results if you rerun the above)

        if kwargs['test']:
            diff = cx.comp_err() # computing the relative error
            print 'relative error ||A-CX||/||A|| is {0}'.format( diff/np.linalg.norm(A,'fro') )
            print 'raltive error of the best rank-{0} approximation is {1}'.format( kwargs['k'], np.sqrt(np.sum(D[kwargs['k']:]**2))/np.sqrt(np.sum(D**2)) )
            results['relative_error'] = diff/np.linalg.norm(A,'fro')
            results['rank_approx_error'] = np.sqrt(np.sum(D[kwargs['k']:]**2))/np.sqrt(np.sum(D**2))
        print 'finished stage 3'

    rtime = time.time() - t
    results['time'] = rtime
    print 'time elapsed: {0} second'.format( rtime )
    return results
    
if __name__ == "__main__":
    #qs = [1,2,3,4,5]
    qs = [3]
    q = qs[0]
    rs = [10,20,30,50,70,100,200]
    r = 20
    res = {}
    logs_dire = '/global/u2/m/msingh/sc_paper/new_version/sc-2015/logs'
    conf = SparkConf().set('spark.eventLog.enabled','true').set('spark.eventLog.dir',logs_dire)
    sc = SparkContext(appName="cx_exp",conf=conf)
    if True:
    #for r in rs:
        errors = []
        rank_approx_error = []
        times = []
        kls = []
        if True:
        #for i in range(5):
            args = {'dims': (10000,100), 'k':5, 'r':r, 'q':q, 'cache':True, 'test':True,'file_source':'local','sc': sc,
                'dataset':'/global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/data/unif_bad_10000_100','save_logs': True, 'scheme':'randomized','stage': 'full','npartitions':200
            }
            result =main(args)
            errors.append(result['relative_error'])
            rank_approx_error.append(result['rank_approx_error'])
            times.append(result['time'])
            kls.append(result['kl_diverge'])

        res[r] = {'relative_error': sum(errors)/len(errors),'rank_approx_error':sum(rank_approx_error)/len(rank_approx_error), 'times':sum(times)/len(times),'kl':sum(kls)/len(kls)}
        print "result so far ", res
    print "final result ", res




