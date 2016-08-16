from pyspark import SparkContext
from pyspark import SparkConf

from utils import prepare_matrix
from cx import CX
from sparse_row_matrix import SparseRowMatrix

conf = SparkConf().set('spark.eventLog.enabled','true').set('spark.driver.maxResultSize', '15g') 
sc = SparkContext(appName='cx_exp',conf=conf)
import ast
import numpy as np
import logging.config
import logging
#logging.config.fileConfig('logging.conf',disable_existing_loggers=False)
logging.config.fileConfig('/global/u2/m/msingh/sc_paper/new_version/newest_version/sc-2015/cx_spark/src/logging.conf', disable_existing_loggers=False) 
logger = logging.getLogger(__name__)
def parse(string):
    s = str(string)
    data = s.split("::")
    return int(data[0]), int(data[1]), float(data[2])
def _indexed(grouped_list):
    indexed, values = [],[]
    for tup in grouped_list:
        indexed.append(tup[0])
        values.append(tup[1])
    return np.array(indexed), np.array(values)

def prepare_matrix(rdd):
    gprdd = rdd.map(lambda x:(x[0],(x[1],x[2]))).groupByKey().map(lambda x :(x[0],list(x[1])))
    flattened_rdd = gprdd.map(lambda x: (x[0],_indexed(x[1])))
    return flattened_rdd
def _indexed(grouped_list):
    indexed, values = [],[]
    for tup in grouped_list:
        indexed.append(tup[0])
        values.append(tup[1])
    return np.array(indexed), np.array(values)
filename = "/global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/data/movielens/ml-10M100K/ratings.dat"
#filename = '/global/u2/m/msingh/sc_paper/new_version/sc-2015/cx_spark/data/ml-100k/u.data'
data = sc.textFile(filename).map(lambda x:parse(x))
row_shape = data.map(lambda x:x[0]).max() +1
column_shape = data.map(lambda x:x[1]).max()+1
drdd = data.map(lambda x:(x[0],(x[1],x[2]))).groupByKey().map(lambda x :(x[0],list(x[1]))).map(lambda x: (x[0],_indexed(x[1])))
print drdd.take(1)
#prep_rdd = prepare_matrix(data)
matrix_A = SparseRowMatrix(drdd,'output', row_shape,column_shape, True)
cx = CX(matrix_A)
k = 2
q = 2
lev, p = cx.get_lev(k,axis=0, q=q) 
#end = time.time()
leverage_scores_file='/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/movielens_leverage_scores_full1'
p_score_file='/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/movielens_p_scores_full1'
np.savetxt(leverage_scores_file, np.array(lev))
np.savetxt(p_score_file, np.array(p))





#825 post street
"""
rows_rdd = data.map(lambda x:str(x)).map(lambda x:x.split(',')).map(lambda x:(int(x[0]), int(x[1]), float(x[2])))
sorted_Rdd = prepare_matrix(rows_rdd)
sorted_Rdd.saveAsTextFile('/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/rows_matrix')
columns_rdd = rows_rdd.map(lambda x: (x[1],x[0],x[2]))
csorted_rdd = prepare_matrix(columns_rdd)
sorted_Rdd.saveAsTextFile('/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/columns_matrix')
print "completed"

d = data.take(2)
r = rdd.take(2)
print d
print r
rdd.map(lambda x:str(x)).map(lambda x:x.split(',')[1]).saveAsTextFile('/scratch1/scratchdirs/msingh/sc_paper/full_output/scratch/columns1')

#.map(lambda x:str(x)).map(lambda x:x.split(',')[1]).saveAsTextFile('/scratch1/scratchdirs/msingh/sc_paper/full_output/scratch/columns')
"""
