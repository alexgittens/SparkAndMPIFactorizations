from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.storagelevel import StorageLevel 
from utils import prepare_matrix
from cx import CX
from sparse_row_matrix import SparseRowMatrix

conf = SparkConf().set('spark.eventLog.enabled','true').set('spark.driver.maxResultSize', '15g').set("spark.executor.memory", "30g")
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
    val = ast.literal_eval(s)
    return val[0], (np.array(val[1][0]), np.array(val[1][1]))


data = sc.textFile('/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/ncolumns_matrix').map(lambda x:parse(x))
#row_shape = 131048
#column_shape = 8258911
#131047 8258910
row_shape = 8258911
column_shape =131048
#column_shape+=20

print data.take(1)

matrix_A = SparseRowMatrix(data,'output', row_shape,column_shape, False)
cx = CX(matrix_A)
k = 2
q = 2
lev, p = cx.get_lev(k,axis=0, q=q) 
#end = time.time()
leverage_scores_file='/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/columns_row_leverage_scores_logged'
p_score_file='/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/columns_p_scores_logged'
np.savetxt(leverage_scores_file, np.array(lev))
np.savetxt(p_score_file, np.array(p))



"""
def parse_func(x):
    stringed  = str(x)
    chunks = stringed.split(",")
    return int(chunks[1]), int(chunks[0]), float(chunks[2])

data = sc.textFile("/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/final_matrix").map(lambda x:    parse_func(x))
#rows_rdd = data.map(lambda x:str(x)).map(lambda x:x.split(',')).map(lambda x:(int(x[0]), int(x[1]), float(x[2])))
#sorted_Rdd = prepare_matrix(rows_rdd)
#sorted_Rdd.saveAsTextFile('/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/rows_matrix')
#columns_rdd = data.map(lambda x: (x[1],x[0],x[2]))
csorted_rdd = prepare_matrix(data)
csorted_rdd.saveAsTextFile('/scratch1/scratchdirs/msingh/sc_paper/experiments/striped_data/ncolumns_matrix')
print "completed"

d = data.take(2)
r = rdd.take(2)
print d
print r
rdd.map(lambda x:str(x)).map(lambda x:x.split(',')[1]).saveAsTextFile('/scratch1/scratchdirs/msingh/sc_paper/full_output/scratch/columns1')
"""
#.map(lambda x:str(x)).map(lambda x:x.split(',')[1]).saveAsTextFile('/scratch1/scratchdirs/msingh/sc_paper/full_output/scratch/columns')

