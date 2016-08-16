import sys
print >> sys.stderr, "job_cx starting"
from pyspark import SparkContext
from spark_msi import MSIDataset, MSIMatrix
from sparse_row_matrix import SparseRowMatrix
from cx import CX
from utils import prepare_matrix
import os
import logging.config
logging.config.fileConfig('logging.conf',disable_existing_loggers=False)
logger = logging.getLogger(__name__)

sc = SparkContext()
logger.info("job_cx starting with appId=" + sc._jsc.sc().applicationId())
prefix = 'hdfs:///sc-2015/'
name = 'Lewis_Dalisay_Peltatum_20131115_hexandrum_1_1-masked'
logger.info("job_cx loading RDD from %s" % name)
#dataset = MSIDataset.load(sc, 'meta/' + name, prefix + name).cache()
#msimat = MSIMatrix.from_dataset(sc, dataset)
#msimat.save(prefix, 'meta', name)
msimat = MSIMatrix.load(sc, prefix, 'meta', name)
logger.info("shape: %s" % (msimat.shape,))
mat = prepare_matrix(msimat.nonzeros).cache()
mat = SparseRowMatrix(mat, "msimat", msimat.shape[0], msimat.shape[1], cache=False)
cx = CX(mat)
k = 32
q = 5
lev, p = cx.get_lev(k,axis=0, q=q)
with open('dump.pkl', 'w') as outf:
  import cPickle as pickle
  data = {
      'lev': lev,
      'p': p
  }
  pickle.dump(data, outf)
