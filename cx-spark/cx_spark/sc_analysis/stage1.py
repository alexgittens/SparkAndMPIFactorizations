import numpy as np
from pyspark import SparkContext
from pyspark import SparkConf

from parse_config import load_configuration
from spark_msi import MSIDataset
from spark_msi import MSIMatrix
from spark_msi import converter


def run_stage1(params_dict):
	imzXMLPath = params_dict.get('imzxmlpath')
	imzBinPath = params_dict.get('imzbinpath')
	logs_dir = params_dict.get('logsdir')
	output_rdd = params_dict.get('outpathrdd')
	output_matrix = params_dict.get('outputmatrix')
	conf = SparkConf().set('spark.eventLog.enabled', 'true').set('spark.eventLog.dir', logs_dir).set('spark.driver.maxResultSize', '2g') 

	sc = SparkContext(appName='Loader', conf=conf)
	dataset = converter(sc, imzXMLPath, imzBinPath, output_rdd)

	rdd = dataset.spectra
	dataset.save(output_rdd)
	data = MSIDataset.load(sc, output_rdd)
	mat = MSIMatrix(data)
	non_zer = mat.nonzeros

	rdd = non_zer.map(lambda x: ' ,'.join(str(ele) for ele in x))  #  str(x[0]) +',' + str(x[1]) + ',' + str(x[2]) )
	rdd.saveAsTextFile(output_matrix)

if __name__ == '__main__':
	config_params = load_configuration()
	stage_1_params = config_params['STAGE1']
	global_param = config_params['GLOBAL']
	stage_1_params.update(global_param)
	print 'run stage 1 with params ', stage_1_params
	run_stage1(stage_1_params)
	print 'run completed'





