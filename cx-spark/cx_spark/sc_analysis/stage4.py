import time


from pyspark import SparkContext
from pyspark import SparkConf

from parse_config import load_configuration
from spark_msi import MSIDataset
from spark_msi import MSIMatrix
from spark_msi import converter
from spark_msi import get_mz_axis


def replace(values):
    new_id = None
    for val in values:
        if len(val) == 3:
            new_id = val[1]
    for val in values:
        if len(val) == 2:
            return (new_id, val[1])

def transform_tomz(x, mz_axis, tlen):
    mz_index = x/tlen
    mz_val = mz_axis[mz_index]
    return mz_val    

def get_t(x, tlen): 
    return x % tlen

def get_mz(x, tlen):
    return x / tlen

def get_x(i, xlen):
    return i % xlen

def get_y(i, xlen):
    return i / xlen

def run_stage4(params_dict):
    #logs_dir = params_dict.get('logsdir')
    column_leverage_score = params_dict.get('leveragescores')
    on_rows = params_dict.get('on_rows')
    sc = params_dict.get('sc')
    mappings = params_dict.get('mappingsfile') 
    
    raw_rdd = params_dict.get('raw_rdd')
    output =  params_dict.get('spatialvals') if on_rows else params_dict.get('columnmzvals')
    
    column_leverage_scores = sc.textFile(column_leverage_score).map(lambda x: float(str(x)))
    zipped = column_leverage_scores.zipWithIndex().map(lambda x:(x[1],x[0]))
    
    mappings = sc.textFile(mappings).map(lambda x: map(int ,x.split(','))).map(lambda x:(x[1],x[0],'zip'))
    unioned = zipped.union(mappings)

    rows_grouped = unioned.map(lambda x:(x[0], (x))).groupByKey().map(lambda x:(x[0], list(x[1])))
    new_ids = rows_grouped.map(lambda x: replace(x[1]))


    data = MSIDataset.load(sc, raw_rdd)
    mz_range = data.mz_range

    xlen,ylen,tlen,mzlen = data.shape
    if on_rows:
        save_rdd = new_ids.map(lambda x: (get_x(x[0], xlen), get_y(x[0], xlen), x[0], x[1]))
        sorted_rdd = save_rdd.sortBy(lambda x:x[3], ascending=False)
    else:
        def f(xs):
          mz_axis = get_mz_axis(mz_range)
          for x in xs:
            yield (get_t(x[0], tlen),get_mz(x[0], tlen), transform_tomz(x[0], mz_axis, tlen),  x[0],x[1])
        get_t_mz = new_ids.mapPartitions(f)
        sorted_rdd = get_t_mz.sortBy(lambda x:x[4], ascending=False)
    formatted_vals = sorted_rdd.map(lambda x: ', '.join(str(i) for i in x))
    formatted_vals.saveAsTextFile(output)

if __name__ == '__main__':
    config_params = load_configuration()
    stage4_params = config_params['STAGE4']
    global_param = config_params['GLOBAL']
    stage4_params.update(global_param)
    logs_dir = stage4_params['logsdir']
    conf = SparkConf().set('spark.eventLog.enabled', 'true').set('spark.eventLog.dir', logs_dir).set('spark.driver.maxResultSize', '2g') 
    sc = SparkContext(appName='post process', conf=conf)
    stage4_params['sc'] = sc
   
    
    stage4_params['leveragescores'] = config_params['STAGE3']['columnleveragescores']
    stage4_params['mappingsfile'] = config_params['STAGE2']['columnmappingsfile']
    stage4_params['raw_rdd'] = config_params['STAGE1']['outpathrdd']
    print 'run stage 4 for column params ', stage4_params
   
    run_stage4(stage4_params)
    print 'run finished'
    stage4_params['on_rows'] = True
    stage4_params['leveragescores'] = config_params['STAGE3']['rowleveragescores']
    stage4_params['mappingsfile'] = config_params['STAGE2']['rowmappingsfile']
    print 'run stage 4 for row params ', stage4_params
   
    run_stage4(stage4_params)
    print 'run finished'





