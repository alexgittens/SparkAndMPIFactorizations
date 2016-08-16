import numpy as np

from pyspark import SparkContext
from pyspark import SparkConf

from parse_config import load_configuration
import time


def zipandsave(rdd,index, output=None):
    ids = rdd.map(lambda x:x[index]).distinct()
    ids_zipped = ids.zipWithIndex()  
    if output:
        ids_zipped.map(lambda x:str(x[0])+',' + str(x[1])).saveAsTextFile(output)
    return ids_zipped
def annotate_and_group(zipped_rdd,rdd,index):
    if index == 1:
        annotated_zipped = zipped_rdd.map(lambda x:(x[1],x[0], 'zip'))
    else:
        annotated_zipped = zipped_rdd.map(lambda x:(x[0],x[1], 'zip'))
    unioned = rdd.union(annotated_zipped)
    grouped = unioned.map(lambda x:(x[index], (x))).groupByKey().map(lambda x:(x[0], list(x[1])))
    return grouped

def replace(long_list, index):
    replace_id = None
    replace_index = 1 if index == 0 else 0
    for tuple in long_list:
        #_, other, val = tuple
        if tuple[2] =='zip':
            replace_id = tuple[replace_index]
            break
    new_list  = []
    for tuple in long_list:
        if tuple[2]!='zip':
            if index == 0 :        
                construct_tup = (replace_id, tuple[1], tuple[2])    
            else:
                construct_tup = (tuple[0], replace_id, tuple[2])  
            new_list.append(construct_tup)
    return new_list

def run_stage2(params_dict):
    logs_dir = params_dict.get('logsdir')
    input_matrix = params_dict.get('inputmatrix') 
    rows_mapping = params_dict.get('rowmappingsfile')
    column_mapping = params_dict.get('columnmappingsfile')
    out_matrix = params_dict.get('mappedmatrix')
   
    conf = SparkConf().set('spark.eventLog.enabled','true').set('spark.eventLog.dir',logs_dir).set('spark.driver.maxResultSize', '2g') 
    sc = SparkContext(appName='matrix remapping',conf=conf)
    data = sc.textFile(input_matrix)

    rdd = data.map(lambda x:x.split(',')).map(lambda x:(int(x[0]), int(x[1]), float(x[2])))
   
    rows_zipped = zipandsave(rdd, 0, rows_mapping)
   
    rows_grouped = annotate_and_group(rows_zipped, rdd, index=0)
    
    rows_assigned = rows_grouped.map(lambda x:replace(x[1], 0)).flatMap(lambda x:x)
    columns_zipped = zipandsave(rows_assigned, 1, column_mapping)
    columns_grouped = annotate_and_group(columns_zipped, rows_assigned, index =1)
    columns_assigned = columns_grouped.map(lambda x:replace(x[1], 1)).flatMap(lambda x:x)
    columns_assigned.map(lambda x:  str(x[0]) + ',' + str(x[1]) + ',' + str(x[2]) ).saveAsTextFile(out_matrix)

if __name__ == '__main__':
    config_params = load_configuration()
    stage_2_params = config_params['STAGE2']
    global_param = config_params['GLOBAL']
    stage_2_params.update(global_param)
    stage_2_params['inputmatrix'] = config_params['STAGE1']['outputmatrix']
    print 'run stage 2 with params ', stage_2_params
   
    run_stage2(stage_2_params)
    print 'run finished'



