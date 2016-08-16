import json

import cPickle as pickle
import numpy as np
from scipy.sparse import coo_matrix



def add(x, y):
    x += y
    return x

def unifSampling(row, n, s):
    #row = row.getA1()
    p = s/n
    coin = np.random.rand()
    if coin < p:
        return row/p

def pickle_load(filename):
    return pickle.load(open( filename, 'rb' ))

def pickle_write(filename,data):
    with open(filename, 'w') as outfile:
        pickle.dump(data, outfile, True)

def json_write(filename,*args):
    with open(filename, 'w') as outfile:
        for data in args:
            json.dump(data, outfile)
            outfile.write('\n')

class BlockMapper:
    """
    process data after receiving a block of records
    """
    def __init__(self, blk_sz=5e4):
        self.blk_sz = blk_sz
        self.keys = []
        self.data = {'row':[],'col':[],'val':[]}
        self.sz = 0

    def __call__(self, records, **kwargs):
        for r in records:
            self.parse(r)
            self.sz += 1
                
            if self.sz >= self.blk_sz:
                for result in self.process(**kwargs):
                    yield result
                self.keys = []
                self.data = {'row':[],'col':[],'val':[]}
                self.sz = 0

        if self.sz > 0:
            for result in self.process(**kwargs):
                yield result

        for result in self.close():
            yield result

    def parse(self,r):
        self.keys.append(r[0])
        self.data.append(r[1])

    def process(self,**kwargs):
        return iter([])
    
    def close(self):
        return iter([])

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

