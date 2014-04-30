#!/usr/bin/python 
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import repl as r
import h2o_rf

# DATASET_NAME="NoNoise/Sorted"
# DATASET_NAME="Biased/NoNoise/Unsorted"
# DATASET_NAME="covtype"
DATASET_NAME="ebird"

def fG(ds, f): return "/Users/jreese/Google Drive/RF/%s/%s" % (ds, f)
def fP(ds, f): return '/homes/reese5/research/h2o/smalldata/%s/%s' % (ds, f)
def f(ds,f): return "/Users/jreese/Documents/uni/purdue/research/dr-api-mock/RF/data/%s/%s" % (ds, f)
def f2(f): return "/Users/jreese/Documents/uni/purdue/TA/14sp240/240s14/projects/4/src/data/%s" % f

trees = 10 if len(sys.argv) < 2 else int(sys.argv[1])
    
ds = DATASET_NAME
c = r.connect()

params = {'separator':',', 'parser_type':'CSV'}

# trainKey = c.getHexKey(fP(ds,"train_1K.csv"),parser_type='CSV',separator=',')
# trainKey = c.getHexKey(fP(ds,"train_1M.csv"),parser_type='CSV',separator=',')
# trainKey = c.getHexKey(f(ds,"train_1M.csv"),parser_type='CSV',separator=',')
# trainKey = c.getHexKey(f(ds,"train_10K.csv"),parser_type='CSV',separator=',')
# trainKey = c.getHexKey(fG(ds,"train.csv"),parser_type='CSV',separator=',')
print f2("train.txt")
trainKey = c.getHexKey(f2("train.txt"),kwargs=params)

trainResult = c.trainRF(trainKey, ntree=trees,
                        model_key="rf_model_{0}".format(ds))
# print "%s" % h2o_rf.pp_rf_result(trainResult)

# testkey = c.getHexKey(f(ds,"test_400K.csv"),parser_type='CSV',separator=',')
testkey = c.getHexKey(f2("test.txt"), kwargs=params)
# testkey = c.getHexKey(fG(ds, "test.csv"), parser_type='CSV', separator=',')
testResult = c.scoreRF(testkey, trainResult, out_of_bag_error_estimate=0)

print """
=============
       Trees: {0}
     Dataset: {1}
=============
Scoring model 
=============
{2}""".format(trees, ds,  h2o_rf.pp_rf_result(testResult))
