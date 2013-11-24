#!/usr/local/bin/python 
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o_rf
import repl as r

DATASET_NAME="covtype_100k"
DATASET_NAME="iris20kcols"
DATASET_NAME="nchess_3_8_1000"
DATASET_NAME="nchess_4_8_1000"
DATASET_NAME="iris"
DATASET_NAME="covtype"

def f(ds,f): return "bench/{0}/R/{1}".format(ds,f)

trees=50
ds = DATASET_NAME
c = r.connect()
trainKey = c.getHexKey(f(ds, "train.csv"))
trainResult = c.trainRF(trainKey, ntree=trees, model_key="rf_model")

testKey = c.getHexKey(f(ds, "test.csv"))
testResult = c.scoreRF(testKey, trainResult,out_of_bag_error_estimate=0)

print """
=============
       Trees: {0}
     Dataset: {1}
=============
Scoring model 
=============
{2}""".format(trees, ds,  h2o_rf.pp_rf_result(testResult))

