#!/usr/local/bin/python 
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import repl as r
import h2o_rf

DATASET_NAME="NoNoise/Sorted"
DATASET_NAME="NoNoise/Unsorted"
DATASET_NAME="Noisy/Sorted"
DATASET_NAME="Noisy/Unsorted"

def f(ds,f): return "/Users/michal/Documents/postdoc/papers/2013/201309_rf_datasets_eda/data/BiasedCycle/{0}/{1}".format(ds,f)

trees=100
ds = DATASET_NAME
c = r.connect()
trainKey = c.getHexKey(f(ds, "train.csv"))
trainResult = c.trainRF(trainKey, ntree=trees, model_key="rf_model_{0}".format(ds))

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

