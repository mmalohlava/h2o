#!/usr/local/bin/python 
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import repl as r
import h2o_rf

DATASET_NAME="covtype_100k"
DATASET_NAME="iris20kcols"
DATASET_NAME="nchess_3_8_1000"
DATASET_NAME="nchess_4_8_1000"
DATASET_NAME="iris"
DATASET_NAME="covtype"

TREES=10

def f(ds,f): return "bench/{0}/R/{1}".format(ds,f)

ds = DATASET_NAME
c = r.connect()
trainKey = c.getHexKey(f(ds, "train.csv"))
testKey = c.getHexKey(f(ds, "test.csv"))
testSmallKey = c.getHexKey(f(ds, "test_5000.csv"))

# train
trainResult = c.trainRF(trainKey, ntree=TREES, test_key=testSmallKey['destination_key'], model_key="rf_model_divotes")
# validate on test dataset
testResult = c.scoreRF(testKey, trainResult, out_of_bag_error_estimate=0)
# output result
print "\nScoring model \n========={0}".format(h2o_rf.pp_rf_result(testResult))
