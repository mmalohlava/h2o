#!/usr/local/bin/python 
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import repl as r
import h2o_rf
import h2o

#DATASET_NAME="covtype_100k"
#DATASET_NAME="iris20kcols"
#DATASET_NAME="nchess_3_8_1000"
#DATASET_NAME="nchess_4_8_1000"
#DATASET_NAME="iris"
#DATASET_NAME="covtype"

def f(ds,f): return "bench/{0}/R/{1}".format(ds,f)

def runExperiment(ds, trees, nodes, test_dataset="test_5000.csv"):
    print """
    =============
           Nodes: {0}
           Trees: {1}
         Dataset: {2}
    Test dataset: {3}
    =============""".format(nodes, trees, ds,test_dataset)

    h2o.build_cloud(node_count=nodes,java_heap_GB=3)

    try:
        c = r.connect()
        trainKey = c.getHexKey(f(ds, "train.csv"))
        testKey = c.getHexKey(f(ds, "test.csv"))
        testSmallKey = c.getHexKey(f(ds, test_dataset))

        # train
        trainResult = c.trainRF(trainKey, ntree=trees, kind='DIVOTES',sampling_strategy="RANDOM_WITH_REPLACEMENT", test_key=testSmallKey['destination_key'], model_key="rf_model_divotes")
        #trainResult = c.trainRF(trainKey, ntree=trees, kind='NORMAL',sampling_strategy="RANDOM", test_key=testSmallKey['destination_key'], model_key="rf_model_divotes")
        # validate on test dataset
        testResult = c.scoreRF(testKey, trainResult, out_of_bag_error_estimate=0)
        # output result
        print """
    =============
           Nodes: {0}
           Trees: {1}
         Dataset: {2}
    Test dataset: {3}
    =============
    Scoring model 
    =============
    {4}""".format(nodes, trees, ds, test_dataset, h2o_rf.pp_rf_result(testResult))

        time.sleep(3600)
        return testResult
    finally:
        c.terminate()
a = {}
#a[1]=runExperiment("covtype", 200, 4, "test_100.csv")
#a[2]=runExperiment("covtype", 200, 4, "test_1000.csv")
#a[3]=runExperiment("covtype", 200, 4, "test_5000.csv")
#a[4]=runExperiment("covtype", 200, 4, "test_10000.csv")
#a[5]=runExperiment("covtype", 200, 4, "test_50000.csv")
#a[6]=runExperiment("covtype", 200, 4, "test_100000.csv")
#a[7]=runExperiment("covtype", 50, 4, "test_150000.csv")
a[8]=runExperiment("covtype", 50, 4, "test.csv")

#print h2o_rf.pp_rf_result(a[1])
#print h2o_rf.pp_rf_result(a[2])
#print h2o_rf.pp_rf_result(a[3])
#print h2o_rf.pp_rf_result(a[4])
#print h2o_rf.pp_rf_result(a[5])
#print h2o_rf.pp_rf_result(a[6])
#print h2o_rf.pp_rf_result(a[7])
print h2o_rf.pp_rf_result(a[8])

