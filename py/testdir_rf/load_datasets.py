#!/usr/local/bin/python 
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import repl as r

DATASET_NAME="covtype_100k"
DATASET_NAME="iris20kcols"
DATASET_NAME="nchess_3_8_1000"
DATASET_NAME="nchess_4_8_1000"
DATASET_NAME="iris"
DATASET_NAME="covtype"

def f(ds,f): return "bench/{0}/R/{1}".format(ds,f)

ds = DATASET_NAME
c = r.connect()
c.getHexKey(f(ds, "train.csv"))
c.getHexKey(f(ds, "test.csv"))
c.getHexKey(f(ds, "test_5000.csv"))

