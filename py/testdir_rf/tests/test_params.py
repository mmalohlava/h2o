#!/usr/bin/python 
import os, json, unittest, time, shutil, sys
sys.path.extend(['..','../..','py'])

import repl as r
import h2o_rf

def p_results(result):
    data = ''
    for c,e in zip(result['confusion_matrix']['header'],
                   result['confusion_matrix']['classes_errors']):
        data += 'class %s errorPct: %.2f\n' % (c, e*100)
    data += '\nConfusion matrix:\n\t'
    data += '%s\t' % ('\t'.join(result['confusion_matrix']['header']))
    # for v in result['confusion_matrix']['header']: data += '%s\t' % v
    data += 'total\n'

    rowTots = [ sum(y) for y in result['confusion_matrix']['scores'] ]
    colTots = map(sum, zip(*result['confusion_matrix']['scores']))

    for v,i in zip(result['confusion_matrix']['scores'],
                   range(len(result['confusion_matrix']['scores']))):
        data += '%s\t' % result['confusion_matrix']['header'][i]
        for col in v:
            data += '%s\t' % col
        
        data += '%s\n' % rowTots[i]
    data += 'total\t%s\t%s\n' % ('\t'.join(map(str, colTots)), sum(colTots))
    data += '\nTime  : %.4fs' % result['python_call_timer']
    data += h2o_rf.pp_rf_result(result)
    # print data
    return data

DATASET_NAME="ebird"

def fG(ds, f): return "/Users/jreese/Google Drive/RF/%s/%s" % (ds, f)
def fP(ds, f): return '/homes/reese5/research/h2o/smalldata/%s/%s' % (ds, f)
def f(ds,f): return "/Users/jreese/Documents/uni/purdue/research/dr-api-mock/RF/data/%s/%s" % (DATASET_NAME, f)

ds = DATASET_NAME
c = r.connect()

trees=10

print '\nParsing training data...',
trainKey = c.getHexKey(fP(ds,"train_10K.csv"),parser_type='CSV',separator=',')
# trainKey = c.getHexKey(f(ds,"train_10k.csv"),parser_type='CSV',separator=',')
# trainKey = c.getHexKey(f(ds,"train_1M.csv"),parser_type='CSV',separator=',')

print '\nParsing testing data...',
# testkey = c.getHexKey(f(ds, "test_400K.csv"),parser_type='CSV',separator=',')
# testkey = c.getHexKey(f(ds, "test_4K.csv"),parser_type='CSV',separator=',')
testkey = c.getHexKey(fP(ds, "test_4K.csv"),parser_type='CSV',separator=',')

print 'Training...'
trainResult = c.trainRF(trainKey, ntree=trees,
                        model_key="rf_model_{0}".format(ds))

print 'Testing...'
testResult = c.scoreRF(testkey, trainResult, out_of_bag_error_estimate=0)

print testResult
p_results(testResult)
with open('%st.txt' % trees, 'w') as f:
    f.write(p_results(testResult))


