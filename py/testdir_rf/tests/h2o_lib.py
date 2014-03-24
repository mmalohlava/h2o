#!/usr/bin/python 
import os, json, unittest, time, shutil, sys, subprocess, traceback
sys.path.extend(['..','../..','py'])

import repl as r
import h2o_rf

ds=''

TREES=75

def fG(ds, f): return "/Users/jreese/Google Drive/RF/%s/%s" % (ds, f)
def fP(ds, f): return '/homes/reese5/research/h2o/smalldata/%s/%s' % (ds, f)
def fJ(ds,f): return "/Users/jreese/Documents/uni/purdue/research/dr-api-mock/RF/data/%s/%s" % (ds, f)

fout = open('../../../out', 'w')
ferr = open('../../../err', 'w')

TREES = 75

params = {'separator':',', 'parser_type':'CSV'}

def dump():
    traceback.print_exception(sys.exc_info()[0], sys.exc_info()[1],
                              sys.exc_info()[2])
    
def cleanup(h2o_p, fd):
    try:
        os.system('rm -rf /tmp/h2o*; rm -rf /tmp/ice*')
        h2o_p.kill()
        fout.close()
        ferr.close()
        fd.close()
    except:
        dump()
        
def p_results(result, t, s, m, fd, trainResult):
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
    with open('results/%st_%ss_%sm.txt' % (t, s, m),
              'w') as f:
        f.write('%.2f\n%s' % (trainResult['python_call_timer'], data))
    fd.write('%s,%s,%s,%.2f,%.2f\n' % (
        t, s, m,
        result['confusion_matrix']['classification_error']*100,
        trainResult['python_call_timer']))
    fd.flush()

def start_h2o():
    try:
        return subprocess.Popen(['java', '-Xmx32g', '-jar',
                              '../../../target/h2o.jar'],
                                stdout=fout, stderr=ferr)
    except OSError as e:
        print 'Failed to start h2o (%s): %s' % (e.errno, e.strerror)
        exit()

def parse(c, ds):
    print '\nParsing training data...',
    # trainKey=c.getHexKey(fJ(ds,"train_10k.csv"),kwargs=params)
    # trainKey=c.getHexKey(fJ(ds,"train_1M.csv"),kwargs=params)
    # trainKey=c.getHexKey(fP(ds,"train_10K.csv"),kwargs=params)
    trainKey=c.getHexKey(fP(ds,"train_1M.csv"),kwargs=params)
    
    print '\nParsing testing data...',
    # testKey = c.getHexKey(fJ(ds,"test_4K.csv"),kwargs=params)
    # testKey=c.getHexKey(fJ(ds,"test_400K.csv"),kwargs=params)
    # testKey = c.getHexKey(fP(ds,"test_4K.csv"),kwargs=params)
    testKey = c.getHexKey(fP(ds,"test_400K.csv"),kwargs=params)

    return trainKey, testKey

def restart(h2o_p):
    try:
        h2o_p.kill()
    except:
        pass
        
    while True:
        try:
            time.sleep(5)
            h2o_p = start_h2o()
            time.sleep(5)
            c = r.connect()
            trainKey, testKey = parse(c, ds)
            
        except:
            dump()
            h2o_p.kill()
            os.system('rm -rf /tmp/h2o*; rm -rf /tmp/ice*')

        else:
            return trainKey, testKey, h2o_p, c
    

def connect(d):
    global ds
    ds = d
    h2o_p = start_h2o()
    time.sleep(3)

    return h2o_p

