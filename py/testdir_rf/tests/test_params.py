#!/usr/bin/python 
import os, json, unittest, time, shutil, sys, subprocess
sys.path.extend(['..','../..','py'])

import repl as r
import h2o_rf

DATASET_NAME="ebird"

def fG(ds, f): return "/Users/jreese/Google Drive/RF/%s/%s" % (ds, f)
def fP(ds, f): return '/homes/reese5/research/h2o/smalldata/%s/%s' % (ds, f)
def fJ(ds,f): return "/Users/jreese/Documents/uni/purdue/research/dr-api-mock/RF/data/%s/%s" % (ds, f)

fout = open('../../../out', 'w')
ferr = open('../../../err', 'w')

trees = [1] + range(25,100,25)
samples = range(1,101)
mtry = range(1,1778)

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

def start_h2o():
    try:
        return subprocess.Popen(['java', '-Xmx8g', '-jar',
                              '../../../target/h2o.jar'],
                                 stdout=fout, stderr=ferr)
    except OSError as e:
        print 'Failed to start h2o (%s): %s' % (e.errno, e.strerror)
        exit()

def main():
    h2o_p = start_h2o()
    time.sleep(3)

    try:
        fd = open('data.csv','a',0)
        ds = DATASET_NAME
        c = r.connect()

        # perr = 0.0
        # cerr = 0.1
        # t=s=m=0
        params = {'separator':',', 'parser_type':'CSV'}

        print '\nParsing training data...',
        # trainKey=c.getHexKey(fJ(ds,"train_10k.csv"),kwargs=params)
        # trainKey=c.getHexKey(fJ(ds,"train_1M.csv"),kwargs=params)
        # trainKey=c.getHexKey(fP(ds,"train_10K.csv"),kwargs=params)
        trainKey=c.getHexKey(fP(ds,"train_1M.csv"),kwargs=params)
            
        print '\nParsing testing data...',
        # testkey = c.getHexKey(fJ(ds,"test_4K.csv"),kwargs=params)
        # testkey=c.getHexKey(fJ(ds,"test_400K.csv"),kwargs=params)
        # testkey = c.getHexKey(fP(ds,"test_4K.csv"),kwargs=params)
        testkey = c.getHexKey(fP(ds,"test_400K.csv"),kwargs=params)

        # while (abs(cerr - perr) > 0.001):
        for t in trees:
            for s in samples:
                for m in mtry:
                    print '\nTraining with trees=%s samples=%s mtry=%s' % \
                        (t, s, m)
                    trainResult = c.trainRF(trainKey, ntree=trees[t],
                                            model_key="rf_model_%s" % ds,
                                            sample=s,features=m)
                    
                    print 'Testing...'
                    testResult=c.scoreRF(testkey,trainResult,
                                         out_of_bag_error_estimate=0)
                    
                    p_results(testResult)
                    with open('results/%st_%ss_%sm.txt' % \
                                  (t, s, m), 'w') as f:
                        f.write('%.2f\n%s' % (trainResult['python_call_timer'],
                                              p_results(testResult)))
                    fd.write('%s,%s,%s,%.2f,%.2f\n' % (
                            t, s, m,
                            testResult['confusion_matrix']\
                                ['classification_error']*100,
                            trainResult['python_call_timer']))
                    fd.flush()

            # perr = cerr
            # print cerr, perr, abs(cerr - perr), t, len(trees)
            # check for break conditions
            # cerr = testResult['confusion_matrix']['classification_error']
            # if run=='t' and t>=len(trees): break
            # if run=='s' and s>=len(samples): break
            # if run=='m' and m>=len(trees): break

        h2o_p.kill()
        fout.close()
        ferr.close()
        fd.close()
    except:
        print '\nException raised (%s)' % sys.exc_info()[0]
        fout.close()
        ferr.close()
        fd.close()
        h2o_p.kill()
        raise

if __name__ == '__main__':
    main()
