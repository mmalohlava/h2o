#!/usr/bin/python
import time, sys
from h2o_lib import *

sys.path.extend(['..','../..','py'])
import repl as r

ds="ebird"

NODES=range(45,66,5)
MTRY=50

def main():
    try:
        fd = open('dist_log.csv','a',0)
        h2o_p = connect_p(ds, NODES[0])
        c = r.connect()
        trainKey, testKey = parse(c, ds)
        n=0
        while n < len(NODES):
            print '\nTraining with trees=%s nodes=%s' % \
                (TREES, NODES[n])
            try:
                trainResult = c.trainRF(trainKey, ntree=TREES,
                                        model_key="rf_model_%s" % ds,
                                        features=MTRY)

            except:
                dump()
                # cleanup and start again
                trainKey, testKey, h2o_p, c = restart_p(h2o_p)
                continue
            try:
                print 'Testing...'
                testResult=c.scoreRF(testKey,trainResult,
                                     out_of_bag_error_estimate=0)

                p_results_p(testResult, TREES, NODES[n], fd, trainResult)
                
            except:
                dump()
                # cleanup and start again
                trainKey, testKey, h2o_p, c = restart_p(h2o_p)
                continue
            n+=1
            h2o_p.append(0)
            restart_p(h2o_p)
        cleanup_p(h2o_p, fd)

    except:
        print '\n%sTerminating Exception Raised!!!!%s\n' % ('*'*20,'*'*20)
        dump()
        cleanup_p(h2o_p, fd)

if __name__ == '__main__':
    main()
