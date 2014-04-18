#!/usr/bin/python
import time, sys
from h2o_lib import *

sys.path.extend(['..','../..','py'])
import repl as r

ds="ebird"

MACHINES=range(5,18,1)
JVMS=1
MTRY=50

def main():
    try:
        fd = open('dist_log.csv','a',0)
        connect_d(ds, MACHINES[0], JVMS)
        n=0
        while n < len(MACHINES):
            try:
                c = r.connect()
                trainKey, testKey = parse(c, ds)
                print '\nTraining with trees=%s machines=%s' % \
                    (TREES, MACHINES[n])
                trainResult = c.trainRF(trainKey, ntree=TREES,
                                        model_key="rf_model_%s" % ds,
                                        features=MTRY)
            except:
                dump()
                # cleanup and start again
                restart_d(ds, MACHINES[n], MACHINES[n], JVMS)
                continue
            try:
                print 'Testing...'
                testResult=c.scoreRF(testKey,trainResult,
                                     out_of_bag_error_estimate=0)
                p_results_d(testResult, TREES, MACHINES[n], fd, trainResult)
                
            except:
                dump()
                # cleanup and start again
                restart_d(ds, MACHINES[n], MACHINES[n], JVMS)
                continue
            if n+1 > len(MACHINES):
                shutdown_d(d, MACHINES[n])
            else:
                restart_d(ds, MACHINES[n], MACHINES[n]+1, JVMS)
            n+=1

    except:
        print '\n%sTerminating Exception Raised!!!!%s\n' % ('*'*20,'*'*20)
        dump()
        shutdown_d(ds, MACHINES[0])
        exit(1)

    shutdown_d(ds, MACHINES[0])
    
if __name__ == '__main__':
    main()
