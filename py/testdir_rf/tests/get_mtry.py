#!/usr/bin/python
import time, sys
from h2o_lib import *

sys.path.extend(['..','../..','py'])
import repl as r

ds="ebird"
mtry = range(10,500,5)
    
def main():
    try:
        fd = open('log.csv','a',0)
        h2o_p = connect(ds)
        c = r.connect()
        trainKey, testKey = parse(c, ds)

        m=0
        while m < range(len(mtry)):
            print '\nTraining with trees=%s samples=%s mtry=%s' % \
                (TREES, .67, mtry[m])
            try:
                trainResult = c.trainRF(trainKey, ntree=TREES,
                                        model_key="rf_model_%s" % ds,
                                        features=mtry[m])

            except:
                dump()
                # cleanup and start again
                trainKey, testKey, h2o_p, c = restart(h2o_p)
                c = r.connect()
                continue
            try:
                print 'Testing...'
                testResult=c.scoreRF(testKey,trainResult,
                                     out_of_bag_error_estimate=0)

                p_results(testResult, TREES, 67, mtry[m], fd,
                          trainResult)
                
            except:
                dump()
                # cleanup and start again
                trainKey, testKey, h2o_p, c = restart(h2o_p)
                c = r.connect()
                continue
            m+=1
            cleanup(h2o_p, fd)
            exit()
                    
        cleanup(h2o_p, fd)

    except:
        print '\n%sTerminating Exception Raised!!!!%s\n' % ('*'*20,'*'*20)
        dump()
        cleanup(h2o_p, fd)

if __name__ == '__main__':
    main()
