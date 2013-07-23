import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts(java_heap_GB=28)

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_short(self):
            csvFilename = 'part-00000b'
            ### csvFilename = 'short'
            importFolderPath = '/home/hduser/data'
            importFolderResult = h2i.setupImportFolder(None, importFolderPath)
            csvPathname = importFolderPath + "/" + csvFilename

            # FIX! does 'separator=' take ints or ?? hex format
            # looks like it takes the hex string (two chars)
            start = time.time()
            # hardwire TAB as a separator, as opposed to white space (9)
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, timeoutSecs=500, separator=9)
            print "Parse of", parseKey['destination_key'], "took", time.time() - start, "seconds"

            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], timeoutSecs=500)
            print "Inspect:", parseKey['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            num_rows = inspect['num_rows']
            num_cols = inspect['num_cols']

            # We should be able to see the parse result?
            ### inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename
            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.get_column_info_from_inspect(parseKey, timeoutSecs=300)

            if missingValuesDict:
                print len(missingValuesDict), "columns with missing values"
                ### m = [str(k) + ":" + str(v) for k,v in missingValuesDict.iteritems()]
                ### raise Exception("Looks like columns got flipped to NAs: " + ", ".join(m))

            if constantValuesDict:
                print len(constantValuesDict), "columns with constant values"

            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(num_rows), \
                "    num_cols:", "{:,}".format(num_cols)

            for maxx in [num_cols]:
                # don't have a quick reverse mapping for col number, but this will work
                y = "is_purchase"

                x = range(maxx)
                xOrig = x[:]
                # now remove any whose names don't match the required pattern
                pattern = "oly_|mt_|b_"
                keepX = re.compile(pattern)
                # need to walk over a copy, cause we change x
                for i in xOrig:
                    iStr = str(i)
                    if i == 5:
                        print "hello", colNameDict[iStr], iStr
                    name = colNameDict[iStr]
                    # remove it if it has the same name as the y output
                    if name == y:
                        print "Removing %s because name: %s matches output %s" % (iStr, name, y)
                        x.remove(i)

                    elif not keepX.match(name):
                        print "Removing %s because name: %s doesn't match desired pattern %s" % (iStr, name, pattern)
                        x.remove(i)

                    elif iStr in constantValuesDict:
                        value = constantValuesDict[iStr]
                        print "Removing %s with name: %s because it has constant value: %s " % (iStr, name, str(value))
                        x.remove(i)

                    # remove all cols with missing values
                    # could change it against num_rows for a ratio
                    elif iStr in missingValuesDict:
                        value = missingValuesDict[iStr]
                        print "Removing %s with name: %s because it has %d missing values" % (iStr, name, value)
                        x.remove(i)

                    # this is extra pruning..
                    # remove all cols with enums, if not already removed
                    elif iStr in enumSizeDict:
                        value = enumSizeDict[k]
                        print "Removing %s %s because it has enums of size: %d" % (iStr, name, value)
                        x.remove(i)


                print "The pruned x has length", len(x)
                x = ",".join(map(str,x))
                print "\nx:", x
                
                print "y:", y

                kwargs = {
                    'x': x, 
                    'y': y,
                    # 'case_mode': '>',
                    # 'case': 0,
                    'family': 'binomial',
                    'lambda': 1.0E-5,
                    'alpha': 0.5,
                    'max_iter': 2,
                    'thresholds': 0.5,
                    'n_folds': 1,
                    'weight': 100,
                    'beta_eps': 1.0E-4,
                    }

                timeoutSecs = 1800
                start = time.time()
                glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, pollTimeoutsecs=60, **kwargs)
                elapsed = time.time() - start
                print "glm completed in", elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()