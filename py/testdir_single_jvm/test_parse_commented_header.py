import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])


import h2o, h2o_cmd, h2o_hosts
import h2o_browse as h2b
import random

def write_syn_dataset(csvPathname, rowCount, headerData, rList):
    dsf = open(csvPathname, "w+")
    dsf.write(headerData + "\n")
    for i in range(rowCount):
        # two choices on the input. Make output choices random
        r = rList[random.randint(0,1)] + "," + str(random.randint(0,7))
        dsf.write(r + "\n")
    dsf.close()

def rand_rowData(colCount):
    rowData = [random.randint(0,7) for i in range(colCount)]
    rowData1= ",".join(map(str,rowData))
    rowData = [random.randint(0,7) for i in range(colCount)]
    rowData2= ",".join(map(str,rowData))
    # RF will complain if all inputs are the same
    r = [rowData1, rowData2]
    return r

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_MB=1300,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
    
    def test_parse_multi_header_single(self):
        print "If header=1, and the first char of first line is #, the # should be removed/ignored" + \
            "and the line parsed normally as a header"
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_ints.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON,output"

        # cols must be 9 to match the header above, otherwise a different bug is hit
        # extra output is added, so it's 10 total
        # try different header prefixes to see if it's okay..only white space is space?
        tryList = [
            (2, 300, 9, 'cA', 60, "#"),
            (2, 300, 9, 'cA', 60, "# "),
            (2, 300, 9, 'cA', 60, " #"),
            (2, 300, 9, 'cA', 60, " # "),
            (2, 300, 9, 'cA', 60, "#  "),
            ]

        trial = 0
        for (fileNum, rowCount, colCount, key2, timeoutSecs, headerPrefix) in tryList:
            trial += 1
            # FIX! should we add a header to them randomly???
            print "Wait while", fileNum, "synthetic files are created in", SYNDATASETS_DIR
            rowxcol = str(rowCount) + 'x' + str(colCount)
            totalCols = colCount + 1 # 1 extra for output
            totalRows = 0

            # might as well try for multi-file parse?
            for fileN in range(fileNum):
                csvFilename = 'syn_' + str(fileN) + "_" + str(SEED) + "_" + rowxcol + '.csv'
                csvPathname = SYNDATASETS_DIR + '/' + csvFilename
                rList = rand_rowData(colCount)
                write_syn_dataset(csvPathname, rowCount, headerData=headerPrefix + headerData, rList=rList)
                totalRows += rowCount

            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            key = "syn_" + str(trial)
            key2 = "syn_" + str(trial) + ".hex"

            # DON"T get redirected to S3! (EC2 hack in config, remember!)
            # use it at the node level directly (because we gen'ed the files.
            # I suppose we could force the redirect state bits in h2o.nodes[0] to False, instead?:w
            h2o.nodes[0].import_files(SYNDATASETS_DIR)
            # use regex. the only files in the dir will be the ones we just created with  *fileN* match
            start = time.time()
            parseKey = h2o.nodes[0].parse('*'+rowxcol+'*', key2=key2, header=1, timeoutSecs=timeoutSecs)
            print "parseKey['destination_key']: " + parseKey['destination_key']
            print 'parse time:', parseKey['response']['time']

            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            # should match # of cols in header or ??
            self.assertEqual(inspect['num_cols'], totalCols, 
                "parse created result with the wrong number of cols %s %s" % (inspect['num_cols'], totalCols))
            self.assertEqual(inspect['num_rows'], totalRows,
                "parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
                (inspect['num_rows'], totalRows))

            kwargs = {'sample': 75, 'depth': 25, 'ntree': 1, 'ignore': 'ID,CAPSULE'}

            start = time.time()
            rfv = h2o_cmd.runRFOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            print "trial #", trial, "totalRows:", totalRows, "parse end on ", csvFilename, \
                'took', time.time() - start, 'seconds'

            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
