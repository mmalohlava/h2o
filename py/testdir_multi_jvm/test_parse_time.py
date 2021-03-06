import random

import unittest, time, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd
import h2o_browse as h2b

# some dates are "wrong"..i.e. the date should be constrained
# depending on month and year.. Assume 1-31 is legal
months = [
    ['Jan', 'JAN'],
    ['Feb', 'FEB'],
    ['Mar', 'MAR'],
    ['Apr', 'APR'],
    ['May', 'MAY'],
    ['Jun', 'JUN'],
    ['Jul', 'JUL'],
    ['Aug', 'AUG'],
    ['Sep', 'SEP'],
    ['Oct', 'OCT'],
    ['Nov', 'NOV'],
    ['Dec', 'DEC']
    ]

def getRandomDate():
    # assume leading zero is option
    day = str(random.randint(1,31)).zfill(2)
    if random.randint(0,1) == 1:
        day = day.zfill(2) 

    year = str(random.randint(0,99)).zfill(2)
    if random.randint(0,1) == 1:
        year = year.zfill(2) 

    # randomly decide on number or translation for month
    ### if random.randint(0,1) == 1:
    # FIX! H2O currently only supports the translate months
    if 1==1:
        month = random.randint(1,12)
        monthTranslateChoices = months[month-1]
        month = random.choice(monthTranslateChoices)
    else:
        month = str(random.randint(1,12)).zfill(2)
        if random.randint(0,1) == 1:
            month = month.zfill(2) 

    a  = "%s-%s-%s" % (day, month, year)
    return a

def rand_rowData(colCount=6):
    a = [getRandomDate() for fields in range(colCount)]
    # put a little white space in!
    b = ", ".join(map(str,a))
    return b

def write_syn_dataset(csvPathname, rowCount, headerData=None, rowData=None):
    dsf = open(csvPathname, "w+")
    if headerData is not None:
        dsf.write(headerData + "\n")
    if rowData is not None:
        for i in range(rowCount):
            dsf.write(rowData + "\n")
    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=10,use_flatfile=True)
        else:
            import h2o_hosts
            h2o_hosts.build_cloud_with_hosts()
        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_parse_time(self):
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn_time.csv"
        csvPathname = SYNDATASETS_DIR + '/' + csvFilename

        headerData = None
        colCount = 6
        rowData = rand_rowData(colCount)
        rowCount = 1000
        write_syn_dataset(csvPathname, rowCount, headerData, rowData)

        for trial in range (20):
            rowData = rand_rowData()
            # make sure all key names are unique, when we re-put and re-parse (h2o caching issues)
            key = csvFilename + "_" + str(trial)
            key2 = csvFilename + "_" + str(trial) + ".hex"

            start = time.time()
            parseKeyA = h2o_cmd.parseFile(csvPathname=csvPathname, key=key, key2=key2)
            print "\nA trial #", trial, "parse end on ", csvFilename, 'took', time.time() - start, 'seconds'

            inspect = h2o_cmd.runInspect(key=key2)
            missingValuesListA = h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "missingValuesListA", missingValuesListA

            num_colsA = inspect['num_cols']
            num_rowsA = inspect['num_rows']
            row_sizeA = inspect['row_size']
            value_size_bytesA = inspect['value_size_bytes']

            self.assertEqual(missingValuesListA, [], "missingValuesList should be empty")
            self.assertEqual(num_colsA, colCount)
            self.assertEqual(num_rowsA, rowCount)

            # do a little testing of saving the key as a csv
            csvDownloadPathname = SYNDATASETS_DIR + "/csvDownload.csv"
            h2o.nodes[0].csv_download(key=key2, csvPathname=csvDownloadPathname)

            # remove the original parsed key. source was already removed by h2o
            h2o.nodes[0].remove_key(key2)
            # interesting. what happens when we do csv download with time data?
            start = time.time()
            parseKeyB = h2o_cmd.parseFile(csvPathname=csvDownloadPathname, key=key, key2=key2)
            print "B trial #", trial, "parse end on ", csvFilename, 'took', time.time() - start, 'seconds'
            inspect = h2o_cmd.runInspect(key=key2)
            missingValuesListB = h2o_cmd.infoFromInspect(inspect, csvPathname)
            print "missingValuesListB", missingValuesListB

            num_colsB = inspect['num_cols']
            num_rowsB = inspect['num_rows']
            row_sizeB = inspect['row_size']
            value_size_bytesB = inspect['value_size_bytes']

            self.assertEqual(missingValuesListA, missingValuesListB,
                "missingValuesList mismatches after re-parse of downloadCsv result")
            self.assertEqual(num_colsA, num_colsB,
                "num_cols mismatches after re-parse of downloadCsv result")
            self.assertEqual(num_rowsA, num_rowsB,
                "num_rowsA: %s num_rowsB: %s mismatch after re-parse of downloadCsv result" % (num_rowsA, num_rowsB) )
            self.assertEqual(row_sizeA, row_sizeB,
                "row_size mismatches after re-parse of downloadCsv result")
            self.assertEqual(value_size_bytesA, value_size_bytesB,
                "value_size_bytes mismatches after re-parse of downloadCsv result")

            # FIX! should do some comparison of values? maybe can use exec to checksum the columns and compare column list.
            # or compare to expected values? (what are the expected values for the number for time inside h2o?)

            # FIX! should compare the results of the two parses. The infoFromInspect result?
            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()

    


