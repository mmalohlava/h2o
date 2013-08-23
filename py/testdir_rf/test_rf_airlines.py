import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_rf

# RF train parameters
paramsTrainRF = { 
            'ntree'      : 50, 
            #'depth'      : 30,
            'parallel'   : 1, 
            'bin_limit'  : 1024,
            'ignore'     : 'DepTime,ArrTime,FlightNum,TailNum,ActualElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed',
            'stat_type'  : 'ENTROPY',
            'out_of_bag_error_estimate': 1, 
            'exclusive_split_limit'    : 0,
            'timeoutSecs': 14800,
            'iterative_cm': 0,
            'nodesize'   : 1,
            'sample'     : 67,
            }

# RF test parameters
paramsScoreRF = {
            'out_of_bag_error_estimate': 0, 
            'timeoutSecs': 14800,
        }

trainDS = {
        's3bucket'    : 'h2o-airlines-unpacked',
        'filename'    : 'year2007.csv',
        #'filename'    : 'year2005to2007.csv',
        #'filename'    : 'allyears2k.csv',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

scoreDS = {
        's3bucket'    : 'h2o-airlines-unpacked',
        'filename'    : 'year2008.csv',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

PARSE_TIMEOUT=14800

class Basic(unittest.TestCase):

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def parseS3NFile(self, s3bucket, filename, **kwargs):
        start      = time.time()
        uri = "s3n://{0}/{1}".format(s3bucket, filename)
        importHDFSResult = h2o.nodes[0].import_hdfs(uri)
        parseKey = h2o.nodes[0].parse(uri, **kwargs)
        parse_time = time.time() - start 
        h2o.verboseprint("py-S3N parse took {0} sec".format(parse_time))
        parseKey['python_call_timer'] = parse_time

        return parseKey
        
    def parseS3File(self, s3bucket, filename, **kwargs):
        start      = time.time()
        parseKey   = h2o_cmd.parseS3File(bucket=s3bucket, filename=filename, **kwargs)
        parse_time = time.time() - start 
        h2o.verboseprint("py-S3 parse took {0} sec".format(parse_time))
        parseKey['python_call_timer'] = parse_time
        return parseKey

    def loadTrainData(self):
        kwargs   = trainDS.copy()
        trainKey = self.parseS3NFile(**kwargs)
        return trainKey
    
    def loadScoreData(self):
        kwargs   = scoreDS.copy()
        scoreKey = self.parseS3NFile(**kwargs)
        return scoreKey 

    def test_RF(self):
	normalRF = False
	#normalRF = True

	print """
Normal RF : {0}
Train data: {1}
Test data : {2}""".format(normalRF, trainDS['filename'], scoreDS['filename'])

	print "Loading data...."
        trainKey = self.loadTrainData()
        kwargs   = paramsTrainRF.copy()
	
	print "Running normal RF: {0}".format(normalRF)
	if normalRF:
        	trainResult = h2o_rf.trainRF(trainKey, model_key="rfm_normal", **kwargs)
	else:
        	trainResult = h2o_rf.trainRF(trainKey, refine=1, model_key="rfm_refined", **kwargs)

        scoreKey = self.loadScoreData()
        kwargs   = paramsScoreRF.copy()
        scoreResult = h2o_rf.scoreRF(scoreKey, trainResult, **kwargs)
	
	print """
Normal RF : {0}
Train data: {1}
Test data : {2}""".format(normalRF, trainDS['filename'], scoreDS['filename'])
        print "\nTrain\n=========={0}".format(h2o_rf.pp_rf_result(trainResult))
        print "\nScoring\n========={0}".format(h2o_rf.pp_rf_result(scoreResult))

if __name__ == '__main__':
    h2o.unit_main()
