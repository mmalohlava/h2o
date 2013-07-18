import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_rf

# RF train parameters
paramsTrainRF = { 
            'ntree'      : 1, 
            #'depth'      : 300,
            'parallel'   : 1, 
            'bin_limit'  : 20000,
            'stat_type'  : 'ENTROPY',
            'out_of_bag_error_estimate': 1, 
            'exclusive_split_limit'    : 0,
            'timeoutSecs': 14800,
            }

# RF test parameters
paramsScoreRF = {
            'timeoutSecs': 14800,
            'out_of_bag_error_estimate': 0, 
        }

trainDS = {
        's3bucket'    : 'h2o-datasets',
        'filename'    : 'covtype.data',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

scoreDS = {
        's3bucket'    : 'h2o-datasets',
        'filename'    : 'covtype.data',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

PARSE_TIMEOUT=14800

class Basic(unittest.TestCase):

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=2)
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        
    def loadTrainData(self):
        df = h2o.find_dataset('bench/covtype/h2o/' + 'train.csv')
        trainKey = h2o_cmd.parseFile(csvPathname=df, key2='train.csv' + ".hex", header=1, timeoutSecs=180)
        return trainKey
    
    def loadScoreData(self):
        df = h2o.find_dataset('bench/covtype/h2o/' +'test.csv')
        scoreKey = h2o_cmd.parseFile(csvPathname=df, key2='test.csv' + ".hex", header=1, timeoutSecs=180)
        return scoreKey 

    def test_RF(self):
        trainKey = self.loadTrainData()
        kwargs   = paramsTrainRF.copy()
        trainResult = h2o_rf.trainRF(trainKey, model_key="rf1", **kwargs)

        #scoreKey = self.loadScoreData()
        #kwargs   = paramsScoreRF.copy()
        #scoreResult = h2o_rf.scoreRF(scoreKey, trainResult, **kwargs)

        print "\nTrain\n=========={0}".format(h2o_rf.pp_rf_result(trainResult))
        trainResult = h2o_rf.trainRF(trainKey, model_key="rf2", **kwargs)
        print "\nTrain\n=========={0}".format(h2o_rf.pp_rf_result(trainResult))
        #print "\nScoring\n========={0}".format(h2o_rf.pp_rf_result(scoreResult))
        time.sleep(3600)

if __name__ == '__main__':
    h2o.unit_main()
