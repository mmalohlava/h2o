import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_rf

# RF train parameters

bench_params = {
        'nodes_count'  : 4,
        'java_heap_GB' : 3
        }

paramsTrainRF = { 
            'ntree'      : 16, 
            #'depth'      : 300,
            'parallel'   : 1, 
            'bin_limit'  : 1024,
            'stat_type'  : 'ENTROPY',
            'out_of_bag_error_estimate': 1, 
            'exclusive_split_limit'    : 0,
            'timeoutSecs': 14800,
            'nodesize'   : 1,
            'sample'     : 67,
            }

# RF test parameters
paramsScoreRF = {
            'timeoutSecs': 14800,
            'out_of_bag_error_estimate': 0, 
        }

DATASET_NAME="covtype_100k"
DATASET_NAME="iris20kcols"
DATASET_NAME="covtype"

trainDS = {
        'dataset'     : 'bench/{0}/R'.format(DATASET_NAME),
        'filename'    : 'train.csv',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

scoreDS = {
        'dataset'     : 'bench/{0}/R'.format(DATASET_NAME),
        'filename'    : 'test.csv',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

class Basic(unittest.TestCase):

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=bench_params['nodes_count'],
                java_heap_GB=bench_params['java_heap_GB'])
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def loadAndParse(self, conf):
        df = h2o.find_dataset("{0}/{1}".format(conf['dataset'], conf['filename']))
        key = h2o_cmd.parseFile(csvPathname=df, header=conf['header'],timeoutSecs=conf['timeoutSecs'])
        dd  = h2o_cmd.dataDistrib(key=key['destination_key'])
        print "\nDistribution of {0} is {1}\n".format(key['destination_key'],json.dumps(dd['nodes'], indent=2)) 
        return key
        
    def loadTrainData(self):
        return self.loadAndParse(trainDS)
    
    def loadScoreData(self):
        return self.loadAndParse(scoreDS)

    def test_RF(self):
        trainKey = self.loadTrainData()
        scoreKey = self.loadScoreData()
        #time.sleep(3600)
        executeNormalRF = True
        executeNormalRF = False
        if executeNormalRF:
            kwargs   = paramsTrainRF.copy()
            trainResultNormal = h2o_rf.trainRF(trainKey, model_key="rfm_normal", **kwargs)
            #print h2o_rf.pp_rf_result(trainResultNormal)
            kwargs   = paramsScoreRF.copy()
            scoreResultNormal = h2o_rf.scoreRF(scoreKey, trainResultNormal, **kwargs)
            print "\nScoring normal forest\n========={0}".format(h2o_rf.pp_rf_result(scoreResultNormal))

        kwargs   = paramsTrainRF.copy()
        trainResultRefined = h2o_rf.trainRF(trainKey, refine=1, model_key="rfm_refined", **kwargs)
        #print h2o_rf.pp_rf_result(trainResultRefined)
        kwargs   = paramsScoreRF.copy()
        scoreResultRefined = h2o_rf.scoreRF(scoreKey, trainResultRefined, **kwargs)
        print "\nScoring refined forest\n========={0}".format(h2o_rf.pp_rf_result(scoreResultRefined))

        time.sleep(3600)

if __name__ == '__main__':
    h2o.unit_main()

