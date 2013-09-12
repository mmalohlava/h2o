import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_rf

# RF train parameters

bench_params = {
        'nodes_count'  : 2,
        'java_heap_GB' : 3
        }

DATASET_NAME="covtype_100k"
DATASET_NAME="iris20kcols"
DATASET_NAME="nchess_3_8_1000"
DATASET_NAME="nchess_4_8_1000"
DATASET_NAME="iris"
DATASET_NAME="covtype"

trainDS = {
        'dataset'     : 'bench/{0}/R'.format(DATASET_NAME),
        'filename'    : 'train.csv',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

scoreDS = {
        'dataset'     : 'bench/{0}/R'.format(DATASET_NAME),
        'filename'    : 'test_5000.csv',
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
        print "Loaded..."
        time.sleep(3600)

if __name__ == '__main__':
    h2o.unit_main()

