import os, json, unittest, time, shutil, sys, socket
import h2o
import h2o_browse as h2b, h2o_rf as h2f

# header, separator, exclude params are passed thru kwargs to node.parse
def parseFile(node=None, csvPathname=None, key=None, key2=None, 
    timeoutSecs=30, retryDelaySecs=0.5, pollTimeoutSecs=30,
    noise=None, noPoll=None, doSummary=True, **kwargs):
    if not csvPathname: raise Exception('No file name specified')
    if not node: node = h2o.nodes[0]
    ### print "parseFile pollTimeoutSecs:", pollTimeoutSecs
    key = node.put_file(csvPathname, key=key, timeoutSecs=timeoutSecs)
    if key2 is None:
        # don't rely on h2o default key name
        myKey2 = key + '.hex'
    else:
        myKey2 = key2
    p = node.parse(key, myKey2, 
        timeoutSecs, retryDelaySecs, 
        pollTimeoutSecs=pollTimeoutSecs, noise=noise, noPoll=noPoll, **kwargs)

    # do SummaryPage here too, just to get some coverage
    if doSummary:
        node.summary_page(myKey2)
    return p

def parseS3File(node=None, bucket=None, filename=None, keyForParseResult=None, 
    timeoutSecs=20, retryDelaySecs=2, pollTimeoutSecs=30, 
    noise=None, noPoll=None, **kwargs):
    ''' Parse a file stored in S3 bucket'''                                                                                                                                                                       
    if not bucket  : raise Exception('No S3 bucket specified')
    if not filename: raise Exception('No filename in bucket specified')
    if not node: node = h2o.nodes[0]
    
    import_result = node.import_s3(bucket)
    s3_key = [f['key'] for f in import_result['succeeded'] if f['file'] == filename ][0]
    
    if keyForParseResult is None:
        myKeyForParseResult = s3_key + '.hex'
    else:
        myKeyForParseResult = keyForParseResult
    # do SummaryPage here too, just to get some coverage
    p = node.parse(s3_key, myKeyForParseResult, 
        timeoutSecs, retryDelaySecs, 
        pollTimeoutSecs=pollTimeoutSecs, noise=noise, noPoll=noPoll, **kwargs)

    # do SummaryPage here too, just to get some coverage
    node.summary_page(myKeyForParseResult)
    return p

def runInspect(node=None, key=None, timeoutSecs=5, **kwargs):
    if not key: raise Exception('No key for Inspect specified')
    if not node: node = h2o.nodes[0]
    # FIX! currently there is no such thing as a timeout on node.inspect
    return node.inspect(key, timeoutSecs=timeoutSecs, **kwargs)

def infoFromInspect(inspect, csvPathname):
    # need more info about this dataset for debug
    cols = inspect['cols']
    # look for nonzero num_missing_values count in each col
    missingValuesList = []
    for i, colDict in enumerate(cols):
        num_missing_values = colDict['num_missing_values']
        if num_missing_values != 0:
            print "%s: col: %d, num_missing_values: %d" % (csvPathname, i, num_missing_values)
            missingValuesList.append(num_missing_values)

    num_cols = inspect['num_cols']
    num_rows = inspect['num_rows']
    row_size = inspect['row_size']
    ptype = inspect['type']
    value_size_bytes = inspect['value_size_bytes']
    response = inspect['response']
    ptime = response['time']

    print "num_cols: %s, num_rows: %s, row_size: %s, ptype: %s, \
           value_size_bytes: %s, time: %s" % \
           (num_cols, num_rows, row_size, ptype, value_size_bytes, ptime)
    return missingValuesList

# Not working in H2O yet, but support the test
def runStore2HDFS(node=None, key=None, timeoutSecs=5, **kwargs):
    if not key: raise Exception('No key for Inspect specified')
    if not node: node = h2o.nodes[0]
    # FIX! currently there is no such thing as a timeout on node.inspect
    return node.Store2HDFS(key, **kwargs)

# since we'll be doing lots of execs on a parsed file, not useful to have parse+exec
# retryDelaySecs isn't used, 
def runExecOnly(node=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    # no such thing as GLMView..don't use retryDelaySecs
    return node.exec_query(timeoutSecs, **kwargs)

def runKMeans(node=None, csvPathname=None, key=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    # use 1/5th the KMeans timeoutSecs for allowed parse time.
    pto = max(timeoutSecs/5,10)
    noise = kwargs.pop('noise',None)
    parseKey = parseFile(node, csvPathname, key, timeoutSecs=pto, noise=noise)
    kmeans = runKMeansOnly(node, parseKey, timeoutSecs, retryDelaySecs, **kwargs)
    return kmeans

def runKMeansOnly(node=None, parseKey=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    if not parseKey: raise Exception('No parsed key for KMeans specified')
    if not node: node = h2o.nodes[0]
    print parseKey['destination_key']
    return node.kmeans(parseKey['destination_key'], None, 
        timeoutSecs, retryDelaySecs, **kwargs)

def runGLM(node=None, csvPathname=None, key=None, 
        timeoutSecs=20, retryDelaySecs=2, noise=None, **kwargs):
    # use 1/5th the GLM timeoutSecs for allowed parse time.
    pto = max(timeoutSecs/5,10)
    noise = kwargs.pop('noise',None)
    parseKey = parseFile(node, csvPathname, key, timeoutSecs=pto, noise=noise)
    return runGLMOnly(node, parseKey, timeoutSecs, retryDelaySecs, noise=noise, **kwargs)

def runGLMOnly(node=None, parseKey=None, 
        timeoutSecs=20, retryDelaySecs=2, noise=None, **kwargs):
    if not parseKey: raise Exception('No parsed key for GLM specified')
    if not node: node = h2o.nodes[0]
    return node.GLM(parseKey['destination_key'], 
        timeoutSecs, retryDelaySecs, noise=noise, **kwargs)

def runGLMScore(node=None, key=None, model_key=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    return node.GLMScore(key, model_key, timeoutSecs, **kwargs)

def runGLMGrid(node=None, csvPathname=None, key=None, 
        timeoutSecs=60, retryDelaySecs=2, noise=None, **kwargs):
    # use 1/5th the GLM timeoutSecs for allowed parse time.
    pto = max(timeoutSecs/5,10)
    noise = kwargs.pop('noise',None)
    parseKey = parseFile(node, csvPathname, key, timeoutSecs=pto, noise=noise)
    return runGLMGridOnly(node, parseKey, 
        timeoutSecs, retryDelaySecs, noise=noise, **kwargs)

def runGLMGridOnly(node=None,parseKey=None,
        timeoutSecs=60, retryDelaySecs=2, noise=None, **kwargs):
    if not parseKey: raise Exception('No parsed key for GLMGrid specified')
    if not node: node = h2o.nodes[0]
    # no such thing as GLMGridView..don't use retryDelaySecs
    return node.GLMGrid(parseKey['destination_key'], timeoutSecs, **kwargs)

def runRF(node=None, csvPathname=None, trees=5, key=None, 
        timeoutSecs=20, retryDelaySecs=2, rfview=True, noise=None, **kwargs):
    # use 1/5th the RF timeoutSecs for allowed parse time.
    pto = max(timeoutSecs/5,30)
    noise = kwargs.pop('noise',None)
    parseKey = parseFile(node, csvPathname, key, timeoutSecs=pto, noise=noise)
    return runRFOnly(node, parseKey, trees, timeoutSecs, retryDelaySecs, 
        rfview=rfview, noise=noise, **kwargs)

# rfView can be used to skip the rf completion view
# for creating multiple rf jobs
def runRFOnly(node=None, parseKey=None, trees=5, 
        timeoutSecs=20, retryDelaySecs=2, rfview=True, noise=None, noPrint=False, **kwargs):
    if not parseKey: raise Exception('No parsed key for RF specified')
    if not node: node = h2o.nodes[0]
    #! FIX! what else is in parseKey that we should check?
    h2o.verboseprint("runRFOnly parseKey:", parseKey)
    Key = parseKey['destination_key']
    rf = node.random_forest(Key, trees, timeoutSecs, **kwargs)

    # FIX! check all of these somehow?
    # if we model_key was given to rf via **kwargs, remove it, since we're passing 
    # model_key from rf. can't pass it in two places. (ok if it doesn't exist in kwargs)
    data_key  = rf['data_key']
    kwargs.pop('model_key',None)
    model_key = rf['model_key']
    rfCloud = rf['response']['h2o']

    # same thing. if we use random param generation and have ntree in kwargs, get rid of it.
    kwargs.pop('ntree',None)

    # this is important. it's the only accurate value for how many trees RF was asked for.
    ntree    = rf['ntree']
    job = rf['response']['redirect_request_args']['job']
    print "THe job is: ", job

    # /ip:port of cloud (can't use h2o name)
    rfClass= rf['response_variable']

    rfViewResult = None
    if rfview:
        rfViewResult = runRFView(node, data_key, model_key, ntree, 
            timeoutSecs, retryDelaySecs, noise=noise, noPrint=noPrint, job=job, **kwargs)
    
    return rfViewResult

def runRFTreeView(node=None, n=None, data_key=None, model_key=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    return node.random_forest_treeview(n, data_key, model_key, timeoutSecs, **kwargs)


def runRFView(node=None, data_key=None, model_key=None, ntree=None, 
    timeoutSecs=15, retryDelaySecs=2, 
    noise=None, noPoll=False, noPrint=False, job=None, **kwargs):
    if not node: node = h2o.nodes[0]

    def test(n, tries=None):
        rfView = n.random_forest_view(data_key, model_key, timeoutSecs,
                noise=noise, job=job, **kwargs)
        status = rfView['response']['status']
        numberBuilt = rfView['trees']['number_built']
        
        if status == 'done': 
            return True
        if status != 'poll': raise Exception('Unexpected status: ' + status)

        progress = rfView['response']['progress']
        progressTotal = rfView['response']['progress_total']

        # want to double check all this because it's new
        # and we had problems with races/doneness before
        errorInResponse = \
            numberBuilt<0 or ntree<0 or \
            progress<0 or progressTotal<0 or progress>progressTotal
            ## progressTotal!=ntree or
            # rfView better always agree with what RF ntree was

        if errorInResponse:
            raise Exception("\nBad values in response during RFView polling.\n" + 
                "progress: %s, progressTotal: %s, ntree: %s, numberBuilt: %s, status: %s" % \
                (progress, progressTotal, ntree, numberBuilt, status))

        # don't print the useless first poll.
        # UPDATE: don't look for done. look for not poll was missing completion when looking for done
        if (status=='poll'):
            if numberBuilt==0:
                h2o.verboseprint(".")
            else:
                h2o.verboseprint("\nRFView polling #", tries,
                    "Status: %s. %s trees done of %s desired" % (status, numberBuilt, ntree))

        return (status!='poll')

    if noPoll:
        return None

    node.stabilize(
            test,
            'random forest reporting...',
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

    # kind of wasteful re-read, but maybe good for testing
    rfView = node.random_forest_view(data_key, model_key, timeoutSecs, noise=noise, **kwargs)
    h2f.simpleCheckRFView(node, rfView, noPrint=noPrint)
    return rfView

def port_live(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip,port))
        s.shutdown(2)
        return True
    except:
        return False

def wait_for_live_port(ip, port, retries=3):
    h2o.verboseprint("Waiting for {0}:{1} {2}times...".format(ip,port,retries))
    if not port_live(ip,port):
        count = 0
        while count < retries:
            if port_live(ip,port):
                count += 1
            else:
                count = 0
            time.sleep(1)
            dot()
    if not port_live(ip,port):
        raise Exception("[h2o_cmd] Error waiting for {0}:{1} {2}times...".format(ip,port,retries))

def dot():
    sys.stdout.write('.')
    sys.stdout.flush()

def sleep_with_dot(sec, message=None):
    if message:
        print message
    count = 0
    while count < sec:
        time.sleep(1)
        dot()
        count += 1

# I use these in testdir_hosts/test_parse_nflx_loop_s3n_hdfs.py
# and testdir_multi_jvm/test_benchmark_import.py
# might be able to use more widely
def check_enums_from_inspect(parseKey):
    inspect = runInspect(key=parseKey['destination_key'])
    print "num_rows:", inspect['num_rows']
    print "num_cols:", inspect['num_cols']
    cols = inspect['cols']
    # trying to see how many enums we get
    # don't print int
    missingValuesDict = {}
    for i,c in enumerate(cols):
        # print i, "name:", c['name']
        msg = "column %d" % i
        msg = msg + " type: %s" % c['type']
        if c['type'] == 'enum':
            msg = msg + (" enum_domain_size: %d" % c['enum_domain_size'])
        if c['num_missing_values'] != 0:
            msg = msg + (" num_missing_values: %s" % c['num_missing_values'])
            # dictionary by col #
            missingValuesDict[str(i)] = c['num_missing_values']
        if c['type'] != 'int' or c['num_missing_values'] != 0:
            print msg

    return missingValuesDict # so we can check if there were any missing values due to flipped enums?

# looks for the key that matches the pattern, in the keys you saved from the 
# import (that you saved from import of the folder/s3/hdfs)
# I guess I should change to just be the raw result of the import? not sure
# see how it's used in tests named above
def delete_csv_key(csvFilename, importFullList):
    # remove the original data key
    for k in importFullList:
        deleteKey = k['key']
        ### print "possible delete:", deleteKey
        # don't delete any ".hex" keys. the parse results above have .hex
        # this is the name of the multi-file (it comes in as a single file?)
        if csvFilename in deleteKey and not '.hex' in deleteKey:
            print "\nRemoving", deleteKey
            removeKeyResult = h2o.nodes[0].remove_key(key=deleteKey)
            ### print "removeKeyResult:", h2o.dump_json(removeKeyResult)

# checks the key distribution in the cloud, and prints warning if delta against avg
# is > expected
def check_key_distribution():
    c = h2o.nodes[0].get_cloud()
    nodes = c['nodes']
    print "Key distribution post parse, should be balanced"
    # get average
    totalKeys = 0
    for n in nodes:
        totalKeys += int(n['num_keys'])
    avgKeys = (totalKeys + 0.0)/len(nodes)
    # if more than 5% difference from average, print warning
    for n in nodes:
        print 'num_keys:', n['num_keys'], 'value_size_bytes:', n['value_size_bytes'],\
            'name:', n['name']
        delta = (abs(avgKeys - int(n['num_keys']))/avgKeys)
        if delta > 0.10:
            print "WARNING. avgKeys:", avgKeys, "and n['num_keys']:", n['num_keys'], "have >", "%.1f" % (100 * delta), "% delta"


