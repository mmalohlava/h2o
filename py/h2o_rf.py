import h2o
import h2o_cmd
import random
import time

# params is mutable here
def pickRandRfParams(paramDict, params):
    colX = 0
    randomGroupSize = random.randint(1,len(paramDict))
    for i in range(randomGroupSize):
        randomKey = random.choice(paramDict.keys())
        randomV = paramDict[randomKey]
        randomValue = random.choice(randomV)
        # note it updates params, so any default values will still be there
        params[randomKey] = randomValue
        if (randomKey=='x'):
            colX = randomValue
        # temp hack to avoid CM=0 results if 100% sample and using OOBEE
        # UPDATE: if the default is oobe=1, it might not be in params...just have to not have the
        # test ask for 100
    return colX

def simpleCheckRFView(node, rfv, noPrint=False, **kwargs):
    if not node:
        node = h2o.nodes[0]

    if 'warnings' in rfv:
        warnings = rfv['warnings']
        # catch the 'Failed to converge" for now
        for w in warnings:
            if not noPrint: print "\nwarning:", w
            if ('Failed' in w) or ('failed' in w):
                raise Exception(w)

    oclass = rfv['response_variable']
    if (oclass<0 or oclass>20000):
        raise Exception("class in RFView seems wrong. class:", oclass)

    # the simple assigns will at least check the key exists
    cm = rfv['confusion_matrix']
    header = cm['header'] # list
    classification_error = cm['classification_error']
    rows_skipped = cm['rows_skipped']
    cm_type = cm['type']
    if not noPrint: 
        print "classification_error * 100 (pct):", classification_error * 100
        print "rows_skipped:", rows_skipped
        print "type:", cm_type

    used_trees = cm['used_trees']
    if (used_trees <= 0):
        raise Exception("used_trees should be >0. used_trees:", used_trees)

    # if we got the ntree for comparison. Not always there in kwargs though!
    ntree = kwargs.get('ntree',None)
    if (ntree is not None and used_trees != ntree):
        raise Exception("used_trees should == ntree. used_trees:", used_trees)

    scoresList = cm['scores'] # list
    totalScores = 0
    totalRight = 0
    # individual scores can be all 0 if nothing for that output class
    # due to sampling
    classErrorPctList = []
    predictedClassDict = {} # may be missing some? so need a dict?
    for classIndex,s in enumerate(scoresList):
        classSum = sum(s)
        if classSum == 0 :
            # why would the number of scores for a class be 0? does RF CM have entries for non-existent classes
            # in a range??..in any case, tolerate. (it shows up in test.py on poker100)
            if not noPrint: print "class:", classIndex, "classSum", classSum, "<- why 0?"
        else:
            # H2O should really give me this since it's in the browser, but it doesn't
            classRightPct = ((s[classIndex] + 0.0)/classSum) * 100
            totalRight += s[classIndex]
            classErrorPct = 100 - classRightPct
            classErrorPctList.append(classErrorPct)
            ### print "s:", s, "classIndex:", classIndex
            if not noPrint: print "class:", classIndex, "classSum", classSum, "classErrorPct:", "%4.2f" % classErrorPct

            # gather info for prediction summary
            for pIndex,p in enumerate(s):
                if pIndex not in predictedClassDict:
                    predictedClassDict[pIndex] = p
                else:
                    predictedClassDict[pIndex] += p

        totalScores += classSum

    if not noPrint: 
        print "Predicted summary:"
        # FIX! Not sure why we weren't working with a list..hack with dict for now
        for predictedClass,p in predictedClassDict.items():
            print str(predictedClass)+":", p

        # this should equal the num rows in the dataset if full scoring? (minus any NAs)
        print "totalScores:", totalScores
        print "totalRight:", totalRight
        if totalScores != 0:  pctRight = 100.0 * totalRight/totalScores
        else: pctRight = 0.0
        print "pctRight:", "%5.2f" % pctRight
        print "pctWrong:", "%5.2f" % (100 - pctRight)

    if (totalScores<=0 or totalScores>5e9):
        raise Exception("scores in RFView seems wrong. scores:", scoresList)

    type = cm['type']
    used_trees = cm['used_trees']
    if (used_trees<=0 or used_trees>20000):
        raise Exception("used_trees in RFView seems wrong. used_trees:", used_trees)

    data_key = rfv['data_key']
    model_key = rfv['model_key']
    ntree = rfv['ntree']
    if (ntree<=0 or ntree>20000):
        raise Exception("ntree in RFView seems wrong. ntree:", ntree)
    response = rfv['response'] # Dict
    rfv_h2o = response['h2o']
    rfv_node = response['node']
    status = response['status']
    time = response['time']

    trees = rfv['trees'] # Dict
    depth = trees['depth']
    # zero depth okay?
    ## if ' 0.0 ' in depth:
    ##     raise Exception("depth in RFView seems wrong. depth:", depth)
    leaves = trees['leaves']
    if ' 0.0 ' in leaves:
        raise Exception("leaves in RFView seems wrong. leaves:", leaves)
    number_built = trees['number_built']
    if (number_built<=0 or number_built>20000):
        raise Exception("number_built in RFView seems wrong. number_built:", number_built)

    h2o.verboseprint("RFView response: number_built:", number_built, "leaves:", leaves, "depth:", depth)

    ### modelInspect = node.inspect(model_key)
    dataInspect = node.inspect(data_key)

    # shouldn't have any errors
    h2o.check_sandbox_for_errors()

    return (classification_error, classErrorPctList, totalScores)

def trainRF(trainParseKey, **kwargs):
    # Train RF
    start = time.time()
    trainResult = h2o_cmd.runRFOnly(parseKey=trainParseKey, **kwargs)
    rftime      = time.time()-start 
    h2o.verboseprint("RF train results: ", trainResult)
    h2o.verboseprint("RF computation took {0} sec".format(rftime))

    trainResult['python_call_timer'] = rftime
    return trainResult

def scoreRF(scoreParseKey, trainResult, **kwargs):
    # Run validation on dataset
    rfModelKey  = trainResult['model_key']
    #ntree       = trainResult['ntree']
    ntree = None
    
    start = time.time()
    data_key = scoreParseKey['destination_key']
    scoreResult = h2o_cmd.runRFView(None, data_key, rfModelKey, ntree, **kwargs)

    rftime      = time.time()-start 
    h2o.verboseprint("RF score results: ", scoreResult)
    h2o.verboseprint("RF computation took {0} sec".format(rftime))

    scoreResult['python_call_timer'] = rftime
    return scoreResult

def pp_rf_result(rf):
    jcm = rf['confusion_matrix']
    header = jcm['header']
    #cm = '   '.join(header)
    cm = '{0:<15}'.format('')
    for h in header: cm = '{0}|{1:<15}'.format(cm, h)
    cm = '{0}|{1:<15}'.format(cm, 'error')
    c = 0
    for line in jcm['scores']:
        lineSum  = sum(line)
        errorSum = lineSum - line[c]
        if (lineSum>0): 
            err = float(errorSum) / lineSum
        else:
            err = 0.0
        fl = '{0:<15}'.format(header[c])
        for num in line: fl = '{0}|{1:<15}'.format(fl, num)
        fl = '{0}|{1:<15}'.format(fl, err)
        cm = "{0}\n{1}".format(cm, fl)
        c += 1

    return """
 Leaves: {0} / {1} / {2}
  Depth: {3} / {4} / {5}
   mtry: {6}
   Type: {7}
    Err: {8} %
   Time: {9} seconds
  Trees: {10}

   Confusion matrix:
{11}
""".format(
        rf['trees']['leaves']['min'],
        rf['trees']['leaves']['mean'],
        rf['trees']['leaves']['max'],
        rf['trees']['depth']['min'],
        rf['trees']['depth']['mean'],
        rf['trees']['depth']['max'],
        rf['mtry'], 
        rf['confusion_matrix']['type'],
        rf['confusion_matrix']['classification_error'] *100,
        rf['response']['time'],
        rf['confusion_matrix']['used_trees'],
        cm)

