#!/usr/bin/python

import h2o
import h2o_cmd
import h2o_rf

class H2OProxy(h2o.H2O):
    def __init__(self, host, port, *args, **kwargs):
        super(H2OProxy, self).__init__(use_this_ip_addr=host, port=port,*args,**kwargs)
        self._defaultTimeout = 14800
        h2o.clean_sandbox()

    def getHexKey(self, f, header=True, separator=None):
        df = h2o.find_dataset(f)
        if header: h=1
        else: h=0
        if separator: separator = ord(separator)
        key = h2o_cmd.parseFile(node=self, csvPathname=df, header=h, separator=separator)
        return key
    
    def trainRF(self, trainKey, **kwargs):
        return h2o_rf.trainRF(trainKey, node=self, timeoutSecs=self._defaultTimeout,**kwargs)

    def scoreRF(self, testKey, trainResult, **kwargs):
        return h2o_rf.scoreRF(testKey, trainResult, node=self, timeoutSecs=self._defaultTimeout, **kwargs)

    def terminate(self):
        self.shutdown_all()

def connect(ip="127.0.0.1", port=54321):
    return H2OProxy(ip, port)

