import time
import threading
from GimmeProxy import RequestInfo, RequestDistributor

#Example of how to use the library when you have pre-built collection of requests you'd like to execute. 
#The distributor will stop processing once all of the requests are completed.
def testHandlerDeterminate():
    infos = []
    
    infos.append(RequestInfo(1, "http://www.github.com", get=True))
    infos.append(RequestInfo(2, "http://www.github.com", get=True))
    infos.append(RequestInfo(3, "http://www.github.com", get=True))
    infos.append(RequestInfo(4, "http://www.github.com", get=True))
    infos.append(RequestInfo(5, "http://www.github.com", get=True))
    infos.append(RequestInfo(6, "http://www.github.com", get=True))
    infos.append(RequestInfo(7, "http://www.github.com", get=True))
    infos.append(RequestInfo(8, "http://www.github.com", get=True))
    
    distributor = RequestDistributor(5, infos, 10, "_proxies.txt", 3, isDeterminate = True)
    for result in distributor.executeRequests():
        print("Result Generated.")
        

class enqueuer(threading.Thread):
    distributor = None
    numOfRequests = 0
    def __init__(self, distributor, numOfRequests):
        super(enqueuer,self).__init__()
        self.distributor = distributor
        self.numOfRequests = numOfRequests
    
    def run(self):
        for i in range(0, 8):
            time.sleep(5)
            self.distributor.enqueueRequest(RequestInfo(i, "http://www.github.com", get = True))
            

#Example of how to use the library when you are continually generating new requests to execute.  Just hand the#
#requests to the library as they are generated, and tell the library to stop when you decide you are finished.
def testHandlerIndeterminate():
    distributor = RequestDistributor(5, [], 10, "_proxies.txt", 3, isDeterminate = False)
    
    num = 8
    enqueue = enqueuer(distributor, num)
    enqueue.start()
    
    processed = 0
    for result in distributor.executeRequests():
        processed += 1
        print("Result Generated: " + str(result.key))
        if processed >= num:
            distributor.stopAllWorkers() # Must call this to stop the request handling when in indeterminate mode.
            break
testHandlerDeterminate()
time.sleep(15)
testHandlerIndeterminate()