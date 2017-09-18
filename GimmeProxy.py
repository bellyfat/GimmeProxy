#todo https
import time
import random
import threading
import requests
import csv
import Queue
import json

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

class ExcessiveProxyFailureException(Exception):
    pass
    
class GimmeProxyAPIException(Exception):
    pass
    
class NewProxyRequestDeniedException(Exception):
    pass

class Proxy(object):
    httpProxyLocation = None
    successCount = 0 # + 1 each time it handles all requests correctly, - 1 each time it doesn't handle them all correctly
    source = ""
    successCounted = False
    failureCounted = False
    proxySource = None
    failureReason = None
    
    def __init__(self, source):
        self.proxySource = source
    
    def regardAsSuccess(self):
        if self.successCount < 4 and not self.successCounted:
            self.successCount += 1

    def regardAsFailure(self, failureReason):
        if not self.failureCounted:
            self.successCount -= 1
            self.failureReason = failureReason

#The point of this class is to retain and manage the proxies to use, as well as
#kind of passively build up a collection of proxies over time.
class ProxySource(object):
    proxiesToUse = None #Proxies that are ready to be allocated to a proxy slot and used.
    proxies = None
    persistFile = "proxies.txt"
    failedFile = "failedproxies.txt"
    minSize = -1
    proxiesLoadedThroughAPI = 0
    
    def __init__(self, persistFile, minSize =  -1):
        self.proxiesToUse = Queue.Queue()
        self.minSize = minSize
        self.proxies = []
        self.persistFile = persistFile

    def enqueueProxy(self, proxy):
        self.proxiesToUse.put(proxy)
        self.proxies.append(proxy)
        
    def getSavedProxies(self):
        try:
            with open(self.persistFile, 'rb') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if len(row) >= 2:
                        proxy = Proxy(self)
                        proxy.httpProxyLocation = row[0]
                        proxy.successCount = int(row[1])
                        proxy.source = "Persisted"
                        yield proxy
        except IOError:
            print("No stored proxies were found: " + self.persistFile)
            pass
                
    def persistProxies(self):
        with open(self.persistFile, "wb") as csvfile:
            writer = csv.writer(csvfile)
            for proxy in self.proxies:
                if proxy.successCount >= 0:
                    writer.writerow([proxy.httpProxyLocation, proxy.successCount])
        
        with open(self.failedFile, "a") as file:
            writer = csv.writer(file)
            for proxy in self.proxies:
                if proxy.successCount < 0:
                    writer.writerow([time.strftime("%d/%m/%Y %I:%M:%S"),proxy.httpProxyLocation,proxy.failureReason])

    def getNewProxy(self):
        try:
            r = requests.get("https://gimmeproxy.com/api/getProxy?post=true&maxCheckPeriod=3600&protocol=http")
        except:
            raise GimmeProxyAPIException("An error occurred while attempting to communicate with GimmeProxy.")
        if r.status_code != 200:
            raise GimmeProxyAPIException("GimmeProxy communication failed.  Status code: " + str(r.status_code))
        response = r.text
        jsonResponse = json.loads(response)
        proxy = Proxy(self)
        proxy.successCount = 0
        proxy.httpProxyLocation = jsonResponse["curl"]
        proxy.source = "GimmeProxy API"
        self.proxiesLoadedThroughAPI += 1 
        return proxy

    def initializeProxies(self):
        proxies = []
        for proxy in self.getSavedProxies():
            proxies.append(proxy)
        while len(proxies) < self.minSize:
            proxies.append(self.getNewProxy())
        
        #Passively try to build our collection.  If we haven't hit the API at all, now's a good chance to hit it just twice to build up the collection.
        if self.proxiesLoadedThroughAPI == 0 and proxies < self.minSize * 4:
            proxies.append(self.getNewProxy())
            proxies.append(self.getNewProxy())
        
        random.shuffle(proxies)
        for proxy in proxies:
            self.enqueueProxy(proxy)

    def dequeueNextProxy(self):
        if self.proxiesToUse.qsize() == 0:
            self.enqueueProxy(self.getNewProxy())    
        return self.proxiesToUse.get()

    def reportStatus(self):
        print("Proxy Collection Status Report:")
        for proxy in self.proxies:
            print("Proxy: " + proxy.httpProxyLocation + " Count: " + str(proxy.successCount) + " Source: " + proxy.source)

class RequestInfo(object):
    url = ""
    get = False
    post = False
    postData = None
    key = None #Arbitrary key the caller can use to pair this with the result.
    def __init__(self, key, url, get = False, post = False, postData = None):
        if (not get) and (not post):
            raise Exception("Must either get or post")
        self.key = key
        self.url = url
        self.get = get
        self.post = post
        self.postData = postData
    
class RequestWorker(threading.Thread):
    name = None
    requestProducer = None
    maxRequestsToConsume = -1
    requestsConsumed = 0
    proxy = None
    status = None
    successCount = 0
    failureCount = 0
    contiguousFailureCount = 0

    def __init__(self, name, maxRequestsToConsume, producer):
        super(RequestWorker,self).__init__()
        self.name = name
        self.requestProducer = producer
        self.maxRequestsToConsume = maxRequestsToConsume
        self.proxy = self.requestProducer.proxySource.dequeueNextProxy()
        self.status = ""
        return
    
    def getStatus(self):
        return "[" + str(self.successCount) + "|" + str(self.failureCount) + "]" + self.status

    def registerSuccess(self):
        self.successCount += 1
        self.proxy.regardAsSuccess()
        self.contiguousFailureCount = 0

    def registerFailure(self, message):
        self.contiguousFailureCount += 1
        self.proxy.regardAsFailure(message)
        self.failureCount += 1

    def sleepAfterSuccessfulRequest(self, sleeptime):
        if sleeptime > 0: 
            for i in range(0, sleeptime):
                if self.requestProducer.stop:
                    break;
                self.status = "Sleeping " + str(i) + " of " + str(sleeptime) + " seconds after target was accessed."
                time.sleep(1)
                
    def run(self):
        while self.maxRequestsToConsume < 0 or self.maxRequestsToConsume > self.requestsConsumed:        
            if self.contiguousFailureCount > 25:
                self.status = "Unrecoverable-- This thread has failed too many times in a row.  It is shutting down."
                self.requestProducer.registerException(ExcessiveProxyFailureException())
                break
            
            if self.maxRequestsToConsume > 0 and self.successCount >= self.maxRequestsToConsume:
                self.status = "Maximum request processing limit of " + str(self.maxRequestsToConsume) + " reached.  Stopping Thread."
                break
                
            request = self.requestProducer.requestsToProcess.get()
            if request is None:
               status = "Stopped from external source."
               break
            
            self.status = "Processing Request: " + request.url + " Proxy: " + self.proxy.httpProxyLocation
            self.requestProducer.reportStatus()

            try:
                if request.get:
                    r = requests.get(request.url, proxies = { "http" : self.proxy.httpProxyLocation }, timeout=40)
                elif request.post:
                    r = requests.post(request.url, proxies = { "http" : self.proxy.httpProxyLocation }, timeout=40, data= request.postData)

                if r.status_code != 200:
                    raise Exception("Request Failed.  Status Code: " + str(r.status_code))
                
                self.status = "Successful results returned."
                self.registerSuccess()
                self.requestProducer.registerResult(request.key, r.text)
                self.sleepAfterSuccessfulRequest(self.requestProducer.workerSleepTime)
            except Exception as e:
                self.requestProducer.requestsToProcess.put(request) #put the failed request back into the queue
                if len(str(e.message)) > 80:
                    message = str(e.message)[:80]
                else:
                    message = str(e.message)
                self.status = "Failed (rank: " + str(self.proxy.successCount) + "): " + message
                self.requestProducer.reportStatus()
                self.registerFailure(message)
                try:
                    self.proxy = self.requestProducer.proxySource.dequeueNextProxy()
                except:
                    self.status = "Failed to dequeue a new proxy."
                    self.requestProducer.registerException(NewProxyRequestDeniedException())
        self.requestProducer.registerCompletedWorker(self) # Tells the parent to stop blocking and check to see if it should stop as well.
        
class RequestResult(object):
    key = None
    result = None
    def __init__(self, key, result):
        self.key = key
        self.result = result
		
# The main director.  This takes a configuration about a request, picks a proxy,
# executes it, and does error handling if it's not 200.
class RequestDistributor(object):
    displayName = ""
    workers = None
    activeWorkers = None
    numberOfWorkers = 0
    proxySource = None
    requestsToProcess = None
    stop = False 
    results = None
    resultsProcessed = 0
    workerSleepTime = 0 #Time each worker sleeps after a successful request.
    reportStatusFlag = False #Set this to true to tell the status reporter to report the status.
    statusLock = threading.Lock() # Not re-initializing this so multimple instances aren't reporting at the samae time
    maxRequestsPerProxy = None
    isDeterminate = False   # Determinate = all requests known beforehand.  Indeterminate = Can add as you go, and caller must tell when to stop accepting them.
	
    def __init__(self, numberOfWorkers, requestsToProcess, sleepTimeBetweenRequests, proxyPersistFile,  maxRequestsPerProxy, isDeterminate = False,): 
        self.workers = []
        self.activeWorkers = []
        self.numberOfWorkers = numberOfWorkers if numberOfWorkers > 0 else 1     
        self.proxySource = ProxySource(proxyPersistFile, numberOfWorkers)
        self.proxySource.initializeProxies()
        self.requestsToProcess = Queue.Queue()
        self.results = Queue.Queue()
        self.workerSleepTime = sleepTimeBetweenRequests
        for request in requestsToProcess:
            self.requestsToProcess.put(request)
        self.isDeterminate = isDeterminate
        self.maxRequestsPerProxy = maxRequestsPerProxy
            
    def registerException(self, exception):
        self.results.put(exception)
        self.reportStatus()
            
    def registerCompletedWorker(self, worker):
        self.results.put(worker)
        self.reportStatus()
        
    def registerResult(self, key, result):
        resultStructure = RequestResult(key, result)
        self.results.put(resultStructure)
        self.resultsProcessed += 1
        self.reportStatus()
		
    def reportStatus(self):
        self.statusLock.acquire()
        try:
            print("***STATUS*** " + self.displayName)
            print("Pending: " + str(self.requestsToProcess.qsize()) + " Succeeded: " + str(self.resultsProcessed) + " Responses Waiting: " + str(self.results.qsize()))
            for worker in self.workers:
                print(worker.name + ": " + worker.getStatus())
                print(worker.name + " Proxy: " + worker.proxy.httpProxyLocation)
            print("")
        finally:
            self.statusLock.release()   

    def initRequestWorkers(self):
        for i in range(0, self.numberOfWorkers):
            worker = RequestWorker("Worker " + str(i), self.maxRequestsPerProxy, self)
            self.workers.append(worker)
            self.activeWorkers.append(worker)
            worker.start()

    def getNumberOfRunningWorkers(self):
        return len(self.activeWorkers)
        
    def stopAllWorkers(self):
        with self.requestsToProcess.mutex:
            self.requestsToProcess.queue.clear()
        for thread in self.activeWorkers:
            self.requestsToProcess.put(None)
        self.stop = True
    
    def enqueueRequest(self, request):
        self.requestsToProcess.put(request)

    #if allow is false, it will stop processing when it counts all the requests and sees that they have responses.
    #if allow is true, it will assume that an outside process will set stop=true and keep running until that happens.
    #This can throw ExcessiveProxyFailureException.  Account for this.
    def executeRequests(self):
        if self.isDeterminate:
            requestCount = self.requestsToProcess.qsize()

        self.initRequestWorkers()
        try:
            while self.getNumberOfRunningWorkers() > 0 or not self.results.empty() :
                if (self.isDeterminate) and requestCount <= self.resultsProcessed:
                    self.stopAllWorkers()           

                result = self.results.get()
                if type(result) is RequestResult:
                    yield result
                if type(result) is Exception:
                    raise result               
                if type(result) is RequestWorker:
                    self.activeWorkers.remove(result)
        finally:
            self.stopAllWorkers()
            self.proxySource.reportStatus()
            self.proxySource.persistProxies()
            
            with self.requestsToProcess.mutex:
                for item in list(self.requestsToProcess.queue):
                    if item is RequestInfo:
                        raise Exception("Something happened that caused the threads to stop with requests waiting.")
