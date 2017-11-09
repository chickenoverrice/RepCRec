# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 16:19:39 2017

@author: zxu
"""

class transaction():
    def __init__(self,id,mode,startTime):
        self.id=id
        self.mode=mode
        self.lock=dict()  #0: read, 1:write
        self.status=0    #0:in progress, 1:commited, 2:aborted, 3:blocked
        self.buffer=[]
        self.currentValue=dict()
        self.startTime=startTime
        self.lockRequest=dict()  #lock not available
        self.accessedItems=set()
        
    def getLock(self,item):
        if item in self.lock:
            return self.lock[item]
        return None
    
    def setLock(self,item,mode):
        if item in self.lock:
            if self.lock[item]==0 and mode==1:
                self.lock[item]=mode
        else:
            self.lock[item]=mode
    
    def releaseLock(self):
        lockedItems=set()
        for key in self.lock:
            lockedItems.add(key)
        self.lock=dict()
        return lockedItems
    
    def updateValue(self,item,value):
        self.currentValue[item]=value
        
    def getCurrentValue(self,item):
        return self.currentValue[item]
        
    def setStatus(self,status):
        self.status=status
        
    def getStatus(self):
        return self.status
    
    def setBuffer(self,op):
        self.buffer.append(op)
    
    def clearBuffer(self):
        self.buffer.clear()
        
    def getBuffer(self):
        return self.buffer
    
    def setLockRequest(self,mode,item):
        if item in self.lockRequest:
            if self.lockRequest[item]==0 and mode==1:
                self.lockRequest[item]=mode
        else:
            self.lockRequest[item]=mode
    
    def getLockRequest(self):
        return self.lockRequest
    
    def setAccessedItems(self,item):
        self.accessedItems.add(item)
        
    def getAccessedItems(self):
        return self.accessedItems
    
class transactionManager():
    def __init__(self):
        self.transactionTable=dict()
        self.dataLastValue=dict()           #most updated data value from commited transactions
        
        for i in range(1,20,2):
            self.dataLastValue['x'+str(i)]=10*i
                
    def readValue(self,item):
        return self.dataLastValue[item]
    
    def createTransaction(self,id,mode,startTime):
        self.transactionTable[id]=transaction(id,mode,startTime)
        self.transactionTable[id].setStatus(0)
        
    def abortTransaction(self,id,sm):
        self.transactionTable[id].setStatus(2)
        lock=self.transactionTable[id].releaseLock()
        for key in lock:
            for i in range(1,11):
                site= sm.getSite(i)
                site.resetLock(key,id)
        return lock    
    
    def commitTransaction(self,id,sm):
        self.transactionTable[id].setStatus(1)
        transaction=self.transactionTable[id]
        lock=self.transactionTable[id].releaseLock()
        for key in lock:
            for i in range(1,11):
                site= sm.getSite(i)
                site.resetLock(key,id)
        for key in transaction.currentValue:
            self.dataLastValue[key]=transaction.currentValue[key]
            for i in range(1,11):
                sm.updateSite(i,key,transaction.currentValue[key])
                if sm.getSiteCondition(i)==2:
                    sm.setSiteCondition(i,1)
        return lock        
                
    def blockTransaction(self,id,op):
        self.transactionTable[id].setStatus(3)
        self.transactionTable[id].setBuffer(op)
        
        
    def unblockTransaction(self,id):
        '''
        blocked transaction gets lock and re-start execution.
        '''
        self.transactionTable[id].setStatus(0)
        op=self.transactionTable[id].getBuffer()[0]
        self.transactionTable[id].clearBuffer()
        return op
        
    def endTransactionStatus(self,id,sm):
        if self.transactionTable[id].getStatus()!=0:
            return False        
        else:
            count=0
            accessedItem=self.transactionTable[id].getAccessedItems()
            #if single copy item accessed on a down site, abort
            for item in accessedItem:
                count+=1
                site=sm.invertSiteList[item]
                if sm.getSiteCondition(site)==0:
                    return False
            #if no site is up:    
            if count<len(accessedItem):
                for i in range(10):
                    if sm.getSiteCondition(i)==1:
                        return True
            else:
                return True
        return False

def processRecordOperation(op,tm,sm,lm,time,verbose):
    tm.transactionTable[op[2]].setAccessedItems(op[3])
    if op[1]==0:     #read
        if tm.transactionTable[op[2]].mode==0:           #RO
            value=tm.readValue(op[3])
            print('T'+str(tm.transactionTable[op[2]].id)+'reads '+op[3]+':'+str(value))
        else:           #RW
            availableLock=tm.transactionTable[op[2]].getLock(op[3])
            if availableLock!=None:     #has lock
                value=tm.transactionTable[op[2]].getCurrentValue(op[3])
                print('T'+str(tm.transactionTable[op[2]].id)+'reads '+op[3]+':'+str(value))
            else:       #no lock
                requestLock=lm.isLockAvailable(op[1],op[3])
                if requestLock:
                    tm.transactionTable[op[2]].setLock(op[3],op[1])
                    lm.setLock(op[1],op[2],op[3])
                    for i in range(1,11):
                        sm.getSite(i).setLock(op[3],op[1],op[2])
                    value=tm.transactionTable[op[2]].getCurrentValue(op[3])
                    print('T'+str(tm.transactionTable[op[2]].id)+'reads '+op[3]+':'+str(value))
                else:
                    tm.transactionTable[op[2]].setLockRequest(op[1],op[3])
                    tm.transactionTable[op[2]].setBuffer(op)
                    tm.transactionTable[op[2]].setStatus(3)
                    lm.setLockRequest(op[2],op[3])
    else:            #write
        availableLock=tm.transactionTable[op[2]].getLock(op[3])
        if availableLock!=None:
            tm.transactionTable[op[2]].updateValue(op[3],op[4])
        else:
            requestLock=lm.isLockAvailable(op[1],op[3])
            if requestLock:
                tm.transactionTable[op[2]].setLock(op[3],op[1])
                lm.setLock(op[1],op[2],op[3])
                for i in range(1,11):
                    sm.getSite(i).setLock(op[3],op[1],op[2])
                tm.transactionTable[op[2]].updateValue(op[3],op[4])
            else:
                tm.transactionTable[op[2]].setLockRequest(op[1],op[3])
                tm.transactionTable[op[2]].setBuffer(op)
                tm.transactionTable[op[2]].setStatus(3)
                lm.setLockRequest(op[2],op[3])    
                
def processTransactionOperation(op,tm,sm,lm,time,verbose):
    if op[1]==0 or op[1]==1:
        tm.createTransaction(op[2],op[1],time)
    else:
        canCommit=tm.endTransactionStatus(op[2],sm)   #check if transaction can commit
        transactionToKill=lm.detectDeadLock(tm)            #check if transaction involved in deadlock
        if op[2]==transactionToKill:
            canCommit=False
        lockToRelease=None
        if canCommit:           #commit
            lockToRelease=tm.commitTransaction(op[2],sm)
        else:                   #abort
            lockToRelease=tm.abortTransaction(op[2],sm)
        if lockToRelease !=None:
            for lock in lockToRelease:
                nextRequester=lm.getLockRequest(lock)[0]
                lm.releaseLock(op[2],lock)
                lm.removeRequest(lock)
                redoOP=tm.unblockTransaction(nextRequester)
                if redoOP[0]==1:
                    processTransactionOperation(redoOP,tm,sm,lm,time,verbose)
                else:
                    processRecordOperation(redoOP,tm,sm,lm,time,verbose)
    if verbose:
        print(tm.transactionTable[op[2]].id,tm.transactionTable[op[2]].mode,
              tm.transactionTable[op[2]].status)
        
def processSiteOperation(op,sm,tm,verbose):
    if op[1]==0:
        sm.failSite(op[2])
    elif op[1]==1:
        sm.recoverSite(op[2])
    else:#dump
        if op[2]==0:       #dump()
            for k,v in sm.siteList.items():
                sm.dumpOneSite(k)
        elif op[2]==1:     #dump(i)
            sm.dumpOneSite(op[3])
        else:              #dump(xi)
            value=tm.readValue(op[3])
            print('The value of '+op[3]+'is ',value)