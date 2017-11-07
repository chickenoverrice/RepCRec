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
        
    def getLock(self):
        return self.lock
    
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
        
    def setStatus(self,status):
        self.status=status
        
    def getStatus(self):
        return self.status
    
    def setBuffer(self,op):
        self.buffer.append(op)
        
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
                
    def createTransaction(self,id,mode,startTime):
        self.transactionTable[id]=transaction(id,mode,startTime)
        self.transactionTable[id].setStatus(0)
        
    def abortTransaction(self,id):
        self.transactionTable[id].setStatus(2)
    
    def commitTransaction(self,id,sm):
        self.transactionTable[id].setStatus(1)
        transaction=self.transactionTable[id]
        lock=self.transactionTable[id].releaseLock()
        for key in transaction.currentValue:
            self.dataLastValue[key]=transaction.currentValue[key]
            for i in range(1,11):
                sm.updateSite(i,key,transaction.currentValue[key])
        return lock        
                
    def blockTransaction(self,id,op):
        self.transactionTable[id].setStatus(3)
        self.transactionTable[id].setBuffer(op)
        
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
