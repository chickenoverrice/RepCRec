# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 12:41:03 2017

@author: yzw
"""

class lockManager:
    def __init__(self):
        self.readLockTable=dict()
        self.writeLockTable=dict()
        self.lockRequest=dict()
        for i in range(1,21):
            self.readLockTable['x'+str(i)]=[]
            self.writeLockTable['x'+str(i)]=[]
            self.lockRequest['x'+str(i)]=set()
            
    def isLockAvailable(self,mode,item):
        if len(self.readLockTable[item])==0 and len(self.writeLockTable[item])==0:
            return True
        return False
    
    def setLock(self,mode,transaction,item):
        if mode==0:
            self.readLockTable[item].append(transaction)
        else:
            self.writeLockTable[item].append(transaction)
            
    def getLockStatus(self,mode,item):
        if mode==0:
            return self.readLockTable[item]
        else:
            return self.writeLockTable[item]
        
    def setLockRequest(self,transaction,item):    #only non available lock requests go in here
        self.lockRequest[item].add(transaction)
        
    def getLockRequest(self):
        return self.lockRequest
    
    def removeRequest(self,transaction,item):
        pass
    
    def releaseLock(self,transaction):
        pass
    
    def detectDeadLock(self):
        '''
        Compare lockRequest to write locks
        '''
        pass