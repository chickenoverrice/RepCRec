# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 16:19:39 2017

@author: zhe&yuzheng
"""

class transaction():
    '''
    This class represents a transaction.
    '''
    def __init__(self,id,mode,startTime):
        self.id=id
        self.mode=mode    #0: RO, 1: RW
        self.lock=dict()  #0: read, 1:write
        self.status=0    #0:in progress, 1:commited, 2:aborted, 3:blocked
        self.buffer=[]   #blocked operations
        self.currentValue=dict()  #data item value after operation
        self.startTime=startTime  #birthday
        self.lockRequest=dict()  #lock not available
        self.accessedItems=set()    #all data items accessed
        
    def getLock(self,item):
        '''
        This method gets the lock that the transaction is holding on a data item.
        
        Param:
            item: target record (xi), type str
        Return:
            int or None
        '''
        if item in self.lock:
            return self.lock[item]
        return None
    
    def setLock(self,item,mode):
        '''
        This method adds a lock to the transaction when it acquires one.
        
        Param:
            item: target record (xi), type str
            mode: lock mode (R/W), type int
        Return:
        '''
        if item in self.lock:
            if self.lock[item]==0 and mode==1:
                self.lock[item]=mode
        else:
            self.lock[item]=mode
    
    def releaseLock(self):
        '''
        This method gets all the locks that the transaction holds.
        
        Param:
        Return:
            set
        '''
        lockedItems=set()
        for key in self.lock:
            lockedItems.add(key)
        self.lock=dict()
        return lockedItems
    
    def updateValue(self,item,value):
        '''
        This method updates the value of a data item in the data table inside the transaction.
        
        Param:
            item: target record (xi), type str
            value: value to write to the item, type int
        '''
        self.currentValue[item]=value
        
    def getCurrentValue(self,item):
        '''
        This method gets the most updated value of a data item.
        
        Param:
            item: target record (xi), type str
        Return: 
            int
        '''
        return self.currentValue[item]
        
    def setStatus(self,status):
        '''
        This method updates the status of the transaction.
        
        Param:
            status: type int
        Return:
        '''
        self.status=status
        
    def getStatus(self):
        '''
        This method gets the status of the transation.
        
        Param:
        Return:
            int
        '''
        return self.status
    
    def setBuffer(self,op):
        '''
        This method adds a blocked operation to the buffer in the transaction.
        
        Param:
            op: an operation, type tuple
        Return:
        '''
        self.buffer.append(op)
    
    def clearBuffer(self):
        '''
        This method empties the buffer of the transaction.
        
        Param:
        Return:
        '''
        self.buffer.clear()
        
    def getBuffer(self):
        '''
        This method gets the list of blocked operations (buffer) of the transaction.
        
        Param:
        Return:
            list
        '''
        return self.buffer
    
    def setLockRequest(self,mode,item):
        '''
        This method adds a lock request to the request table in the transaction.
        
        Param:
            mode: lock mode (R/W), type int
            item: target record (xi), type str
        Return:   
        '''
        if item in self.lockRequest:
            if self.lockRequest[item]==0 and mode==1:
                self.lockRequest[item]=mode
        else:
            self.lockRequest[item]=mode
    
    def getLockRequest(self):
        '''
        This method gets the lock requests that the transaction is waiting on.
        
        Param:
        Return:
            dict
        '''
        return self.lockRequest
    
    def setAccessedItems(self,item):
        '''
        This method adds a data item to the accessedItems list when the transaction is reading/writing it.
        
        Param:
            item: target record (xi), type str
        Return:
        '''
        self.accessedItems.add(item)
        
    def getAccessedItems(self):
        '''
        This method gets the list of data items that the transaction touched.
        
        Param:
        Return:
            set
        '''
        return self.accessedItems
    
class transactionManager():
    '''
    Transaction manager maintains a list of transactions and coordinates transactions. 
    '''
    def __init__(self):
        self.transactionTable=dict()
        self.dataLastValue=dict()           #most updated data value from commited transactions
        
        for i in range(1,20):
            self.dataLastValue['x'+str(i)]=10*i
                
    def readValue(self,item):
        '''
        This method gets the last committed value of a data item.
        
        Param:
            item: target record (xi), type str
        Return:
            int
        '''
        return self.dataLastValue[item]
    
    def createTransaction(self,id,mode,startTime):
        '''
        This method creates a transaction.
        
        Param:
            id: transaction id, type int
            mode: transaction mode (RO/RW), type int
            startTime: the time when the transaction was created, type int
        Return:
        '''
        self.transactionTable[id]=transaction(id,mode,startTime)
        self.transactionTable[id].setStatus(0)
        if mode==0:
            for item in self.dataLastValue:
                self.transactionTable[id].updateValue(item,self.dataLastValue[item])
        
    def abortTransaction(self,id,sm,lm):
        '''
        This method aborts a transaction and returns a list of locks the transaction is holding.
        
        Param:
            id: transaction id, type int
            sm: site manager, type siteManager
            lm: lock manager, type lockManager
        Return:
            set
        '''
        self.transactionTable[id].setStatus(2)
        lm.removeAllRequestFromTransaction(id)
        lock=self.transactionTable[id].releaseLock()
        for key in lock:
            for i in range(1,11):
                site= sm.getSite(i)
                site.resetLock(key,id)
        return lock    
    
    def commitTransaction(self,id,sm,lm):
        '''
        This method commits a transaction and returns a list of locks the transaction is holding.
        
        Param:
            id: transaction id, type int
            sm: site manager, type siteManager
            lm: lock manager, type lockManager
        Return:
            set
        '''
        self.transactionTable[id].setStatus(1)
        lm.removeAllRequestFromTransaction(id)
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
                
    def blockTransaction(self,id,op,lm):
        '''
        This method blocks a transaction and updates the transaction's buffer.
        
        Param:
            id: transaction id, type int
            op: operation, type tuple
            lm: lock manager, type lockManager
        Return:
            set
        '''
        self.transactionTable[op[2]].setLockRequest(op[1],op[3])
        self.transactionTable[op[2]].setBuffer(op)
        self.transactionTable[op[2]].setStatus(3)
        lm.setLockRequest(op[2],op[3])
        
    def unblockTransaction(self,id):
        '''
        This method change the status of a blocked transaction to be in progress
        and return the buffer of blocked operations.
        
        Param:
            id: transaction id, type int
        Return:
            tuple
        '''
        self.transactionTable[id].setStatus(0)
        op=self.transactionTable[id].getBuffer()[0]
        self.transactionTable[id].clearBuffer()
        return op
    
    def endTransactionStatus(self,id,sm):#check if site has all locks
        '''
        This method checks whether a transaction can commit.
        
        Param:
            id: transaction id, type int
            sm: site manager, type siteManager
        Return:
            Boolean
        '''
        if self.transactionTable[id].getStatus()!=0:
            return False        
        else:
            count=0
            accessedItem=self.transactionTable[id].getAccessedItems()
            canCommit=True
            #if single copy item accessed on a down site, abort
            for item in accessedItem:
                count+=1
                if item in sm.invertSiteList:
                    site=sm.invertSiteList[item]
                    if sm.getSiteCondition(site)==0:
                        canCommit=False
                    else:
                        if item not in sm.getSite(site).lockTable:
                            canCommit=False
                        else:
                            lock=sm.getSite(site).getLockStatus(item)
                            if (id not in lock[0]) and (id not in lock[1]):
                                canCommit=False

            #if no site is up:    
            upSite=0
            if count<len(accessedItem):
                for i in range(10):
                    if sm.getSiteCondition(i)==1:
                        upSite+=1
                if upSite==0:
                    canCommit=False
        return canCommit

def processRecordOperation(op,tm,sm,lm,time,verbose):
    '''
    This method processes operations that read or write a data item and returns if the operation
    can be performed or has to wait.
    
    Param:
        op: operation, type tuple
        tm: transaction manager, type transactionManager
        sm: site manager, type siteManager
        lm: lock manager, type lockManager
        time: current time point, type int
        verbose: printing option, type Boolean
    '''
    if tm.transactionTable[op[2]].getStatus()==2:#if already aborted, do nothing
        return False
    tm.transactionTable[op[2]].setAccessedItems(op[3])
    if op[1]==0:     #read
        if tm.transactionTable[op[2]].mode==0:           #RO
            value=tm.transactionTable[op[2]].getCurrentValue(op[3])
            print('T'+str(tm.transactionTable[op[2]].id)+' reads '+op[3]+':'+str(value))
        else:           #RW
            availableLock=tm.transactionTable[op[2]].getLock(op[3])
            if availableLock!=None:     #has lock
                if op[3] in tm.transactionTable[op[2]].currentValue:
                    value=tm.transactionTable[op[2]].getCurrentValue(op[3])
                else:
                    value=tm.readValue(op[3])
                print('T'+str(tm.transactionTable[op[2]].id)+' reads '+op[3]+':'+str(value))
            else:       #no lock
                requestLock=lm.isLockAvailable(op[1],op[3],sm)
                if requestLock:
                    tm.transactionTable[op[2]].setLock(op[3],op[1])
                    lm.setLock(op[1],op[2],op[3])
                    for i in range(1,11):
                        sm.getSite(i).setLock(op[3],op[1],op[2])
                    if op[3] in tm.transactionTable[op[2]].currentValue:
                        value=tm.transactionTable[op[2]].getCurrentValue(op[3])
                    else:
                        value=tm.readValue(op[3])
                    print('T'+str(tm.transactionTable[op[2]].id)+' reads '+op[3]+':'+str(value))
                else:
                    tm.blockTransaction(op[2],op,lm)
                    if verbose:
                        print('Transaction T'+str(op[2])+' is waiting for read lock on item '+op[3])
                    return False
        return True
    else:            #write
        availableLock=tm.transactionTable[op[2]].getLock(op[3])
        if availableLock!=None:
            tm.transactionTable[op[2]].updateValue(op[3],op[4])
        else:
            requestLock=lm.isLockAvailable(op[1],op[3],sm)
            if requestLock:
                tm.transactionTable[op[2]].setLock(op[3],op[1])
                lm.setLock(op[1],op[2],op[3])
                for i in range(1,11):
                    sm.getSite(i).setLock(op[3],op[1],op[2])
                tm.transactionTable[op[2]].updateValue(op[3],op[4])
                if verbose:
                    print('Transaction T'+str(op[2])+' write value '+str(op[4])+' to item '+op[3])
            else:
                tm.blockTransaction(op[2],op,lm)
                if verbose:
                    print('Transaction T'+str(op[2])+' is waiting for write lock on item '+op[3])
                return False
        return True
                
def processTransactionOperation(op,tm,sm,lm,time,verbose):
    '''
    This method processes operations that creates or ends a transaction.
    
    Param:
        op: operation, type tuple
        tm: transaction manager, type transactionManager
        sm: site manager, type siteManager
        lm: lock manager, type lockManager
        time: current time point, type int
        verbose: printing option, type Boolean
    Return:
    '''
    if op[1]==0 or op[1]==1:
        tm.createTransaction(op[2],op[1],time)
        if verbose:
            if op[1]==0:
                print('Transaction T'+str(op[2])+' is created at time '+str(time)+'. Mode is read only.')
            else:
                print('Transaction T'+str(op[2])+' is created at time '+str(time)+'. Mode is read write.')
    else:
        if tm.transactionTable[op[2]].mode==0:
            print('RO transaction T'+str(op[2])+' commits at time '+str(time))
        else:  
            canCommit=tm.endTransactionStatus(op[2],sm) #check if transaction can commit
            transactionToKill=lm.detectDeadLock(tm)            #check if transaction involved in deadlock
            for t in transactionToKill:
                if op[2]==t:
                    canCommit=False
                    if verbose:
                        print('Transaction T'+str(op[2])+' killed in deadlock.')
                    break
            lockToRelease=None
            if canCommit:           #commit
                lockToRelease=tm.commitTransaction(op[2],sm,lm)
                if verbose:
                    print('Transaction T'+str(op[2])+' commits at time '+str(time))
            else:                   #abort
                lockToRelease=tm.abortTransaction(op[2],sm,lm)
                if verbose:
                    print('Transaction T'+str(op[2])+' aborts at time '+str(time))
            if lockToRelease !=None:
                for lock in lockToRelease:
                    lm.releaseLock(op[2],lock)
                    if len(lm.getLockRequest(lock))!=0: #some transaction is waiting on lock
                        nextRequester=lm.getLockRequest(lock)[0]                
                        redoOP=tm.unblockTransaction(nextRequester)
                        if verbose:
                            print('Operation ',redoOP, 'is allowed to execute.')
                        if redoOP[0]==1:
                            processTransactionOperation(redoOP,tm,sm,lm,time,verbose)
                        else:
                            canProceed=processRecordOperation(redoOP,tm,sm,lm,time,verbose)
                            if canProceed:
                                lm.removeRequest(lock)
        
def processSiteOperation(op,sm,tm,verbose):
    '''
    This method processes operations related to a site, including site failure, recovery and dumping.
    
    Param:
        op: operation, type tuple
        sm: site manager, type siteManager
        tm: transaction manager, type transactionManager
        verbose: printing option, type Boolean
    '''
    if op[1]==0:
        sm.failSite(op[2])
        if verbose:
            print('Site'+str(op[2])+' failed.')
    elif op[1]==1:
        sm.recoverSite(op[2])
        if verbose:
            print('Site'+str(op[2])+' is being recovered.')
    else:#dump
        if op[2]==0:       #dump()
            for k,v in sm.siteList.items():
                sm.dumpOneSite(k)
        elif op[2]==1:     #dump(i)
            sm.dumpOneSite(op[3])
        else:              #dump(xi)
            value=tm.readValue(op[3])
            print('The value of '+op[3]+'is ',value)

def killTransaction(tm,sm,lm,time,verbose):
    '''
    This method kills a transaction that is involved in deadlock and allows other transactions to proceed.
    
    Param:
        tm: transaction manager, type transactionManager
        sm: site manager, type siteManager
        lm: lock manager, type lockManager
        time: current time point, type int
        verbose: printing option, type Boolean
    '''
    transactionToKill=lm.detectDeadLock(tm)
    for t in transactionToKill:
        lockToRelease=tm.abortTransaction(t,sm,lm)
        if verbose:
            print('Transaction T'+str(t)+' aborts at time '+str(time))
        if lockToRelease !=None:
            for lock in lockToRelease:
                lm.releaseLock(t,lock)
                if len(lm.getLockRequest(lock))!=0: #some transaction is waiting on lock
                    nextRequester=lm.getLockRequest(lock)[0]                
                    redoOP=tm.unblockTransaction(nextRequester)
                    if verbose:
                        print('Operation ',redoOP, 'is allowed to execute.')
                    if redoOP[0]==1:
                        processTransactionOperation(redoOP,tm,sm,lm,time,verbose)
                    else:
                        canProceed=processRecordOperation(redoOP,tm,sm,lm,time,verbose)
                        if canProceed:
                            lm.removeRequest(lock)            