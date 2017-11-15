# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 12:41:03 2017

@author: zhe&yuzheng
"""
import sys
class lockManager:
    '''
    Lock manager maintains and updates global lock table and lock requests.
    '''
    def __init__(self):
        self.readLockTable=dict()
        self.writeLockTable=dict()
        self.lockRequest=dict()
        for i in range(1,21):
            self.readLockTable['x'+str(i)]=[]
            self.writeLockTable['x'+str(i)]=[]
            self.lockRequest['x'+str(i)]=[]
            
    def isLockAvailable(self,mode,item,sm):
        '''
        This method checks if a lock of certain type is available.
        
        Param: 
            mode: lock mode (R/W), type int
            item: target record (xi), type str
            sm: site manager, type siteManager
        Return: Boolean
        '''
        #first check if sm is up or down
        siteUp=False
        if item in sm.invertSiteList:
            site=sm.invertSiteList[item]
            if sm.getSiteCondition(site)!=0:
                siteUp=True
        else:            
            for i in range(10):
                if sm.getSiteCondition(i)!=0:
                    siteUp=True
                    break
        if siteUp:
            if mode==0:
                if len(self.writeLockTable[item])==0:
                    return True
                else:
                    return False
            else:
                if len(self.readLockTable[item])==0 and len(self.writeLockTable[item])==0:
                    return True
                else:
                    return False
        else:
            return False
    
    def setLock(self,mode,transaction,item):
        '''
        This method updates the global lock table in lock manager when a transaction acquires a lock.
        
        Param:
            mode: lock mode (R/W), type int
            transaction: transaction id, type int
            item: target record (xi), type str
        Return: 
        '''
        if mode==0:
            self.readLockTable[item].append(transaction)
        else:
            self.writeLockTable[item].append(transaction)
            #check size of writeLockTable[item]==1?
            
    def getLockStatus(self,mode,item):
        '''
        This method gets the list of transactions that currently holding locks on a record.
        
        Param:
            mode: lock mode (R/W), type int
            item: target record (xi), type str
        Return: list
        '''
        if mode==0:
            return self.readLockTable[item]
        else:
            return self.writeLockTable[item]
        
    def setLockRequest(self,transaction,item):    #only non available lock requests go in here
        '''
        This method adds a lock request to the lockRequest table.
        
        Param:
            transaction: transaction id, type int
            item: target record (xi), type str
        Return:
        '''
        if transaction not in self.lockRequest[item]:
            self.lockRequest[item].append(transaction)
        
    def getLockRequest(self,item):
        '''
        This method gets the list of transactions that are waiting for the lock of some record.
        
        Param:
            item: target record (xi), type str
        Return: list
        '''
        return self.lockRequest[item]
    
    def removeRequest(self,item):
        '''
        This method removes the first transaction in the queue that is waiting for the lock of some record.
        
        Param:
            item: target record (xi), type str
        Return:
        '''
        self.lockRequest[item].pop(0)
    
    def removeAllRequestFromTransaction(self,id):
        '''
        This method removes a transaction from the lock request list.
        
        Param:
            id: transaction id, type int
        Return:
        '''
        for item in self.lockRequest:
            try:
                self.lockRequest[item].remove(id)
            except:
                pass
    
    def releaseLock(self,transaction,item):
        '''
        This method removes a transaction that is holding locks of some records from the global lock table.
        
        Param:
            transaction: transaction id, type int
            item: target record (xi), type str
        Return:
        '''
        try:
            self.readLockTable[item].remove(transaction)
        except:
            pass
        try:
            self.writeLockTable[item].remove(transaction)
        except:
            pass
        
    def detectDeadLock(self,tm):#Compare lockRequest to write locks
        '''
        This method detect if deadlock is present among lock requests and gives a list of transactions
        to be killed if there is deadlock.
        
        Param:
            tm: transaction manager, type transactionManager
        Returns:
        '''
        #build graph
        waitForGraph=dict()
        for key in self.lockRequest:
            requestList=self.lockRequest[key]
            occupyList=self.writeLockTable[key]
            for r in requestList:
                if r in waitForGraph:
                    waitForGraph[r].extend(occupyList)
                else:
                    waitForGraph[r]=occupyList 
        #BSF
        visited=set()
        transactionToKill=set()
        for key in waitForGraph:
            if key in visited:
                continue
            transactionToKill=set()
            queue=[]
            parent=dict()
            path=set()
            startNode=key
            queue.append(startNode)
            parent[startNode]=None
            cycleStart=None
            while(len(queue)!=0):
                if cycleStart!=None:
                    break
                node=queue.pop(0)
                visited.add(node)
                if node not in waitForGraph:
                    break
                for neighbor in waitForGraph[node]:
                    if neighbor in visited:
                        cycleStart=node
                        break
                    if neighbor not in visited:
                        queue.append(neighbor)
                        parent[neighbor]=node 
            
            if cycleStart != None:
                while(parent[cycleStart]!=None):
                    path.add(cycleStart)
                    path.add(parent[cycleStart])
                    cycleStart=parent[cycleStart]
                    
                maxDOB=-sys.maxsize
                temp=-1
                for id in path:
                    DOB=tm.transactionTable[id].startTime
                    if DOB>maxDOB:
                        maxDOB=DOB
                        temp=id
                if temp!=-1:
                    transactionToKill.add(temp)
        return transactionToKill