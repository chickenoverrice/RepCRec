# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 12:41:03 2017

@author: yzw
"""
import sys
class lockManager:
    def __init__(self):
        self.readLockTable=dict()
        self.writeLockTable=dict()
        self.lockRequest=dict()
        for i in range(1,21):
            self.readLockTable['x'+str(i)]=[]
            self.writeLockTable['x'+str(i)]=[]
            self.lockRequest['x'+str(i)]=[]
            
    def isLockAvailable(self,mode,item,sm):
        #TO DO: first check if sm is up or down
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
    
    def setLock(self,mode,transaction,item):
        if mode==0:
            self.readLockTable[item].append(transaction)
        else:
            self.writeLockTable[item].append(transaction)
            #check size of writeLockTable[item]==1?
            
    def getLockStatus(self,mode,item):
        if mode==0:
            return self.readLockTable[item]
        else:
            return self.writeLockTable[item]
        
    def setLockRequest(self,transaction,item):    #only non available lock requests go in here
        self.lockRequest[item].append(transaction)
        
    def getLockRequest(self,item):
        return self.lockRequest[item]
    
    def removeRequest(self,item):
        self.lockRequest[item].pop(0)
    
    def releaseLock(self,transaction,item):
        try:
            self.readLockTable[item].remove(transaction)
        except:
            pass
        try:
            self.writeLockTable[item].remove(transaction)
        except:
            pass
        
    def detectDeadLock(self,tm):
        '''
        Compare lockRequest to write locks
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
                try:
                    for neighbor in waitForGraph[node]:
                        if neighbor in visited:
                            cycleStart=neighbor
                            parent[cycleStart]=node
                            break
                        if neighbor not in visited:
                            queue.append(neighbor)
                            parent[neighbor]=node 
                except:
                    pass
            
            if cycleStart != None:
                while(parent[cycleStart]!=None):
                    path.add(cycleStart)
                    cycleStart=parent[cycleStart]
                    
                minDOB=sys.maxsize
                
                for id in path:
                    DOB=tm.transactionTable[id].startTime
                    if DOB<minDOB:
                        minDOB=DOB
                        transactionToKill.add(id)
        
        return transactionToKill