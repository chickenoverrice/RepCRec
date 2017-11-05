# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 12:41:03 2017

@author: zxu
"""

class site:
    def __init__(self,id):
        self.id=id
        self.lockTable=dict()     #{item:[[read locks],[write locks]]}
        self.value=dict()
        
        for i in range(2,21,2):
            self.value['x'+str(i)]=10*i
        if id%2==0:
            for i in range(1,20,2):
                if id==(i%10+1):
                    self.value['x'+str(i)]=10*i
            
    def getValue(self,item):
        return self.value[item]
    
    def setValue(self,item,value):
        if item in self.value:
            self.value[item]=value
    
    def cleanValue(self):
        for i in range(2,21,2):
            self.value['x'+str(i)]=None
        if id%2==0:
            for i in range(1,20,2):
                if id==(i%10+1):
                    self.value['x'+str(i)]=None
    
    def getLockStatus(self,item):
        return self.lockTable[item]
    
    def cleanLockTable(self):
        self.lockTable=dict()
        
    def setLock(self,item,mode,transaction):            #Assume lock is accessible. Check should be done by LM!!            
        if item not in self.lockTable:
            temp=[item]
            self.lockTable[item] = [[None for _ in range(1)] for _ in range(2)]
            if mode==0:
                self.lockTable[item][0]=temp
            else:
                self.lockTable[item][1]=temp
        else:
            if mode==0:
                self.lockTable[item][0].append(transaction)
            else:
                self.lockTable[item][1].append(transaction)            
    
    def resetLock(self,item,transaction):
        if item not in self.lockTable:
            print(item+'is not locked!')
            return
        readLock=self.lockTable[item][0]
        writeLock=self.lockTable[item][1]
        for i in range(len(readLock)-1,-1):
            if readLock[i]==transaction:
                del readLock[i]                  #del self.lockTable[item][0][i]
        for i in range(len(writeLock)-1,-1):
            if writeLock[i]==transaction:
                del writeLock[i]                 #del self.lockTable[item][1][i] 
        
class siteManager:
    def __init__(self):
        self.siteCondition=[1 for i in range(10)]    # index of SiteCondition Table is 1 smaller than site id. 0:down 1:up 2:recover 
        self.siteList=dict()
        
    def initSite(self):
        for i in range(1,11):
            self.siteList[i]=site(i)
            
    def getSiteCondition(self,id):
        return self.siteCondition[id-1]
    
    def setSiteCondition(self,id,condition):
        self.siteCondition[id-1]=condition
        
    def getSite(self,id):
        return self.siteList[id]
        
    def failSite(self,id):
        self.siteCondition[id-1]=0
        self.siteList[id].cleanLockTable()
        self.siteList[id].cleanValue()
        
    def recoverSite(self,id):
        self.setSiteCondition(id,2)
        self.siteList[id].cleanValue()
        self.siteList[id].cleanLockTable()
        
    def updateSite(self,id,target,value):
        self.siteList[id].setValue(target,value)
        