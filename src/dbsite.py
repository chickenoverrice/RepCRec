# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 12:41:03 2017

@author: zhe&yuzheng
"""

class site:
    '''
    This class represents an individual site that contains records and lock table.
    '''
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
        '''
        This method gets the commited value of a data item.
        
        Param:
            item: target record (xi), type str
        Return:
            int
        '''
        return self.value[item]
    
    def setValue(self,item,value):
        '''
        This method updates the value of a data item.
        
        Param:
            item: target record (xi), type str
            value: value to write to the item, type int
        Return:
        '''
        if item in self.value:
            self.value[item]=value
    
    def cleanValue(self):
        '''
        This method removes all the data items in the site.
        
        Param:
        Return:
        '''
        self.value=dict()
    
    def getLockStatus(self,item):
        '''
        This method gets a list of transactions holding locks on a data item.
        
        Param:
            item: target record (xi), type str
        Return:
            list
        '''
        return self.lockTable[item]
    
    def cleanLockTable(self):
        '''
        This method removes all the locks in the site, i.e. reset the lock table.
        
        Param:
        Return:
        '''
        self.lockTable=dict()
        
    def setLock(self,item,mode,transaction):   #Assume lock is accessible. Check should be done by LM!!            
        '''
        This method updates the lock table when a transaction acquires a lock.
        
        Param:
            item: target record (xi), type str
            mode: lock mode (R/W), type int
            transaction: transaction id, type int
        Return:
        '''
        if item not in self.lockTable:
            temp=[transaction]
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
        '''
        This method updates the lock table when a transaction releases locks.
        
        Param:
            item: target record (xi), type str
            transaction: transaction id, type int
        Return:
        '''
        if item not in self.lockTable:
            print(item+' is not locked!')
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
    '''
    Site manager maintains a list of sites and monitor the health of each site.
    '''
    def __init__(self):
        self.siteCondition=[1 for i in range(10)]    # index of SiteCondition Table is 1 smaller than site id. 0:down 1:up 2:recover 
        self.siteList=dict()
        self.invertSiteList={'x1':2,'x11':2,'x3':4,'x13':4,'x5':6,'x15':6,'x7':8,'x17':8,'x9':10,'x19':10} #easy access for single copy items
        
    def initSite(self):
        '''
        This method initializes all sites.
        
        Param:
        Return:
        '''
        for i in range(1,11):
            self.siteList[i]=site(i)
            
    def getSiteCondition(self,id):
        '''
        This method gets the condition of one site.
        
        Param:
            id: site id, type int
        Return:
            int
        '''
        return self.siteCondition[id-1]
    
    def setSiteCondition(self,id,condition):
        '''
        This method updates the condition of one site.
        
        Param:
            id: site id, type int
            condition: site condition (0:down 1:up 2:recover), type int
        Return:    
        '''
        self.siteCondition[id-1]=condition
        
    def getSite(self,id):
        '''
        This method gets a site instance.
        
        Param:
            id: site id, type int
        Return:
            site
        '''
        return self.siteList[id]
        
    def failSite(self,id):
        '''
        This method fails a site.
        
        Param:
            id: site id, type int
        Return:
        '''
        self.siteCondition[id-1]=0
        self.siteList[id].cleanLockTable()
        self.siteList[id].cleanValue()
        
    def recoverSite(self,id):
        '''
        This method recovers a site.
        
        Param:
            id: site id, type int
        Return:
        '''
        self.setSiteCondition(id,2)
        self.siteList[id].cleanValue()
        self.siteList[id].cleanLockTable()
        
    def updateSite(self,id,target,value):
        '''
        This method updates a data item on a given site.
        
        Param:
            id: site id, type int
            target: target record (xi), type str
            value to write to the item, type int
        Return:
        '''
        self.siteList[id].setValue(target,value)        
    
    def dumpOneSite(self,id):
        '''
        This method prints out all committed data items in a given site.
        
        Param:
            id: site id, type int
        Return
        '''
        if self.getSiteCondition(id)==0:    #site is down
            print('Site '+str(id)+' is down. No data available.')
        else:  #site is up or recovering
            print('Dumpping site'+str(id)+':')
            for k,v in self.getSite(id).value.items():
                print(k,' ', v)
            
    