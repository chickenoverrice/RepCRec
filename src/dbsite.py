# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 12:41:03 2017

@author: zxu
"""

class Site:
    def __init__(self,id):
        self.id=id
        self.lockTable=dict()
        self.value=dict()
        
        for i in range(2,21,2):
            self.value['x'+str(i)]=10*i
        if id%2==0:
            for i in range(1,20,2):
                if id==(i%10+1):
                    self.value['x'+str(i)]=10*i
            
    def get_value(self,item):
        return self.value[item]
    
    def get_lockStatus(self,item):
        return self.lockTable[item]

        
            
            
        