# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 16:19:39 2017

@author: zxu
"""


class transactionManager():
    def __int__(self):
        self.transactionTable=dict()
        
    def createTransaction(self,id):
        pass
        
    def abortTransaction(self,id):
        pass
    
    def processOperation(self,op):
        if op[0]==0:
            pass
        elif op[0]==1:
            pass
        else:
            pass