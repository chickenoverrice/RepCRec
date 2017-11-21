# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 16:37:35 2017

@author: zhe&yuzheng
"""
from dbsite import siteManager
import argparse
from transaction import transactionManager
from transaction import processRecordOperation
from transaction import processTransactionOperation
from transaction import processSiteOperation
from transaction import killTransaction
import os
import utility
import lock
import fileinput
import sys


#file directory. Change!!!!!
os.chdir('..')
directory=os.path.abspath(os.curdir)

def main(verbose,inputFile):  
    #file read
    if inputFile !=None:
        inputFile = os.path.join(directory, inputFile) 
    elif inputFile ==None:
        inputFile=sys.stdin
        
    #initialize managers    
    newSM=siteManager()
    newSM.initSite()
    newTM=transactionManager()
    newLM=lock.lockManager()
    time=0
 
    #process instructions
    with fileinput.input(files=inputFile) as f:
        for instruction in f:
            time+=1
            instruction=instruction.strip().lower().split(')')
            for item in instruction:
                if item:
                    op=utility.parseCommand(item)
                    if op ==None:
                        continue
                    if verbose:
                        print('current time:'+str(time)+' executing operation:',op)
                    if op[0]==0:
                        processSiteOperation(op,newTM,newSM,newLM,time,verbose)
                    elif op[0]==1:
                        processTransactionOperation(op,newTM,newSM,newLM,time,verbose)
                    else:
                        processRecordOperation(op,newTM,newSM,newLM,time,verbose)
            if time%2==1:
                killTransaction(newTM,newSM,newLM,time,verbose)                        
                            
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A script to build a distributed database and process transactions", formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--verbose', dest='verbose', default=False, action='store_true', help='This option prints out detailed execution information.')
    parser.add_argument('--f', type=str,  default=None, dest='inputFile', help='This option allows input from a file. Please enter full file directory after --f.')
    args = parser.parse_args()
    args = parser.parse_args()
    main(verbose=args.verbose,inputFile=args.inputFile)