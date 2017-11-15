# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 16:37:35 2017

@author: zxu
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

os.chdir('..')
directory=os.path.abspath(os.curdir)

def main(verbose,inputFile):  
    if inputFile !=None:
        inputFile = os.path.join(directory, inputFile) 
    elif inputFile ==None:
        inputFile=sys.stdin
    newSM=siteManager()
    newSM.initSite()
    newTM=transactionManager()
    newLM=lock.lockManager()
    time=0
 
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
                        processSiteOperation(op,newSM,newTM,verbose)
                    elif op[0]==1:
                        processTransactionOperation(op,newTM,newSM,newLM,time,verbose)
                    else:
                        processRecordOperation(op,newTM,newSM,newLM,time,verbose)
<<<<<<< HEAD
            if time%2==1:
                killTransaction(newTM,newSM,newLM,time,verbose)                        
=======
                        
>>>>>>> ab0eb38be1ffdb5936ba9775a5d2f8ae3189f543
                            
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A script to build a distributed database and process transactions", formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--verbose', dest='verbose', default=False, action='store_true', help='This option prints out detailed execution information.')
    parser.add_argument('--f', type=str,  default=None, dest='inputFile', help='This option allows input from a file. Please enter full file directory after --f.')
    args = parser.parse_args()
    args = parser.parse_args()
    main(verbose=args.verbose,inputFile=args.inputFile)