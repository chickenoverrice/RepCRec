# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 16:37:35 2017

@author: zxu
"""
from dbsite import siteManager
import argparse
import sys
from transaction import transactionManager
import os
import utility
import lock

os.chdir('..')
directory=os.path.abspath(os.curdir)

def main(verbose,inputFile,stdIN):  
    if inputFile and stdIN:
        sys.exit('Please choose only one option: --f or --s')
      
    abs_file_path = os.path.join(directory, inputFile)  
    newSM=siteManager()
    newSM.initSite()
    newTM=transactionManager()
    newLM=lock.lockManager()
    time=0
    
    if inputFile != None:    
        with open(abs_file_path,'r') as f:
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
                            if op[1]==0:
                                newSM.failSite(op[2])
                            elif op[1]==1:
                                newSM.recoverSite(op[2])
                            else:
                                pass     #dump????
                        elif op[0]==1:
                            if op[1]==0 or op[1]==1:
                                newTM.createTransaction(op[2],op[1],time)
                            else:
                                newTM.endTransaction(op[2])
                                #if commit: newTM.commitTransaction(op[2])
                                # lock=newTM.transactionTable(op[2]).releaseLock()
                                # newLM.releaseLock(op[2])
                                # remove lock in each site
                                # update value in each site (if site is down for single copy items, abort transaction)
                                
                        else:
                            if op[1]==0:
                                pass
                            else:
                                pass

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A script to build a distributed database and process transactions", formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--verbose', dest='verbose', default=False, action='store_true', help='This will look for an incomplete copy and redo only the incomplete days')
    parser.add_argument('--f', type=str,  default=None, dest='inputFile', help='This option allows input from a file. Please enter full file directory after --f.')
    parser.add_argument('--s', dest='stdIN', default=False, action='store_true', help='This option allows input from stdin.')
    args = parser.parse_args()
    args = parser.parse_args()
    args = parser.parse_args()
    main(verbose=args.verbose,inputFile=args.inputFile,stdIN=args.stdIN)