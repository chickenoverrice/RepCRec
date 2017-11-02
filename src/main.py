# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 16:37:35 2017

@author: zxu
"""
import dbsite
import argparse
import sys
import transactionManager as tm
import os
import utility

os.chdir('..')
directory=os.path.abspath(os.curdir)

def main(verbose,inputFile,stdIN):  
    if inputFile and stdIN:
        sys.exit('Please choose only one option: --f or --s')
      
    abs_file_path = os.path.join(directory, inputFile)    
    siteCondition=[True for i in range(10)]    # index of SiteCondition Table is 1 smaller than site id
    siteDict=dict()
    newTM=tm.transactionManager()
    for i in range(1,11):
        siteDict[i]=dbsite.Site(i)
        
    if inputFile != None:    
        with open(abs_file_path,'r') as f:
            for instruction in f:
                instruction=instruction.strip().lower().split(')')
                for item in instruction:
                    if item:
                        op=utility.parseCommand(item)
                        tm.processOperation(op)

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A script to build a distributed database and process transactions", formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--verbose', dest='verbose', default=False, action='store_true', help='This will look for an incomplete copy and redo only the incomplete days')
    parser.add_argument('--f', type=str,  default=None, dest='inputFile', help='This option allows input from a file. Please enter full file directory after --f.')
    parser.add_argument('--s', dest='stdIN', default=False, action='store_true', help='This option allows input from stdin.')
    args = parser.parse_args()
    args = parser.parse_args()
    args = parser.parse_args()
    main(verbose=args.verbose,inputFile=args.inputFile,stdIN=args.stdIN)