# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 16:19:39 2017

@author: zhe&yuzheng
"""
import re
        
def parseCommand(item):
    '''
    This method converts an input string to an operation.
    Param: str
    Return: tuple
    
    Output code:
    mode 0: operation on site
    mode 1: operation on transaction
    mode 2: operation on single record
    site_mode 0: fail
    site_mode 1: recover
    site_mode 2: dump
    transaction_mode 0: RO
    transaction_mode 1: RW/W
    transaction_mode 2: end
    record_mode 0: R
    record_mode 1: W  
    dump_mode 0: dump all
    dump_mode 1: dump one site
    dump_mode 2: dump one value
    '''
    if item[0]=='/':                        #comment
        return None
    item = re.sub('[^,begindumpfailrecoverwxt0-9(\.]', '', item)
    if item==None or item=='':
        return None
    if item[0]=='b' or item[0]=='e':        #begin or end
        mode=1
        start=item.find('t')
        transaction_id=int(item[(start+1):])
        if 'ro' in item:                    #RO
            transaction_mode=0
        elif item[0]=='e':                  #end  
            transaction_mode=2
        else:
            transaction_mode=1
        return (mode,transaction_mode,transaction_id)        
    elif item[0]=='w':                      #write
        mode=2
        record_mode=1
        item=item.split(',')
        start=item[0].find('t')
        transaction_id=int(item[0][start+1:])
        target=item[1]
        value=int(item[2])
        return (mode,record_mode, transaction_id,target,value)
    elif item[0]=='f':                      #fail
        mode=0
        site_mode=0
        target=int(item[5:])
        return (mode,site_mode,target)
    elif item[0]=='d':                      #dump
        mode=0
        site_mode=2
        if len(item)<6:                    #dump()
            dump_mode=0
            target=None
        elif item[5]=='x':                  #dump(xi)
            dump_mode=2
            target=item[5:]
        else:
            dump_mode=1                     #dump(i)
            target=int(item[5:])
        return (mode,site_mode,dump_mode, target)
    elif item[0]=='r': 
        if item[1]=='e':                    #recover
            mode=0
            site_mode=1
            target=int(item[8:])
            return (mode, site_mode,target)
        else:                               #read
            mode=2
            record_mode=0
            item=item.split(',')
            start=item[0].find('t')
            transaction_id=int(item[0][start+1:])
            target=item[1]
            return (mode,record_mode, transaction_id,target)
    else:
        return None

        