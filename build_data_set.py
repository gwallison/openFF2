# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the tables used to make data sets.

Change the file handles at the top of core.Data_set_constructor to point to appropriate
directories.

    
"""
#bulk_fn = 'ff_archive_2021-03-05'
bulk_fn = 'currentData'
make_output_files = False
do_abbrev = False

startfile = 0  # 0 for full set
endfile = None  # None for no upper limit
inc_skyTruth = True
#inc_skyTruth = False

if (startfile!=0) | (endfile!=None) | (inc_skyTruth==False):
    mode = 'TEST'
    print(60*'+'+'\n'+60*'-'+'\n'+60*'.')
    print('                   Performing in TEST mode!')
    print(60*'.'+'\n'+60*'-'+'\n'+60*'+')
else:
    mode = 'PRODUCTION'

import core.Data_set_constructor as set_const


t = set_const.Data_set_constructor(bulk_fn=bulk_fn, const_mode=mode,
                                   make_files=make_output_files,
                                   startfile=startfile,
                                   endfile=endfile,
                                   abbreviated=do_abbrev)\
             .create_full_set(inc_skyTruth=inc_skyTruth)

