# -*- coding: utf-8 -*-
"""
Created on Wed May  5 14:22:20 2021

@author: Gary
"""

import core.Table_manager as c_tab
import pandas as pd
import os
import datetime

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)

def banner(text):
    print()
    print('*'*80)
    space = ' '*int((80 - len(text))/2)
    print(space,text,space)
    print('*'*80)


class Basic_set():
    
    def __init__(self,bulk_fn='currentData',
                 sources='./sources/',
                 outdir='./out/'):
        self.set_name='basic'
        self.bulk_fn = bulk_fn
        self.sources = sources
        self.outdir = outdir
        self.df = None
        self.pkldir = self.outdir+self.bulk_fn+'_pickles/'
        self.pkl_fn = self.pkldir+self.set_name+'_df.pkl'
        
        self.t_man = c_tab.Table_constructor(pkldir=self.pkldir)
        self.table_date = self.t_man.get_table_creation_date()
        if self.table_date==False:
            banner(f"Pickles for data tables don't exist for {self.bulk_fn}.")
            banner('Run "build_data_set.py" first')
            exit()
            
    def pickle_is_valid(self):
        try:
            df_date = modification_date(self.pkl_fn)
            print(df_date)
            return True
        except:
            return False
        
    def create_set(self):
        disc_df = self.t_man.fetch_df('disclosures')
        rec_df = self.t_man.fetch_df('records')
        self.df = pd.merge(rec_df,disc_df,on='UploadKey',how='left')
        
        