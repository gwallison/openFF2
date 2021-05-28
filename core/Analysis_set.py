# -*- coding: utf-8 -*-
"""
Created on Wed May  5 14:22:20 2021

@author: Gary
"""

import core.Table_manager as c_tab
import pandas as pd
import os
import datetime
import core.cas_tools as ct

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)

def banner(text):
    print()
    print('*'*80)
    space = ' '*int((80 - len(text))/2)
    print(space,text,space)
    print('*'*80)


class Template_data_set():
    
    def __init__(self,bulk_fn='currentData',
                 sources='./sources/',
                 outdir='./out/',
                 set_name = 'template',
                 pkl_when_creating=True,
                 force_new_creation=False):
        self.set_name= set_name
        self.bulk_fn = bulk_fn
        self.sources = sources
        self.outdir = outdir
        self.df = None
        self.pkldir = self.outdir+self.bulk_fn+'_pickles/'
        self.pkl_fn = self.pkldir+self.set_name+'_df.pkl'
        self.pkl_when_creating = pkl_when_creating
        self.force_new_creation = force_new_creation
        
        self.t_man = c_tab.Table_constructor(pkldir=self.pkldir)
        self.table_date = self.t_man.get_table_creation_date()
        if self.table_date==False:
            banner(f"Pickles for data tables don't exist for {self.bulk_fn}.")
            banner('Run "build_data_set.py" first')
            exit()

    def prep_for_creation(self):        
        # workTables: point to set of tables to manipulate for current data set
        self.wT = self.t_man.tables
        ct.na_check(self.wT['records'],txt=' prep_for_creation: records')
        ct.na_check(self.wT['disclosures'],txt=' prep_for_creation: disclosures')
        
        #print('in self.wT:')
        #print(self.wT['records'].columns)
        
        self.wC = {} # this is the set of working columns
        for t in self.wT.keys():
            self.wC[t] = set()
            
    def show_source_columns(self):
        self.t_man.load_pickled_tables()
        self.prep_for_creation()
        print(self.wT.keys())
        for t in self.wT.keys():
            print(f'{t}:\n{self.wT[t].columns}')

    def pickle_is_valid(self):
        try:
            df_date = modification_date(self.pkl_fn)
            print(f'Pickle created: {df_date}')
            return True
        except:
            return False
    
    def add_fields_to_keep(self,to_add = {}):
        # ex. {'records' : ['eh_blah_blah'], 'disclosures': ['WellName']}
        for tname in to_add.keys():
            for fn in to_add[tname]:
                self.wC[tname].add(fn)


    def keep_all_fields(self):
        for t in self.t_man.tables.keys():
            for fn in list(self.t_man.tables[t].columns):
                self.wC[t].add(fn) 
                
    def keep_mininal_fields(self):
        self.wC['disclosures'] = set(['UploadKey','date','APINumber'])
        self.wC['records'] = set(['UploadKey','bgCAS','calcMass','PercentHFJob'])
        
    def keep_basic_fields(self):
        self.wC['disclosures'] = set(['UploadKey','date','APINumber',
                                     'bgStateName','bgCountyName',
                                     'bgLatitude','bgLongitude',
                                     'TotalBaseWaterVolume','TotalBaseNonWaterVolume',
                                     'TVD','bgOperatorName','primarySupplier']
                                     )
        self.wC['records'] = set(['UploadKey',
                                 'bgCAS','calcMass','category','PercentHFJob',
                                 'Purpose','TradeName','bgSupplier'])
        self.wC['bgCAS'] = set(['bgCAS','bgIngredientName'])
        
    def keep_basic_fields_with_original(self):
        self.keep_basic_fields()
        self.add_fields_to_keep({'disclosures':['StateName','CountyName',
                                                'Latitude','Longitude',
                                                'OperatorName','WellName'],
                                 'records':['CASNumber','IngredientName',
                                            'Supplier']})
        
    def std_filter(self):
        t = self.wT['disclosures']
        print(len(t))
        cond = (t.is_duplicate)|(t.no_chem_recs)
        t = t[~cond].copy()
        print(len(t))
        self.wT['disclosures'] = t
        
        t = self.wT['records']
        print(len(t))
        t = t[~(t.dup_rec)].copy()
        self.wT['records'] = t
        print(len(t))

                
    def pickle_set(self):
        self.df.to_pickle(self.pkl_fn)

    def get_set(self,verbose=True):
        if (self.pickle_is_valid())&~(self.force_new_creation):
            print('Using saved pickle of analysis set...')
            t = pd.read_pickle(self.pkl_fn)
        else:
            print('Creating data set from scratch...')
            self.t_man.load_pickled_tables()
            self.prep_for_creation()
            self.create_set()
            t = self.df
        if verbose:
            print(f'Dataframe ***"{self.set_name}"***\n')
            print(t.info())
        return t
    

    def merge_tables(self):
        if self.location_only:
            self.df = self.wT['disclosures'][self.wC['disclosures']].copy()
        else:
            self.df = pd.merge(self.wT['disclosures'][self.wC['disclosures']],
                               self.wT['records'][self.wC['records']],
                               on='UploadKey',
                               how='inner',validate='1:m')
            if len(self.wC['bgCAS'])>0:
                self.df = pd.merge(self.df,
                                   self.wT['bgCAS'][self.wC['bgCAS']],
                                   on='bgCAS',
                                   how='left',validate='m:1')        

    def create_set(self):
        self.location_only = False
        self.keep_basic_fields_with_original()
        #self.keep_mininal_fields()
        #self.keep_all_fields()
        #self.std_filter()
        #print(self.wC)
        self.merge_tables()
        if self.pkl_when_creating:
            self.pickle_set()
        
        

class Std_filtered(Template_data_set):
    def __init__(self,bulk_fn='currentData',
                 sources='./sources/',
                 outdir='./out/',
                 pkl_when_creating = True,
                 set_name='std_filtered'):
        Template_data_set.__init__(self,bulk_fn=bulk_fn,
                           sources=sources,
                           outdir=outdir,
                           pkl_when_creating=pkl_when_creating,
                           set_name=set_name)
    
    def create_set(self):
        self.std_filter()
        self.keep_basic_fields_with_original()

        if self.pkl_when_creating:
            self.pickle_set()
