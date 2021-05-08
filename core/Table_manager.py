# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:19:41 2019

@author: Gary
"""
import pandas as pd
import numpy as np
import gc
import os
import datetime
import core.mass_tools as mt

class Table_constructor():
    
    def __init__(self,pkldir='./tmp/',sources='./sources/'):
        self.pkldir = pkldir
        self.sources = sources
        self.tables = {'disclosures': None,
                       'records': None,
                       'cas_ing': None,
                       'bgCAS': None,
                       'companies': None}

        self.pickle_fn = {'disclosures': self.pkldir+'disclosures.pkl',
                          'records': self.pkldir+'chemrecs.pkl',
                          'cas_ing': self.pkldir+'cas_ing.pkl',
                          'bgCAS': self.pkldir+'bgCAS.pkl',
                          'companies': self.pkldir+'companies.pkl'}

        self.cas_ing_fn = sources+'casing_curate_master.csv'
        self.cas_ing_source = pd.read_csv(self.cas_ing_fn,quotechar='$',encoding='utf-8')
        
    def print_step(self,txt,indent=0,newlinefirst=False):
        if newlinefirst:
            print()
        s = ''
        if indent>0:
            s = '   '*indent
        print(f' {s}-- {txt}')
        
    def print_size(self,df,name='table'):
        rows,cols = df.shape
        self.print_step(f'{name:12}: rows: {rows:7}, cols: {cols:3}',1)
        #print(f'       rows: {rows:10}, cols: {cols:4}')



    def assemble_cas_ing_table(self):
        self.print_step('assembling CAS/IngredientName table')
        df = self.cas_ing_source[['CASNumber','IngredientName',
                                            'bgCAS','category']]
        self.tables['cas_ing'] = df

        
    def assemble_companies_table(self):
        self.print_step('assembling companies table')
        self.tables['companies'] = pd.read_csv(self.sources+'company_xlate.csv',
                                                  encoding='utf-8',quotechar='$')
        
    def assemble_bgCAS_table(self,cas_ing):
        self.print_step('assembling bgCAS table')
        ref = pd.read_csv(self.sources+'CAS_ref_and_names.csv',
                          quotechar='$',encoding='utf-8')
        ref.columns=['bgCAS','bgIngredientName']
        df = pd.DataFrame({'bgCAS':cas_ing.bgCAS.unique().tolist()})
        df = pd.merge(df,ref,on='bgCAS',how='left')
        self.tables['bgCAS'] = df


    #########   DISCLOSURE TABLE   ################
    def assemble_disclosure_table(self,raw_df):
        self.print_step('assembling disclosure table')
        df = raw_df.groupby('UploadKey',as_index=False)\
                                [['JobEndDate','JobStartDate','OperatorName',
                                  'APINumber', 'TotalBaseWaterVolume',
                                  'TotalBaseNonWaterVolume','FFVersion','TVD',
                                  'StateName','StateNumber',
                                  'CountyName','CountyNumber',
                                  'Latitude','Longitude','Projection',
                                  'WellName','FederalWell','IndianWell',
                                  'data_source']].first()
        self.print_step('create bgOperatorName',1)
        cmp = self.tables['companies'][['original','primary']]
        cmp.columns = ['opN','bgOperatorName']
        df['opN'] = df.OperatorName.str.strip().str.lower()
        df = pd.merge(df,cmp,on='opN', how='left')
        self.tables['disclosures']= df


    ##########   CHEMICAL RECORDS TABLE   #############
    def flag_duplicated_records(self,records):
        records['dup_rec'] = records.duplicated(subset=['UploadKey',
                                                    'IngredientName',
                                                    'CASNumber',
                                                    'MassIngredient',
                                                    'PercentHFJob',
                                                    'PercentHighAdditive'],
                                        keep=False)
        c0 = records.ingKeyPresent
        c1 = records.Supplier.str.lower().isin(['listed above'])
        c2 = records.Purpose.str.lower().str[:9]=='see trade'
        records['dup_rec'] = np.where(records.dup_rec&c0&c1&c2,True,False)
        self.print_step(f'Number dups: {records.dup_rec.sum()}',2)
        return records
    
        
    def assemble_chem_rec_table(self,raw_df):
        self.print_step('assembling chemical records table')
        df= raw_df[['UploadKey',
                               'CASNumber','IngredientName','PercentHFJob',
                               'Supplier','Purpose','TradeName',
                               'PercentHighAdditive','MassIngredient',
                               'ingKeyPresent']]

        self.print_step('adding bgCAS',1)
        df = pd.merge(df,self.tables['cas_ing'],
                                on=['CASNumber','IngredientName'],
                                how='left')     
        unCAS = df[df.bgCAS.isna()]\
                    .groupby(['CASNumber',
                              'IngredientName'],as_index=False).size()
        self.print_step(f'Number uncurated CAS/Ingred pairs: {len(unCAS)}, n: {unCAS["size"].sum()}',2)
        unCAS.to_csv(self.pkldir+'new_CAS_ING.csv',encoding='utf-8',
                     index=False, quotechar = '$')

        self.print_step('create bgSupplier',1)
        df['sup'] = df.Supplier.str.strip().str.lower()

        cmp = self.tables['companies'][['original','primary']]
        cmp.columns = ['sup','bgSupplier']
        self.tables['records'] = pd.merge(df,cmp,on='sup',
                                 how='left',validate='m:1')
        
        self.print_step('flagging duplicate records',1)
        self.tables['records'] = self.flag_duplicated_records(self.tables['records'])

    ############   POST ASSEMBLY PROCESSING   ############

    def flag_empty_disclosures(self):
        self.print_step('flagging disclosures without chem records')
        gb = self.tables['records'].groupby('UploadKey',as_index=False)['ingKeyPresent'].sum()
        gb['no_chem_recs'] = np.where(gb.ingKeyPresent==0,True,False)
        df = pd.merge(self.tables['disclosures'],
                      gb[['UploadKey','no_chem_recs']],
                      on='UploadKey',how='left')
        self.print_step(f'number empty disclosures: {df.no_chem_recs.sum()}',1)
        self.tables['disclosures'] = df

    def mass_calculations(self):
        rec_df, disc_df = mt.prep_datasets(rec_df=self.tables['records'],
                                           disc_df=self.tables['disclosures'])
        self.tables['records'] = rec_df
        self.tables['disclosures'] = disc_df
                                         
        
    def pickle_tables(self):
        self.print_step('pickling all tables',newlinefirst=True)
        for name in self.tables.keys():
            self.tables[name].to_pickle(self.pickle_fn[name])
        
    def show_size(self):
        for name in self.tables.keys():
            self.print_size(self.tables[name],name)
     
    def assemble_all_tables(self,df):
        self.assemble_cas_ing_table()
        self.assemble_companies_table()
        self.assemble_bgCAS_table(self.tables['cas_ing'])
        self.assemble_disclosure_table(df)
        self.assemble_chem_rec_table(df)
        self.flag_empty_disclosures()
        self.mass_calculations()
        self.pickle_tables()
        self.show_size()
        
    def fetch_df(self,df_name='bgCAS',verbose=True):
        df = pd.read_pickle(self.pickle_fn[df_name])
        if verbose:
            print(f'fetching {df_name} dataframe')
        return df
        
    def release_tables(self):
        for name in self.tables:
            self.tables[name] = None
        gc.collect()

    def get_table_creation_date(self):
        try:
            t = os.path.getmtime(self.pickle_fn['records'])
            return datetime.datetime.fromtimestamp(t)
        except:
            return False
