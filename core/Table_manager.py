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
import core.cas_tools as ct

class Table_constructor():
    
    def __init__(self,pkldir='./tmp/',sources='./sources/'):
        self.pkldir = pkldir
        self.sources = sources
        self.tables = {'disclosures': None,
                       'records': None,
                       'cas_ing': None,
                       'bgCAS': None,
                       'companies': None # all company data is also in records/disc
                       }

        self.pickle_fn = {'disclosures': self.pkldir+'disclosures.pkl',
                          'records': self.pkldir+'chemrecs.pkl',
                          'cas_ing': self.pkldir+'cas_ing.pkl',
                          #'cas_ing_xlate': self.pkldir+'cas_ing_xlate.pkl',
                          'bgCAS': self.pkldir+'bgCAS.pkl',
                          'companies': self.pkldir+'companies.pkl'
                          }

        self.cas_ing_fn = sources+'casing_curate_master.csv'
        self.cas_ing_source = pd.read_csv(self.cas_ing_fn,quotechar='$',encoding='utf-8')
        self.location_ref_fn = sources+'uploadKey_ref.csv'
        self.loc_ref_df = pd.read_csv(self.location_ref_fn,quotechar='$',
                                      encoding='utf-8')
        #self.casing_xlate_fn = sources+'CAS_ING_xlate.csv'
        #self.casing_xlate = pd.read_csv(self.casing_xlate_fn,quotechar='$',encoding='utf-8')
        #print(f'Non-bgCAS in casing: {self.cas_ing_source[self.cas_ing_source.bgCAS.isna()]}')

    def print_step(self,txt,indent=0,newlinefirst=False):
        if newlinefirst:
            print()
        s = ''
        if indent>0:
            s = '   '*indent
        print(f' {s}-- {txt}')
        
    def print_size(self,df,name='table'):
        rows,cols = df.shape
        self.print_step(f'{name:15}: rows: {rows:7}, cols: {cols:3}',1)


    def assemble_cas_ing_table(self):
        self.print_step('assembling CAS/IngredientName table')
        df = self.cas_ing_source[['CASNumber','IngredientName',
                                  'bgCAS','category']]
        self.tables['cas_ing'] = df
        ct.na_check(df,txt='assembling cas_ing table')

        
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
                                  'StateNumber','CountyNumber',
                                  #'Latitude','Longitude',
                                  'Projection',
                                  'WellName','FederalWell','IndianWell',
                                  'data_source']].first()
        df['date'] = pd.to_datetime(df.JobEndDate,errors='coerce')
        
        self.print_step('create bgOperatorName',1)
        cmp = self.tables['companies'][['rawName','xlateName']]
        cmp.columns = ['OperatorName','bgOperatorName']
        #df['opN'] = df.OperatorName.str.strip().str.lower()
        df = pd.merge(df,cmp,on='OperatorName', how='left')

        unOp = df[df.bgOperatorName.isna()]
        if len(unOp)>0: flag = '<******'
        else: flag= ''
        self.print_step(f'Number uncurated Operators: {len(unOp)} {flag}',2)

        df = pd.merge(df,self.loc_ref_df,on='UploadKey',how='left',
                      validate='1:1')

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
        df= raw_df[['UploadKey','CASNumber','IngredientName','PercentHFJob',
                    'Supplier','Purpose','TradeName',
                    'PercentHighAdditive','MassIngredient',
                    'ingKeyPresent','reckey',
                    'density_from_comment']].copy()
        ct.na_check(df,txt='assembling chem_rec 1')
        
        df.Supplier = df.Supplier.fillna('MISSING')

        self.print_step('adding bgCAS',1)
        df = pd.merge(df,self.tables['cas_ing'],
                                on=['CASNumber','IngredientName'],
                                how='left')     
        ct.na_check(df,txt='bgCAS add')
        unCAS = df[df.bgCAS.isna()]\
                    .groupby(['CASNumber','IngredientName'],as_index=False).size()
        if unCAS["size"].sum() > 0:
            s = ' <<******'
        else:
            s = ''
        self.print_step(f'Number uncurated CAS/Ingred pairs: {len(unCAS)}, n: {unCAS["size"].sum()}{s}',2)
        unCAS.to_csv(self.pkldir+'new_CAS_ING.csv',encoding='utf-8',
                     index=False, quotechar = '$')

        self.print_step('create bgSupplier',1)

        cmp = self.tables['companies'][['rawName','xlateName']]
        cmp.columns = ['Supplier','bgSupplier']
        #print(f'COMPANY table: {len(cmp)}')
        df = pd.merge(df,cmp,on='Supplier',
                                 how='left',validate='m:1')
        ct.na_check(df,txt='bgSupplier add')
        
        unSup = df[df.bgSupplier.isna()]
        if len(unSup)>0: flag = '<******'
        else: flag= ''
        self.print_step(f'Number uncurated Suppliers: {len(unSup)} {flag}',2)

        self.print_step('flagging duplicate records',1)
        self.tables['records'] = self.flag_duplicated_records(df)
        ct.na_check(df,txt='assembling chem_rec end')

    ############   POST ASSEMBLY PROCESSING   ############

    def flag_empty_disclosures(self):
        self.print_step('flagging disclosures without chem records')
        gb = self.tables['records'].groupby('UploadKey',as_index=False)['ingKeyPresent'].sum()
        gb['no_chem_recs'] = np.where(gb.ingKeyPresent==0,True,False)
        df = pd.merge(self.tables['disclosures'],
                      gb[['UploadKey','no_chem_recs']],
                      on='UploadKey',how='left')
        self.print_step(f'number empty disclosures: {df.no_chem_recs.sum()} of {len(df)}',1)
        self.tables['disclosures'] = df
        
    def flag_duplicate_disclosures(self):
        self.print_step('flag duplicate disclosures')
        df = self.tables['disclosures'].copy()
        df['api10'] = df.APINumber.str[:10]
        df['dup_disclosures'] = df[~df.no_chem_recs]\
                                  .duplicated(subset=['APINumber',
                                                      'date'],
                                              keep=False)
        df.dup_disclosures = np.where(df.no_chem_recs,False,
                                      df.dup_disclosures)
        bulk_api10 = df[(df.data_source=='bulk')&~(df.no_chem_recs)]\
                       .api10.unique().tolist()


        df['redund_skytruth'] = (df.api10.isin(bulk_api10))&\
                                (df.data_source=='SkyTruth') 
                                
        cond = df.data_source=='SkyTruth'
        df['duplicate_skytruth'] = df[cond].duplicated(subset=['api10','date'],
                                                       keep=False)
        df.duplicate_skytruth = np.where(df.data_source=='bulk',
                                         False,df.duplicate_skytruth)
        
        df['is_duplicate'] = df.dup_disclosures | df.redund_skytruth | df.duplicate_skytruth
        
        upk = df[df.is_duplicate].UploadKey.unique().tolist()

        self.tables['disclosures']['is_duplicate'] = self.tables['disclosures'].UploadKey.isin(upk)
        self.print_step(f'n duplicate disclosures within v2 and v3: {df.dup_disclosures.sum()}',1)
        self.print_step(f'n redundant SkyTruth disclosures: {df.redund_skytruth.sum()}',1)
        self.print_step(f'n duplicate SkyTruth disclosures: {df.duplicate_skytruth.sum()}',1)
        self.print_step(f'n is_duplicate: {df.is_duplicate.sum()}',1)
        
    def mass_calculations(self):
        rec_df, disc_df = mt.prep_datasets(rec_df=self.tables['records'],
                                           disc_df=self.tables['disclosures'])
        self.tables['records'] = rec_df
        self.tables['disclosures'] = disc_df
        #self.print_step(f'after mass {len(disc_df)}')                

    def gen_primarySupplier(self): 
        non_company = ['third party','operator','ambiguous',
                       'company supplied','customer','multiple suppliers',
                       'not a company','missing']
        rec = self.tables['records'].copy()
        rec = rec[~(rec.bgSupplier.isin(non_company))]
        gb = rec.groupby('UploadKey')['bgSupplier'].agg(lambda x: x.value_counts().index[0])
        gb = gb.reset_index()
        gb.rename({'bgSupplier':'primarySupplier'},axis=1,inplace=True)
        self.tables['disclosures'] = pd.merge(self.tables['disclosures'],
                                              gb,on='UploadKey',how='left',
                                              validate='1:1')
        
    def pickle_tables(self):
        self.print_step('pickling all tables',newlinefirst=True)
        for name in self.tables.keys():
            self.tables[name].to_pickle(self.pickle_fn[name])
            
    def load_pickled_tables(self):
        for t in self.tables.keys():
            self.tables[t] = self.fetch_df(df_name=t)
        
    def show_size(self):
        for name in self.tables.keys():
            self.print_size(self.tables[name],name)
     
    def assemble_all_tables(self,df):
        ct.na_check(df,txt='top of assemble all tables')
        self.assemble_cas_ing_table()
        self.assemble_companies_table()
        self.assemble_bgCAS_table(self.tables['cas_ing'])
        self.assemble_disclosure_table(df)
        self.assemble_chem_rec_table(df)
        self.flag_empty_disclosures()
        self.flag_duplicate_disclosures()
        self.gen_primarySupplier()
        self.mass_calculations()
        self.pickle_tables()
        self.show_size()
        
    def fetch_df(self,df_name='bgCAS',verbose=True):
        df = pd.read_pickle(self.pickle_fn[df_name])
        if verbose:
            print(f'  -- fetching {df_name} df')
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
