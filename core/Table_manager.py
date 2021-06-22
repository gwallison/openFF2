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
import core.external_dataset_tools as et

class Table_constructor():
    
    def __init__(self,pkldir='./tmp/',sources='./sources/',
                 outdir = './out/'):
        self.pkldir = pkldir
        self.sources = sources
        self.outdir = outdir
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
        self.carrier_cur_fn = sources+'carrier_curate.csv'
        self.cas_ing_source = pd.read_csv(self.cas_ing_fn,quotechar='$',encoding='utf-8')
        self.location_ref_fn = sources+'uploadKey_ref.csv'
        self.loc_ref_df = pd.read_csv(self.location_ref_fn,quotechar='$',
                                      encoding='utf-8')
        dates = pd.read_csv(self.sources+'upload_dates.csv')

        self.loc_ref_df = pd.merge(self.loc_ref_df,dates[['UploadKey','date_added']],
                           on='UploadKey',how='left',validate='1:1')
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
        
        self.print_step('add external references such as TEDX and SDWA',1)
        ext_sources_dir = self.sources+'external_refs/'
        df = et.add_all_bgCAS_tables(df,sources=ext_sources_dir,
                                     outdir=self.outdir)
        
        self.tables['bgCAS'] = df


    #########   DISCLOSURE TABLE   ################
    def make_date_fields(self,df):
        self.print_step('constructing dates',1)
        # drop the time portion of the datatime
        df['d1'] = df.JobEndDate.str.split().str[0]
        # fix some obvious typos that cause hidden problems
        df['d2'] = df.d1.str.replace('3012','2012')
        df['d2'] = df.d2.str.replace('2103','2013')
        # instead of translating ALL records, just do uniques records ...
        tab = pd.DataFrame({'d2':list(df.d2.unique())})
        tab['date'] = pd.to_datetime(tab.d2)
        # ... then merge back to whole data set
        df = pd.merge(df,tab,on='d2',how='left',validate='m:1')
        df = df.drop(['d1','d2'],axis=1)
        
        #convert date_added field
        df.date_added = pd.to_datetime(df.date_added)
        df['pub_delay_days'] = (df.date_added-df.date).dt.days
        # Any date_added earlier than 10/2018 is unknown
        refdate = datetime.datetime(2018,10,1) # date that I started keeping track
        df.pub_delay_days = np.where(df.date_added<refdate,
                                     np.NaN,
                                     df.pub_delay_days)# is less recent than refdate
        # any fracking date earlier than 4/1/2011 is before FF started taking data
        refdate = datetime.datetime(2011,4,1) # date that fracfocus started
        df.pub_delay_days = np.where(df.date<refdate,
                                     np.NaN,
                                     df.pub_delay_days)# is less recent than refdate
        return df


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
        #df['date'] = pd.to_datetime(df.JobEndDate,errors='coerce')
        
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
        df = self.make_date_fields(df)

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
        
    def get_cur_carrier(self,upk,curDic):
        try:
            return curDic[upk]
        except:
            return np.NaN
            
    def get_cur_category(self,upk,curDic,oldcat):
        if upk in curDic.keys():
            return 'curated_carrier'
        return oldcat
    
    def update_carrier_flag(self,upk,curDic,oldflag):
        if upk in curDic.keys():
            return True
        return oldflag
    

    def process_curated_carriers(self):
        self.print_step('incorporate curated carrier data')
        # now incorporate carrier_curation file
        cur_carrier_df = pd.read_csv(self.carrier_cur_fn,encoding='utf-8',
                                     quotechar = '$')
        curDic = {}
        #pjob = {}
        upk = cur_carrier_df.UploadKey.tolist()
        cc = cur_carrier_df.curated_carrier.tolist()
        #phfj = cur_carrier_df.PercentHFJob.tolist()
        for i,k in enumerate(upk):
            if k in curDic.keys():
                self.print_step(f'DUPLICATE UploadKey in cur_carrier_df {k}',1)
            curDic[k] = cc[i]

        df = pd.merge(self.tables['records'],
                      self.tables['disclosures'][['UploadKey','has_water_carrier',
                                                  'TotalBaseWaterVolume',
                                                  'TotalBaseNonWaterVolume',
                                                  'within_total_tolerance']],
                      on='UploadKey',how='left',validate='m:1')
        c0 = df.UploadKey.isin(upk)
        c1 = ~df.has_water_carrier
#        c2 = df.within_total_tolerance
        c3 = df.PercentHFJob>=50
        c4 = ~(df.bgCAS.str[0].isin(['0','1','2','3','4','5','6','7','8','9']))
        c5 = df.TotalBaseWaterVolume>0
        cond = c0&c1&c3&c4&c5
        df['curated_carrier_rec']= np.where(cond,
                                       df.UploadKey.map(lambda x: self.get_cur_carrier(x,curDic)),
                                       np.NaN)


        df.is_water_carrier= np.where(cond,
                                      df.UploadKey.map(lambda x: self.update_carrier_flag(x,
                                                                                          curDic,
                                                                                          df.is_water_carrier)),
                                      df.is_water_carrier)
# =============================================================================
#         df.is_water_carrier = np.where(df.bgCAS.isin(['non_water_carrier',
#                                                       'ambiguous_carrier',
#                                                       'proprietary_carrier']),
#                                        False,
#                                        df.is_water_carrier)
# =============================================================================
        # update disclosures tables too
        #disc = self.tables['disclosures']
        cd0 = self.tables['disclosures'].UploadKey.isin(curDic.keys())
        self.tables['disclosures']['has_curated_carrier'] = np.where(cd0,True,False)
        wb = df[df.curated_carrier_rec=='water-based'].UploadKey.unique().tolist()
        cd1 = self.tables['disclosures'].UploadKey.isin(wb)
        #print(f'Len wb: {len(wb)},  len cd1: {cd1.sum()}')
        self.tables['disclosures'].has_water_carrier = np.where(cd1,True,
                                                                 self.tables['disclosures'].has_water_carrier)
        self.print_step(f'n curated carriers: {len(df[df.curated_carrier_rec.notna()])}',1)

        # re-run conditions on updated df to find uncurated        
        c1 = ~df.has_water_carrier
        #c2 = df.within_total_tolerance
        c3 = df.PercentHFJob>=50
        c4 = ~(df.bgCAS.str[0].isin(['0','1','2','3','4','5','6','7','8','9']))
        c5 = df.TotalBaseWaterVolume>0
        c6 = df.curated_carrier_rec.isna()
        unc = df[c1&c3&c4&c5&c6][['UploadKey','CASNumber','IngredientName','Purpose',
                                  'PercentHFJob','bgCAS',
                                  'TotalBaseWaterVolume',
                                  'TotalBaseNonWaterVolume']].copy()
        unc.to_csv('./tmp/car_curateNEW.csv',quotechar='$',encoding='utf-8',
                   index=False)
        if len(unc)>0:
            self.print_step(f'*** There are still {len(unc)} CARRIERS to CURATE!!! ***',1)
        self.tables['records'] = df.drop(['has_water_carrier',
                                          'TotalBaseWaterVolume',
                                          'TotalBaseNonWaterVolume',
                                          'within_total_tolerance'],axis=1)

    def make_whole_dataset_flags(self):
        rec_df, disc_df = mt.prep_datasets(rec_df=self.tables['records'],
                                           disc_df=self.tables['disclosures'])
        self.tables['records'] = rec_df
        self.tables['disclosures'] = disc_df

    def recalc_percentages(self):
        disc_df = mt.calc_overall_percentages(rec_df=self.tables['records'],
                                                   disc_df=self.tables['disclosures'])
        self.tables['disclosures'] = disc_df

    def mass_calculations(self):
        rec_df, disc_df = mt.calc_mass(rec_df=self.tables['records'],
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
#        tdict = {}
#        for t in self.tables.keys():
#            for col in list(self.tables[t].columns)


            
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
        self.make_whole_dataset_flags()
        self.process_curated_carriers()
        self.recalc_percentages()
        self.mass_calculations()
        self.pickle_tables()
        self.show_size()
        
    def fetch_df(self,df_name='bgCAS',verbose=False):
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
