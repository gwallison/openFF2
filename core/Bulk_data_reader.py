
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:15:03 2019

@author: GAllison

This module is used to read all the raw data in from a FracFocus zip 
of CSV files; and the preprocessed SkyTruth archive.

"""
import zipfile
import re
import csv
import pandas as pd
import numpy as np
#import core.FF_stats as ffstats


class Read_FF():
    
    def __init__(self,zname='./sources/bulk_data/currentData.zip',
                 skytruth_name='./sources/bulk_data/sky_truth_final.zip',
                 outdir = './out/', sources = './sources/',
                 tab_const=None,
                 # start and endfile allows user to import a subset of whole
                 startfile=0, endfile=None):
        self.zname = zname
        self.stname = skytruth_name
        self.outdir = outdir
        self.sources = sources
        self.missing_values = self.getMissingList()
        self.dropList = ['ClaimantCompany', 'DTMOD', 'DisclosureKey', 
                         'IngredientComment', 'IngredientMSDS',
                         'IsWater', 'PurposeIngredientMSDS',
                         'PurposeKey', 'PurposePercentHFJob', 'Source', 
                         'SystemApproach'] # not used, speeds up processing
        self.startfile = startfile
        self.endfile = endfile
        
    def getMissingList(self):
        df = pd.read_csv(self.sources+ 'missing_values.csv')
        return df.missing_value.tolist()

    def import_raw(self):
        """
        """
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
                    
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            if self.endfile==None:
                self.endfile=len(infiles)
            #print(self.startfile,self.endfile)
            for fn in infiles[self.startfile:self.endfile]:
                with z.open(fn) as f:
                    print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False,
                                    dtype={'APINumber':'str',
                                           'FederalWell':'str',
                                           'IndianWell':'str'},
                                    na_values = self.missing_values)
                    # we need an indicator of the presence of IngredientKey
                    # whitout keeping the whole honking thing around
                    t['ingKeyPresent'] = np.where(t.IngredientKey.isna(),
                                                  False,True)
                    t['raw_filename'] = fn # helpful for manual searches of raw files
                    t['record_flags'] = 'B'  #bulk download flag (in allrec)
                    t['data_source'] = 'bulk' # for event table
                    
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        return final
        
    def import_raw_as_str(self,varsToKeep=['UploadKey','APINumber',
                                           'IngredientName','CASNumber',
                                           'StateName','StateNumber',
                                           'CountyName','CountyNumber',
                                           'FederalWell','IndianWell',
                                           'JobStartDate','JobEndDate',
                                           'Latitude','Longitude',
                                           'MassIngredient','OperatorName',
                                           'PercentHFJob','PercentHighAdditive',
                                           'Purpose','Supplier','TVD',
                                           'TotalBaseWaterVolume','TotalBaseNonWaterVolume',
                                           'TradeName','WellName',
                                           'IngredientKey']):
        """
        """
        dflist = []
        with zipfile.ZipFile(self.zname) as z:
            inf = []
            for fn in z.namelist():
                # the files in the FF archive with the Ingredient records
                #  always start with this prefix...
                if fn[:17]=='FracFocusRegistry':
                    # need to extract number of file to correctly order them
                    num = int(re.search(r'\d+',fn).group())
                    inf.append((num,fn))
                    
            inf.sort()
            infiles = [x for _,x in inf]  # now we have a well-sorted list
            dtypes = {}
            for v in varsToKeep:
                dtypes[v] = 'str'
            for fn in infiles: #[-3:]:
                with z.open(fn) as f:
                    print(f' -- processing {fn}')
                    t = pd.read_csv(f,low_memory=False,
                                    usecols=varsToKeep,
                                    dtype=dtypes,
                                    na_values=self.missing_values)

                    t['raw_filename'] = fn
                    
                    dflist.append(t)
        final = pd.concat(dflist,sort=True)
        return final

    def import_skytruth_as_str(self,varsToKeep=['UploadKey','APINumber',
                                           'IngredientName','CASNumber',
                                           'StateName','StateNumber',
                                           'CountyName','CountyNumber',
                                           'JobEndDate',
                                           'Latitude','Longitude',
                                           'OperatorName',
                                           'PercentHFJob','PercentHighAdditive',
                                           'Purpose','Supplier','TVD',
                                           'TotalBaseWaterVolume',
                                           'TradeName','WellName']):
        """
        Like import_raw_as_str, but for SkyTruth data        
        """
        dtypes = {}
        for v in varsToKeep:
            dtypes[v] = 'str'
        with zipfile.ZipFile(self.stname) as z:
            fn = z.namelist()[0]
            with z.open(fn) as f:
                print(f' -- processing {fn}')
                t = pd.read_csv(f,low_memory=False,
                                quotechar='$',quoting=csv.QUOTE_ALL,
                                usecols=varsToKeep,
                                dtype=dtypes,
                                na_values=self.missing_values)
                t['raw_filename'] = 'SkyTruth'
        return t
    
    
    def import_skytruth(self):
        """
        This function pulls in a pre-processed file with the Skytruth data.
        The pre-processing reformated the Skytruth data to match the FracFocus
        bulk download format, to allow merging.  Note, however, that we do NOT
        link the FF 'placeholder' events for FFVersion 1 to these skytruth data
        Those place holders are essentially removed from the working set
        because they have no chemical records associated.  While those
        placeholders have metadata that is important, we will rely on the Skytruth
        versions of that metatdata (which should be identical).
        
        The pdfs from which skytruth scraped only reported 10 digits in the 
        APINumber field.  However, the bulk download reports 14 digits. So for
        the output of this function, we append four X's to fill out the numbers.
        It may make sense to get that piece of metadata from the bulk download.
        
        """
        with zipfile.ZipFile(self.stname) as z:
            fn = z.namelist()[0]
            with z.open(fn) as f:
                print(f' -- processing {fn}')
                t = pd.read_csv(f,low_memory=False,
                                quotechar='$',quoting=csv.QUOTE_ALL,
                                dtype={'APINumber':'str'},
                                na_values=self.missing_values)
                t['raw_filename'] = 'SkyTruth'
                t['record_flags'] = 'Y'  #skytruth flag
                t['data_source'] = 'SkyTruth'
                cond1 = (t.APINumber.str.len()==9)|(t.APINumber.str.len()==10)
                t.record_flags = np.where(cond1,
                                          t.record_flags+'-T',
                                          t.record_flags)
                t.APINumber = np.where(t.APINumber.str.len()==13, #shortened state numbers
                                       '0'+ t.APINumber,
                                       t.APINumber)
                t.APINumber = np.where(t.APINumber.str.len()==9, #shortened state numbers
                                       '0'+ t.APINumber + 'XXXX',
                                       t.APINumber)
                t.APINumber = np.where(t.APINumber.str.len()==10,
                                       t.APINumber + 'XXXX',
                                       t.APINumber)
                t['ingKeyPresent'] = True  # all SkyTruth events have chem records
        return t

    def import_all(self,inc_skyTruth=True): 
        if inc_skyTruth:
            t = pd.concat([self.import_raw(),
                           self.import_skytruth()],
                           sort=True)
        else:
            t = self.import_raw()
        t.reset_index(drop=True,inplace=True) #  single integer as index
        t['reckey'] = t.index.astype(int)
           
        t.drop(columns=self.dropList,inplace=True)
        assert(len(t)==len(t.reckey.unique()))
        return t
    
    def import_all_str(self,varsToKeep=['UploadKey','Latitude','Longitude']):
        t = pd.concat([self.import_raw_as_str(varsToKeep),
                       self.import_skytruth_as_str(varsToKeep)],
                       sort=True)
        t.reset_index(drop=True,inplace=True) #  single integer as index
        return t
         