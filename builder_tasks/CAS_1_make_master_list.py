# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:00:34 2021

@author: Gary

This creates the initial data frame of the CAS master list.  

The CASNumber and IngredientName values are unmodified. They may contain
new line characters, etc.
"""
import numpy as np
import pandas as pd
import core.Read_FF_from_bulk as rff
import core.cas_tools as ct


def initial_CAS_master_list(zname='./sources/bulk_data/currentData.zip'):
    old = pd.read_csv('./sources/CAS_master_list.csv',quotechar='$',
                            encoding='utf-8')
    old = old[['CASNumber','IngredientName']]


    df = rff.Read_FF(zname=zname).import_all_str(varsToKeep=(['CASNumber',
                                                              'IngredientName']))
    # we want to be able to see the cells that were really empty of any text, so:
#    print(f'Num NA in new CAS {df.CASNumber.isna().sum()}')
#    print(f'Num NA in new ing {df.IngredientName.isna().sum()}')
    df = df.fillna('MISSING')

# =============================================================================
#     df['CASNumber'] = np.where(df.CASNumber=='','_empty_in_raw_',df.CASNumber)
#     df['IngredientName'] = np.where(df.IngredientName=='','_empty_in_raw_',df.IngredientName)
# 
# =============================================================================
    new = df.groupby(['CASNumber','IngredientName']).size().reset_index()
    new = new[['CASNumber','IngredientName']]
    
    mg = pd.merge(new,old,on=['CASNumber','IngredientName'],
                  how = 'outer',indicator=True)
    new = mg[mg['_merge']=='left_only'].copy() # only want new stuff
    new = new[['CASNumber','IngredientName']]    
    new['clean_wo_work'] = new.CASNumber.map(lambda x: ct.is_valid_CAS_code(x))
    
    new['tent_CAS'] = new.CASNumber.map(lambda x:ct.cleanup_cas(x))
    new['valid_after_cleaning'] = new.tent_CAS.map(lambda x: ct.is_valid_CAS_code(x))

    #print(f'Num NA in new CAS {new.CASNumber.isna().sum()}')
    #print(f'Num NA in new ing {new.IngredientName.isna().sum()}')
    
    return new
