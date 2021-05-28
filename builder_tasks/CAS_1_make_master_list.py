# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:00:34 2021

@author: Gary

This creates the initial data frame of the CAS master list.  

The CASNumber and IngredientName values are unmodified. They may contain
new line characters, etc.
"""
#import numpy as np
import pandas as pd
import core.cas_tools as ct


        
def initial_CAS_master_list(rawdf): # rawdf
    old = pd.read_csv('./sources/casing_curate_master.csv',quotechar='$',
                            encoding='utf-8')
    old = old[['CASNumber','IngredientName']]
    ct.na_check(old,txt='CAS_1 for old')
    ###############
    #old.IngredientName = old.IngredientName.str.strip().str.lower()
    new = rawdf.groupby(['CASNumber','IngredientName'],as_index=False).size()
# =============================================================================
#     xlate, new = ct.make_clean_casing(rawdf[['CASNumber','IngredientName']].copy())
#     xlate = xlate.sort_values(['CASNumber','IngredientName']).reset_index(drop=True)
#     xlate['iCAS_ing'] = xlate.index
#     # make new CAS_ING translate file
#     xlate.to_csv('./sources/CAS_ING_xlateNEW.csv',quotechar='$',encoding='utf-8',
#                  index=False)
# 
# =============================================================================
    print('checking for non printables in CASNumber...')
    new.CASNumber.map(lambda x: ct.has_non_printable(x))
    print('checking for non printables in IngredientName...')
    new.IngredientName.map(lambda x: ct.has_non_printable(x))


    mg = pd.merge(new,old,on=['CASNumber','IngredientName'],
                  how = 'outer',indicator=True)
    ct.na_check(old,txt='CAS_1 for mg')
    new = mg[mg['_merge']=='left_only'].copy() # only want new stuff
    new = new[['CASNumber','IngredientName']]    
    new['clean_wo_work'] = new.CASNumber.map(lambda x: ct.is_valid_CAS_code(x))
    
    new['tent_CAS'] = new.CASNumber.map(lambda x:ct.cleanup_cas(x))
    new['valid_after_cleaning'] = new.tent_CAS.map(lambda x: ct.is_valid_CAS_code(x))
    ct.na_check(old,txt='CAS_1 for new')
    
    
    return new
