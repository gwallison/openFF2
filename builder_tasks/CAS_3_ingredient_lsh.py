# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 11:37:22 2021

@author: Gary
"""

import pandas as pd
#import numpy as np

import core.process_CAS_ref_files as pCAS
import core.lsh_tools as lt

#df = pd.read_csv('./tmp/CAS_master_list_CORRECTED_to_annotate.csv',quotechar='$',encoding='utf-8')
df = pd.read_csv('./tmp/casing_with_lsh_NEW.csv',quotechar='$',encoding='utf-8')
df['ig_clean'] = df.IngredientName.str.strip().str.lower()

cdic = pCAS.get_CAS_syn_dict(remove_paren=True)
ref_lsh = pCAS.get_syn_lsh_obj(fn='lsh_synonym_no_final_paren.pkl')

raw_lsh = lt.LSH(rawlist=list(df.ig_clean.unique()))

    

def make_df_row(row):
    ig = row.IngredientName.strip().lower()
    try:
        lshdic = ref_lsh.get_bucket_dic_with_max_threshold(raw_lsh.get_minhash(ig))
    except:
        return {'CASNumber':row.CASNumber,
               'IngredientName':row.IngredientName,
               #'bgCAS':row.bgCAS,
               #'category':row.category,
               'threshold': 'unable to lsh',
               'match_set': 'unable to lsh',
               'match_result': 'unable to lsh'}        
    maxthresh = 0
    for k in lshdic.keys():
        if lshdic[k]> maxthresh:
            maxthresh = lshdic[k]
    casset = set()
    for k in lshdic.keys():
        if lshdic[k]==maxthresh:
            for i in cdic[k]:
                casset.add(i)

    if row.bgCAS[0] in '0123456789':
        if row.bgCAS in casset:
            if len(casset)== 1:
                matches = 'VERIFIED'
            else:
                matches = 'AMBIGUOUS'
        else:
            if len(casset)>0:
                matches = 'CONFLICT'
            else:
                matches = 'NO MATCHES'
    else:
        matches = ' -- '

    if len(casset)> 4:
        casset = f'Num matches = {len(casset)}'
    else:
        if len(casset)==0:
            casset = 'No synonym match'
    out = {'CASNumber':row.CASNumber,
           'IngredientName':row.IngredientName,
           #'bgCAS':row.bgCAS,
           #'category':row.category,
           'threshold': maxthresh,
           'match_set':casset,
           'match_result':matches}
    return out
           
def add_ing_lsh():
    
    lst = []
    df.bgCAS = df.bgCAS.fillna('empty')
    
    for i,row in df.iterrows():
        #if i > 20:
        #    break
        lst.append(make_df_row(row))
    
    mg = pd.merge(df[['CASNumber','IngredientName','bgCAS','category',
                      'close_syn','comment'
                      ]],
                      #'is_new']],
                  pd.DataFrame(lst),
                  on=['CASNumber','IngredientName'],how='left')
#    mg.to_csv('./tmp/casing_with_lsh_NEW.csv',encoding='utf-8',
    mg.to_csv('./tmp/casing_with_lsh_TEST.csv',encoding='utf-8',
              quotechar='$',index=False)
    
def test_paren():
    return cdic