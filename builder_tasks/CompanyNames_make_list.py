# -*- coding: utf-8 -*-
"""
Created on Thu May 20 12:59:40 2021

@author: Gary
"""

import numpy as np
import pandas as pd
import core.cas_tools as ct

def add_new_to_Xlate(rawdf):
    old = pd.read_csv('./sources/company_xlate.csv',quotechar='$')
    old['cleanName'] = old.rawName.str.lower()
    #old.cleanName = old.cleanName.map(lambda x: ct.remove_non_printable(x))
    oldlst = old.rawName.unique().tolist()
    
    newset = set()
    nlst = rawdf.OperatorName.unique().tolist()
    for n in nlst:
        newset.add(n)
    nlst = rawdf.Supplier.unique().tolist()
    for n in nlst:
        newset.add(n)
    newraw = []
    for i in newset:
        if not i in oldlst:
            newraw.append(i)
    print(f'\nNumber of new company lines to curate: {len(newraw)}')
    if len(newraw)>0:
        newdf = pd.DataFrame({'rawName':newraw})
        newdf['cleanName'] = newdf.rawName.str.lower()
        out = pd.concat([newdf,old],sort=True)
        out = out[['rawName','cleanName','xlateName','status']]
        out.to_csv('./tmp/company_xlateNEW.csv',quotechar='$',encoding='utf-8',index=False)

    return len(newraw)