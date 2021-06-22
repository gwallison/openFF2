# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:00:34 2021

@author: Gary

"""
#import numpy as np
import gc
import builder_tasks.CAS_1_make_master_list as CAS1
import builder_tasks.CAS_2_incorporate_reference as CAS2
import builder_tasks.CompanyNames_make_list as complist
import builder_tasks.Location_cleanup as loc_clean
import core.Bulk_data_reader as rff

zname='./sources/bulk_data/currentData.zip'

### use these for debugging; set to full set for main operation
startfile = 0  # 0 for full set
endfile = None  # None for no upper limit
inc_skyTruth = True
#inc_skyTruth = False




master_raw = rff.Read_FF(zname=zname,
                    startfile=startfile,endfile=endfile)\
                        .import_all()
# =============================================================================
#                         .import_raw_as_str(varsToKeep=\
#                                     (['CASNumber','IngredientName',
#                                       'OperatorName','Supplier',
#                                       'Latitude','Longitude','UploadKey',
#                                       'StateName','StateNumber',
#                                       'CountyName','CountyNumber','Projection']))
# =============================================================================
rawdf = master_raw[['CASNumber','IngredientName',
                    'OperatorName','Supplier',]].fillna('MISSING')

master = CAS1.initial_CAS_master_list(rawdf)
df, casing_to_curate = CAS2.merge_with_ref(master)
df.to_csv('./tmp/casing_curate_masterNEW.csv',index=False,encoding='utf-8',quotechar='$')

c_xlate_to_curate = complist.add_new_to_Xlate(rawdf)

loc_clean.clean_location(master_raw)

s = f"""\n\n
CAS/Ingredient, Company and Location pre-processing has been completed.

Number of new CAS/Ingredient pairs to curate: {casing_to_curate}
Number of new company names to curate:        {c_xlate_to_curate}

NEXT STEPS:
     1) The CAS master list has now been updated with new disclosures and
        is ready for the CURATION step.  Find it as /tmp/casing_curate_masterNEW.csv.

        If there are new CAS numbers that are valid but unknown, run them through
        the SciFinder process before proceeding to the curating process.

    2)  The company_xlate file has be updated with new names and is ready for
        the CURATION step. Find it as /tmp/company_xlateNEW.csv IF THERE ARE
        ANY TO CURATE.
        
When finished curating, move to /sources (be sure to save old one as backup)
and move on to the next step.  (After curating and moving, it is a good idea
to run this again to verify that you caught all that needed to be curated.)
"""

print(s)
rawdf = None
df = None
gc.collect()