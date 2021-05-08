# -*- coding: utf-8 -*-
"""
Created on Thu May  6 21:26:21 2021

@author: Gary
"""
import pandas as pd

large_perc_value = 50
lower_perc_tolerance = 95
upper_perc_tolerance = 105

def calc_overall_percentages(rec_df,disc_df):
    """across the whole disclosure"""
    gb = rec_df[~rec_df.dup_rec].groupby('UploadKey',as_index=False)['PercentHFJob'].sum()
    gb.columns = ['UploadKey','totalPercent']
    c1 = gb.totalPercent>lower_perc_tolerance
    c2 = gb.totalPercent<upper_perc_tolerance
    gb['within_total_tolerance'] =c1&c2
    disc_df = pd.merge(disc_df,gb,on='UploadKey',how='left',
                       validate='1:1')
    return disc_df
    
# =============================================================================
# def make_purpose_ing_table(rec_df):
#     cond = rec_df.bgCAS=='ambiguous'
#     df = rec_df[rec_df.large_percent_rec&~(rec_df.dup_rec)&cond].copy()
#     gb = df.groupby(['IngredientName','Purpose'],as_index=False).size()
#     gb.to_csv('./tmp/purpose_ing_ALL.csv', quotechar='$')
# =============================================================================
    
def prep_datasets(rec_df,disc_df):
    disc_df['has_TBWV'] = disc_df.TotalBaseWaterVolume>0
    disc_df = calc_overall_percentages(rec_df, disc_df)
    upk = disc_df[disc_df.within_total_tolerance].UploadKey.unique().tolist()
    rec_df['large_percent_rec'] = rec_df.PercentHFJob>large_perc_value
    rec_df['is_water_carrier'] = rec_df.large_percent_rec & \
                                 (rec_df.bgCAS=='7732-18-5') &\
                                 ~(rec_df.dup_rec) 
                                 
    # because we are dependent on 50% and within tolerance, we
    # don't merge for those disc out of tolerance, too many multiple 50%'ers. 
    cond = rec_df.UploadKey.isin(upk)                             
    t = rec_df[(rec_df.is_water_carrier)&cond][['PercentHFJob','UploadKey']].copy()
    t.columns = ['carrier_percent','UploadKey']
    disc_df = pd.merge(disc_df[disc_df.within_total_tolerance],
                       t,on='UploadKey',how='left',validate='1:1')
    
    disc_df['carrier_mass'] = disc_df.TotalBaseWaterVolume * 8.34
    disc_df['job_mass'] = disc_df.carrier_mass/(disc_df.carrier_percent/100)
    
    rec_df = pd.merge(rec_df,disc_df[['UploadKey','job_mass']],
                      on='UploadKey',how='left',validate='m:1')
    rec_df['calcMass'] = (rec_df.PercentHFJob/100)*rec_df.job_mass
    
    #make_purpose_ing_table(rec_df)
    
    return rec_df,disc_df