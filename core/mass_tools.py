# -*- coding: utf-8 -*-
"""
Created on Thu May  6 21:26:21 2021

@author: Gary
"""
import pandas as pd
import numpy as np

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
    #print(f'in calc_ {len(disc_df)}')
    return disc_df

def calc_MI_values(rec_df,disc_df):
    
    too_small = 0 # anything this size or small is disregarded. 0 is especially a problem
    """This calculates the carrier density based on the MassIngredient mass
    and TotalBaseWaterVolume as well as some other values
    """
    rec_df['job_mass_MI'] = rec_df.MassIngredient/rec_df.PercentHFJob
    rec_df.job_mass_MI = np.where(rec_df.MassIngredient>too_small,
                                  rec_df.job_mass_MI,
                                  np.NaN)
    
    gb = rec_df.groupby('UploadKey',as_index=False)['job_mass_MI'].median()
    gb.columns = ['UploadKey','job_mass_MI_med']
    gb1 = rec_df.groupby('UploadKey',as_index=False)['job_mass_MI'].std()
    gb1.columns = ['UploadKey','job_mass_MI_std']
    gb = pd.merge(gb,gb1,on='UploadKey',how='left')
    
    disc_df = pd.merge(disc_df,gb,on='UploadKey',how='left')

    t = rec_df[rec_df.is_water_carrier][['UploadKey','MassIngredient',
                                         'density_from_comment',
                                         'PercentHFJob']]
    t.columns = ['UploadKey','carrier_mass_MI','carrier_density_from_comment',
                                         'carrier_percent']
    disc_df = pd.merge(disc_df,
                       t,on='UploadKey',
                       how='left',validate='1:1')
    disc_df['carrier_density_MI'] = np.where(disc_df.within_total_tolerance,
                                             disc_df.carrier_mass_MI/disc_df.TotalBaseWaterVolume,
                                             np.NaN)
    disc_df['bgDensity'] = np.where(disc_df.carrier_density_from_comment>7,
                                    disc_df.carrier_density_from_comment,
                                    8.34)
    disc_df['bgDensity_source'] = np.where(disc_df.carrier_density_from_comment>7,
                                    'from_comment',
                                    'default')

    return disc_df
    
# =============================================================================
# def generate_bgDensity(row):
#     if row.density_from_comment.notna(): # give direct report priority
#         dens = row.density_from_comment
#     else:
#         dens = 8.34
#     return dens
# =============================================================================
        
    
def prep_datasets(rec_df,disc_df):
    #print(f'in prep: {len(disc_df)}')
    disc_df['has_TBWV'] = disc_df.TotalBaseWaterVolume>0
    disc_df = calc_overall_percentages(rec_df, disc_df)
    upk = disc_df[disc_df.within_total_tolerance].UploadKey.unique().tolist()
    rec_df['large_percent_rec'] = rec_df.PercentHFJob>large_perc_value
    rec_df['is_water_carrier'] = rec_df.large_percent_rec & \
                                 (rec_df.bgCAS=='7732-18-5') &\
                                 ~(rec_df.dup_rec) &\
                                 rec_df.UploadKey.isin(upk)
    disc_df = calc_MI_values(rec_df, disc_df)
                             
    # because we are dependent on 50% and within tolerance, we
    # don't merge for those disclosures out of tolerance, too many multiple 50%'ers. 

    cond = rec_df.UploadKey.isin(upk)                             
# =============================================================================
#     t = rec_df[(rec_df.is_water_carrier)&cond][['PercentHFJob','UploadKey']].copy()
#     t.columns = ['carrier_percent','UploadKey']
#     disc_df = pd.merge(disc_df,t,on='UploadKey',
#                        how='left',validate='1:1')
# =============================================================================
    
    cond = disc_df.within_total_tolerance&disc_df.has_TBWV
    disc_df.carrier_percent = np.where(cond,
                                       disc_df.carrier_percent,
                                       np.NaN)    
    disc_df['carrier_mass'] = disc_df.TotalBaseWaterVolume * disc_df.bgDensity
    disc_df['job_mass'] = disc_df.carrier_mass/(disc_df.carrier_percent/100)
    
    rec_df = pd.merge(rec_df,disc_df[['UploadKey','job_mass']],
                      on='UploadKey',how='left',validate='m:1')
    rec_df['calcMass'] = (rec_df.PercentHFJob/100)*rec_df.job_mass
    rec_df = rec_df.drop('job_mass',axis=1)
    
    
    return rec_df,disc_df