# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 09:32:29 2019

@author: Gary
"""
import pandas as pd

def add_Elsner_table(df,sources='./sources/',
                     outdir='./out/',
                     ehfn='elsner_corrected_table.csv'):
    #print('Adding Elsner/Hoelzer table to CAS table')
    ehdf = pd.read_csv(sources+ehfn,quotechar='$')
    # checking overlap first:
    ehcas = list(ehdf.eh_CAS.unique())
    dfcas = list(df.bgCAS.unique())
    with open(outdir+'elsner_non_overlap.txt','w') as f:
        f.write('**** bgCAS numbers without an Elsner entry: *****\n')
        for c in dfcas:
            if c not in ehcas:
                f.write(f'{c}\n')
        f.write('\n\n***** Elsner CAS numbers without a FF entry: *****\n')
        for c in ehcas:
            if c not in dfcas:
                f.write(f'{c}\n')

    mg = pd.merge(df,ehdf,left_on='bgCAS',right_on='eh_CAS',
                  how='left',validate='1:1')
    return mg

def add_WellExplorer_table(df,sources='./sources/',
                     outdir='./out/',
                     wefn='well_explorer_corrected.csv'):
    """Add the WellExplorer data table. """
    #print('Adding WellExplorer table to CAS table')
    wedf = pd.read_csv(sources+wefn)
    #print(wedf.head())
    # checking overlap first:
    wecas = list(wedf.we_CASNumber.unique())
    dfcas = list(df.bgCAS.unique())
    with open(outdir+'wellexplorer_non_overlap.txt','w') as f:
        f.write('**** bgCAS numbers without an WellExplorer entry: *****\n')
        for c in dfcas:
            if c not in wecas:
                f.write(f'{c}\n')
        f.write('\n\n***** WellExplorer CAS numbers without a FF entry: *****\n')
        for c in wecas:
            if c not in dfcas:
                f.write(f'{c}\n')

    mg = pd.merge(df,wedf,left_on='bgCAS',right_on='we_CASNumber',
                  how='left',validate='1:1')
    return mg

    
def add_TEDX_ref(df,sources='./sources/',
                 tedx_fn = 'TEDX_EDC_trimmed.xls'):
    #print('Adding TEDX link to CAS table')
    tedxdf = pd.read_excel(sources+tedx_fn)
    tedx_cas = tedxdf.CAS_Num.unique().tolist()
    df['is_on_TEDX'] = df.bgCAS.isin(tedx_cas)
    return df
    
def add_TSCA_ref(df,sources='./sources/',
                 tsca_fn = 'TSCAINV_092019.csv'):
    #print('Adding TSCA to CAS table')
    tscadf = pd.read_csv(sources+tsca_fn)
    tsca_cas = tscadf.CASRN.unique().tolist()
    df['is_on_TSCA'] = df.bgCAS.isin(tsca_cas)
    return df
    
def add_Prop65_ref(df,sources='./sources/',
                 p65_fn = 'p65list12182020.csv'):
    #print('Adding California Prop 65 to CAS table')
    p65df = pd.read_csv(sources+p65_fn,encoding='iso-8859-1')
    p65_cas = p65df['CAS No.'].unique().tolist()
    df['is_on_prop65'] = df.bgCAS.isin(p65_cas)
    return df
    
def add_CWA_SDWA_ref(df,sources='./sources/',
                     cwa_fn = 'sara_sdwa_cwa.csv'):
    #print('Adding SDWA/CWA lists to CAS table')
    cwadf = pd.read_csv(sources+cwa_fn)
    cwa_cas = cwadf['CASNo'].unique().tolist()
    df['is_on_CWA_SDWA'] = df.bgCAS.isin(cwa_cas)
    return df
    
def add_all_bgCAS_tables(df,sources='./sources/external_refs/',
                         outdir='./outdir/'):
    df = add_CWA_SDWA_ref(df,sources)
    df = add_Prop65_ref(df,sources)
    df = add_TSCA_ref(df,sources)
    df = add_TEDX_ref(df,sources)
    df = add_WellExplorer_table(df,sources,outdir)
    df = add_Elsner_table(df,sources,outdir)
    return df
    