# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 12:05:07 2020

@author: Gary
"""
import numpy as np
import pandas as pd
import shutil
import os
import subprocess
from datetime import datetime
import pickle
import core.get_google_map as ggmap
import core.Analysis_set as ana_set

today = datetime.today()

# for nicer displays of numbers: round to significant figures.
from math import log10, floor
def round_sig(x, sig=2):
    try:
        return int(round(x, sig-int(floor(log10(abs(x))))-1))
    except:
        return x

class Web_gen():
    
    def __init__(self,data_date='UNKNOWN',caslist = []):
        self.data_date = data_date
        self.outdir = './tmp/website/'
        #self.outdir = './tmp/website_test/'
        self.css_fn = './catalog/style.css'
        #self.default_empty_fn = './website_gen/default_empty.html'
        self.jupyter_fn = './catalog/chemical_report.html'
        self.ref_fn = './catalog/ref.csv'
        self.filtered_fields = ['reckey', 'PercentHFJob', 'record_flags', 
                                'calcMass', 'UploadKey', 'OperatorName',
                                'bgOperatorName',
                                'APINumber', 'TotalBaseWaterVolume',
                                'TotalBaseNonWaterVolume', 'FFVersion', 
                                'TVD', 'StateName', 'StateNumber', 'CountyName', 
                                'CountyNumber', 'TradeName',
                                'Latitude', 'Longitude', 'Projection',
                                'data_source', 'bgStateName', 'bgCountyName', 
                                'bgLatitude', 'bgLongitude', 'date',
                                'IngredientName', 'Supplier', 'bgSupplier', 
                                'Purpose', 'CASNumber', 'bgCAS','primarySupplier',
                                'bgIngredientName', 'proprietary', 
                                'eh_Class_L1', 'eh_Class_L2', 'eh_CAS', 
                                'eh_IngredientName', 'eh_subs_class', 
                                'eh_function','is_on_TEDX']
        self.caslist = caslist
        self.allrec = ana_set.Catalog_set().get_set()
        self.allrec['TradeName_trunc'] = np.where(self.allrec.TradeName.str.len()>30,
                                                  self.allrec.TradeName.str[:30]+'...',
                                                  self.allrec.TradeName)
# =============================================================================
#         # use this to make shortened versions
#         if caslist != []:
#             self.allrec = self.allrec[self.allrec.bgCAS.isin(caslist)]
# =============================================================================
        self.num_events = len(self.allrec.UploadKey.unique())
        self.num_events_wo_FFV1 = len(self.allrec[~self.allrec.no_chem_recs].UploadKey.unique())
        self.num_events_fil = len(self.allrec[self.allrec.in_std_filtered].UploadKey.unique())

        
    def initialize_dir(self,dir):
        shutil.rmtree(dir,ignore_errors=True)
        os.mkdir(dir)
                      
    def make_dir_structure(self,caslist=[]):
        self.initialize_dir(self.outdir)
        for cas in caslist:
            self.initialize_dir(self.outdir+cas)
        shutil.copyfile(self.css_fn,self.outdir+'style.css')

    def compile_page(self,title='empty title',header='',body=''):
        return f"""<!DOCTYPE html>
<html>
  <head>
    <title>{title}</title>
    <link rel='stylesheet' href='style.css' />
  </head>
  <body>
      <h1>{header}</h1>
      <h4>Data cleaned and extracted from <a href=https://www.FracFocus.org >FracFocus</a>, downloaded on {self.data_date}</h4>
    {body}
  </body>
</html>
"""
    def save_page(self,webtxt='', fn='index.html'):
        with open(self.outdir+fn,'w') as f:
            f.write(webtxt)
    
    def add_table_line(self,vals=[]):
        s = '  <tr>\n'
        for item in vals:        
            s+= f'    <td> {item} </td> \n'
        s+= '   </tr>\n'
        return s
    
    def add_table_head(self,vals=[]):
        s = '  <tr>\n'
        for item in vals:        
            s+= f'    <th> {item} </th> \n'
        s+= '   </tr>\n'
        return s

# =============================================================================
#     def save_global_vals(self,num_UploadKey=None,cas='?',
#                          IngredientName='?',eh_IngredientName='?'):
#         """put numbers used by all analyses into a file for access
#         within Jupyter scripts."""
#         with open(self.ref_fn,'w') as f:
#             f.write('varname,value\n')
#             f.write(f'tot_num_disc,{self.num_events}\n')
#             f.write(f'tot_num_disc_less_FFV1,{self.Upload_wo_FFV1}\n')
#             f.write(f'tot_num_disc_fil,{self.num_events_fil}\n')
#             f.write(f'data_date,{self.data_date}\n')
#             f.write(f'today,{today}\n')
#             f.write(f'target_cas{cas}\n')
#             #f.write(f'{IngredientName}\n')
#             #f.write(f'{eh_IngredientName}\n')
# 
# =============================================================================
    def save_global_vals(self,num_UploadKey=None,cas='?',
                         IngredientName='?',eh_IngredientName='?'):
        """put numbers used by all analyses into a file for access
        within Jupyter scripts."""
        vname = ['tot_num_disc','tot_num_disc_less_FFV1','tot_num_disc_fil',
                 'data_date','today','target_cas']
        vals = [self.num_events,self.num_events_wo_FFV1,self.num_events_fil,
                self.data_date,today,cas]
        pd.DataFrame({'varname':vname,'value':vals}).to_csv(self.ref_fn,
                                                            index=False)

    def make_map_link(self,row):
        l = ggmap.getSearchLink(lat=row.bgLatitude,lon=row.bgLongitude)
        return ggmap.wrap_URL_in_html(l,'map')
            
    def make_bgCAS_name_df(self,namedf):
        fns = ['Chemical Abstracts Index Name5521926666129599114.csv',
              'Chemical Abstracts Index Name6033273653638177995.csv']
        epa0 = pd.read_csv('./catalog/'+fns[0],low_memory=False)
        epa1 = pd.read_csv('./catalog/'+fns[1],low_memory=False)
        epa = pd.concat([epa0,epa1],sort=True)
        epa = epa.rename({'CAS #':'bgCAS'},axis=1)
        namedf = pd.merge(namedf,epa,on='bgCAS',how='left')
        namedf = namedf.rename({'Registry Name':'epa_Registry_Name',
                                'Substance Name':'epa_Substance_Name'},axis=1)
        namedf['eh_IngredientName'] = ''
        namedf[['bgCAS','bgIngredientName','eh_IngredientName',
                'epa_Registry_Name','epa_Substance_Name']].to_csv('./catalog/bgCAS.csv',
                                                                  encoding='utf-8',
                                                                  index=False)
    def make_chem_list(self):
        t = self.allrec
        if self.caslist != []: # then do all
            t = t[t.bgCAS.isin(self.caslist)]
        gb = t.groupby('bgCAS',as_index=False)[['bgIngredientName']].first()
        self.make_bgCAS_name_df(gb)
        #gb = gb[:4]  #limit length for development
        lst = gb.bgCAS.unique().tolist()
        lst.sort()
        #self.make_dir_structure(lst)
        
        
        for (i, row) in gb.iterrows():
            if i < 1276:  # control the overall list
                continue # skip the rest
            chem = row.bgCAS
            print(f'\nWorking on ** {chem} **  ({i})')
            #ingred = row.bgIngredientName
            self.save_global_vals(self.num_events,chem,
                                  row.bgIngredientName)
            
            tt = self.allrec[self.allrec.bgCAS==chem].copy()
            # re-run the non-mass ones
            #if tt.bgMass.max() >0:
            #    continue
            print(f'{chem}: {tt.PercentHFJob.max()}, {tt.calcMass.max()}')
            
            if len(tt)>0:
                tt['map_link'] = tt.apply(lambda x: self.make_map_link(x),axis=1)
            else:
                tt['map_link'] = ''
            # save data to file for later notebook access
            tt.to_csv('catalog/data.csv',index=False)
            tt.to_csv(self.outdir+chem+'/data.csv',index=False)
            self.make_jupyter_output()
            self.fix_html_title(chem)
            an_fn = f'/analysis_{chem}.html'
            shutil.copyfile(self.jupyter_fn,self.outdir+chem+an_fn)

    def fix_html_title(self,cas):
        with open(self.jupyter_fn,'r') as f:
            alltext = f.read()
        alltext  = alltext.replace('<title>chemical_report</title>',
                                   f'<title>{cas}: Open-FF report</title>',1)
        with open(self.jupyter_fn,'w') as f:
            f.write(alltext)
            

    def make_10perc_dict(self,fromScratch=True):
        if fromScratch:
            self.perc90dic = {}
            caslist = self.allrec.bgCAS.unique().tolist()
            for cas in caslist:
                print(cas)
                df = self.allrec[self.allrec.bgCAS==cas][['bgCAS','bgMass']]
                try:
                    perc90_mass = np.percentile(df[df.bgMass>0].bgMass,90)
                except:
                    perc90_mass = '???'
                self.perc90dic[cas] = perc90_mass
            with open('perc90dic.pkl','wb') as f:
                pickle.dump(self.perc90dic,f)
        else:
            with open('perc90dic.pkl','rb') as f:
                self.perc90dic = pickle.load(f)

    def make_robot_file(self):
        """create robots.txt file"""
        s = "User-agent: * \n"
        s+= "Disallow: / \n"
        self.save_page(webtxt=s,fn='robots.txt')

    def make_jupyter_output(self,subfn=''):
        s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute catalog/chemical_report.ipynb --to=html '
        print(subprocess.run(s))

    def make_new_index_pages(self):
        print('creating front page')
        s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute Open-FF_Catalog.ipynb --to=html '
        print(subprocess.run(s))

        print('creating chemical page')
        s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute Open-FF_Chemicals.ipynb --to=html '
        print(subprocess.run(s))

        print('creating synonym page')
        s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute Open-FF_Synonyms.ipynb --to=html '
        print(subprocess.run(s))

        print('creating disclosure page')
        s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute Open-FF_Disclosures.ipynb --to=html '
        print(subprocess.run(s))

        print('creating company page')
        s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute Open-FF_Companies.ipynb --to=html '
        print(subprocess.run(s))
        
        print('creating tradename page')
        s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute Open-FF_TradeNames.ipynb --to=html '
        print(subprocess.run(s))
        
