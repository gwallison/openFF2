# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:34:50 2019

@author: GAllison

This script performs the overall task of creating a FracFocus database from
the raw excel collection and creating the output data sets.

Change the file handles at the top of this code to appropriate directories.
    
"""
#### -----------   File handles  -------------- ####

####### uncomment below for local runs
outdir = './out/'
sources = './sources/bulk_data/'
tempfolder = './tmp/'

### uncomment below for running on CodeOcean
#outdir = '../results/'
#sources = '../data/'
#tempfolder = '../'


####### zip input files
bulk_fn = 'currentData'
stfilename = 'sky_truth_final'


#### ----------    end File Handles ----------  ####

make_files = True # do we create the gigantic output csv/zip files?

import shutil
import os
import core.Bulk_data_reader as rff
import core.Table_manager as c_tab


class Data_set_constructor():
    def __init__(self, bulk_fn=bulk_fn,const_mode='TEST',
                 stfilename=stfilename,tempfolder=tempfolder,
                 sources=sources,outdir=outdir,
                 make_files=make_files,
                 startfile=0,endfile=None,
                 abbreviated=False):

        self.outdir = outdir
        self.sources = sources
        self.tempfolder = tempfolder
        self.zfilename = self.sources+bulk_fn+'.zip'
        self.stfilename = self.sources+stfilename+'.zip'
        self.make_files=make_files
        self.abbreviated = abbreviated # used to do partial constructions
        self.startfile= startfile
        self.endfile = endfile
        self.const_mode = const_mode
        if const_mode == "TEST": added = '_TEST'
        else: added = ''
        self.picklefolder = self.outdir+bulk_fn+added+'_pickles/'

        
    def initialize_dir(self,dir):
        shutil.rmtree(dir,ignore_errors=True)
        os.mkdir(dir)
                           
    def _banner(self,text):
        print()
        print('*'*50)
        space = ' '*int((50 - len(text))/2)
        print(space,text,space)
        print('*'*50)
        
    def create_full_set(self,inc_skyTruth=True):
        tab_const = c_tab.Table_constructor(pkldir=self.picklefolder)
        self.initialize_dir(self.picklefolder)
        self._banner('PROCESS RAW DATA FROM SCRATCH')
        self._banner('Reading Bulk Data')
        raw_df = rff.Read_FF(zname=self.zfilename,
                             skytruth_name=self.stfilename,
                             #inc_skyTruth = inc_skyTruth,
                             outdir = self.outdir,
                             startfile=self.startfile,
                             endfile=self.endfile).\
                                  import_all(inc_skyTruth=inc_skyTruth)
        self._banner('Table_manager')
        tab_const.assemble_all_tables(raw_df)
        raw_df = None
        
        
# =============================================================================
#             raw_df = tab_const.add_indexes_to_full(raw_df)
#             tab_const.build_tables(raw_df)
#             tab_const.pickleAll(tmp=self.tempfolder)
#             tab_const.listTables()
# 
#             self._banner('Clean_event')
#             clean_ev.Clean_event(tab_manager=tab_const,
#                                  sources=self.sources).process_events()
#             tab_const.pickleAll(tmp=self.tempfolder) # needed for new table
# 
#             self._banner('Clean_allrec')
#             clean_ar.Clean_allrec(tab_manager=tab_const,
#                                   sources=self.sources).process_records()
# 
#             #if not self.abbreviated:
#             self._banner('Generate_composite_fields')
#             gf = gen_fields.Gen_composite_fields(tab_manager=tab_const)
#             print(' -- making primarySupplier and cluster table')
#             gf.make_primarySupplier()
#             gf.make_cluster_table(fn=self.sources+'clusters.csv')
# 
#             self._banner('Categorize_CAS')
#             cat_rec.Categorize_CAS(tab_manager=tab_const,
#                                    sources=self.sources,
#                                    outdir=self.outdir).do_all()
# 
#             self._banner('Process_mass')
#             proc_mass.Process_mass(tab_const).run()
#             #proc_mass.Process_mass_V2(tab_const).run()
#     
#             if not self.abbreviated:
#             
#                 self._banner('Add_External_datasets')
#                 aed.add_Elsner_table(tab_const,sources=self.sources,outdir=self.outdir)
#                 aed.add_WellExplorer_table(tab_const,sources=self.sources,outdir=self.outdir)
#                 aed.add_TEDX_ref(tab_const,sources=self.sources,outdir=self.outdir)
#                 aed.add_TSCA_ref(tab_const,sources=self.sources,outdir=self.outdir)
#                 aed.add_Prop65_ref(tab_const,sources=self.sources,outdir=self.outdir)
#                 aed.add_CWA_SDWA_ref(tab_const,sources=self.sources,outdir=self.outdir)
# 
# 
#             self._banner('Compile event level flags')
#             tab_const.compile_ev_level_flags()
#             
#             self._banner('pickle all tables')
#             tab_const.pickleAll()
# 
#             raw_df = None
#             
#             if self.make_files:
#                 self._banner('Make_working_sets')
#                 mws.Make_working_sets(tab_const,outdir=self.outdir,
#                                       tmpdir=self.tempfolder).make_all_sets()
#         else: 
#             #print('loading from pickles...')
#             tab_const.loadAllPickles()
#             tab_const.listTables()
        return tab_const
# 
# =============================================================================

    def create_quick_set(self,inc_skyTruth=True):
        """ generates a set of mostly of raw values - used mostly
        in pre-process screening. """
        
        tab_const = c_tab.Construct_tables(pkldir=self.picklefolder)
        raw_df = rff.Read_FF(zname=self.zfilename,
                             startfile=self.startfile,
                             endfile=self.endfile,
                             skytruth_name=self.stfilename).import_all(inc_skyTruth=inc_skyTruth)
# =============================================================================
#         raw_df = tab_const.add_indexes_to_full(raw_df)
#         tab_const.build_tables(raw_df)
#         raw_df = None
# =============================================================================
        return tab_const
