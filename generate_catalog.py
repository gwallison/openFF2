# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 12:00:34 2020

@author: Gary

This code is used to direct the process of making a static web site of
a chemical catelog.
"""
import catalog.gen_chemical_catalog as gen_cat
#import website_gen.API_web_gen as API_gen

data_date = '2021-06-06'


#wg = gen_cat.Web_gen(data_date=data_date,caslist=['50-00-0',#'7732-18-5',
#                                                  '111-76-2',#'100-52-7',
#                                                  '10028-15-6'])
wg = gen_cat.Web_gen(data_date=data_date,caslist=[])
#wg.make_chem_list()
#wg.make_all_catalogs()
wg.make_new_index_pages()
