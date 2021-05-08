# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 21:09:24 2021

@author: Gary
"""

import core.process_CAS_ref_files as pCAS

dic = pCAS.processAll()
pCAS.create_syn_lsh_pkl()

#cdic = pCAS.get_CAS_syn_dict()
