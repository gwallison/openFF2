# -*- coding: utf-8 -*-
"""
Created on Fri Feb 12 22:05:13 2021

@author: Gary
"""
import subprocess
#import shutil
#import datetime
#import pandas as pd
#import numpy as np

s= 'jupyter nbconvert --template=nbextensions --ExecutePreprocessor.allow_errors=True --ExecutePreprocessor.timeout=-1 --execute Open-FF_Data_Dictionary.ipynb --to=html '
print(subprocess.run(s))

