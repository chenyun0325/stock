# -*- coding: utf-8 -*-
"""
Created on Wed Nov 02 22:25:58 2016

@author: cy111966
"""

from sqlalchemy import create_engine

engine = create_engine('mysql://root:123456@localhost/db_test?charset=utf8',echo=True)