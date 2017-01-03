# -*- coding: utf-8 -*-
"""
Created on Tue Nov 08 22:52:28 2016

@author: cy111966
"""

import tushare as ts

def top_list(date =None,retry_count =5,pause=1):
    df = ts.top_list(date =date,retry_count=retry_count,pause=pause)
    return df
    

    
def cap_list(days =5,retry_count =5,pause=1):
    df = ts.cap_tops(days =days,retry_count=retry_count,pause=pause)
    return df

def broker_top(days=5,retry_count=5,pause=1):
    df = ts.broker_tops(days =days,retry_count=retry_count,pause=pause)
    return df

def inst_tops(days =5,retry_count =5,pause=1):
    df = ts.inst_tops(days =days,retry_count=retry_count,pause=pause)
    return df

def inst_detail(retry_count =5,pause=1):
    df = ts.inst_detail(retry_count=retry_count,pause=pause)
    return df
    
def stock_basics():
    df = ts.get_stock_basics()
    return df    

def save_db(data =None,table_name=None,con=None ,flag ='append'):
    res = data.to_sql(table_name,con,if_exists=flag)
    return res        