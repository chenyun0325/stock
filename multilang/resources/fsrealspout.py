# -*- coding: utf-8 -*-
from __future__ import division
import storm
import random
import tushare as ts
import multiprocessing
import time
import sys
# Define some sentences
SENTENCES = """
the cow jumped over the moon
an apple a day keeps the doctor away
four score and seven years ago
snow white and the seven dwarfs
i am at two with nature
""".strip().split('\n')

class FsRealSpout(storm.Spout):
    # Not much to do here for such a basic spout
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        storm.logInfo("Spout instance starting...")

    # Process the next tuple
    def nextTuple(self):
        # 停止一段时间(设置状态位)
        for i in sys.argv:
            print i
        time.sleep(5)
        batch =10
        bases = ts.get_stock_basics().head(10)
        code_list = bases.index
        total = code_list.__len__()
        batch_size = total // batch
        pool = multiprocessing.Pool(processes=batch)
        results =[]
        for i in range(batch + 1):
            begin_index = i * batch_size
            end_index = (i + 1) * batch_size
            if end_index > total:
                end_index = total
            batch_data = code_list.tolist().__getslice__(begin_index, end_index)
            res = pool.apply_async(ts.get_realtime_quotes, (batch_data,))
            results.append(res)
            # get_stock_hist_data_batch(code_list = batch_data,start=start,end=end,sh_df=sh_df,sz_df=sz_df,cyb_df=cyb_df,table_name=table_name)
        #等待执行完毕
        for item in results:
            for i,row in item.get().iterrows():
             code = row['code']
             json={}
             json['open']=row['open']
             json['code']=row['code']
             json['date']=row['date']
             json['time']=row['time']
             json['name']=row['name']
             json['pre_close']=row['pre_close']
             json['price']=row['price']
             json['high']=row['high']
             json['low']=row['low']
             json['bid']=row['bid']
             json['ask']=row['ask']
             json['volume']=row['volume']
             json['amount']=row['amount']
             json['b1_v']=row['b1_v']
             json['b1_p']=row['b1_p']
             sentence = random.choice(SENTENCES)
             storm.logInfo("Emiting %s" % sentence)
             storm.logInfo("Emiting code:%s row:%s" %(code,json))
             storm.emit([code,json])
             pool.close()
             pool.join()

# Start the spout when it's invoked
FsRealSpout().run()
