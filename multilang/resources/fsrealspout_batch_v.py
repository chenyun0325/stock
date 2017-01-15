# -*- coding: utf-8 -*-
from __future__ import division
import storm
import random
import tushare as ts
import multiprocessing
import time
import sys
import logging as log
import traceback
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
        #time.sleep(5)
        flag = True
        if flag:
           flag = False
           try:
               code_list = sys.argv[1].split(',')
               batch = int(sys.argv[2])
               total = code_list.__len__()
               batch_size = total // batch
               for i in range(batch+1):
                   begin_index = i * batch_size
                   end_index = (i + 1) * batch_size
                   if end_index > total:
                       end_index = total
                   code_list_split = code_list.__getslice__(begin_index, end_index)
                   results = ts.get_realtime_quotes(code_list_split)
        #results.drop('name',axis=1,inplace=True)
        #等待执行完毕
                   for i,row in results.iterrows():
                     code = row['code']
                     json={}
                     json['name']=row['name']
                     json['code']=row['code']
                     json['date']=row['date']
                     json['time']=row['time']
                     json['open']=row['open']
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
                     json['b2_v']=row['b2_v']
                     json['b2_p']=row['b2_p']
                     json['b3_p']=row['b3_p']
                     json['b3_v']=row['b3_v']
                     json['b4_p']=row['b4_p']
                     json['b4_v']=row['b4_v']
                     json['b5_p']=row['b5_p']
                     json['b5_v']=row['b5_v']
                     json['a1_v']=row['a1_v']
                     json['a1_p']=row['a1_p']
                     json['a2_v']=row['a2_v']
                     json['a2_p']=row['a2_p']
                     json['a3_p']=row['a3_p']
                     json['a3_v']=row['a3_v']
                     json['a4_p']=row['a4_p']
                     json['a4_v']=row['a4_v']
                     json['a5_p']=row['a5_p']
                     json['a5_v']=row['a5_v']
                     #json['arg']=sys.argv[1]
                     #sentence = random.choice(SENTENCES)
                     #storm.logInfo("Emiting %s" % sentence)
                     #storm.logInfo("Emiting code:%s row:%s" %(code,json))
                     storm.emit([code,json])
               flag=True
           except Exception,e:
                log.error(traceback.print_exc())
                log.error(e)

# Start the spout when it's invoked
FsRealSpout().run()
