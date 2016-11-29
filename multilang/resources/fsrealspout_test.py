# -*- coding: utf-8 -*-
from __future__ import division
import storm
import random
import tushare as ts
import multiprocessing
import time
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
        time.sleep(5)
        results = ts.get_realtime_quotes(['600848','000980','000981'])
        #results.drop('name',axis=1,inplace=True)
        #等待执行完毕
        for i,row in results.iterrows():
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

# Start the spout when it's invoked
FsRealSpout().run()
