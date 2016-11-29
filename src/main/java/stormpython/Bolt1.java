package stormpython;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by chenyun on 16/2/2.
 */
public class Bolt1 extends BaseBasicBolt {

    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String msg = input.getString(0);
            Object item = input.getValue(1);
            msg = msg + "bolt1";
            MessageId messageId = input.getMessageId();
            System.err.println(messageId);
            System.err.println("对消息加工第1次-------[arg0]:" + msg  + "---[arg2]:" + item + "------->" + msg);
            if (msg != null) {
                collector.emit(new Values(msg));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(new Fields("code","row"));
    }
}
