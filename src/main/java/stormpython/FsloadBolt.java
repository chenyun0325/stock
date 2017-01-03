package stormpython;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by cy111966 on 2016/12/26.
 */
public class FsloadBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String code = tuple.getString(0);
    Object item = tuple.getValue(1);
    System.out.println(code);
    System.out.println(item);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }
}
