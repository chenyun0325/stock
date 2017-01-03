package stormpython;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by cy111966 on 2016/12/26.
 */
public class FsloadDataEntry {

  public static void main(String[] args) {
    try {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("fs_spout",new FsloadSpout("000798","2016-12-25"),2);
      FsloadBolt fsBolt = new FsloadBolt();
      BoltDeclarer boltDeclarer = builder.setBolt("fs_bolt", fsBolt, 2);
      boltDeclarer.fieldsGrouping("fs_spout",new Fields("code"));
      Config conf = new Config();
      conf.put(Config.TOPOLOGY_DEBUG, true);
      //conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, 500);//CPU飙升
      conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, 1000);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("FsloadDataEntry", conf, builder.createTopology());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
