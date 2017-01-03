package stormtest;

/**
 * Created by cy111966 on 2016/12/30.
 */
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

//import storm configuration packages
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

//Create main class LogAnalyserStorm submit topology.
public class LogAnalyserStorm {
  public static void main(String[] args) throws Exception{
    //Create Config instance for cluster configuration
    Config config = new Config();
    config.setDebug(false);
    config.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, 1000);

    //
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout(),1);

    builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt(),4)
        .shuffleGrouping("call-log-reader-spout");

    builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt(),2)
        .fieldsGrouping("call-log-creator-bolt", new Fields("call"));

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());

  }
}