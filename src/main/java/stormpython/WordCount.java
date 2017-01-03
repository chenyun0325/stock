package stormpython;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import datacrawler.Constant;

// The topology
public class WordCount {

  public static void main(String[] args) throws AuthorizationException {
    String filter_mount=args[0];
    String filter_per=args[1];
    String slide_size= args[2];
    String max_siz=args[3];
    String wind_size=args[4];
    String price_dif_var=args[5];
    String amount=args[6];
    String price_dif_var1=args[7];
    String amount1=args[8];

    TopologyBuilder builder = new TopologyBuilder();
    String[] codes = Constant.stock_all.split(",");
    int batchsize = 800;
    int total = codes.length;
    int batch = total / batchsize;
    //int mod = total%batchsize;
    Bolt2 bolt = new Bolt2(Double.valueOf(filter_mount), Double.valueOf(filter_per), Integer.valueOf(slide_size));
    BoltDeclarer splitBolt = builder.setBolt("SplitBolt", bolt, 4);
    for (int i = 0; i <= batch; i++) {
      int start = i * batchsize;
      int end = (i + 1) * batchsize;
      if (end > total) {
        end = total;
      }
      List<String> codeslist = new ArrayList<String>();
      for (int j = start; j < end; j++) {
        codeslist.add(codes[j]);
      }
      String codeListStr = Joiner.on(",").join(codeslist);
      // Spout emits random sentences
      builder.setSpout("FsRealSpout" + i, new SentenceSpout(codeListStr), 1);
      // Split bolt splits sentences and emits words
      splitBolt.fieldsGrouping("FsRealSpout" + i, new Fields("code"));
      codeslist.clear();
    }
    builder.setBolt("slidBolt", new SlidingWindowBolt(Integer.valueOf(max_siz), Integer.valueOf(wind_size), Double.valueOf(price_dif_var), Double.valueOf(amount),Double.valueOf(price_dif_var1), Double.valueOf(amount1)), 2)
        .fieldsGrouping("SplitBolt", new Fields
            ("code"));

    // Counter consumes words and emits words and counts
    // FieldsGrouping is used so the same words get routed
    //  to the same bolt instance
    //builder.setBolt("CountBolt", new CountBolt(), 4).fieldsGrouping("SplitBolt", new Fields("word"));

    //New configuration
    Config conf = new Config();
    conf.put(Config.TOPOLOGY_DEBUG, false);
    //conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, 500);//CPU飙升
    conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, 1000);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, builder.createTopology());
//    // If there are arguments, we must be on a cluster
//    if (args != null && args.length > 0) {
//      conf.setNumWorkers(3);
//      try {
//        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
//      } catch (AlreadyAliveException e) {
//        e.printStackTrace();
//      } catch (InvalidTopologyException e) {
//        e.printStackTrace();
//      }
//    } else {
//      // Otherwise, we are running locally
//      LocalCluster cluster = new LocalCluster();
//      cluster.submitTopology("test", conf, builder.createTopology());
//    }
  }
}
