package stormpython;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import datacrawler.Constant;

// The topology
public class WordCount 
{
    public static void main(String[] args) throws AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
	        String[] codes = Constant.stock_all.split(",");
	        int batchsize=800;
	        int total = codes.length;
	        int batch = total/batchsize;
	        //int mod = total%batchsize;
	        BoltDeclarer splitBolt = builder.setBolt("SplitBolt", new Bolt1(), 4);
	        for (int i=0;i<=batch;i++){
			int start = i*batchsize;
			int end = (i+1)*batchsize;
			if (end>total){
				end=total;
			}
			List<String> codeslist = new ArrayList<String>();
			for (int j=start;j<end;j++){
				codeslist.add(codes[j]);
			}
			String codeListStr = Joiner.on(",").join(codeslist);
			// Spout emits random sentences
			builder.setSpout("FsRealSpout"+i, new SentenceSpout(codeListStr), 1);
			// Split bolt splits sentences and emits words
			splitBolt.fieldsGrouping("FsRealSpout" + i, new Fields("code"));
			codeslist.clear();
		}

		// Counter consumes words and emits words and counts
		// FieldsGrouping is used so the same words get routed
		//  to the same bolt instance
		//builder.setBolt("CountBolt", new CountBolt(), 4).fieldsGrouping("SplitBolt", new Fields("word"));
		
		//New configuration
		Config conf = new Config();
	        conf.put(Config.TOPOLOGY_DEBUG,true);
	        conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS,500);
		
		// If there are arguments, we must be on a cluster
		if(args != null && args.length > 0) {
			conf.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			// Otherwise, we are running locally
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
		}
    }
}
