package stormpython;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

// The topology
public class WordCount 
{
    public static void main(String[] args) throws AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
		// Spout emits random sentences
		builder.setSpout("FsRealSpout", new SentenceSpout(), 1);
		// Split bolt splits sentences and emits words
		builder.setBolt("SplitBolt", new Bolt1(), 4).fieldsGrouping("FsRealSpout",new Fields("code"));
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
