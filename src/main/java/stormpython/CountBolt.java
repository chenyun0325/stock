package stormpython;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

/*
This is basically just defining some JVM things for Storm, such as
the output fields, or passing around configuration. Then it invokes the
countbolt.py using Python.
*/
public class CountBolt extends ShellBolt implements IRichBolt {
	// Call the countbolt.py using Python
	public CountBolt() {
		super("python", "countbolt.py");
	}
	
	// Declare that we emit a 'word' and 'count'
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	// Nothing to do for configuration
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}