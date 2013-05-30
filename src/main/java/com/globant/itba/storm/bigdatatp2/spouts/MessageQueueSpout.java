package com.globant.itba.storm.bigdatatp2.spouts;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MessageQueueSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	public static Logger LOG = Logger.getLogger(TestWordSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    private Random rand = new Random();
    
    public MessageQueueSpout() {
        this(true);
    }

    public MessageQueueSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    @SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
        
    }
        
    public void nextTuple() {
    	//TODO read these from activemq
    	Utils.sleep(100);
    	String json = getRandomJsonLine();
    	long box_id = Long.valueOf(getField(json, "box_id"));
    	String channelString = getField(json, "channel");
    	Integer channel = channelString == null? null : Integer.valueOf(channelString);
    	String power = getField(json, "power");
    	long timestamp = Long.valueOf(getField(json, "timestamp"));
        _collector.emit(new Values(box_id, channel, power, timestamp));
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    //TODO remove this
    private String getRandomJsonLine() {
    	int randomChannel = rand.nextInt(10);
    	int randomBox = rand.nextInt(5);
    	return String.format("{\"box_id\":%d, \"channel\":%d, \"timestamp\":%d}",
    			randomBox, randomChannel, System.currentTimeMillis());
    }
    
    private String getField(String line, String name) {
		line = line.replace("{", "").replace("}", "");
		// Not very defensive or elegant, but it works with our input
		String[] jsonFields = line.split(",");
		for (String field : jsonFields) {
			if (field.contains(name)) {
				return field.split(":")[1];
			}
		}
		return null;
	}
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("box_id", "channel", "power", "timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }
    
}