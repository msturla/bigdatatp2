package com.globant.itba.storm.bigdatatp2.spouts;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class PeriodicSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	
	private int seconds;
	
	public static Logger LOG = Logger.getLogger(PeriodicSpout.class);
    
	SpoutOutputCollector _collector;
	
	private long tick;
    

    public PeriodicSpout(int seconds) {
        this.seconds = seconds;
    }

        
    @SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
    	
    }
        
    public void nextTuple() {
    	try {
    		Thread.sleep(seconds * 1000);
    		tick++;
    		_collector.emit(new Values(tick));
    	} catch (InterruptedException e) {
    		// should never happen
    		LOG.error("Periodic spout was interrupted.");
    	}
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tick"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
    }
    
}