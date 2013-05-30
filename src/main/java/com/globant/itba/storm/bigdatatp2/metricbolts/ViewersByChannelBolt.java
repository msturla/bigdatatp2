package com.globant.itba.storm.bigdatatp2.metricbolts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.globant.itba.storm.bigdatatp2.hbase.ChannelRepository;

public class ViewersByChannelBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	private Map<Integer, Long> viewersByChannel;
	private Map<Long, Integer> previousChannel;
	
	
	public ViewersByChannelBolt() {
		viewersByChannel = new HashMap<Integer, Long>();
		previousChannel = new HashMap<Long, Integer>();
	}

	
    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
    	
    	if (tuple.contains("exportData")) {
    		emitData();
    	} else {
    		updateData(
    				tuple.getLongByField("box_id"),
    				tuple.getIntegerByField("channel"),
    				tuple.getStringByField("power"));
    	}
        _collector.ack(tuple);
    }
    
    //emit top 10 as a string in the form: channel,count;channel,count;
    private void emitData() {
    	List<Entry<Integer, Long>> viewerList = new ArrayList<Entry<Integer,Long>>(viewersByChannel.entrySet());
    	Collections.sort(viewerList, new Comparator<Entry<Integer, Long>>() {
			@Override
			public int compare(Entry<Integer, Long> o1, Entry<Integer, Long> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
    	});
    	StringBuffer buf = new StringBuffer();
    	try {    		
    		for (int i = 0; i < 10 && i < viewerList.size();i ++) {
    			int channel = viewerList.get(i).getKey();
    			buf.append(ChannelRepository.getChannelName(channel));
    			buf.append(",");
    			buf.append(viewerList.get(i).getValue());
    			buf.append(";");
    		}
    		_collector.emit(new Values(buf.toString()));
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    
    private void updateData(long box_id, Integer channel, String power) {
    	if (channel != null && !viewersByChannel.containsKey(channel)) {
    		viewersByChannel.put(channel, (long) 0);
    	}
    	if (previousChannel.containsKey(box_id)) {
    		int prevChannel  = previousChannel.get(box_id);
    		long prevCount = viewersByChannel.get(prevChannel);
    		viewersByChannel.put(prevChannel, prevCount - 1);
    	}
    	if ("OFF".equals(power)) {
    		previousChannel.remove(box_id);
    	} else {
    		viewersByChannel.put(channel, viewersByChannel.get(channel) + 1);
    		previousChannel.put(box_id, channel);
    	}
    	
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }
}