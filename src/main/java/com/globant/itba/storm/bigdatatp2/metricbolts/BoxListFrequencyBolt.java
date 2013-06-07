package com.globant.itba.storm.bigdatatp2.metricbolts;

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

import com.globant.itba.storm.bigdatatp2.functions.Function;
import com.globant.itba.storm.bigdatatp2.hbase.Repositories;

/**
 * Accumulates the frequency of box ids grouped by a certain 
 * characteristic defined by the constructor function.
 *
 */
public class BoxListFrequencyBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	// Given the tuple, returns a list of "characteristics" which we are
	// grouping by.
	// Calls to this function may imply a HBase query.
	// However, most HBase queries are cached.
	private Function<Tuple, List<String>> characteristicFunction;
	
	// Boolean which indicates if the "characteristic" depends solely on the box id.
	// If it does not, then it must be recalculated on every single channel change.
	boolean recalculateOnChannelChange;
	
	private Map<Long, List<String>> lastSeenValues;
	private Map<String, Long> frequencyTable;
	
	private long currMinuteFromEpoch = -1;
	
	
	public BoxListFrequencyBolt(Function<Tuple, List<String>> func, boolean recalculateOnChannelChange) {
		this.characteristicFunction = func;
		this.recalculateOnChannelChange = recalculateOnChannelChange;
		frequencyTable = new HashMap<String, Long>();
		lastSeenValues = new HashMap<Long, List<String>>();			

	}


	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		Repositories.initRepositories();
	}


	@Override
	public void execute(Tuple input) {
		long minuteFromEpoch = input.getLongByField("timestamp") / 1000;
		if (currMinuteFromEpoch == -1) {
			currMinuteFromEpoch = minuteFromEpoch;
		}
		if (mustExportData(minuteFromEpoch)) {
			exportData(minuteFromEpoch);
			currMinuteFromEpoch = minuteFromEpoch;
		}
		if (input.getStringByField("power") != null) {
			String power = input.getStringByField("power").toUpperCase();
			updateDataPowerChange(input, power.equals("\"OFF\""));
		} else {
			updateDataChannelChange(input);
		}
	
	}
	
	private void exportData(long minuteFromEpoch) {
		for (long i = currMinuteFromEpoch; i < minuteFromEpoch; i ++) {
			for (Entry<String, Long> entry : frequencyTable.entrySet()) {
				_collector.emit(new Values(entry.getKey(), entry.getValue(), i));
			}
			_collector.emit(new Values(null, null, i + 1));
		}
	}
	
	private boolean mustExportData(long minuteFromEpoch) {
		return minuteFromEpoch != currMinuteFromEpoch;
	}
	
	/**
	 * Updates the data with the given channel change.
	 */
	private void updateDataChannelChange(Tuple tuple) {
		long boxId = tuple.getLongByField("box_id");
    	if (lastSeenValues.containsKey(boxId) && recalculateOnChannelChange) {
    		List<String> prevValues  = lastSeenValues.get(boxId);
    		for (String key : prevValues) {
    			long prevCount = frequencyTable.get(key);
    			frequencyTable.put(key, prevCount - 1);
    		}
    	}
    	if (recalculateOnChannelChange || (!lastSeenValues.containsKey(boxId))) {
    		List<String> keys = characteristicFunction.eval(tuple);
    		for (String key : keys) {
    			increaseFreqTable(key);    			
    		}
    		lastSeenValues.put(boxId, keys);
    	}
	}
	
	/**
	 * Updates the data with the given power change.
	 */
	private void updateDataPowerChange(Tuple tuple, boolean off) {
		long boxId = tuple.getLongByField("box_id");
		if (!off) {
			List<String> keys = characteristicFunction.eval(tuple);
			lastSeenValues.put(boxId, keys);
			for (String key : keys) {
    			increaseFreqTable(key);    			
    		}
		} else if (lastSeenValues.containsKey(boxId)){
			List<String> prevKeys = lastSeenValues.get(boxId);
			lastSeenValues.remove(boxId);
			for (String key : prevKeys) {
    			decreaseFreqTable(key);
    		}
		}
	}
	
	private void increaseFreqTable(String key) {
		if (!frequencyTable.containsKey(key)) {
			frequencyTable.put(key, (long) 0);
		}
		frequencyTable.put(key, frequencyTable.get(key) + 1);
	}
	
	private void decreaseFreqTable(String key) {
		frequencyTable.put(key, frequencyTable.get(key) - 1);
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "frequency", "minute"));
		
	}
	
	

}
