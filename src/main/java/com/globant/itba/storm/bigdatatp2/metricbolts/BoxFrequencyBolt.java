package com.globant.itba.storm.bigdatatp2.metricbolts;

import java.util.HashMap;
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
public class BoxFrequencyBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	// Given the tuple, returns the "characteristic" which we are
	// grouping by.
	// Calls to this function may imply a HBase query.
	// However, most HBase queries are cached.
	private Function<Tuple, String> characteristicFunction;
	
	// Boolean which indicates if the "characteristic" depends solely on the box id.
	// If it does not, then it must be recalculated on every single channel change.
	boolean recalculateOnChannelChange;
	
	private Map<Long, String> lastSeenValues;
	private Map<String, Long> frequencyTable;
	
	private long currMinuteFromEpoch = -1;
	
	
	public BoxFrequencyBolt(Function<Tuple, String> func, boolean recalculateOnChannelChange) {
		this.characteristicFunction = func;
		this.recalculateOnChannelChange = recalculateOnChannelChange;
		frequencyTable = new HashMap<String, Long>();
		lastSeenValues = new HashMap<Long, String>();			

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
    		String prevValue  = lastSeenValues.get(boxId);
    		long prevCount = frequencyTable.get(prevValue);
    		frequencyTable.put(prevValue, prevCount - 1);
    	}
    	if (recalculateOnChannelChange || (!lastSeenValues.containsKey(boxId))) {
    		String key = characteristicFunction.eval(tuple);
    		if (key == null) {
    			System.out.println("ACA1");
    		}
    		increaseFreqTable(key);
    		lastSeenValues.put(boxId, key);
    	}
	}
	
	/**
	 * Updates the data with the given power change.
	 */
	private void updateDataPowerChange(Tuple tuple, boolean off) {
		long boxId = tuple.getLongByField("box_id");
		if (!off) {
			String key = characteristicFunction.eval(tuple);
			lastSeenValues.put(boxId, key);
			increaseFreqTable(key);
		} else if (lastSeenValues.containsKey(boxId)){
			String prevKey = lastSeenValues.get(boxId);
			lastSeenValues.remove(boxId);
			decreaseFreqTable(prevKey);
		} else {
			//got an OFF but have never seen this client before
			//pass
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
