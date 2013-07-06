package com.globant.itba.storm.bigdatatp2.metricbolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.functions.Function;

/**
 * Accumulates the frequency of box ids grouped by a certain 
 * characteristic defined by the constructor function.
 *
 */
public class BoxFrequencyBolt extends AbstractFrequencyBolt {
	
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	// Given the tuple, returns the "characteristic" which we are
	// grouping by.
	// Calls to this function may imply a HBase query.
	// However, most HBase queries are cached.
	private Function<Tuple, String> characteristicFunction;
	
	private Map<Long, String> lastSeenValues;
	
	public BoxFrequencyBolt(Function<Tuple, String> func, boolean recalculateOnChannelChange) {
		super(recalculateOnChannelChange);
		this.characteristicFunction = func;
		lastSeenValues = new HashMap<Long, String>();			

	}
	
	/**
	 * Updates the data with the given channel change.
	 */
	protected void updateDataChannelChange(Tuple tuple) {
		long boxId = tuple.getLongByField("box_id");
    	if (lastSeenValues.containsKey(boxId) && recalculateOnChannelChange) {
    		String prevValue  = lastSeenValues.get(boxId);
    		int prevCount = frequencyTable.get(prevValue);
    		frequencyTable.put(prevValue, prevCount - 1);
    	}
    	if (recalculateOnChannelChange || (!lastSeenValues.containsKey(boxId))) {
    		String key = characteristicFunction.eval(tuple);
    		increaseFreqTable(key);
    		lastSeenValues.put(boxId, key);
    	}
	}
	
	/**
	 * Updates the data with the given power change.
	 */
	protected void updateDataPowerChange(Tuple tuple, boolean off) {
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
	
	

}
