package com.globant.itba.storm.bigdatatp2.metricbolts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.functions.Function;

/**
 * Accumulates the frequency of box ids grouped by a certain 
 * characteristic defined by the constructor function.
 *
 */
public class BoxListFrequencyBolt extends AbstractFrequencyBolt {
	
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
	
	
	public BoxListFrequencyBolt(Function<Tuple, List<String>> func, boolean recalculateOnChannelChange) {
		super(recalculateOnChannelChange);
		this.characteristicFunction = func;
		lastSeenValues = new HashMap<Long, List<String>>();			

	}

	
	/**
	 * Updates the data with the given channel change.
	 */
	protected void updateDataChannelChange(Tuple tuple) {
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
	protected void updateDataPowerChange(Tuple tuple, boolean off) {
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

}
