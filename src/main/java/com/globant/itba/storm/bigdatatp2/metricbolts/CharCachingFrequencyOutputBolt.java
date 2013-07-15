package com.globant.itba.storm.bigdatatp2.metricbolts;

import java.util.HashMap;
import java.util.Map;

import com.globant.itba.storm.bigdatatp2.functions.Function;

public class CharCachingFrequencyOutputBolt extends FrequencyOutputBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Function<String, String> func;
	
	private Map<String, String> map = new HashMap<String, String>();
	
	public CharCachingFrequencyOutputBolt(String characteristic, Function<String, String> func) {
		super(characteristic);
		this.func = func;
	}
	
	
	protected void addChanges(long minuteFromEpoch, String key, int quantity) {
		super.addChanges(minuteFromEpoch, getMapping(key), quantity);
	}
	
	private String getMapping(String key) {
		if (!map.containsKey(key)) {
			map.put(key, func.eval(key));
		}
		return map.get(key);
	}
	
	

}
