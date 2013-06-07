package com.globant.itba.storm.bigdatatp2.metricbolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.functions.Function;
import com.globant.itba.storm.bigdatatp2.functions.mappers.IdentityFunction;
import com.globant.itba.storm.bigdatatp2.hbase.Repositories;

/**
 * Adds the frequencies of a certain characteristic.
 *
 */
public class FrequencyOutputBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	// Given the characeristic ID, maps to a "friendly" name.
	// If not required, just use identity function
	private final Function<String, String> mapperFunction;
	
	private Map<String, String> mappings;
	
	private String characteristic;
		
	public FrequencyOutputBolt(Function<String, String> func, String characteristic) {
		this.mapperFunction = func;
		mappings = new HashMap<String, String>();
		this.characteristic = characteristic;
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
		long minute = input.getLongByField("minute");
		String key = input.getStringByField("key");
		long quantity = input.getLongByField("frequency");
		exportRow(minute, getMapping(key), quantity);
	}
	
	private String getMapping(String key) {
		// Breaks the functional aspect but saves memory...
		if (mapperFunction instanceof IdentityFunction) return key;
		if (!mappings.containsKey(key)) {
			String mappedValue  = mapperFunction.eval(key);
			mappings.put(key, mappedValue);
		}
		return mappings.get(key);
	}
	
	private void exportRow(long minuteFromEpoch, String key, long quantity) {
		//TODO hacer estos inserts
		// Insert into table: characteristic
		// minute: minuteFromEpoch, key: key, quantity: quantity
		System.out.printf("%d, %s, %d\n", minuteFromEpoch, key, quantity);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
