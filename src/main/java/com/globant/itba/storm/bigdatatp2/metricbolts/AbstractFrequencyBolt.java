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

import com.globant.itba.storm.bigdatatp2.hbase.Repositories;

/**
 * Accumulates the frequency of box ids grouped by a certain characteristic
 * defined by the constructor function.
 * 
 */
public abstract class AbstractFrequencyBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	

	// Boolean which indicates if the "characteristic" depends solely on the box
	// id.
	// If it does not, then it must be recalculated on every single channel
	// change.
	protected final boolean recalculateOnChannelChange;

	// Holds the CHANGES for the current minute.
	protected final Map<String, Integer> frequencyTable;

	private long currMinuteFromEpoch = -1;
	
	private boolean staleData;

	public AbstractFrequencyBolt(boolean recalculateOnChannelChange) {
		this.recalculateOnChannelChange = recalculateOnChannelChange;
		frequencyTable = new HashMap<String, Integer>();
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
		long minuteFromEpoch = input.getLongByField("timestamp") / 60;
		if (currMinuteFromEpoch == -1) {
			currMinuteFromEpoch = minuteFromEpoch;
		}
		if (input.contains("tick")) {
			if(staleData)
				exportData();
			else
				staleData = true;
			return;
		}
		if (mustExportData(minuteFromEpoch)) {
			exportData();
			currMinuteFromEpoch = minuteFromEpoch;
		}
		if (input.getStringByField("power") != null) {
			String power = input.getStringByField("power").toUpperCase();
			updateDataPowerChange(input, power.equals("\"OFF\""));
		} else {
			updateDataChannelChange(input);
		}

	}

	private void exportData() {
		for (Entry<String, Integer> entry : frequencyTable.entrySet()) {
			_collector.emit(new Values(entry.getKey(), entry.getValue(), currMinuteFromEpoch));
		}
		frequencyTable.clear();
	}

	private boolean mustExportData(long minuteFromEpoch) {
		return minuteFromEpoch != currMinuteFromEpoch;
	}

	/**
	 * Updates the data with the given channel change.
	 */
	protected abstract void updateDataChannelChange(Tuple tuple);

	/**
	 * Updates the data with the given power change.
	 */
	protected abstract void updateDataPowerChange(Tuple tuple, boolean off);

	protected void increaseFreqTable(String key) {
		if (!frequencyTable.containsKey(key)) {
			frequencyTable.put(key, 0);
		}
		frequencyTable.put(key, frequencyTable.get(key) + 1);
	}

	protected void decreaseFreqTable(String key) {
		frequencyTable.put(key, frequencyTable.get(key) - 1);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key", "frequency", "minute"));

	}

	@Override
	public void cleanup() {
		Repositories.closeRepositories();
		super.cleanup();
	}

}
