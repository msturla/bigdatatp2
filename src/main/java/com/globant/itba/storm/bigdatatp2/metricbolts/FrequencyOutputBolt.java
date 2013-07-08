package com.globant.itba.storm.bigdatatp2.metricbolts;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.db.MySql;
import com.globant.itba.storm.bigdatatp2.hbase.Repositories;

/**
 * Stores a base version (checkpoint) of the frequencies. Groups frequencies by VALUE.
 * Stores CHANGES per minute. When the data(changes) becomes old or too large, will persist it.
 * When data is persisted, it becomes the new checkpoint.
 *
 */
public class FrequencyOutputBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	
	private static final int TICK_DELTA = 20;
	
	private static final int MAX_CHANGES_SIZE = 10000;
	
	private static Logger LOG = Logger.getLogger(FrequencyOutputBolt.class);
	
	private Connection con;
	
	
	
	private long currentTick;
	
	
	
	//Characteristic being stored.
	private String characteristic;
	
	// Changes stored as a map
	private Map<Long, Changes> changesMap;
	
	// Changes stored as a heap (order is currMinute)
	private PriorityQueue<Changes> changesHeap;
	
	// frequencies of the checkpoint
	private Map<String, Long> checkpointFrequencies;
	private long checkpointMinute = -1;
		
	public FrequencyOutputBolt(String characteristic) {
		
		this.characteristic = characteristic;
		checkpointFrequencies = new HashMap<String, Long>();
		changesMap = new HashMap<Long, Changes>();
		changesHeap = new PriorityQueue<Changes>();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		Repositories.initRepositories();
			try {
				con = MySql.connect();
			} catch (ClassNotFoundException e) {
				LOG.error(String.format("ClassNotFoundError while connecting to mysql: %s\n",
						e.getMessage()));
			} catch (SQLException e) {
				LOG.error(String.format("SQLError while connecting to mysql: %s\n",
						e.getMessage()));
			}			
	}

	@Override
	public void execute(Tuple input) {
		if (input.contains("tick")) {
			long tick = input.getLongByField("tick");
			currentTick = tick;
			updateCheckPoint();
		} else {
			long minute = input.getLongByField("minute");
			String key = input.getStringByField("key");
			int quantity = input.getIntegerByField("frequency");
			addChanges(minute, key, quantity);			
		}
	}
	
	private boolean mustUpdateCheckpoint() {
		Changes firstChanges = changesHeap.peek();
		boolean ret = (firstChanges != null && firstChanges.tick  < currentTick - TICK_DELTA)
				|| changesHeap.size() > MAX_CHANGES_SIZE;
		return ret;
	}
	
	private void updateCheckPoint() {
		while (mustUpdateCheckpoint()) {
			Changes changes  = changesHeap.poll();
			changesMap.remove(changes.minuteFromEpoch);
			createNewCheckpoint(changes);
			persistCheckpoint(changes.minuteFromEpoch);
			checkpointMinute = changes.minuteFromEpoch;
		}
	}
	
	private void createNewCheckpoint(Changes changes) {
		for (Entry<String, Integer> entry : changes.changesMap.entrySet()) {
			if (!checkpointFrequencies.containsKey(entry.getKey())) {
				checkpointFrequencies.put(entry.getKey(), (long) 0);
			}
			long newValue = checkpointFrequencies.get(entry.getKey()) + entry.getValue();
			checkpointFrequencies.put(entry.getKey(), newValue < 0 ? 0 : newValue);
		}
	}
	
	private void persistCheckpoint(long newMinute) {
		if (checkpointMinute != -1) {
			for (long i = checkpointMinute + 1; i <= newMinute; i ++) {
				for (Entry<String, Long> entry: checkpointFrequencies.entrySet()) {
					MySql.insertRow(con, characteristic, i, entry.getKey(), entry.getValue());
				}
			}
		}
		
	}
	
	protected void addChanges(long minuteFromEpoch, String key, int quantity) {
		if (minuteFromEpoch <= checkpointMinute) {
			// Very old changes, which were already persisted.
			fixOldValues(minuteFromEpoch, key, quantity);
			return;
		}
		Changes currChanges = null;
		if (!changesMap.containsKey(minuteFromEpoch)) {
			currChanges = new Changes(minuteFromEpoch);
			changesMap.put(minuteFromEpoch, currChanges);
			changesHeap.offer(currChanges);
		} else {
			currChanges = changesMap.get(minuteFromEpoch);
		}
		currChanges.changeValue(key, quantity, currentTick);
	}
	
	private void fixOldValues(long minuteFromEpoch, String key, int quantity) {
		// Need to loockup values previous to checkpoint
		for (long i = minuteFromEpoch; i < checkpointMinute - 1; i ++) {
			try {
				long value = MySql.getValue(con, characteristic, minuteFromEpoch, key);
				long newValue = quantity + value;
				if (newValue < 0) newValue = 0;
				MySql.insertRow(con, characteristic, minuteFromEpoch, key, newValue);				
			} catch (SQLException e) {
				LOG.error("MySql error while fetching data: " + e.getMessage());
			}
		}
		// Persist new checkpoint value in DB
		long value = 0;
		if (!checkpointFrequencies.containsKey(key)) {
			value = checkpointFrequencies.get(key);
		}
		if (value < 0) value = 0;
		MySql.insertRow(con, characteristic, minuteFromEpoch, key, value);
		// Fix checkpoint in memory. This automatically propagates changes forward.
		checkpointFrequencies.put(key, value);
		
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// never emits
	}
	
	@Override
	public void cleanup() {
		try {
			con.close();
		} catch(SQLException e) { 
				LOG.error("Error while closing SQL Connection: " + e.getMessage());
		} finally {
			Repositories.closeRepositories();
			super.cleanup();
		}
	}
	
	private class Changes implements Comparable<Changes> {
		private Map<String, Integer> changesMap;
		private long minuteFromEpoch;
		
		// last tick during which this data was modified.
		private long tick = 0;
		
		public Changes(long minuteFromEpoch) {
			this.minuteFromEpoch = minuteFromEpoch;
			changesMap = new HashMap<String, Integer>();
		}
		
		public void changeValue(String value, int quantity, long tick) {
			this.tick = tick;
			if (!changesMap.containsKey(value)) {
				changesMap.put(value, 0);
			}
			int newQuantity = changesMap.get(value) + quantity;
			if (newQuantity != 0) {
				changesMap.put(value, changesMap.get(value) + quantity);				
			} else {
				changesMap.remove(value);
			}
		}

		@Override
		public int compareTo(Changes that) {
			return (int )(this.minuteFromEpoch - that.minuteFromEpoch);
		}
	}

}