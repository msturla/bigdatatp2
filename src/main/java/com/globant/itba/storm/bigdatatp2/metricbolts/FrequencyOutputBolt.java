package com.globant.itba.storm.bigdatatp2.metricbolts;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.db.MySql;
import com.globant.itba.storm.bigdatatp2.functions.Function;
import com.globant.itba.storm.bigdatatp2.functions.mappers.IdentityFunction;
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
	
	//logging only
	private long highestMinute = -1;
	
	private Connection con;
	
	// Given the characeristic ID, maps to a "friendly" name.
	// If not required, just use identity function
	private final Function<String, String> mapperFunction;
	
	private long currentTick;
	
	// Caching of mappings.
	private Map<String, String> mappings;
	
	//Characteristic being stored.
	private String characteristic;
	
	// Changes stored as a map
	private Map<Long, Changes> changesMap;
	
	// Changes stored as a heap (order is currMinute)
	private PriorityQueue<Changes> changesHeap;
	
	// frequencies of the checkpoint
	private Map<String, Integer> checkpointFrequencies;
	private long checkpointMinute = -1;
		
	public FrequencyOutputBolt(Function<String, String> func, String characteristic) {
		this.mapperFunction = func;
		mappings = new HashMap<String, String>();
		this.characteristic = characteristic;
		checkpointFrequencies = new HashMap<String, Integer>();
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
			check();
			updateCheckPoint();
		} else {
			long minute = input.getLongByField("minute");
			String key = input.getStringByField("key");
			int quantity = input.getIntegerByField("frequency");
			addChanges(minute, getMapping(key), quantity);			
		}
	}
	
	private boolean mustUpdateCheckpoint() {
		Changes firstChanges = changesHeap.peek();
		boolean ret = (firstChanges != null && firstChanges.tick  < currentTick - TICK_DELTA && changesHeap.size() > 1)
				|| changesHeap.size() > MAX_CHANGES_SIZE;
		return ret;
	}
	
	private void check() {
		Set<Long> seen = new HashSet<Long>();
		for (Changes changes : changesHeap) {
			if (seen.contains(changes.minuteFromEpoch)) {
				System.out.println("GGGGG");
			}
			seen.add(changes.minuteFromEpoch);
		}
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
				checkpointFrequencies.put(entry.getKey(), 0);
			}
			int newValue = checkpointFrequencies.get(entry.getKey()) + entry.getValue();
			checkpointFrequencies.put(entry.getKey(), newValue < 0 ? 0 : newValue);
		}
	}
	
	private void persistCheckpoint(long newMinute) {
		if (checkpointMinute != -1) {
			for (long i = checkpointMinute + 1; i <= newMinute; i ++) {
				for (Entry<String, Integer> entry: checkpointFrequencies.entrySet()) {
					MySql.insertRow(con, characteristic, i, getMapping(entry.getKey()), entry.getValue());
				}
			}
		}
		
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
	
	private void addChanges(long minuteFromEpoch, String key, int quantity) {
		if (minuteFromEpoch <= checkpointMinute) {
			LOG.warn(String.format("Got very old changes. min: %d, curr: %d, highest: %d. Discarding\n",
					minuteFromEpoch, checkpointMinute, highestMinute));
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

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// never emits
	}
	
	@Override
	public void cleanup() {
		try {
			con.close();
		} catch(SQLException e) { 
				e.printStackTrace();
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
			if (minuteFromEpoch > highestMinute) {
				highestMinute = minuteFromEpoch;
			}
		}
		
		public void changeValue(String value, int quantity, long tick) {
			this.tick = tick;
			if (!changesMap.containsKey(value)) {
				changesMap.put(value, 0);
			}
			changesMap.put(value, changesMap.get(value) + quantity);
		}

		@Override
		public int compareTo(Changes that) {
			return (int )(this.minuteFromEpoch - that.minuteFromEpoch);
		}
	}

}