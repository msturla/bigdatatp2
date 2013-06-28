package com.globant.itba.storm.bigdatatp2.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ChannelRepository {
	
	private static HTable channelTable;
	private static Map<String, List<String>> cachedCategories;
	
	private static boolean closed = false;
	
	public static synchronized void setConf(Configuration conf) throws IOException {
		if (channelTable == null) {
			channelTable = new HTable(conf, "channel");
			cachedCategories = new HashMap<String, List<String>>();
		}
	}
	
	public static synchronized void close() throws IOException {
		if (!closed) {
			channelTable.close();
			closed = true;
		}
	}
	
	public static String getChannelName(String channel) throws IOException {
		
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(channel)));
		Scan scan = new Scan();
		scan.setFilter(rowFilter);
		ResultScanner resultScanner = channelTable.getScanner(scan);
		try {
			Iterator<Result> results = resultScanner.iterator();
			if (results.hasNext()) {
				Result result = results.next();
				List<KeyValue> columns = result.list();
				for (KeyValue column : columns) {
					if (new String(column.getQualifier()).equals("name")) {
						return new String(column.getValue());
					}
				}
			}			
		} finally {
			resultScanner.close();
		}
	    return null;
	}
	
	public static List<String> getChannelCategories(String channel) throws IOException {
		if (cachedCategories.containsKey(channel)) {
			return cachedCategories.get(channel);
		}
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(channel)));
		Scan scan = new Scan();
		scan.setFilter(rowFilter);
		ResultScanner resultScanner = channelTable.getScanner(scan);
		try {
			Iterator<Result> results = resultScanner.iterator();
			if (results.hasNext()) {
				Result result = results.next();
				List<KeyValue> columns = result.list();
				for (KeyValue column : columns) {
					if (new String(column.getQualifier()).equals("categories")) {
						List<String> ret = Arrays.asList((new String(column.getValue()).split(",")));
						 if (cachedCategories.size() < 5000) {
							 cachedCategories.put(channel, ret);
						 }
						 return ret;
					}
				}
			}			
		} finally {
			resultScanner.close();
		}
	    return null;
	}

}
