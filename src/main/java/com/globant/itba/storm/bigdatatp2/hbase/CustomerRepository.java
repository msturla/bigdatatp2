package com.globant.itba.storm.bigdatatp2.hbase;

import java.io.IOException;
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

public class CustomerRepository {
	
	private static HTable customerTable;
	private static Map<String, String> cachedClientTypes;
	private static Map<String, String> cachedFamilyGroups;
	
	public static synchronized void setConf(Configuration conf) throws IOException {
		if (customerTable == null) {
			customerTable = new HTable(conf, "customer");
			cachedClientTypes = new HashMap<String, String>();
			cachedFamilyGroups = new HashMap<String, String>();
		}
	}
	
	public static String getClientType(String box_id) throws IOException {
		if (cachedClientTypes.containsKey(box_id)) {
			return cachedClientTypes.get(box_id);
		}
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(box_id)));
		Scan scan = new Scan();
		scan.setFilter(rowFilter);
		ResultScanner resultScanner = customerTable.getScanner(scan);
		try {
			Iterator<Result> results = resultScanner.iterator();
			if (results.hasNext()) {
				Result result = results.next();
				List<KeyValue> columns = result.list();
				for (KeyValue column : columns) {
					if (new String(column.getQualifier()).equals("client_type")) {
						if (cachedClientTypes.size() < 5000) {
							cachedClientTypes.put(box_id, column.getValue().toString());
						}
						return new String(column.getValue());
					}
				}
			}			
		} finally {
			resultScanner.close();
		}
	    return null;
	}
	
	public static String getFamilyGroup(String box_id) throws IOException {
		if (cachedFamilyGroups.containsKey(box_id)) {
			return cachedFamilyGroups.get(box_id);
		}
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(box_id)));
		Scan scan = new Scan();
		scan.setFilter(rowFilter);
		ResultScanner resultScanner = customerTable.getScanner(scan);
		try {
			Iterator<Result> results = resultScanner.iterator();
			if (results.hasNext()) {
				Result result = results.next();
				List<KeyValue> columns = result.list();
				for (KeyValue column : columns) {
					if (new String(column.getQualifier()).equals("family_group")) {
						if (cachedFamilyGroups.containsKey(box_id)) {
							return cachedFamilyGroups.get(box_id);
						}
						return new String(column.getValue());
					}
				}
			}			
		} finally {
			resultScanner.close();
		}
	    return null;
	}
}
