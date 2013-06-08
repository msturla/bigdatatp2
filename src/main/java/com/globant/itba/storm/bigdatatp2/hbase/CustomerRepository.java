package com.globant.itba.storm.bigdatatp2.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class CustomerRepository {
	
	private static HTable customerTable;
	
	public static synchronized void setConf(Configuration conf) throws IOException {
		if (customerTable == null) {
			customerTable = new HTable(conf, "customer");
		}
	}
	
	public static String getClientType(String box_id) throws IOException {
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
