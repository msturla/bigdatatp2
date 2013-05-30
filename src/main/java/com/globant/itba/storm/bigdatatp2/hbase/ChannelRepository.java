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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ChannelRepository {
	
	private static HTable channelTable;
	
	public static void setConf(Configuration conf) throws IOException {
		channelTable = new HTable(conf, "channel");
	}
	
	public static String getChannelName(Integer channel) throws IOException {
		
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(channel.toString())));
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

}
