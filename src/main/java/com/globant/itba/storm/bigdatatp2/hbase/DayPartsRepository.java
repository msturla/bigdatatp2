package com.globant.itba.storm.bigdatatp2.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.conf.Configuration;

public class DayPartsRepository {
	
	private static HTable dayPartsTable;
	
	public static void setConf(Configuration conf) throws IOException {
		dayPartsTable = new HTable(conf, "day_parts");
	}
}
