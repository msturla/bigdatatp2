package com.globant.itba.storm.bigdatatp2.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Repositories {
	
	public static void initRepositories() {
		Configuration hbaseConf = HBaseConfiguration.create();
    	hbaseConf.set("hbase.rootdir", "hdfs://hadoop-2013-namenode:9000/hbase");
    	hbaseConf.set("hbase.zookeeper.quorum", "hadoop-2013-datanode-1");
    	hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
    	try {
    		//add extra repositories here
    		ChannelRepository.setConf(hbaseConf);
    		CustomerRepository.setConf(hbaseConf);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
	}

}
