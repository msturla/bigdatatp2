package com.globant.itba.storm.bigdatatp2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.globant.itba.storm.bigdatatp2.hbase.ChannelRepository;
import com.globant.itba.storm.bigdatatp2.iobolts.SystemOutBolt;
import com.globant.itba.storm.bigdatatp2.metricbolts.ViewersByChannelBolt;
import com.globant.itba.storm.bigdatatp2.spouts.MessageQueueSpout;
import com.globant.itba.storm.bigdatatp2.spouts.PeriodicSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class MainTopology {
    
    public static void main(String[] args) throws Exception {
    	
    	Configuration hbaseConf = HBaseConfiguration.create();
    	hbaseConf.addResource("/home/hadoop/hbase-0.94.6.1/conf/hbase-site.xml");
    	hbaseConf.set("hbase.rootdir", "hdfs://hadoop-2013-namenode:9000/hbase");
    	hbaseConf.set("hbase.zookeeper.quorum", "hadoop-2013-datanode-1");
    	hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
    	
    	TopologyBuilder builder = new TopologyBuilder();
    	try {
    		// init repos
    		ChannelRepository.setConf(hbaseConf);
    		
    		//build the topology
    		builder.setSpout("periodic", new PeriodicSpout(1));
    	    builder.setSpout("msgqueue", new MessageQueueSpout());
    	    builder.setBolt("viewersByChannel", new ViewersByChannelBolt())
    	        .noneGrouping("msgqueue")
    	        .noneGrouping("periodic");
    	        // TODO change stdout to a bolt that creates a file or something for the UI
    	    builder.setBolt("stdout", new SystemOutBolt())
    	        .noneGrouping("viewersByChannel");
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	
        
        
        
       
                
        Config conf = new Config();
        conf.setDebug(false);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }
}