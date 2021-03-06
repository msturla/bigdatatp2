package com.globant.itba.storm.bigdatatp2;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.functions.Function;
import com.globant.itba.storm.bigdatatp2.functions.chars.GetCategoryListFunction;
import com.globant.itba.storm.bigdatatp2.functions.chars.GetChannelFunction;
import com.globant.itba.storm.bigdatatp2.functions.chars.GetClientTypeFunction;
import com.globant.itba.storm.bigdatatp2.functions.chars.GetFamilyGroupFunction;
import com.globant.itba.storm.bigdatatp2.functions.chars.UnitaryImageFunction;
import com.globant.itba.storm.bigdatatp2.functions.mappers.GetChannelNameFunction;
import com.globant.itba.storm.bigdatatp2.metricbolts.AbstractFrequencyBolt;
import com.globant.itba.storm.bigdatatp2.metricbolts.BoxFrequencyBolt;
import com.globant.itba.storm.bigdatatp2.metricbolts.BoxListFrequencyBolt;
import com.globant.itba.storm.bigdatatp2.metricbolts.CharCachingFrequencyOutputBolt;
import com.globant.itba.storm.bigdatatp2.metricbolts.FrequencyOutputBolt;
import com.globant.itba.storm.bigdatatp2.spouts.MessageQueueSpout;
import com.globant.itba.storm.bigdatatp2.spouts.PeriodicSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class MainTopology {
	
	private static final int BOX_PARALLELISM = 3;
	
	private static final int OUTPUT_PARALLELISM = 3;
	
	private static final int SECONDS_PER_TICK = 3;
    
    public static void main(String[] args) throws Exception {
    	
    	TopologyBuilder builder = new TopologyBuilder();
    	boolean flags[] = new boolean[2];
    	String parameters[] = new String[2];
    	
    	if( args != null && args.length > 0){
    		for( String arg: args){
    			if( arg.toLowerCase().startsWith("--topologyname=" )){
    				flags[0] = true;
    				parameters[0] = arg.substring(15);
    			}else if( arg.toLowerCase().startsWith("--msgqueuename=" ) ){
    				flags[1] = true;
    				parameters[1] = arg.substring(15);
    			}
    		}
    	}
    	if( flags[1] ){
    		builder.setSpout("msgqueue", new MessageQueueSpout(parameters[1]));
    	}else{
    		builder.setSpout("msgqueue", new MessageQueueSpout());
    	}
    	builder.setSpout("ticker", new PeriodicSpout(SECONDS_PER_TICK));
    	
    	
    	addMetricToBuilder(builder, new GetChannelFunction(), new GetChannelNameFunction(), "ViewersPerChannel", true);
    	addMetricToBuilder(builder, new UnitaryImageFunction(), null, "TotalViewers", false);
    	addMetricToBuilder(builder, new GetClientTypeFunction(), null, "ViewersPerType", true);
    	addMetricToBuilder(builder, new GetFamilyGroupFunction(), null, "ViewersPerFamilyGroup", true);
    	addListMetricToBuilder(builder, new GetCategoryListFunction(), null, "ViewersPerCategory", true);
   
                
        Config conf = new Config();
        conf.setDebug(false);
        
        if( flags[0]) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(parameters[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("local", conf, builder.createTopology());
        }
    }
    
    private static void addMetricToBuilder(TopologyBuilder builder, Function<Tuple, String> charFunc, 
    		Function<String, String> mapperFunc, String charName, boolean checkOnChannelChange) {
    	FrequencyOutputBolt outBolt = null;
    	if (mapperFunc == null) {
    		outBolt = new FrequencyOutputBolt(charName);
    	} else {
    		outBolt = new CharCachingFrequencyOutputBolt(charName, mapperFunc);
    	}
    	addGenericMetricToBuilder(builder, new BoxFrequencyBolt(charFunc, checkOnChannelChange), outBolt, charName);
    }
    
    private static void addListMetricToBuilder(TopologyBuilder builder, Function<Tuple, List<String>> charFunc, 
    		Function<String, String> mapperFunc, String charName, boolean checkOnChannelChange) {
    	FrequencyOutputBolt outBolt = null;
    	if (mapperFunc == null) {
    		outBolt = new FrequencyOutputBolt(charName);
    	} else {
    		outBolt = new CharCachingFrequencyOutputBolt(charName, mapperFunc);
    	}
    	addGenericMetricToBuilder(builder, new BoxListFrequencyBolt(charFunc, checkOnChannelChange), outBolt, charName);
    }
    
    private static void addGenericMetricToBuilder(TopologyBuilder builder, AbstractFrequencyBolt bolt, FrequencyOutputBolt outBolt,
    		String charName) {
    	builder.setBolt(charName + "Counter", bolt, BOX_PARALLELISM)
        .fieldsGrouping("msgqueue", new Fields("box_id"))
        .noneGrouping("ticker");
    builder.setBolt(charName + "Dumper", outBolt, OUTPUT_PARALLELISM)
    	.fieldsGrouping(charName + "Counter", new Fields("key"))
    	 .noneGrouping("ticker");
    }
}