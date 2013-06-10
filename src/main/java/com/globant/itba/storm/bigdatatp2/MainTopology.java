package com.globant.itba.storm.bigdatatp2;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.globant.itba.storm.bigdatatp2.functions.Function;
import com.globant.itba.storm.bigdatatp2.functions.chars.GetCategoryListFunction;
import com.globant.itba.storm.bigdatatp2.functions.chars.GetChannelFunction;
import com.globant.itba.storm.bigdatatp2.functions.chars.GetClientTypeFunction;
import com.globant.itba.storm.bigdatatp2.functions.chars.GetFamilyGroupFunction;
import com.globant.itba.storm.bigdatatp2.functions.chars.UnitaryImageFunction;
import com.globant.itba.storm.bigdatatp2.functions.mappers.GetChannelNameFunction;
import com.globant.itba.storm.bigdatatp2.functions.mappers.IdentityFunction;
import com.globant.itba.storm.bigdatatp2.metricbolts.BoxFrequencyBolt;
import com.globant.itba.storm.bigdatatp2.metricbolts.BoxListFrequencyBolt;
import com.globant.itba.storm.bigdatatp2.metricbolts.FrequencyOutputBolt;
import com.globant.itba.storm.bigdatatp2.spouts.MessageQueueSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class MainTopology {
    
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
    	if( flags[0] ){
    		builder.setSpout("msgqueue", new MessageQueueSpout(true, parameters[0]));
    	}else{
    		builder.setSpout("msgqueue", new MessageQueueSpout());
    	}
    	
    	
    	addMetricToBuilder(builder, new GetChannelFunction(), new GetChannelNameFunction(), "ViewersPerChannel", true);
    	addMetricToBuilder(builder, new UnitaryImageFunction(), new IdentityFunction(), "TotalViewers", false);
    	addMetricToBuilder(builder, new GetClientTypeFunction(), new IdentityFunction(), "ViewersPerType", true);
    	addMetricToBuilder(builder, new GetFamilyGroupFunction(), new IdentityFunction(), "ViewersPerFamilyGroup", true);
    	addListMetricToBuilder(builder, new GetCategoryListFunction(), new IdentityFunction(), "ViewersPerCategory", true);
   
                
        Config conf = new Config();
        conf.setDebug(false);
        
        if( flags[1]) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(parameters[1], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(300000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }
    
    private static void addMetricToBuilder(TopologyBuilder builder, Function<Tuple, String> charFunc, 
    		Function<String, String> mapperFunc, String charName, boolean checkOnChannelChange) {
    	builder.setBolt(charName + "Counter", new BoxFrequencyBolt(charFunc, checkOnChannelChange), 1)
        .noneGrouping("msgqueue");
    builder.setBolt(charName + "Dumper", new FrequencyOutputBolt(mapperFunc, charName), 3)
    	.fieldsGrouping(charName + "Counter", new Fields("key"));
    }
    
    private static void addListMetricToBuilder(TopologyBuilder builder, Function<Tuple, List<String>> charFunc, 
    		Function<String, String> mapperFunc, String charName, boolean checkOnChannelChange) {
    	builder.setBolt(charName + "Counter", new BoxListFrequencyBolt(charFunc, checkOnChannelChange), 1)
        .noneGrouping("msgqueue");
    builder.setBolt(charName + "Dumper", new FrequencyOutputBolt(mapperFunc, charName), 1)
    	.noneGrouping(charName + "Counter");
    }
}