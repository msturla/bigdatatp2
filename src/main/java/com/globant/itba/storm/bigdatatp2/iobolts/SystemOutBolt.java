package com.globant.itba.storm.bigdatatp2.iobolts;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

//print data in stdout
public class SystemOutBolt extends BaseBasicBolt {

		private static final long serialVersionUID = 1L;


		@Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String results = tuple.getStringByField("data");
            System.out.println(results);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
        }
        
    }