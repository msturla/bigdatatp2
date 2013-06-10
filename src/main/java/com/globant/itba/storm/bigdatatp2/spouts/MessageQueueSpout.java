package com.globant.itba.storm.bigdatatp2.spouts;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageQueueSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	private static String URL = "tcp://hadoop-2013-datanode-2:61616";
	private String queueName;
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;
	
	public static Logger LOG = Logger.getLogger(TestWordSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    

    public MessageQueueSpout() {
        this(true, "cheese");
    }

    public MessageQueueSpout(boolean isDistributed, String queueName) {
        _isDistributed = isDistributed;
        this.queueName = queueName;
    }
        
    @SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(URL);
        System.out.println("Preparing con");
        try {
        	System.out.println("creating con");
			connection = connectionFactory.createConnection();
			System.out.println("starting");
			connection.start();
			System.out.println("creating session");
			session = connection.createSession(false,
	                Session.AUTO_ACKNOWLEDGE);
			System.out.println("creating queue");
			Destination queue = session.createQueue("cheese");
			System.out.println("creating consumer");
			consumer = session.createConsumer(queue);
			System.out.println("created consumer. done setting up mq");
		} catch (JMSException e) {
			System.out.println("fuck");
			e.printStackTrace();
		}
        
    }
    
    public void close() {
        try {
        	consumer.close();
        	session.close();
        	connection.close();
        } catch (JMSException e) {
        	
        }
    }
        
    public void nextTuple() {
    	//TODO read these from activemq
    	//String json = getRandomJsonLine();
    	try{
    		Message msg = consumer.receive();
    		TextMessage textmsg = (TextMessage) msg;
    		String json = textmsg.getText();
    		long box_id = Long.valueOf(getField(json, "box_id"));
    		String channelString = getField(json, "channel");
    		String power = getField(json, "power");
    		long timestamp = Long.valueOf(getField(json, "timestamp"));
    		_collector.emit(new Values(box_id, channelString, power, timestamp, json));    		
    	} catch (JMSException e) {
    		System.out.printf("Error while reading from JMS: %s\n", e.getMessage());
    	}
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    private String getField(String line, String name) {
		line = line.replace("{", "").replace("}", "");
		// Not very defensive or elegant, but it works with our input
		String[] jsonFields = line.split(",");
		for (String field : jsonFields) {
			if (field.contains(name)) {
				return field.split(":")[1];
			}
		}
		return null;
	}
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("box_id", "channel", "power", "timestamp", "json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
    
}