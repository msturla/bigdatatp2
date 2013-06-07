package com.globant.itba.storm.bigdatatp2.functions.chars;

import com.globant.itba.storm.bigdatatp2.functions.Function;

import backtype.storm.tuple.Tuple;

public class GetChannelFunction implements Function<Tuple, String> {

	private static final long serialVersionUID = 1438575868808084322L;

	@Override
	public String eval(Tuple tuple) {
		if (tuple.contains("channel")) {
			return tuple.getStringByField("channel");
		}
		return null;
	}
	

}
