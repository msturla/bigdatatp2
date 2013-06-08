package com.globant.itba.storm.bigdatatp2.functions.chars;

import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.functions.Function;

public class GetClientTypeFunction implements Function<Tuple, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String eval(Tuple param) {
		// TODO return the client type of this tuple
		return null;
	}

}
