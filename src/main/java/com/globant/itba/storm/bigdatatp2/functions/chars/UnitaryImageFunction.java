package com.globant.itba.storm.bigdatatp2.functions.chars;

import com.globant.itba.storm.bigdatatp2.functions.Function;

import backtype.storm.tuple.Tuple;

public class UnitaryImageFunction implements Function<Tuple, String>{

	private static final long serialVersionUID = 1L;

	@Override
	public String eval(Tuple param) {
		return "totalviewers";
	}

}
