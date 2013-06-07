package com.globant.itba.storm.bigdatatp2.functions.chars;

import java.util.List;

import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.functions.Function;

public class GetCategoryListFunction implements Function<Tuple, List<String>> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<String> eval(Tuple param) {
		//TODO given the channel of this tuple, return the list of categories
		return null;
	}

	
	
}
