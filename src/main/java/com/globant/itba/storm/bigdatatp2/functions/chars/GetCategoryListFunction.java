package com.globant.itba.storm.bigdatatp2.functions.chars;

import java.io.IOException;
import java.util.List;

import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.functions.Function;
import com.globant.itba.storm.bigdatatp2.hbase.ChannelRepository;

public class GetCategoryListFunction implements Function<Tuple, List<String>> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<String> eval(Tuple param) {
		//given the channel of this tuple, return the list of categories
		try {
			return ChannelRepository.getChannelCategories(param.getStringByField("channel"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}
}
