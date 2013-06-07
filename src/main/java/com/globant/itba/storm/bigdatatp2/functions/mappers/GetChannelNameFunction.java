package com.globant.itba.storm.bigdatatp2.functions.mappers;

import java.io.IOException;

import com.globant.itba.storm.bigdatatp2.functions.Function;
import com.globant.itba.storm.bigdatatp2.hbase.ChannelRepository;

public class GetChannelNameFunction implements Function<String, String>{

	private static final long serialVersionUID = 1L;

	@Override
	public String eval(String param) {
		try {
			return ChannelRepository.getChannelName(param);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
