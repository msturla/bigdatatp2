package com.globant.itba.storm.bigdatatp2.functions.chars;

import java.io.IOException;

import backtype.storm.tuple.Tuple;

import com.globant.itba.storm.bigdatatp2.functions.Function;
import com.globant.itba.storm.bigdatatp2.hbase.CustomerRepository;

public class GetFamilyGroupFunction implements Function<Tuple, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String eval(Tuple param) {
		//return the family group of the box id of the tuple
		try {
			return CustomerRepository.getFamilyGroup(String.valueOf(param.getLongByField("box_id")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
