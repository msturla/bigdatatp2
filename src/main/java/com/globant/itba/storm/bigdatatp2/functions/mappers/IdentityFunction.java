package com.globant.itba.storm.bigdatatp2.functions.mappers;

import com.globant.itba.storm.bigdatatp2.functions.Function;

public class IdentityFunction implements Function<String, String> {

	private static final long serialVersionUID = 2095474640628925198L;

	@Override
	public String eval(String param) {
		return param;
	}

}
