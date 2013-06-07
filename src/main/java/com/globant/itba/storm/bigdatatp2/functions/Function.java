package com.globant.itba.storm.bigdatatp2.functions;

import java.io.Serializable;

public interface Function<P, T> extends Serializable {

	public T eval(P param);
}
