package com.tchaicatkovsky.jleveldb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tchaicatkovsky.jleveldb.Logger0;


public class Logger0Impl extends Logger0 {
	
	private static Logger logger = LoggerFactory.getLogger(Logger0Impl.class);
	
	public Logger0Impl() {
		
	}
	
	public void delete() {

	}
	
	@Override
	public void log(String format, Object... objects) {
		logger.info(format, objects);
	}
}
