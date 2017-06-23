package org.ht.jleveldb.util;

import org.ht.jleveldb.Logger0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
