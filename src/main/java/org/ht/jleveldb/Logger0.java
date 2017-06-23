package org.ht.jleveldb;

public abstract class Logger0 {

	public abstract void delete();
	
	public abstract void log(String format, Object... objects);
	
	public static void log0(Logger0 log, String format, Object... objects) {
		if (log != null)
			log.log(format, objects);
	}
}
