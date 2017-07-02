package com.tchaicatkovsky.jleveldb;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Logger0 {

	public abstract void delete();
	
	public abstract void log(String format, Object... objects);
	
	public static void log0(Logger0 log, String format, Object... objects) {
		if (log != null)
			log.log(format, objects);
	}
	
	static AtomicBoolean kDebug = new AtomicBoolean(false);
	
	public static void setDebug(boolean b) {
		kDebug.set(b);
	}
	
	public static boolean getDebug() {
		return kDebug.get();
	}
	
	public static void debug(String format, Object... objects) {
		if (kDebug.get())
			System.out.printf(format, objects);
	}
}
