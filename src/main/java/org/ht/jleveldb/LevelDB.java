package org.ht.jleveldb;

import java.util.ServiceLoader;

import org.ht.jleveldb.util.Object0;

public class LevelDB {
	
	private static DBTools dbtools = null;
	private static Env defaultEnv = null;
	private static DB defaultDB = null; 
	
	public static Status newDB(Options options, String name, Object0<DB> dbOut) throws Exception {
		if (defaultDB == null) {
			ServiceLoader<DB> serviceLoader = ServiceLoader.load(DB.class);
			for (DB db0 : serviceLoader) {
				defaultDB = db0;
				break;
			}
		}
		DB newOne = defaultDB.getClass().newInstance();
		Status s = newOne.open(options, name);
		dbOut.setValue(newOne);
		
		return s;
	}
	
	public static Status destroyDB(String dbname, Options options) throws Exception {
		if (dbtools == null){ 
			ServiceLoader<DBTools> serviceLoader = ServiceLoader.load(DBTools.class);
			for (DBTools dbt0 : serviceLoader) {
				dbtools = dbt0;
				break;
			}
		}
		return dbtools.destroyDB(dbname, options);
	}
	
	public static Status repairDB(String dbname, Options options) throws Exception {
		if (dbtools == null){ 
			ServiceLoader<DBTools> serviceLoader = ServiceLoader.load(DBTools.class);
			for (DBTools dbt0 : serviceLoader) {
				dbtools = dbt0;
				break;
			}
		}
		return dbtools.repairDB(dbname, options);
	}
	
	public static Env defaultEnv() {
		if (defaultEnv == null) {
		ServiceLoader<Env> serviceLoader = ServiceLoader.load(Env.class);
			for (Env env0 : serviceLoader) {
				defaultEnv = env0;
				break;
			}
		}
		return defaultEnv;
	}
}
