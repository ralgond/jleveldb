package com.tchaicatkovsky.jleveldb;

public interface DBTools {

	/**
	 * Destroy the contents of the specified database.</br>
	 * Be very careful using this method.
	 * 
	 * @param name
	 * @param options
	 * @return
	 */
	Status destroyDB(String dbname, Options options) throws Exception;

	/**
	 * If a DB cannot be opened, you may attempt to call this method to
	 * resurrect as much of the contents of the database as possible.</br>
	 * Some data may be lost, so be careful when calling this function 
	 * on a database that contains important information.
	 * 
	 * @param dbname
	 * @param options
	 * @return
	 */
	Status repairDB(String dbname, Options options) throws Exception;
}
