package org.ht.jleveldb.test;

import static org.junit.Assert.assertNotNull;

import org.ht.jleveldb.Env;
import org.ht.jleveldb.LevelDB;
import org.junit.Test;

public class TestLevelDB {
	@Test
	public void test01() {
		Env env = LevelDB.defaultEnv();
		assertNotNull(env);
	}
}
