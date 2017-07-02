package com.tchaicatkovsky.jleveldb.test;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.LevelDB;

public class TestLevelDB {
	@Test
	public void test01() {
		Env env = LevelDB.defaultEnv();
		assertNotNull(env);
	}
}
