package com.tchaicatkovsky.jleveldb.util;

import java.util.concurrent.locks.Condition;

public class CondVar {
	Condition cond;
	
	public CondVar(Condition cond) {
		this.cond = cond;
	}
	
	public void signal() {
		cond.signal();
	}
	
	public void signalAll() {
		cond.signalAll();
	}
	
	public void await() throws InterruptedException {
		cond.await();
	}
}
