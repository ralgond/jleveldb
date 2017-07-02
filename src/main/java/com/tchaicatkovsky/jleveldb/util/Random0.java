package com.tchaicatkovsky.jleveldb.util;

/**
 * A very simple random number generator.  Not especially good at
 * generating truly random bits, but good enough for our needs in this
 * package.
 */
public class Random0 {
	final static long kUint32Mask = 0xFFFFFFFFL;
	
	public long seed;
	
	public Random0(long seed0) {
		seed = seed0 & 0x7fffffffL;
		// Avoid bad seeds.
	    if (seed == 0 || seed == 2147483647L) {
	    	seed = 1;
	    }
	}
	
	final static long M = 2147483647L;   // 2^31-1
    final static long A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
	public long next() {
		// We are computing
	    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
	    //
	    // seed_ must not be zero or M, or else all subsequent computed values
	    // will be zero or M respectively.  For all other values, seed_ will end
	    // up cycling through every number in [1,M-1]
		long product = seed * A;
		
		// Compute (product % M) using the fact that ((x << 31) % M) == x.
		seed = (((product >> 31) + (product & M)) & kUint32Mask);
		
		// The first reduction may overflow by 1 bit, so we may need to
	    // repeat.  mod == M is not possible; using > allows the faster
	    // sign-bit-based test.
		if (seed > M) {
			seed -= M;
		}
		return seed;
	}
	
	// Returns a uniformly distributed value in the range [0..n-1]
	// REQUIRES: n > 0
	public long uniform(int size) {
		return ((next() % size) & kUint32Mask);
	}
	
	// Randomly returns true ~"1/n" of the time, and false otherwise.
	// REQUIRES: n > 0
	public boolean oneIn(int n) { 
		return (next() % n) == 0; 
	}
	  
	// Skewed: pick "base" uniformly from range [0,max_log] and then
	// return "base" random bits.  The effect is to pick a number in the
	// range [0,2^max_log-1] with exponential bias towards smaller numbers.
	public long skewed(int maxLog) {
	    return uniform(1 << uniform(maxLog + 1));
	}
}
