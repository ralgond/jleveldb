package org.ht.jleveldb.util;

public class Hash {
	//Similar to murmur hash
	public static int hash0(byte[] data, int offset, int n, int seed) {
		int m = 0xc6a4a793;
		int r = 24;
		int limit = offset + n;
		int h = seed ^ (n * m);
		
		// Pick up four bytes at a time
		while (offset + 4 <= limit) {
		    int w = Coding.decodeFixedNat32(data, offset);
		    offset += 4;
		    h += w;
		    h *= m;
		    h ^= (h >> 16);
		}
		
		// Pick up remaining bytes
		switch (limit - offset) {
		case 3:
			h += (((int)data[2] | 0x0ff) << 16);
			break;
		case 2:
			h += (((int)data[1] | 0x0ff) << 8);
			break;
		case 1:
			h += (((int)data[1] | 0x0ff));
			h *= m;
			h ^= (h >> r);
			break;
		}
		
		return h;
	}
}
