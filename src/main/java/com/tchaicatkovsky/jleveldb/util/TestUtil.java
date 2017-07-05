package com.tchaicatkovsky.jleveldb.util;

public class TestUtil {
	public static Slice randomString(Random0 rnd, int len, ByteBuf dst) {
		dst.resize(len);
		for (int i = 0; i < len; i++) {
			dst.setByte(i, (byte)(0x20+rnd.uniform(95)));  // ' ' .. '~'
		}
		return new UnpooledSlice(dst);
	}

	public static ByteBuf randomKey(Random0 rnd, int len) {
		// Make sure to generate a wide variety of characters so we
		// test the boundary conditions for short-key optimizations.
		byte[] kTestChars = new byte[] {
		    //'\0', '\1', 'a', 'b', 'c', 'd', 'e', '\xfd', '\xfe', '\xff'
			(byte)0x00, (byte)0x01, (byte)0x61, (byte)0x62, (byte)0x63, (byte)0x64, (byte)0x65, 
			(byte)0xfd, (byte)0xfe, (byte)0xff
		};
		
		ByteBuf result = ByteBufFactory.newUnpooled() ;
		for (int i = 0; i < len; i++) {
			result.addByte(kTestChars[(int)rnd.uniform(kTestChars.length)]);;
		}
		return result;
	}


	public static Slice compressibleString(Random0 rnd, double compressed_fraction,
		                                int len, ByteBuf dst) {
		int raw = (int)(len * compressed_fraction);
		if (raw < 1) 
			raw = 1;
		ByteBuf raw_data = ByteBufFactory.newUnpooled();
		randomString(rnd, raw, raw_data);

		// Duplicate the random data until we have filled "len" bytes
		dst.clear();
		while (dst.size() < len) {
			dst.append(raw_data);
		}
		dst.resize(len);
		return new UnpooledSlice(dst);
	}
	
	public static String tmpDir() {
		return "./data/test";
	}
	
	public static int randomSeed() {
		return 301;
	}
	
	public static String makeString(int n, char c) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < n; i++)
			sb.append(c);
		return sb.toString();
	}
}
