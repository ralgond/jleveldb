package org.ht.jleveldb.util;

import java.util.zip.CRC32;

public class Crc32c {
	// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
	// crc32c of some string A.  Extend() is often used to maintain the
	// crc32c of a stream of data.
	public static long extend(long initCrc, byte[] data, int offset, int n) {
		//TODO
		return 0;
	}

	// Return the crc32c of data[0,n-1]
	public static long value(byte[] data, int offset, int n) {
		CRC32 c = new CRC32();
		c.update(data, offset, n);
		return c.getValue();
	}

	static final long kMaskDelta = 0xa282ead8L;

	// Return a masked representation of crc.
	//
	// Motivation: it is problematic to compute the CRC of a string that
	// contains embedded CRCs.  Therefore we recommend that CRCs stored
	// somewhere (e.g., in files) should be masked before being stored.
	public final static long mask(long crc) {
		// Rotate right by 15 bits and add a constant.
		return ((crc >> 15) | (crc << 17)) + kMaskDelta;
	}

	// Return the crc whose masked representation is masked_crc.
	public final static long unmask(long maskedCrc) {
		long rot = maskedCrc - kMaskDelta;
		return ((rot >> 17) | (rot << 15));
	}
}
