package org.ht.jleveldb.test;

import static org.junit.Assert.assertEquals;

import org.ht.jleveldb.util.Hash;
import org.junit.Test;

public class TestHash {
	//TODO
	@Test
	public void testSignedUnsignedIssue() {
		byte[] data1 = new byte[] {(byte)0x62};
		byte[] data2 = new byte[] {(byte)0xc3, (byte)0x97};
		byte[] data3 = new byte[] {(byte)0xe2, (byte)0x99, (byte)0xa5};
		byte[] data4 = new byte[] {(byte)0xe1, (byte)0x80, (byte)0xb9, (byte)0x32};
		byte[] data5 = new byte[] {
				(byte)0x01, (byte)0xc0, (byte)0x00, (byte)0x00,
				(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
				(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
				(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
				(byte)0x14, (byte)0x00, (byte)0x00, (byte)0x00,
				(byte)0x00, (byte)0x00, (byte)0x04, (byte)0x00,
				(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x14,
				(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x18,
				(byte)0x28, (byte)0x00, (byte)0x00, (byte)0x00,
				(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
				(byte)0x02, (byte)0x00, (byte)0x00, (byte)0x00,
			    (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
			  };
		
		assertEquals(Hash.hash0(null, 0, 0, 0xbc9f1d34L), 0xbc9f1d34L);
		assertEquals(Hash.hash0(data1, 0, data1.length, 0xbc9f1d34L), 0xef1345c4L);
		assertEquals(Hash.hash0(data2, 0, data2.length, 0xbc9f1d34L), 0x5b663814L);
		assertEquals(Hash.hash0(data3, 0, data3.length, 0xbc9f1d34L), 0x323c078fL);
		assertEquals(Hash.hash0(data4, 0, data4.length, 0xbc9f1d34L), 0xed21633aL);
		assertEquals(Hash.hash0(data5, 0, data5.length, 0x12345678L), 0xf333dabbL);
	}
}
