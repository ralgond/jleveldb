package com.tchaicatkovsky.jleveldb.test;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.db.VersionEdit;
import com.tchaicatkovsky.jleveldb.db.format.InternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.DefaultSlice;

import static org.junit.Assert.assertTrue;

public class TestVersionEdit {
	static void testEncodeDecode(VersionEdit edit) {
		  ByteBuf encoded = ByteBufFactory.defaultByteBuf();
		  ByteBuf encoded2 =  ByteBufFactory.defaultByteBuf();
		  edit.encodeTo(encoded);
		  VersionEdit parsed = new VersionEdit();
		  Status s = parsed.decodeFrom(new DefaultSlice(encoded));
		  assertTrue(s.ok());
		  parsed.encodeTo(encoded2);
		  assertTrue(encoded.equals(encoded2));
	}
	
	@Test
	public void testEncodeDecode() {
		long kBig = 1L << 50;

		VersionEdit edit = new VersionEdit();
		for (int i = 0; i < 4; i++) {
		    testEncodeDecode(edit);
		    edit.addFile(3, kBig + 300 + i, kBig + 400 + i,
		                 new InternalKey(new DefaultSlice("foo"), kBig + 500 + i, ValueType.Value),
		                 new InternalKey(new DefaultSlice("zoo"), kBig + 600 + i, ValueType.Deletion),
		                 10);
		    edit.deleteFile(4, kBig + 700 + i);
		    edit.setCompactPointer(i, new InternalKey(new DefaultSlice("x"), kBig + 900 + i, ValueType.Value));
		}

		edit.setComparatorName("foo");
		edit.setLogNumber(kBig + 100);
		edit.setNextFile(kBig + 200);
		edit.setLastSequence(kBig + 1000);
		testEncodeDecode(edit);
	}
}
