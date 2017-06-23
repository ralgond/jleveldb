package org.ht.jleveldb.test;

import org.ht.jleveldb.Status;
import org.ht.jleveldb.db.VersionEdit;
import org.ht.jleveldb.db.format.InternalKey;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.Slice;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestVersionEdit {
	static void testEncodeDecode(VersionEdit edit) {
		  ByteBuf encoded = ByteBufFactory.defaultByteBuf();
		  ByteBuf encoded2 =  ByteBufFactory.defaultByteBuf();
		  edit.encodeTo(encoded);
		  VersionEdit parsed = new VersionEdit();
		  Status s = parsed.decodeFrom(new Slice(encoded));
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
		                 new InternalKey(new Slice("foo"), kBig + 500 + i, ValueType.Value),
		                 new InternalKey(new Slice("zoo"), kBig + 600 + i, ValueType.Deletion));
		    edit.deleteFile(4, kBig + 700 + i);
		    edit.setCompactPointer(i, new InternalKey(new Slice("x"), kBig + 900 + i, ValueType.Value));
		}

		edit.setComparatorName("foo");
		edit.setLogNumber(kBig + 100);
		edit.setNextFile(kBig + 200);
		edit.setLastSequence(kBig + 1000);
		testEncodeDecode(edit);
	}
}
