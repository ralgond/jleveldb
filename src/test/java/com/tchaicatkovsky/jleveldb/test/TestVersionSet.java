package com.tchaicatkovsky.jleveldb.test;

import java.util.ArrayList;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.db.FileMetaData;
import com.tchaicatkovsky.jleveldb.db.VersionSetGlobal;
import com.tchaicatkovsky.jleveldb.db.format.InternalKey;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.UnpooledSlice;
import com.tchaicatkovsky.jleveldb.util.Slice;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestVersionSet {
	static class FindFileTest {
		ArrayList<FileMetaData> files = new ArrayList<>();
		boolean disjointSortedFiles;
		
		public FindFileTest() {
			disjointSortedFiles = true;
		}
		
		public void delete() {
			files.clear();
		}
		
		public void add(String smallest, String largest,
		           long smallest_seq,
		           long largest_seq) {
		    FileMetaData f = new FileMetaData();
		    f.number = files.size() + 1;
		    f.smallest = new InternalKey(new UnpooledSlice(smallest), smallest_seq, ValueType.Value);
		    f.largest = new InternalKey(new UnpooledSlice(largest), largest_seq, ValueType.Value);
		    files.add(f);
		}
		
		public int find(String key) {
		    InternalKey target = new InternalKey(new UnpooledSlice(key), 100, ValueType.Value);
		    InternalKeyComparator cmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		    return VersionSetGlobal.findFile(cmp, files, target.encode());
		}
		
		public boolean overlaps(String smallest, String largest) {
		    InternalKeyComparator cmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		    Slice s = new UnpooledSlice(smallest != null ? new UnpooledSlice(smallest) : new UnpooledSlice(""));
		    Slice l = new UnpooledSlice(largest != null ? new UnpooledSlice(largest) : new UnpooledSlice(""));
		    return VersionSetGlobal.someFileOverlapsRange(cmp, disjointSortedFiles, files,
		                                 (smallest != null ? s : null),
		                                 (largest != null ? l : null));
		}
	}
	
	@Test
	public void testEmpty() {
		FindFileTest f = new FindFileTest();
		assertEquals(0, f.find("foo"));
		assertTrue(! f.overlaps("a", "z"));
		assertTrue(! f.overlaps(null, "z"));
		assertTrue(! f.overlaps("a", null));
		assertTrue(! f.overlaps(null, null));
	}
	
	@Test
	public void testSingle() {
		FindFileTest f = new FindFileTest();
		f.add("p", "q", 100, 100);
		assertEquals(0, f.find("a"));
		assertEquals(0, f.find("p"));
		assertEquals(0, f.find("p1"));
		assertEquals(0, f.find("q"));
		assertEquals(1, f.find("q1"));
		assertEquals(1, f.find("z"));

		assertTrue(! f.overlaps("a", "b"));
		assertTrue(! f.overlaps("z1", "z2"));
		assertTrue(f.overlaps("a", "p"));
		assertTrue(f.overlaps("a", "q"));
		assertTrue(f.overlaps("a", "z"));
		assertTrue(f.overlaps("p", "p1"));
		assertTrue(f.overlaps("p", "q"));
		assertTrue(f.overlaps("p", "z"));
		assertTrue(f.overlaps("p1", "p2"));
		assertTrue(f.overlaps("p1", "z"));
		assertTrue(f.overlaps("q", "q"));
		assertTrue(f.overlaps("q", "q1"));

		assertTrue(! f.overlaps(null, "j"));
		assertTrue(! f.overlaps("r", null));
		assertTrue(f.overlaps(null, "p"));
		assertTrue(f.overlaps(null, "p1"));
		assertTrue(f.overlaps("q", null));
		assertTrue(f.overlaps(null, null));
	}
	
	@Test
	public void testMultiple() {
		FindFileTest f = new FindFileTest();
		f.add("150", "200", 100, 100);
		f.add("200", "250", 100, 100);
		f.add("300", "350", 100, 100);
		f.add("400", "450", 100, 100);
		assertEquals(0, f.find("100"));
		assertEquals(0, f.find("150"));
		assertEquals(0, f.find("151"));
		assertEquals(0, f.find("199"));
		assertEquals(0, f.find("200"));
		assertEquals(1, f.find("201"));
		assertEquals(1, f.find("249"));
		assertEquals(1, f.find("250"));
		assertEquals(2, f.find("251"));
		assertEquals(2, f.find("299"));
		assertEquals(2, f.find("300"));
		assertEquals(2, f.find("349"));
		assertEquals(2, f.find("350"));
		assertEquals(3, f.find("351"));
		assertEquals(3, f.find("400"));
		assertEquals(3, f.find("450"));
		assertEquals(4, f.find("451"));

		assertTrue(! f.overlaps("100", "149"));
		assertTrue(! f.overlaps("251", "299"));
		assertTrue(! f.overlaps("451", "500"));
		assertTrue(! f.overlaps("351", "399"));

		assertTrue(f.overlaps("100", "150"));
		assertTrue(f.overlaps("100", "200"));
		assertTrue(f.overlaps("100", "300"));
		assertTrue(f.overlaps("100", "400"));
		assertTrue(f.overlaps("100", "500"));
		assertTrue(f.overlaps("375", "400"));
		assertTrue(f.overlaps("450", "450"));
		assertTrue(f.overlaps("450", "500"));
	}
	
	@Test
	public void testMultipleNullBoundaries() {
		FindFileTest f = new FindFileTest();
		f.add("150", "200", 100, 100);
		f.add("200", "250", 100, 100);
		f.add("300", "350", 100, 100);
		f.add("400", "450", 100, 100);
		assertTrue(! f.overlaps(null, "149"));
		assertTrue(! f.overlaps("451", null));
		assertTrue(f.overlaps(null, null));
		assertTrue(f.overlaps(null, "150"));
		assertTrue(f.overlaps(null, "199"));
		assertTrue(f.overlaps(null, "200"));
		assertTrue(f.overlaps(null, "201"));
		assertTrue(f.overlaps(null, "400"));
		assertTrue(f.overlaps(null, "800"));
		assertTrue(f.overlaps("100", null));
		assertTrue(f.overlaps("200", null));
		assertTrue(f.overlaps("449", null));
		assertTrue(f.overlaps("450", null));
	}
	
	@Test
	public void testOverlapSequenceChecks() {
		FindFileTest f = new FindFileTest();
		f.add("200", "200", 5000, 3000);
		assertTrue(! f.overlaps("199", "199"));
		assertTrue(! f.overlaps("201", "300"));
		assertTrue(f.overlaps("200", "200"));
		assertTrue(f.overlaps("190", "200"));
		assertTrue(f.overlaps("200", "210"));
	}
	
	@Test
	public void testOverlappingFiles() {
		FindFileTest f = new FindFileTest();
		f.add("150", "600", 100, 100);
		f.add("400", "500", 100, 100);
		f.disjointSortedFiles = false;
		assertTrue(! f.overlaps("100", "149"));
		assertTrue(! f.overlaps("601", "700"));
		assertTrue(f.overlaps("100", "150"));
		assertTrue(f.overlaps("100", "200"));
		assertTrue(f.overlaps("100", "300"));
		assertTrue(f.overlaps("100", "400"));
		assertTrue(f.overlaps("100", "500"));
		assertTrue(f.overlaps("375", "400"));
		assertTrue(f.overlaps("450", "450"));
		assertTrue(f.overlaps("450", "500"));
	  	assertTrue(f.overlaps("450", "700"));
	  	assertTrue(f.overlaps("600", "700"));
	}
}
