package org.ht.jleveldb.test;


import org.ht.jleveldb.db.VersionSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;

public class TestUpperBound {
	@Test
	public void test01() {
		ArrayList<Integer> l = new ArrayList<>();
		l.add(3);
		l.add(6);
		l.add(9);
		l.add(12);
		l.add(15);
		l.add(18);
		
		Comparator<Integer> cmp = new Comparator<Integer>() {
			public int compare(Integer a, Integer b) {
				return Integer.compare(a, b);
			}
		};
		
		assertEquals(VersionSet.upperBound(l, 0, cmp), 0);
		assertEquals(VersionSet.upperBound(l, 3, cmp), 1);
		assertEquals(VersionSet.upperBound(l, 4, cmp), 1);
		assertEquals(VersionSet.upperBound(l, 6, cmp), 2);
		assertEquals(VersionSet.upperBound(l, 7, cmp), 2);
		assertEquals(VersionSet.upperBound(l, 9, cmp), 3);
		assertEquals(VersionSet.upperBound(l, 10, cmp), 3);
		assertEquals(VersionSet.upperBound(l, 12, cmp), 4);
		assertEquals(VersionSet.upperBound(l, 13, cmp), 4);
		assertEquals(VersionSet.upperBound(l, 15, cmp), 5);
		assertEquals(VersionSet.upperBound(l, 16, cmp), 5);
		assertEquals(VersionSet.upperBound(l, 18, cmp), 6);
		assertEquals(VersionSet.upperBound(l, 19, cmp), 6);
		assertEquals(VersionSet.upperBound(l, 20, cmp), 6);
		

//		System.out.println(VersionSet.upperBound(l, 0, cmp));
//		System.out.println(VersionSet.upperBound(l, 3, cmp));
//		System.out.println(VersionSet.upperBound(l, 4, cmp));
//		System.out.println(VersionSet.upperBound(l, 6, cmp));
//		System.out.println(VersionSet.upperBound(l, 7, cmp));
//		System.out.println(VersionSet.upperBound(l, 9, cmp));
//		System.out.println(VersionSet.upperBound(l, 10, cmp));
//		System.out.println(VersionSet.upperBound(l, 12, cmp));
//		System.out.println(VersionSet.upperBound(l, 13, cmp));
//		System.out.println(VersionSet.upperBound(l, 15, cmp));
//		System.out.println(VersionSet.upperBound(l, 16, cmp));
//		System.out.println(VersionSet.upperBound(l, 18, cmp));
//		System.out.println(VersionSet.upperBound(l, 19, cmp));
//		System.out.println(VersionSet.upperBound(l, 20, cmp));
	}
}
