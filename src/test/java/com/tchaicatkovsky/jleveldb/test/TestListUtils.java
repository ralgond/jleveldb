package com.tchaicatkovsky.jleveldb.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.util.ListUtils;

public class TestListUtils {
	@Test
	public void testUpperBound() {
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
		
		assertEquals(ListUtils.upperBound(l, 0, cmp), 0);
		assertEquals(ListUtils.upperBound(l, 3, cmp), 1);
		assertEquals(ListUtils.upperBound(l, 4, cmp), 1);
		assertEquals(ListUtils.upperBound(l, 6, cmp), 2);
		assertEquals(ListUtils.upperBound(l, 7, cmp), 2);
		assertEquals(ListUtils.upperBound(l, 9, cmp), 3);
		assertEquals(ListUtils.upperBound(l, 10, cmp), 3);
		assertEquals(ListUtils.upperBound(l, 12, cmp), 4);
		assertEquals(ListUtils.upperBound(l, 13, cmp), 4);
		assertEquals(ListUtils.upperBound(l, 15, cmp), 5);
		assertEquals(ListUtils.upperBound(l, 16, cmp), 5);
		assertEquals(ListUtils.upperBound(l, 18, cmp), 6);
		assertEquals(ListUtils.upperBound(l, 19, cmp), 6);
		assertEquals(ListUtils.upperBound(l, 20, cmp), 6);
		
//		System.out.println(ListUtils.upperBound(l, 0, cmp));
//		System.out.println(ListUtils.upperBound(l, 3, cmp));
//		System.out.println(ListUtils.upperBound(l, 4, cmp));
//		System.out.println(ListUtils.upperBound(l, 6, cmp));
//		System.out.println(ListUtils.upperBound(l, 7, cmp));
//		System.out.println(ListUtils.upperBound(l, 9, cmp));
//		System.out.println(ListUtils.upperBound(l, 10, cmp));
//		System.out.println(ListUtils.upperBound(l, 12, cmp));
//		System.out.println(ListUtils.upperBound(l, 13, cmp));
//		System.out.println(ListUtils.upperBound(l, 15, cmp));
//		System.out.println(ListUtils.upperBound(l, 16, cmp));
//		System.out.println(ListUtils.upperBound(l, 18, cmp));
//		System.out.println(ListUtils.upperBound(l, 19, cmp));
//		System.out.println(ListUtils.upperBound(l, 20, cmp));
	}
	
	@Test
	public void testLowerBound() {
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

		assertEquals(ListUtils.lowerBound(l, 0, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 3, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 4, cmp), 1);
		assertEquals(ListUtils.lowerBound(l, 6, cmp), 1);
		assertEquals(ListUtils.lowerBound(l, 7, cmp), 2);
		assertEquals(ListUtils.lowerBound(l, 9, cmp), 2);
		assertEquals(ListUtils.lowerBound(l, 10, cmp), 3);
		assertEquals(ListUtils.lowerBound(l, 12, cmp), 3);
		assertEquals(ListUtils.lowerBound(l, 13, cmp), 4);
		assertEquals(ListUtils.lowerBound(l, 15, cmp), 4);
		assertEquals(ListUtils.lowerBound(l, 16, cmp), 5);
		assertEquals(ListUtils.lowerBound(l, 18, cmp), 5);
		assertEquals(ListUtils.lowerBound(l, 19, cmp), 6);
		assertEquals(ListUtils.lowerBound(l, 20, cmp), 6);
		
//		System.out.println(ListUtils.lowerBound(l, 0, cmp));
//		System.out.println(ListUtils.lowerBound(l, 3, cmp));
//		System.out.println(ListUtils.lowerBound(l, 4, cmp));
//		System.out.println(ListUtils.lowerBound(l, 6, cmp));
//		System.out.println(ListUtils.lowerBound(l, 7, cmp));
//		System.out.println(ListUtils.lowerBound(l, 9, cmp));
//		System.out.println(ListUtils.lowerBound(l, 10, cmp));
//		System.out.println(ListUtils.lowerBound(l, 12, cmp));
//		System.out.println(ListUtils.lowerBound(l, 13, cmp));
//		System.out.println(ListUtils.lowerBound(l, 15, cmp));
//		System.out.println(ListUtils.lowerBound(l, 16, cmp));
//		System.out.println(ListUtils.lowerBound(l, 18, cmp));
//		System.out.println(ListUtils.lowerBound(l, 19, cmp));
//		System.out.println(ListUtils.lowerBound(l, 20, cmp));
	}
	
	@Test
	public void testLowerBound01() {
		ArrayList<Integer> l = new ArrayList<>();
		l.add(3);
		l.add(3);
		l.add(3);
		l.add(6);
		l.add(6);
		l.add(6);
		
		Comparator<Integer> cmp = new Comparator<Integer>() {
			public int compare(Integer a, Integer b) {
				return Integer.compare(a, b);
			}
		};
		

		assertEquals(ListUtils.lowerBound(l, 0, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 2, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 3, cmp), 0);
		assertEquals(ListUtils.lowerBound(l, 4, cmp), 3);
		assertEquals(ListUtils.lowerBound(l, 6, cmp), 3);
		assertEquals(ListUtils.lowerBound(l, 7, cmp), 6);
	}
}
