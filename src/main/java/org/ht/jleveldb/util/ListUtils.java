package org.ht.jleveldb.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ListUtils {
	public static <T> int upperBound(ArrayList<T> l, T target, Comparator<T> cmp) {
		int offset = 0;
		int size = l.size();
		List<T> view = l.subList(offset, offset+size);
		int ret = Collections.binarySearch(view, target, cmp);
		while (ret >= 0) {
			//System.out.printf("offset=%d, size=%d, ret=%d\n", offset, size, ret);
			offset += (ret + 1);
			size -= (ret + 1);
			view = l.subList(offset, offset+size);
			ret = Collections.binarySearch(view, target, cmp);
		}
		
		int insertionPoint = -1 * (ret + 1);
		
		return (insertionPoint == size ? l.size() : insertionPoint + offset);
	}
	
	public static<T> int lowerBound(ArrayList<T> l, T target, Comparator<T> cmp) {
		int size = l.size();
		List<T> view = l.subList(0, size);
		boolean found = false;
		int ret = Collections.binarySearch(view, target, cmp);
		int lastFoundIdx = -1;
		while (ret >= 0) {
			found = true;
			size = ret + 1;
			view = l.subList(0, size);
			lastFoundIdx = ret;
			ret = Collections.binarySearch(view, target, cmp);
			if (ret == lastFoundIdx) {
				break;
			}
		}
		
		if (found)
			return size - 1;
		else 
			return -1 * (ret + 1);
	}
}
