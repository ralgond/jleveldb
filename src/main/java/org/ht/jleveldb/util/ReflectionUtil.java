/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ht.jleveldb.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
/**
 * @author Teng Huang ht201509@163.com
 */
public class ReflectionUtil {
	public static Field getDeclaredFieldRecursively(Object instance, String fieldName) throws Exception {
		if (instance == null)
			throw new NullPointerException();
		Class<?> clz = instance.getClass();
		while (clz != null) {
			try {
				return clz.getDeclaredField(fieldName);
			} catch(NoSuchFieldException e) {
				clz = clz.getSuperclass();
			}
		}
		
		throw new NoSuchFieldException();
	}
	public static Object getValue(Object instance, String fieldName) throws Exception {
		Field field = getDeclaredFieldRecursively(instance, fieldName);
		field.setAccessible(true);
		return field.get(instance);
	}
	
	public static void setValue(Object instance, String fieldName, Object value) throws Exception {
		Field field = getDeclaredFieldRecursively(instance, fieldName);
		field.setAccessible(true);
		field.set(instance, value);
	}
	
	public static ArrayList<Field> getAllFields(Object o) {
		ArrayList<Field> ret = new ArrayList<>();
		Class<?> clz = o.getClass();
		while (clz != null) {
			Field fieldArray[] = clz.getDeclaredFields();
			if (fieldArray != null) {
				for (Field f : fieldArray)
					ret.add(f);
			}
			clz = clz.getSuperclass();
		}
		return ret;
	}
	
	public static boolean isEquals(Object a, Object b) {
		return isEquals2(a, b);
	}
	
	public static boolean isEquals2Array(Object a, Object b) {
		if (int[].class == a.getClass()) {
			return Arrays.equals((int[])a, (int[])b);
		} else if (long[].class == a.getClass()) {
			return Arrays.equals((long[])a, (long[])b);
		} else if (short[].class == a.getClass()) {
			return Arrays.equals((short[])a, (short[])b);
		} else if (byte[].class == a.getClass()) {
			return Arrays.equals((byte[])a, (byte[])b);
		} else if (float[].class == a.getClass()) {
			return Arrays.equals((float[])a, (float[])b);
		} else if (double[].class == a.getClass()) {
			return Arrays.equals((double[])a, (double[])b);
		} else if (boolean[].class == a.getClass()) {
			return Arrays.equals((boolean[])a, (boolean[])b);
		} else if (char[].class == a.getClass()) {
			return Arrays.equals((char[])a, (char[])b);
		} else {
			Object[] aa = (Object[])a;
			Object[] bb = (Object[])b;
			
			if (aa.length != bb.length)
				return false;
			
			for (int i = 0; i < aa.length; i++) {
				if (!isEquals2(aa[i], bb[i]))
					return false;
			}
			
			return true;
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static boolean isEquals2List(Object a, Object b) {
		List la = (List)a;
		List lb = (List)b;
		
		if (la.size() != lb.size())
			return false;
		
		for (int i = 0; i < la.size(); i++) {
			if (!isEquals2(la.get(i), lb.get(i)))
				return false;
		}
		
		return true;
	}
	
	@SuppressWarnings("rawtypes")
	public static boolean isEquals2Map(Object a, Object b) {
		Map ma = (Map)a;
		Map mb = (Map)b;
		
		if (ma.size() != mb.size())
			return false;
		
		Iterator ita = ma.entrySet().iterator();
		
		while (ita.hasNext()) {
			Map.Entry enta = (Map.Entry)ita.next();
			Object valueB = mb.get(enta.getKey());
			
			if (!isEquals2(enta.getValue(), valueB))
				return false;
		}
		
		return true;
	}
	
	public static boolean isEquals2Primitive(Object a, Object b) {
		if (int.class == a.getClass() || Integer.class == a.getClass()) {
			return (int)a == (int)b;
		} else if (long.class == a.getClass() || Long.class == a.getClass()) {
			return (long)a == (long)b;
		} else if (short.class == a.getClass() || Short.class == a.getClass()) {
			return (short)a == (short)b;
		} else if (byte.class == a.getClass() || Byte.class == a.getClass()) {
			return (byte)a == (byte)b;
		} else if (float.class == a.getClass() || Float.class == a.getClass()) {
			return (float)a == (float)b;
		} else if (double.class == a.getClass() || Double.class == a.getClass()) {
			return (double)a == (double)b;
		} else if (boolean.class == a.getClass() || Boolean.class == a.getClass()) {
			return (boolean)a == (boolean)b;
		} else if (char.class == a.getClass() || Character.class == a.getClass()) {
			return (char)a == (char)b;
		}
		
		return false;
	}
	
	protected static boolean isPrimitive(Object a) {
		Class<?> clz = a.getClass();
		return (clz == Integer.class || clz == Long.class || clz == Short.class ||
				clz == Byte.class || clz == Float.class || clz == Double.class ||
				clz == Boolean.class || clz == Character.class);
	}
	
	public static boolean isEquals2(Object a, Object b) {
		if (a == null && b == null)
			return true;
		
		if (a != null && b == null || a == null && b != null)
			return false;
		
		if (a.getClass() != b.getClass())
			return false;
		
		if (a.getClass().isArray()) {
			return isEquals2Array(a, b);
		} else if (a instanceof List) {
			return isEquals2List(a, b);
		} else if (a instanceof Map) {
			return isEquals2Map(a, b);
		} else if (isPrimitive(a)) {
			return isEquals2Primitive(a, b);
		} else if (a.getClass() == String.class) {
			return a.equals(b);
		} else if (a.getClass().isEnum()) {
			return a.equals(b);
		} else {
			ArrayList<Field> afields = getAllFields(a);
			ArrayList<Field> bfields = getAllFields(b);
			
			if (afields.size() != bfields.size())
				return false;
			
			for (int i = 0; i < afields.size(); i++) {
				Field af = afields.get(i);
				Field bf = bfields.get(i);
				
				af.setAccessible(true);
				bf.setAccessible(true);
				
				if (af.getType() != bf.getType()) {
					return false;
				}
				
				try {
					Object afv = af.get(a);
					Object bfv = af.get(b);
					
					if (!isEquals2(afv, bfv)) {
						return false;
					}
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
			
			return true;
		}
	}
	
}

