package org.ht.jleveldb.util;

public class Strings {
	public static String escapeString(ByteBuf buf) {
		return escapeString(buf.data(), 0, buf.size());
	}
	
	public static String escapeString(Slice slice) {
		return escapeString(slice.data, slice.offset, slice.offset);
	}
	
	public static String escapeString(byte[] data, int offset, int size) {
		return null;
	}
}
