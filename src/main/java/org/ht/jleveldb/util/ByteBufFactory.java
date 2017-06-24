package org.ht.jleveldb.util;

public class ByteBufFactory {
	public static ByteBuf defaultByteBuf() {
		return new DefaultByteBuf();
	}
	
	public static ByteBuf defaultByteBuf(byte[] data, int size) {
		return defaultByteBuf(data, 0, size);
	}
	
	public static ByteBuf defaultByteBuf(byte[] data, int offset, int size) {
		DefaultByteBuf ret = new DefaultByteBuf();
		ret.assign(data, offset, size);
		return ret;
	}
}
