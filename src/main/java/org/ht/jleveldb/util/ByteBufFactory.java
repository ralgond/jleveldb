package org.ht.jleveldb.util;

public class ByteBufFactory {
	public static ByteBuf defaultByteBuf() {
		return new DefaultByteBuf();
	}
	
	public static ByteBuf defaultByteBuf(byte[] data, int size) {
		DefaultByteBuf ret = new DefaultByteBuf();
		ret.assign(data, size);
		return ret;
	}
}
