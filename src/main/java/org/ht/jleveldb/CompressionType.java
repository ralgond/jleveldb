package org.ht.jleveldb;

public enum CompressionType {
	
	NoCompression((byte)0x00),
	SnappyCompression((byte)0x01);
	
	private byte type;
	
	private CompressionType(byte type) {
		this.type = type;
	}
	
	public byte getType() {
		return type;
	}
}
