package com.tchaicatkovsky.jleveldb;

public enum CompressionType {
	
	kNoCompression((byte)0x00),
	kSnappyCompression((byte)0x01);
	
	private byte type;
	
	private CompressionType(byte type) {
		this.type = type;
	}
	
	public byte getType() {
		return type;
	}
}
