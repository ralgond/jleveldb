package org.ht.jleveldb.util;

public interface ByteBuf {
	
	byte[] data();
	
	int capacity();
	
	int position();

	int size();
	
	boolean empty();
	
	boolean isDirect();
	
	void clear();
	
	void resize(int bytes);
	
	void resize(int bytes, byte value);
	
	void require(int bytes);
	
	void swap(ByteBuf buf);
	
	byte getByte(int idx);
	
	byte[] copyData();
	
	public String encodeToString();
	
	
	void setByte(int idx, byte b);
	
	void assign(byte[] data, int size);
	
	void assign(byte[] data, int offset, int size);

	void append(byte[] buf, int size);
	
	void append(byte[] buf, int offset, int size);
	
	void addByte(Byte b);
	
	/**
	 * append 32bit fixed natural number.
	 * @param value
	 */
	public void writeFixedNat32(int value);
	
	/**
	 * append 32bit fixed natural number.
	 * @param value
	 */
	public void writeFixedNat32Long(long value);
	
	/**
	 * append 64bit fixed natural number.
	 * @param value
	 */
	public void writeFixedNat64(long value);
	
	/**
	 * append 32bit var natural number.
	 * @param value
	 * @throws Exception
	 */
	public void writeVarNat32(int value);
	
	/**
	 * append 32bit var natural number.
	 * @param value
	 */
	public void writeVarNat64(long value);
	
	/**
	 * append slice.
	 * @param value
	 */
	public void writeLengthPrefixedSlice(Slice value);
	
	/**
	 * read 32bit fixed natural number.
	 * @return
	 */
	public int readFixedNat32();
	
	/**
	 * read 64bit fixed natural number.
	 * @return
	 */
	public long readFixedNat64();
	
	/**
	 * read 32bit var natural number.
	 * @return
	 */
	public int readVarNat32();
	
	/**
	 * read 64bit var natural number.
	 * @return
	 */
	public long readVarNat64();
	
	/**
	 * read slice.
	 * @param value
	 */
	public Slice readLengthPrefixedSlice();
}
