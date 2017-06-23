package org.ht.jleveldb.util;

import java.nio.BufferUnderflowException;

public class Coding {
	public final static long MAX_UNSIGNED_INT_VALUE = 4294967296L;
	public final static long MIN_UNSIGNED_INT_VALUE = 0L;
	public final static long MAX_SIGNED_INT_VALUE = Integer.MAX_VALUE;
	public final static long MIN_SIGNED_INT_VALUE = Integer.MIN_VALUE;
	
	// Standard Put... routines append to a string
	final public static void writeFixedNat32(ByteBuf dst, int value) {
		dst.writeFixedNat32(value);
	}
	
	final public static void writeFixedNat64(ByteBuf dst, long value) {
		dst.writeFixedNat64(value);
	}
	
	final public static void writeVarNat32(ByteBuf dst, int value) {
		dst.writeVarNat32(value);
	}
	
	final public static void writeVarNat64(ByteBuf dst, long value) {
		dst.writeVarNat64(value);
	}
	
	final public static void writeLengthPrefixedSlice(ByteBuf dst, Slice value) {
		dst.writeLengthPrefixedSlice(value);
	}
	
	// Standard Get... routines parse a value from the beginning of a Slice
	// and advance the slice past the parsed value.
	final public static int getVarNat32(Slice input) {
		return getVarNat32Ptr(input);
	}
	
	final public static long getVarNat64(Slice input) {
		return getVarNat64Ptr(input);
	}

	// Returns the length of the varint32 or varint64 encoding of "v"
	final public static int varNatLength(long v) {
		if (v < 0)
			throw new CodingException("overflow: "+v);
		
		int len = 1;
		while (v >= 128) {
			v >>= 7;
		    len++;
		}
		return len;
	}
	
	final public static void encodeFixedNat32Long(byte[] buf, int offset, int limit, long value) {
		//Big Endian
		if (value < 0 || value > MAX_UNSIGNED_INT_VALUE)
			throw new EncodeException("[fix32] uint32 overflow: "+value);
		
		if (limit - offset < 4)
			throw new BufferUnderflowException();
		
		buf[0+offset] = (byte)(value & 0xffL);
	    buf[1+offset] = (byte)((value >> 8) & 0xffL);
	    buf[2+offset] = (byte)((value >> 16) & 0xffL);
	    buf[3+offset] = (byte)((value >> 24) & 0xffL);
	}
	
	final public static void encodeFixedNat32Long(byte[] buf, int offset, long value) {
		encodeFixedNat32Long(buf, offset, offset + 4, value);
	}
	
	// Lower-level versions of Put... that write directly into a character buffer
	// REQUIRES: dst has enough space for the value being written
	final public static void encodeFixedNat32(byte[] buf, int offset, int limit, int value) {
		//Big Endian
		encodeFixedNat32Long(buf, offset, limit, value);
	}
	
	final public static void encodeFixedNat32(byte[] buf, int offset, int value) {
		encodeFixedNat32(buf, offset, offset + 4, value);
	}
	
	final public static void encodeFixedNat32(Slice ref, int value) {
		encodeFixedNat32(ref.data, ref.offset, ref.limit, value);
	}
	
	final public static void encodeFixedNat64(byte[] buf, int offset, int limit, long value) {
		//Big Endian
		if (value < 0)
			throw new EncodeException("[fix64] uint64 overflow: "+value);
		
		if (limit - offset < 8)
			throw new BufferUnderflowException();
		
		buf[0+offset] = (byte)(value & 0x0ff);
	    buf[1+offset] = (byte)((value >> 8) & 0x0ff);
	    buf[2+offset] = (byte)((value >> 16) & 0x0ff);
	    buf[3+offset] = (byte)((value >> 24) & 0x0ff);
	    buf[4+offset] = (byte)((value >> 32) & 0x0ff);
	    buf[5+offset] = (byte)((value >> 40) & 0x0ff);
	    buf[6+offset] = (byte)((value >> 48) & 0x0ff);
	    buf[7+offset] = (byte)((value >> 56) & 0x0ff);
	}
	
	final public static void encodeFixedNat64(byte[] buf, int offset, long value) {
		encodeFixedNat64(buf, offset, offset + 8, value);
	}
	
	final public static void encodeFixed64(Slice ref, long value) throws Exception {
		encodeFixedNat64(ref.data, ref.offset, ref.limit, value);
	}
	
	// Lower-level versions of Put... that write directly into a character buffer
	// and return a pointer just past the last byte written.
	// REQUIRES: dst has enough space for the value being written
	final public static int  encodeVarNat32(byte[] buf, int offset, int limit, int v) {
		if (v < 0 || v > MAX_SIGNED_INT_VALUE)
			throw new EncodeException("[varint32] uint32 overflow: "+v);
		
		final int B = 128;
		if (v < (1<<7)) {
			if (limit - offset < 1) throw new BufferUnderflowException();
			buf[offset++] = (byte)v;
		} else if (v < (1<<14)) {
			if (limit - offset < 2) throw new BufferUnderflowException();
			buf[offset++] = (byte)(v | B);
			buf[offset++] = (byte)(v>>7);
		} else if (v < (1<<21)) {
			if (limit - offset < 3) throw new BufferUnderflowException();
			buf[offset++] = (byte)(v | B);
			buf[offset++] = (byte)((v>>7) | B);
			buf[offset++] = (byte)(v>>14);
		} else if (v < (1<<28)) {
			if (limit - offset < 4) throw new BufferUnderflowException();
			buf[offset++] = (byte)(v | B);
			buf[offset++] = (byte)((v>>7) | B);
			buf[offset++] = (byte)((v>>14) | B);
			buf[offset++] = (byte)(v>>21);
		} else {
			if (limit - offset < 5) throw new BufferUnderflowException();
			buf[offset++] = (byte)(v | B);
			buf[offset++] = (byte)((v>>7) | B);
			buf[offset++] = (byte)((v>>14) | B);
			buf[offset++] = (byte)((v>>21) | B);
			buf[offset++] = (byte)(v>>28);
		}
		return offset;
	}
	
	final public static void  encodeVarint32(Slice dst, int v) {
		int offset = encodeVarNat32(dst.data, dst.offset, dst.limit, v);
		dst.offset = offset;
	}
	
	final public static int encodeVarNat64(byte[] buf, int offset, int limit, long v) {		
		if (v < 0)
			throw new EncodeException("[varint64] uint64 overflow: "+v);
		
		final int B = 128;
		while (v >= B) {
			if (limit - offset < 1) throw new BufferUnderflowException();
		    buf[offset++] = (byte)((v & (B-1)) | B);
		    v >>= 7;
		}
		buf[offset++] = (byte)(v);
		return offset;
	}
	
	final public static void encodeVarint64(Slice dst, long v) {
		int offset = encodeVarNat64(dst.data, dst.offset, dst.limit, v);
		dst.offset = offset;
	}
	
	final public static int decodeFixedNat32(byte[] data, int offset, int limit) {
		//java byte order is Big Endian
		if (limit - offset < 4)
			throw new BufferUnderflowException();
		
//		long tmp = ((long)(data[offset] & 0x0ff)) |
//				(((long)(data[offset+1] & 0x0ff)) << 8) |
//				(((long)(data[offset+2] & 0x0ff)) << 16) |
//				(((long)(data[offset+3]  & 0x0ff)) << 24);
		long tmp = ((data[offset] & 0x0ffL) |
				   ((data[offset+1] & 0x0ffL) << 8) |
				   ((data[offset+2] & 0x0ffL) << 16) |
				   ((data[offset+3] & 0x0ffL) << 24));
		
		if (tmp > MAX_SIGNED_INT_VALUE || tmp < 0) 
			throw new DecodeException("[fix32] uint32 overflow: "+tmp);
		
		return (int)tmp;
	}
	
	final public static int decodeFixedNat32(byte[] data, int offset) {
		int limit = offset + 4;
		return decodeFixedNat32(data, offset, limit);
	}
	
	final public static int decodeFixedNat32(Slice ptr) {
		return decodeFixedNat32(ptr.data, ptr.offset, ptr.limit);
	}
	
	final public static long decodeFixedNat32Long(byte[] data, int offset, int limit) {
		//java byte order is Big Endian
		if (limit - offset < 4)
			throw new BufferUnderflowException();
		
		long tmp = ((data[offset] & 0x0ffL) |
				   ((data[offset+1] & 0x0ffL) << 8) |
				   ((data[offset+2] & 0x0ffL) << 16) |
				   ((data[offset+3] & 0x0ffL) << 24));
		
		return tmp & 0xFFFFFFFFL;
	}
	
	final public static long decodeFixedNat32Long(byte[] data, int offset) {
		return decodeFixedNat32Long(data, offset, offset+4);
	}
	
	final public static long decodeFixedNat64(byte[] data, int offset, int limit) {
		//java byte order is Big Endian
		if (limit - offset < 8)
			throw new BufferUnderflowException();
		
		long tmp = ((long)(data[offset] & 0x0ff)) |
				(((long)(data[offset+1] & 0x0ff)) << 8) |
				(((long)(data[offset+2] & 0x0ff)) << 16) |
				(((long)(data[offset+3] & 0x0ff)) << 24) |
				(((long)(data[offset+4] & 0x0ff)) << 32) |
				(((long)(data[offset+5] & 0x0ff)) << 40) |
				(((long)(data[offset+6] & 0x0ff)) << 48) |
				(((long)(data[offset+7] & 0x0ff)) << 56);
		
		if (tmp > Long.MAX_VALUE || tmp < 0)
			throw new DecodeException("fix64: uint64 overflow: "+tmp);
		
		return tmp;
	}
	
	final public static long decodeFixedNat64(byte[] data, int offset) {
		int limit = offset + 8;
		return decodeFixedNat64(data, offset, limit);
	}
	
	final public static long decodeFixedNat64(Slice ptr) {
		return decodeFixedNat64(ptr.data, ptr.offset, ptr.limit);
	}
	
	// Internal routine for use by fallback path of GetVarint32Ptr
	final public static int getVarNat32PtrFallback(Slice ref) {
		byte data[] = ref.data;
		int limit = ref.limit;
		
		int result = 0;
		for (int shift = 0; shift <= 28 && ref.offset < limit; shift += 7) {
		    int b = data[ref.offset];
		    ref.offset++;
		    if ((b & 128) != 0) {
		      // More bytes are present
		      result |= ((b & 127) << shift);
		    } else {
		      result |= (b << shift);
		      return result;
		    }
		  }
		throw new CodingException("getVarint32PtrFallback failed");
	}
	
	final public static int getVarNat32Ptr(Slice ref) {
		if (ref.offset < ref.limit) {
			int result = ref.data[ref.offset];
			if ((result & 128) == 0) {
				ref.offset += 1;
				return result;
			}
		}
		return getVarNat32PtrFallback(ref);
	}
	
	final public static long getVarNat64Ptr(Slice ref) {
		long result = 0;
		byte[] data = ref.data;
		int limit = ref.limit;
		for (int shift = 0; shift <= 63 && ref.offset < limit; shift += 7) {
			long b = data[ref.offset];
			ref.offset++;
			if ((b & 128) != 0) {
				// More bytes are present
				result |= ((b & 127) << shift);
			} else {
				result |= (b << shift);
				return result;
			}
		}
		throw new CodingException("getVarint64Ptr failed");
	}
	
	final public static boolean getLengthPrefixedSlice(Slice input, Slice result) {
		int len;
		if ((len = getVarNat32(input)) > 0 && input.size() >= len) {
			//System.out.printf("getLengthPrefixedSlice len=%d\n", len);
		    result.init(input.data, input.offset, len);
		    input.removePrefix(len);
		    return true;
		} else {
		    return false;
		}
	}
}
