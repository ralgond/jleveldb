package com.tchaicatkovsky.jleveldb.util;

public class DecodeException extends CodingException {
	private static final long serialVersionUID = -763618247875562001L;
	
	public DecodeException() {
		super();
	}
	
	public DecodeException(String message) {
		super(message);
	}
	
	public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public DecodeException(Throwable cause) {
        super(cause);
    }
}
