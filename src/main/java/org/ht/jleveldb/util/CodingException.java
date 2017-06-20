package org.ht.jleveldb.util;

public class CodingException extends RuntimeException {
	private static final long serialVersionUID = -763618247875562000L;
	
	public CodingException() {
		super();
	}
	
	public CodingException(String message) {
		super(message);
	}
	
	public CodingException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public CodingException(Throwable cause) {
        super(cause);
    }
}
