package org.ht.jleveldb.util;

public class EncodeException extends CodingException {
	private static final long serialVersionUID = -763618247875562002L;
	
	public EncodeException() {
		super();
	}
	
	public EncodeException(String message) {
		super(message);
	}
	
	public EncodeException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public EncodeException(Throwable cause) {
        super(cause);
    }
}
