package org.ht.jleveldb;

public final class Status {
	public enum Code {
	    Ok,
	    NotFound,
	    Corruption,
	    NotSupported,
	    InvalidArgument,
	    IOError,
	    OtherError
	};
	
	final Code code;
	final String message;
	
	
	
	private Status() {
		code = Code.Ok;
		message = null;
	}
	
	public Code code() {
		return code;
	}
	
	public String message() {
		return message;
	}
	
	public Status(Code code, String message) {
		this.code = code;
		this.message = message;
	}
	
	public boolean ok() {
		return code == Code.Ok;
	}
	
	@Override
	public String toString() {
		return String.format("{%s,%s}", code.name(), message);
	}
	
	@Override
	public Status clone() {
		return new Status(code, message);
	}
	
	private static Status OK_STATUS = new Status();
	private static Status NOT_FOUND_STATUS = new Status(Code.NotFound, null);
	
	public static Status ok0() {
		return OK_STATUS;
	}
	
	public static Status notFound(String message) {
		return new Status(Code.NotFound, message);
	}
	
	public static Status notFound() {
		return NOT_FOUND_STATUS;
	}
	
	public static Status corruption(String message) {
		return new Status(Code.Corruption, message);
	}

	public static Status notSupported(String message) {
		return new Status(Code.NotSupported, message);
	}
	
	public static Status invalidArgument(String message) {
		return new Status(Code.InvalidArgument, message);
	}
	
	public static Status ioError(String message) {
		return new Status(Code.IOError, message);
	}
	
	public static Status otherError(String message) {
		return new Status(Code.OtherError, message);
	}
}
