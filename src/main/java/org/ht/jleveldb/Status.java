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
	
	private static Status  DEFAULT_STATUS = new Status();
	
	public static Status defaultStatus() {
		return DEFAULT_STATUS;
	}
	
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
	
	public static Status ok0() {
		return defaultStatus();
	}
	
	public static Status notFound(String message) {
		return new Status(Code.NotFound, message);
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
