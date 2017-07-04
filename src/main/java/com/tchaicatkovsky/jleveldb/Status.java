/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tchaicatkovsky.jleveldb;

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
	
	public boolean isOtherError() {
		return code == Code.OtherError;
	}
	
	public boolean isNotFound() {
		return code == Code.NotFound;
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
