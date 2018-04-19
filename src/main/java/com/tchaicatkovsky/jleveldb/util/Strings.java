/**
 * Copyright (c) 2017-2018 Teng Huang <ht201509 at 163 dot com>
 * All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tchaicatkovsky.jleveldb.util;

public class Strings {

	final static int ASCII_SPACE = 0x20; // ' '
	final static int ASCII_TILDE = 0x7E; // '~'

	public static String escapeString(byte[] data, int offset, int size) {
		StringBuilder sb = new StringBuilder();
		for (int i = offset; i < size + offset; i++) {
			int c = (data[i] & 0xff);
			if (c >= ASCII_SPACE && c <= ASCII_TILDE) {
				sb.append((char) ('\0' + c));
			} else {
				sb.append(String.format("\\x%02x", c));
			}
		}
		return sb.toString();
	}

	public static String escapeString(ByteBuf buf) {
		return escapeString(buf.data(), buf.offset(), buf.size());
	}

	public static String escapeString(Slice slice) {
		return escapeString(slice.data(), slice.offset(), slice.size());
	}

}
