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
package com.tchaicatkovsky.jleveldb.util;

public class ByteBufFactory {
	public static ByteBuf defaultByteBuf() {
		return new DefaultByteBuf();
	}
	
	public static ByteBuf defaultByteBuf(byte[] data, int size) {
		return defaultByteBuf(data, 0, size);
	}
	
	public static ByteBuf defaultByteBuf(byte[] data, int offset, int size) {
		DefaultByteBuf ret = new DefaultByteBuf();
		ret.assign(data, offset, size);
		return ret;
	}
	
	public static ByteBuf defaultByteBuf(String s) {
		byte[] b = s.getBytes();
		return defaultByteBuf(b, b.length);
	}
}
