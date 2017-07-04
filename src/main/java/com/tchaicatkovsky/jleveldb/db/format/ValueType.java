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
package com.tchaicatkovsky.jleveldb.db.format;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
public enum ValueType {
    Deletion((byte)0x0),
    Value((byte)0x1);
    
	byte type;
    
    private ValueType(byte type) {
		  this.type = type;
	}
    
    public byte type() {
    	return type;
    }
    
    final public static ValueType create(byte type) {
    	if (type == Deletion.type)
    		return Deletion;
    	else if (type == Value.type)
    		return Value;
    	else
    		return null;
    }
};
