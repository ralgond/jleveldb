/**
 * Copyright (c) 2017-2018, Teng Huang <ht201509 at 163 dot com>
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

package com.tchaicatkovsky.jleveldb;

public interface DBTools {

	/**
	 * Destroy the contents of the specified database.</br>
	 * Be very careful using this method.
	 * 
	 * @param name
	 * @param options
	 * @return
	 */
	Status destroyDB(String dbname, Options options) throws Exception;

	/**
	 * If a DB cannot be opened, you may attempt to call this method to resurrect as much of the contents of the database as possible.</br>
	 * Some data may be lost, so be careful when calling this function on a database that contains important information.
	 * 
	 * @param dbname
	 * @param options
	 * @return
	 */
	Status repairDB(String dbname, Options options) throws Exception;
}
