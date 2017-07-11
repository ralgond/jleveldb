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
package com.tchaicatkovsky.jleveldb.db;

import java.util.ArrayList;

import com.tchaicatkovsky.jleveldb.DBTools;
import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.FileLock0;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;

public class DBToolsImpl implements DBTools {

	@Override
	public Status destroyDB(String dbname, Options options) throws Exception {
		Env env = options.env;
		ArrayList<String> filenames = new ArrayList<>();
		// Ignore error in case directory does not exist
		env.getChildren(dbname, filenames);
		if (filenames.isEmpty()) {
		    return Status.ok0();
		}

		Object0<FileLock0> lock0 = new Object0<>();
		String lockname = FileName.getLockFileName(dbname);
		Status result = env.lockFile(lockname, lock0);
		if (result.ok()) {
		    Long0 number0 = new Long0();
		    Object0<FileType> type0 = new Object0<>();
		    for (int i = 0; i < filenames.size(); i++) {
		    	if (FileName.parseFileName(filenames.get(i), number0, type0) &&
		          type0.getValue() != FileType.DBLockFile) {  // Lock file will be deleted at end
		    		Status del = env.deleteFile(dbname + "/" + filenames.get(i));
		    		if (result.ok() && !del.ok()) {
		    			result = del;
		    		}
		    	}
		    }
		    
		    env.unlockFile(lock0.getValue());  // Ignore error since state is already gone
		    env.deleteFile(lockname);
		    env.deleteDir(dbname);  // Ignore error in case dir contains other files
		}
		return result;
	}

	@Override
	public Status repairDB(String dbname, Options options) throws Exception {
		Repairer repairer = new Repairer(dbname, options);
		return repairer.run();
	}
}
