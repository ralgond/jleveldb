package org.ht.jleveldb;

import org.ht.jleveldb.util.FuncOutput;
import org.ht.jleveldb.util.FuncOutputLong;
import org.ht.jleveldb.util.Slice;

public class FileName {
	// Return the name of the log file with the specified number
	// in the db named by "dbname".  The result will be prefixed with
	// "dbname".
	public static String getLogFileName(String dbname, long number) {
		//TODO
		return null;
	}
	
	// Return the name of the sstable with the specified number
	// in the db named by "dbname".  The result will be prefixed with
	// "dbname".
	public static String getTableFileName(String dbname, long number) {
		//TODO
		return null;
	}

	// Return the legacy file name for an sstable with the specified number
	// in the db named by "dbname". The result will be prefixed with
	// "dbname".
	public static String getSSTTableFileName(String dbname, long number) {
		//TODO
		return null;
	}

	// Return the name of the descriptor file for the db named by
	// "dbname" and the specified incarnation number.  The result will be
	// prefixed with "dbname".
	public static String getDescriptorFileName(String dbname,  long number) {
		//TODO
		return null;
	}

	// Return the name of the current file.  This file contains the name
	// of the current manifest file.  The result will be prefixed with
	// "dbname".
	public static String getCurrentFileName(String dbname) {
		//TODO
		return null;
	}

	// Return the name of the lock file for the db named by
	// "dbname".  The result will be prefixed with "dbname".
	public static String getLockFileName(String dbname) {
		//TODO
		return null;
	}

	// Return the name of a temporary file owned by the db named "dbname".
	// The result will be prefixed with "dbname".
	public static String getTempFileName(String dbname, long number) {
		//TODO
		return null;
	}

	// Return the name of the info log file for "dbname".
	public static String getInfoLogFileName(String dbname) {
		//TODO
		return null;
	}

	// Return the name of the old info log file for "dbname".
	public static String getOldInfoLogFileName(String dbname) {
		//TODO
		return null;
	}


	// If filename is a leveldb file, store the type of the file in *type.
	// The number encoded in the filename is stored in *number.  If the
	// filename was successfully parsed, returns true.  Else return false.
	// Owned filenames have the form:
	//  dbname/CURRENT
	//  dbname/LOCK
	//  dbname/LOG
	//  dbname/LOG.old
	//  dbname/MANIFEST-[0-9]+
	//  dbname/[0-9]+.(log|sst|ldb)
	public static boolean parseFileName(String fname,
			FuncOutputLong number,
			FuncOutput<FileType> type) {
		byte[] b = fname.getBytes();
		Slice rest = new Slice(b, 0, b.length);
		if (fname.equals("CURRENT")) {
			number.setValue(0);
			type.setValue(FileType.CurrentFile);
		} else if (fname.equals("LOCK")) {
			number.setValue(0);
			type.setValue(FileType.DBLockFile);
		} else if (fname.equals("LOG") || fname.equals("LOG.old") {
			number.setValue(0);
			type.setValue(FileType.InfoLogFile);
		} else if (fname.startsWith("MANIFEST-")) {
			rest.removePrefix("MANIFEST-".length());
			FuncOutputLong num = new FuncOutputLong();
			if (!consumeDecimalNumber(&rest, &num)) {
				return false;
			}
			if (!rest.empty()) {
				return false;
			}
			type.setValue(FileType.DescriptorFile);
			number.setValue(num.getValue());
		} else {
			// Avoid strtoull() to keep filename format independent of the
			// current locale
			FuncOutputLong num = new FuncOutputLong();
			if (!consumeDecimalNumber(&rest, &num)) {
				return false;
			}
			Slice suffix = rest;
			if (suffix == Slice(".log")) {
				type.setValue(FileType.LogFile);
			} else if (suffix == Slice(".sst") || suffix == Slice(".ldb")) {
				type.setValue(FileType.TableFile);
			} else if (suffix == Slice(".dbtmp")) {
				type.setValue(FileType.TempFile);
			} else {
				return false;
			}
			number.setValue(num.getValue());
		}
		return true;
	}
	// Make the CURRENT file point to the descriptor file with the
	// specified number.
	public static Status setCurrentFile(Env env, String dbname, long descriptorNumber) {
		//TODO
		return null;
	}
}
