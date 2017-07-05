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

import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.RandomAccessFile0;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.SequentialFile;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.table.Table;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.UnpooledSlice;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;

public class Dumpfile {
	// Dump the contents of the file named by fname in text format to
	// *dst.  Makes a sequence of dst->Append() calls; each call is passed
	// the newline-terminated text corresponding to a single item found
	// in the file.
	//
	// Returns a non-OK result if fname does not name a leveldb storage
	// file, or if the file cannot be read.
	public static Status dumpFile(Env env, String fname, WritableFile dst) {
		Object0<FileType> ftype0 = new Object0<FileType>();
		if (!guessType(fname, ftype0)) {
		    return Status.invalidArgument(fname + ": unknown file type");
		}
		switch (ftype0.getValue()) {
		    case LogFile:         return dumpLog(env, fname, dst);
		    case DescriptorFile:  return dumpDescriptor(env, fname, dst);
		    case TableFile:       return dumpTable(env, fname, dst);
		    default:
		      break;
		}
		return Status.invalidArgument(fname + ": not a dump-able file type");
	}
	
	// Notified when log reader encounters corruption.
	static class CorruptionReporter implements LogReader.Reporter {
		public WritableFile dst;
		public void corruption(int bytes, Status status) {
			String r = "corruption: ";
			r += bytes;//AppendNumberTo(&r, bytes);
			r += " bytes; ";
			r += status.toString();
			r += '\n';
			byte[] b = r.getBytes();
			dst.append(new UnpooledSlice(b, 0, b.length));
		}
	};
	
	public interface LogReadCallback {
		public void run(long offset, Slice slice, WritableFile f);
	}
	
	// Print contents of a log file. (*func)() is called on every record.
	static Status printLogContents(Env env, String fname,
			LogReadCallback callback,  WritableFile dst) {
		Object0<SequentialFile> file0 = new Object0<SequentialFile>();
		Status s = env.newSequentialFile(fname, file0);
		if (!s.ok()) {
			return s;
		}
		SequentialFile file = file0.getValue();
		CorruptionReporter reporter = new CorruptionReporter();
		reporter.dst = dst;
		LogReader reader = new LogReader(file, reporter, true, 0);
		Slice record = new UnpooledSlice();
		ByteBuf scratch = ByteBufFactory.newUnpooled();
		while (reader.readRecord(record, scratch)) {
			//(*func)(reader.LastRecordOffset(), record, dst);
			callback.run(reader.lastRecordOffset(), record, dst);
			scratch.clear();
		}
		file.delete();
		file = null;
		return Status.ok0();
	}
	
	static void appendEscapedStringTo(String s, Slice value) {
		
	}
	
	// Called on every item found in a WriteBatch.
	static class WriteBatchItemPrinter implements WriteBatch.Handler {
		public WritableFile dst;
		public void put(Slice key, Slice value) {
			String r = "  put '";
			appendEscapedStringTo(r, key);
			r += "' '";
			appendEscapedStringTo(r, value);
			r += "'\n";
			byte[] b = r.getBytes();
			dst.append(new UnpooledSlice(b, 0, b.length));
		}
		public void delete(Slice key) {
			String r = "  del '";
			appendEscapedStringTo(r, key);
			r += "'\n";
			byte[] b = r.getBytes();
			dst.append(new UnpooledSlice(b, 0, b.length));
		}
	}
	
	// Called on every log record (each one of which is a WriteBatch)
	// found in a kLogFile.
	static class WriteBatchPrinter implements LogReadCallback {
		public void run(long pos, Slice record, WritableFile dst) {
			String r = "--- offset ";
			r += pos;
			r += "; ";
			if (record.size() < 12) {
				r += "log record length ";
				r += record.size();
				r += " is too small\n";
				byte[] b = r.getBytes();
				dst.append(new UnpooledSlice(b, 0, b.length));
				return;
			}
			WriteBatch batch = new WriteBatch();
			WriteBatchInternal.setContents(batch, record);
			r += "sequence ";
		  	r += WriteBatchInternal.sequence(batch);
		  	r += '\n';
		  	byte[] b = r.getBytes();
		  	dst.append(new UnpooledSlice(b, 0, b.length));
		  	WriteBatchItemPrinter batchItemPrinter = new WriteBatchItemPrinter();
		  	batchItemPrinter.dst = dst;
		  	Status s = batch.iterate(batchItemPrinter);
		  	if (!s.ok()) {
		  		byte[] b2 = ("  error: " + s.toString() + "\n").getBytes();
		  		dst.append(new UnpooledSlice(b2, 0, b2.length));
		  	}
		}
	}
	
	static Status dumpLog(Env env, String fname, WritableFile dst) {
		return printLogContents(env, fname, new WriteBatchPrinter(), dst);
	}
	
	// Called on every log record (each one of which is a WriteBatch)
	// found in a kDescriptorFile.
	static class VersionEditPrinter implements LogReadCallback {
		public void run(long pos, Slice record, WritableFile dst) {
			String r = "--- offset ";
			r += pos;
			r += "; ";
			VersionEdit edit = new VersionEdit();
			Status s = edit.decodeFrom(record);
			if (!s.ok()) {
				r += s.toString();
				r += '\n';
			} else {
				r += edit.debugString();
			}
			byte[] b = r.getBytes();
			dst.append(new UnpooledSlice(b, 0, b.length));
		}
	}
	
	static Status dumpDescriptor(Env env, String fname, WritableFile dst) {
		return printLogContents(env, fname, new VersionEditPrinter(), dst);
	}
		
	static Status dumpTable(Env env, String fname, WritableFile dst) {
		  Long0 fileSize = new Long0();
		  Object0<RandomAccessFile0> file0 = new Object0<RandomAccessFile0>();
		  Object0<Table> table0 = new Object0<Table>();
		  Status s = env.getFileSize(fname, fileSize);
		  if (s.ok()) {
			  s = env.newRandomAccessFile(fname, file0);
		  }
		  if (s.ok()) {
		    // We use the default comparator, which may or may not match the
		    // comparator used in this database. However this should not cause
		    // problems since we only use Table operations that do not require
		    // any comparisons.  In particular, we do not call Seek or Prev.
		    s = Table.open(new Options(), file0.getValue(), fileSize.getValue(), table0);
		  }
		  if (!s.ok()) {
			  if (table0.getValue() != null)
				  table0.getValue().delete();
			  if (file0.getValue() != null)
				  file0.getValue().delete();
			  return s;
		  }

		  Table table = table0.getValue();
		  RandomAccessFile0 file = file0.getValue();
		  ReadOptions ro = new ReadOptions();
		  ro.fillCache = false;
		  Iterator0 iter = table.newIterator(ro);
		  String r = new String();
		  for (iter.seekToFirst(); iter.valid(); iter.next()) {
			  r = "";
			  ParsedInternalKey key = new ParsedInternalKey();
			  if (!key.parse(iter.key())) {
				  r = "badkey '";
				  appendEscapedStringTo(r, iter.key());
				  r += "' => '";
				  appendEscapedStringTo(r, iter.value());
				  r += "'\n";
				  byte[] b = r.getBytes();
				  dst.append(new UnpooledSlice(b, 0, b.length));
			  } else {
				  r = "'";
				  appendEscapedStringTo(r, key.userKey);
				  r += "' @ ";
				  r += key.sequence;
				  r += " : ";
				  if (key.type == ValueType.Deletion) {
					  r += "del";
				  } else if (key.type == ValueType.Value) {
					  r += "val";
				  } else {
					  r += key.type;
				  }
				  r += " => '";
				  appendEscapedStringTo(r, iter.value());
				  r += "'\n";
				  byte[] b = r.getBytes();
				  dst.append(new UnpooledSlice(b, 0, b.length));
			  }
		  }
		  s = iter.status();
		  if (!s.ok()) {
			  byte[] b = ("iterator error: " + s.toString() + "\n").getBytes();
			  dst.append(new UnpooledSlice(b, 0, b.length));
		  }

		  iter.delete();
		  table.delete();
		  file.delete();
		  return Status.ok0();
	}
	
	
	
	static boolean guessType(String fname, Object0<FileType> type) {
		  int pos = fname.lastIndexOf('/'); //size_t pos = fname.rfind('/');
		  String basename;
		  if (pos < 0) {
			  basename = fname;
		  } else {
			  basename = fname.substring(pos+1); //basename = std::string(fname.data() + pos + 1, fname.size() - pos - 1);
		  }
		  Long0 ignored = new Long0();
		  return FileName.parseFileName(basename, ignored, type);
	}
}
