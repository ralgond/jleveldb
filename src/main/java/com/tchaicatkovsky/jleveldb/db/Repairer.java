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
 * 
 * This file is translated from source code file Copyright (c) 2011 
 * The LevelDB Authors and licensed under the BSD-3-Clause license.
 */

package com.tchaicatkovsky.jleveldb.db;

import java.util.ArrayList;

import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.FileName;
import com.tchaicatkovsky.jleveldb.FileType;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.Logger0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.SequentialFile;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.db.format.InternalFilterPolicy;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.table.TableBuilder;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Long0;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;
import com.tchaicatkovsky.jleveldb.util.Strings;

public class Repairer {
	static class TableInfo {
		FileMetaData meta = new FileMetaData();
		long maxSequence;
	}

	String dbname;
	Env env;
	InternalKeyComparator icmp;
	InternalFilterPolicy ipolicy;
	Options options = new Options();
	boolean ownsInfoLog;
	boolean ownsCache;
	TableCache tableCache;
	VersionEdit edit = new VersionEdit();

	ArrayList<String> manifests = new ArrayList<>();
	ArrayList<Long> tableNumbers = new ArrayList<>();
	ArrayList<Long> logs = new ArrayList<>();
	ArrayList<TableInfo> tables = new ArrayList<>();
	long nextFileNumber;

	public Repairer(String dbname, Options options) {
		this.dbname = dbname;
		this.env = options.env;
		icmp = new InternalKeyComparator(options.comparator);
		ipolicy = new InternalFilterPolicy(options.filterPolicy);
		this.options = DBImpl.sanitizeOptions(dbname, icmp, ipolicy, options);
		ownsInfoLog = (options.infoLog != options.infoLog);
		ownsCache = (options.blockCache != options.blockCache);
		nextFileNumber = 1;
		// TableCache can be small since we expect each table to be opened once.
		tableCache = new TableCache(dbname, options, 0);
	}
	
	public void delete() {
		tableCache.delete();
		if (ownsInfoLog) {
			options.infoLog.delete();
		}
		if (ownsCache) {
			options.blockCache.delete();
		}
	}
	
	public Status run() {
		Status status = findFiles();
		if (status.ok()) {
			convertLogFilesToTables();
			extractMetaData();
			status = writeDescriptor();
		}
		if (status.ok()) {
			long bytes = 0;
			for (int i = 0; i < tables.size(); i++) {
				bytes += tables.get(i).meta.fileSize;
			}
			Logger0.log0(options.infoLog, 
					"**** Repaired leveldb {}; " + 
					"recovered {} files; {} bytes. " + 
					"Some data may have been lost. " + 
					"****", 
					dbname, tables.size(), bytes);
		}
		return status;
	}
	
	Status findFiles() {
		ArrayList<String> filenames = new ArrayList<>();
		Status status = env.getChildren(dbname, filenames);
		if (!status.ok())
			return status;
			
		if (filenames.isEmpty())
			return Status.ioError(dbname + " repair found no files");

		Long0 number = new Long0();
		Object0<FileType> type = new Object0<>();

		for (int i = 0; i < filenames.size(); i++) {
			if (FileName.parseFileName(filenames.get(i), number, type)) {
				if (type.getValue() == FileType.DescriptorFile) {
					manifests.add(filenames.get(i));
				} else {
					if (number.getValue() + 1 > nextFileNumber) {
						nextFileNumber = number.getValue() + 1;
					}
					if (type.getValue() == FileType.LogFile) {
						logs.add(number.getValue());
					} else if (type.getValue() == FileType.TableFile) {
						tableNumbers.add(number.getValue());
					} else {
						// Ignore other files
					}
				}
			}
		}
		return status;
	}
	
	void convertLogFilesToTables() {
		for (int i = 0; i < logs.size(); i++) {
			String logname = FileName.getLogFileName(dbname, logs.get(i));
			Status status = convertLogToTable(logs.get(i));
			if (!status.ok()) {
				Logger0.log0(options.infoLog, "Log #{}: ignoring conversion error: {}", logs.get(i), status);
			}
			archiveFile(logname);
		}
	}
	
	static class LogReporter0 implements LogReader.Reporter {
		Env env;
		Logger0 infoLog;
		long lognum;

		public void corruption(int bytes, Status s) {
			// We print error messages for corruption, but continue repairing.
			Logger0.log0(infoLog, "Log #{}: dropping {} bytes; {}", lognum, bytes, s);
		}
	}
	    
	Status convertLogToTable(long log) {
		// Open the log file
		String logname = FileName.getLogFileName(dbname, log);
		Object0<SequentialFile> lfile = new Object0<>();
		Status status = env.newSequentialFile(logname, lfile);
		if (!status.ok()) {
			return status;
		}

		// Create the log reader.
		LogReporter0 reporter = new LogReporter0();
		reporter.env = env;
		reporter.infoLog = options.infoLog;
		reporter.lognum = log;
		// We intentionally make LogReader do checksumming so that
		// corruptions cause entire commits to be skipped instead of
		// propagating bad information (like overly large sequence
		// numbers).
		LogReader reader = new LogReader(lfile.getValue(), reporter, false/* do not checksum */, 0/* initial_offset */);

		// Read all the records and add to a memtable
		ByteBuf scratch = ByteBufFactory.newUnpooled();
		Slice record = SliceFactory.newUnpooled();
		WriteBatch batch = new WriteBatch();
		MemTable mem = new MemTable(icmp);
		mem.ref();
		int counter = 0;
		while (reader.readRecord(record, scratch)) {
			if (record.size() < 12) {
				reporter.corruption(record.size(), Status.corruption("log record too small"));
				continue;
			}
			WriteBatchInternal.setContents(batch, record);
			status = WriteBatchInternal.insertInto(batch, mem);
			if (status.ok()) {
				counter += WriteBatchInternal.count(batch);
			} else {
				Logger0.log0(options.infoLog, "Log #{}: ignoring {}", log, status);
				status = Status.ok0(); // Keep going with rest of file
			}
		}
		lfile.getValue().delete();

		// Do not record a version edit for this conversion to a Table
		// since ExtractMetaData() will also generate edits.
		FileMetaData meta = new FileMetaData();
		meta.number = nextFileNumber++;
		Iterator0 iter = mem.newIterator();
		status = Builder.buildTable(dbname, env, options, tableCache, iter, meta);
		iter.delete();
		mem.unref();
		mem = null;
		if (status.ok()) {
			if (meta.fileSize > 0)
				tableNumbers.add(meta.number);
		}
		Logger0.log0(options.infoLog, "Log #{}: {} ops saved to Table #{} {}", log, counter, meta.number, status);
		return status;
	}
	
	void extractMetaData() {
		for (int i = 0; i < tableNumbers.size(); i++)
			scanTable(tableNumbers.get(i));
	}
	
	Iterator0 newTableIterator(FileMetaData meta) {
		// Same as compaction iterators: if paranoid_checks are on, turn
		// on checksum verification.
		ReadOptions r = new ReadOptions();
		r.verifyChecksums = options.paranoidChecks;
		return tableCache.newIterator(r, meta.number, meta.fileSize);
	}
	
	void scanTable(long number) {
		TableInfo t = new TableInfo();
		t.meta.number = number;
		String fname = FileName.getTableFileName(dbname, number);
		Long0 fileSize0 = new Long0();
		Status status = env.getFileSize(fname, fileSize0);
		t.meta.fileSize = fileSize0.getValue();
		if (!status.ok()) {
			// Try alternate file name.
			fname = FileName.getSSTTableFileName(dbname, number);
			fileSize0.setValue(0);
			Status s2 = env.getFileSize(fname, fileSize0);
			t.meta.fileSize = fileSize0.getValue();
			if (s2.ok()) {
				status = Status.ok0();
			}
		}
		if (!status.ok()) {
			archiveFile(FileName.getTableFileName(dbname, number));
			archiveFile(FileName.getSSTTableFileName(dbname, number));
			Logger0.log0(options.infoLog, "Table #{}: dropped: {}", t.meta.number, status);
			return;
		}

		// Extract metadata by scanning through table.
		int counter = 0;
		Iterator0 iter = newTableIterator(t.meta);
		boolean empty = true;
		ParsedInternalKey parsed = new ParsedInternalKey();
		t.maxSequence = 0;
		for (iter.seekToFirst(); iter.valid(); iter.next()) {
			Slice key = iter.key();
			if (!parsed.parse(key)) {
				Logger0.log0(options.infoLog, "Table #{}: unparsable key {}", t.meta.number, Strings.escapeString(key));
				continue;
			}

			counter++;
			if (empty) {
				empty = false;
				t.meta.smallest.decodeFrom(key);
			}
			t.meta.largest.decodeFrom(key);
			if (parsed.sequence > t.maxSequence) {
				t.maxSequence = parsed.sequence;
			}
		}
		if (!iter.status().ok()) {
			status = iter.status();
		}
		iter.delete();
		Logger0.log0(options.infoLog, "Table #{}: {} entries {}", t.meta.number, counter, status);

		if (status.ok()) {
			tables.add(t);
		} else {
			repairTable(fname, t); // RepairTable archives input file.
		}
	}

	void repairTable(String src, TableInfo t) {
		// We will copy src contents to a new table and then rename the
		// new table over the source.

		// Create builder.
		String copy = FileName.getTableFileName(dbname, nextFileNumber++);
		Object0<WritableFile> file = new Object0<>();
		Status s = env.newWritableFile(copy, file);
		if (!s.ok())
			return;
			
		TableBuilder builder = new TableBuilder(options, file.getValue());

		// Copy data.
		Iterator0 iter = newTableIterator(t.meta);
		int counter = 0;
		for (iter.seekToFirst(); iter.valid(); iter.next()) {
			builder.add(iter.key(), iter.value());
			counter++;
		}
		iter.delete();

		archiveFile(src);
		if (counter == 0) {
			builder.abandon(); // Nothing to save
		} else {
			s = builder.finish();
			if (s.ok()) {
				t.meta.fileSize = builder.fileSize();
			}
		}
		builder.delete();
		builder = null;

		if (s.ok())
			s = file.getValue().close();
		file.getValue().delete();

		if (counter > 0 && s.ok()) {
			String orig = FileName.getTableFileName(dbname, t.meta.number);
			s = env.renameFile(copy, orig);
			if (s.ok()) {
				Logger0.log0(options.infoLog, "Table #{}: {} entries repaired", t.meta.number, counter);
				tables.add(t);
			}
		}
		
		if (!s.ok())
			env.deleteFile(copy);
	}
	
	Status writeDescriptor() {
		String tmp = FileName.getTempFileName(dbname, 1);
		Object0<WritableFile> file = new Object0<>();
		Status status = env.newWritableFile(tmp, file);
		if (!status.ok()) {
			return status;
		}

		long max_sequence = 0;
		for (int i = 0; i < tables.size(); i++) {
			if (max_sequence < tables.get(i).maxSequence) {
				max_sequence = tables.get(i).maxSequence;
			}
		}

		edit.setComparatorName(icmp.userComparator().name());
		edit.setLogNumber(0);
		edit.setNextFile(nextFileNumber);
		edit.setLastSequence(max_sequence);

		for (int i = 0; i < tables.size(); i++) {
			// TODO(opt): separate out into multiple levels
			TableInfo t = tables.get(i);
			edit.addFile(0, t.meta.number, t.meta.fileSize, t.meta.smallest, t.meta.largest, t.meta.numEntries);
		}

		// fprintf(stderr, "NewDescriptor:\n%s\n", edit_.DebugString().c_str());
		{
			LogWriter log = new LogWriter(file.getValue());
			ByteBuf record = ByteBufFactory.newUnpooled();
			edit.encodeTo(record);
			status = log.addRecord(SliceFactory.newUnpooled(record));
		}

		if (status.ok())
			status = file.getValue().close();
		file.getValue().delete();

		if (!status.ok()) {
			env.deleteFile(tmp);
		} else {
			// Discard older manifests
			for (int i = 0; i < manifests.size(); i++)
				archiveFile(dbname + "/" + manifests.get(i));

			// Install new manifest
			status = env.renameFile(tmp, FileName.getDescriptorFileName(dbname, 1));
			if (status.ok()) {
				status = FileName.setCurrentFile(env, dbname, 1);
			} else {
				env.deleteFile(tmp);
			}
		}
		return status;
	}
	
	void archiveFile(String fname) {
		// Move into another directory. E.g., for
		// dir/foo
		// rename to
		// dir/lost/foo
		int idx = fname.lastIndexOf('/');

		String newDir = null;
		if (idx >= 0) {
			newDir = fname.substring(0, idx);
		}

		newDir += "/lost";
		env.createDir(newDir); // Ignore error
		String newFile = newDir;
		newFile += ("/");
		newFile += ((idx < 0) ? fname : fname.substring(idx+1));
		Status s = env.renameFile(fname, newFile);
		Logger0.log0(options.infoLog, "Archiving {}: {}\n", fname, s);
	}
}
