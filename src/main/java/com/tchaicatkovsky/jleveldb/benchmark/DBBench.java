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
package com.tchaicatkovsky.jleveldb.benchmark;

import java.util.ArrayList;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;

import com.tchaicatkovsky.jleveldb.DB;
import com.tchaicatkovsky.jleveldb.Env;
import com.tchaicatkovsky.jleveldb.FilterPolicy;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.WriteOptions;
import com.tchaicatkovsky.jleveldb.util.BloomFilterPolicy;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.Cache;
import com.tchaicatkovsky.jleveldb.util.CondVar;
import com.tchaicatkovsky.jleveldb.util.Crc32C;
import com.tchaicatkovsky.jleveldb.util.UnpooledSlice;
import com.tchaicatkovsky.jleveldb.util.Histogram;
import com.tchaicatkovsky.jleveldb.util.Mutex;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Random0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.Snappy;
import com.tchaicatkovsky.jleveldb.util.TestUtil;

public class DBBench {

	static Env g_env = LevelDB.defaultEnv();

	static String FLAGS_benchmarks = "fillseq," + "fillseqsync," + "fillseqbatch," + "fillrandom," + "fillrandsync," + "fillrandbatch," + "overwrite," + "overwritebatch," + "readrandom," + "readseq,"
			+ "fillrand100K," + "fillseq100K," + "readseq," + "readrand100K,";
	static String[] FLAGS_benchmarks2 = FLAGS_benchmarks.split(",");

	// Comma-separated list of operations to run in the specified order
	// Actual benchmarks:
	// fillseq -- write N values in sequential key order in async mode
	// fillrandom -- write N values in random key order in async mode
	// overwrite -- overwrite N values in random key order in async mode
	// fillsync -- write N/100 values in random key order in sync mode
	// fill100K -- write N/1000 100K values in random order in async mode
	// deleteseq -- delete N keys in sequential order
	// deleterandom -- delete N keys in random order
	// readseq -- read N times sequentially
	// readreverse -- read N times in reverse order
	// readrandom -- read N times in random order
	// readmissing -- read N missing keys in random order
	// readhot -- read N times in random order from 1% section of DB
	// seekrandom -- N random seeks
	// open -- cost of opening a DB
	// crc32c -- repeated crc32c of 4K of data
	// acquireload -- load N*1000 times
	// Meta operations:
	// compact -- Compact the entire DB
	// stats -- Print DB stats
	// sstables -- Print sstable info
	// heapprofile -- Dump a heap profile (if supported by this port)

	// Number of key/values to place in database
	static int FLAGS_num = 1000000;

	// Number of read operations to do. If negative, do FLAGS_num reads.
	static int FLAGS_reads = -1;

	// Number of concurrent threads to run.
	static int FLAGS_threads = 1;

	// Size of each value
	static int FLAGS_value_size = 100;

	// Arrange to generate values that shrink to this fraction of
	// their original size after compression
	static double FLAGS_compression_ratio = 0.5;

	// Print histogram of operation timings
	static boolean FLAGS_histogram = false;

	// Number of bytes to buffer in memtable before compacting
	// (initialized to default value by "main")
	static int FLAGS_write_buffer_size = 0;

	// Number of bytes written to each file.
	// (initialized to default value by "main")
	static int FLAGS_max_file_size = 0;

	// Approximate size of user data packed per block (before compression.
	// (initialized to default value by "main")
	static int FLAGS_block_size = 0;

	// Number of bytes to use as a cache of uncompressed data.
	// Negative means use default settings.
	static int FLAGS_cache_size = -1;

	// Maximum number of files to keep open at the same time (use default if == 0)
	static int FLAGS_open_files = 0;

	// Bloom filter bits per key.
	// Negative means use default settings.
	static int FLAGS_bloom_bits = -1;

	// If true, do not destroy the existing database. If you set this
	// flag and also specify a benchmark that wants a fresh database, that
	// benchmark will fail.
	static boolean FLAGS_use_existing_db = false;

	// If true, reuse existing log/MANIFEST files when re-opening a database.
	static boolean FLAGS_reuse_logs = false;

	// Use the db with the following name.
	static String FLAGS_db = null;

	static class RandomGenerator {
		ByteBuf data = ByteBufFactory.newUnpooled();
		int pos;

		public RandomGenerator() {
			// We use a limited amount of data over and over again and ensure
			// that it is larger than the compression window (32KB), and also
			// large enough to serve all typical value sizes we want to write.
			Random0 rnd = new Random0(301);

			ByteBuf piece = ByteBufFactory.newUnpooled();

			while (data.size() < 1048576) {
				// Add a short fragment that is as compressible as specified
				// by FLAGS_compression_ratio.
				TestUtil.compressibleString(rnd, FLAGS_compression_ratio, 100, piece);
				data.append(piece);
			}
			pos = 0;
		}

		public Slice generate(int len) {
			if (pos + len > data.size()) {
				pos = 0;
				assert (len < data.size());
			}
			pos += len;
			return new UnpooledSlice(data.data(), data.offset() + pos - len, len);
		}
	}

	static Slice trimSpace(Slice s) {
		int start = 0;
		byte[] data = s.data();
		int offset = s.offset();
		while (start < s.size()) {
			if (Character.isSpaceChar((char) (data[offset + start] & 0xff)))
				start++;
			else
				break;
		}

		int limit = s.size();
		while (limit > start) {
			if (Character.isSpaceChar((char) (data[offset + limit - 1] & 0xff)))
				limit--;
			else
				break;
		}
		return new UnpooledSlice(data, offset + start, limit - start);
	}

	static void appendWithSpace(ByteBuf str, Slice msg) {
		if (msg.empty())
			return;
		if (!str.empty()) {
			str.addByte((byte) (' ' - '\0'));
		}
		str.append(msg.data(), msg.offset(), msg.size());
	}

	static class Stats {
		double start;
		double finish;
		double seconds;
		int done;
		int next_report;
		long bytes;
		double last_op_finish;
		Histogram hist = new Histogram();
		ByteBuf message = ByteBufFactory.newUnpooled();

		public Stats() {
			start();
		}

		void start() {
			next_report = 100;
			last_op_finish = start;
			hist.clear();
			done = 0;
			bytes = 0;
			seconds = 0;
			start = g_env.nowMillis();
			finish = start;
			message.clear();
		}

		void merge(Stats other) {
			hist.merge(other.hist);
			done += other.done;
			bytes += other.bytes;
			seconds += other.seconds;
			if (other.start < start)
				start = other.start;
			if (other.finish > finish)
				finish = other.finish;

			// Just keep the messages from one thread
			if (message.empty())
				message.assign(other.message);
		}

		void stop() {
			finish = g_env.nowMillis();
			seconds = (finish - start) * 1e-3;
		}

		void addMessage(Slice msg) {
			appendWithSpace(message, msg);
		}

		void finishedSingleOp() {
			if (FLAGS_histogram) {
				double now = g_env.nowMillis();
				double millis = now - last_op_finish;
				hist.add(millis);
				if (millis > 20) {
					System.err.printf("long op: %.1f millis%30s\r", millis, "");
					System.err.flush();
				}
				last_op_finish = now;
			}

			done++;
			if (done >= next_report) {
				if (next_report < 1000)
					next_report += 100;
				else if (next_report < 5000)
					next_report += 500;
				else if (next_report < 10000)
					next_report += 1000;
				else if (next_report < 50000)
					next_report += 5000;
				else if (next_report < 100000)
					next_report += 10000;
				else if (next_report < 500000)
					next_report += 50000;
				else
					next_report += 100000;
				System.err.printf("... finished %d ops%30s\r", done, "");
				System.err.flush();
			}
		}

		void addBytes(long n) {
			bytes += n;
		}

		void report(Slice name) {
			// Pretend at least one op was done in case we are running a benchmark
			// that does not call FinishedSingleOp().
			if (done < 1)
				done = 1;

			ByteBuf extra = ByteBufFactory.newUnpooled();
			if (bytes > 0) {
				// Rate is computed on actual elapsed time, not the sum of per-thread
				// elapsed times.
				double elapsed = (finish - start) * 1e-3;
				byte[] buf = String.format("%6.1f MB/s", (bytes / 1048576.0) / elapsed).getBytes();
				extra.assign(buf, 0, buf.length);
			}
			appendWithSpace(extra, message);

			System.out.printf("%-12s : %11.3f millis/op;%s%s\n", name.encodeToString(), seconds * 1e3 / done, (extra.empty() ? "" : " "), extra.encodeToString());

			if (FLAGS_histogram) {
				System.out.printf("Milliseconds per op:\n%s\n", hist.toString());
			}

			System.out.flush();
		}
	}

	// State shared by all concurrent executions of the same benchmark.
	static class SharedState {
		Mutex mu;
		CondVar cv;
		int total;

		// Each thread goes through the following states:
		// (1) initializing
		// (2) waiting for others to be initialized
		// (3) running
		// (4) done

		int num_initialized;
		int num_done;
		boolean start;

		public SharedState() {

			mu = new Mutex();
			cv = mu.newCondVar();
		}
	}

	// Per-thread state for concurrent executions of the same benchmark.
	static class ThreadState {
		int tid; // 0..n-1 when running in n threads
		Random0 rand; // Has different seeds for different threads
		Stats stats = new Stats();
		SharedState shared;

		public ThreadState(int index) {
			tid = index;
			rand = new Random0(1000 + index);
		}
	}

	public interface BenchmarkMethod {
		void run(ThreadState state);
	}

	static class ThreadArg {
		Benchmark bm;
		SharedState shared;
		ThreadState thread;
		BenchmarkMethod bmMethod;
	};

	static class ThreadBody implements Runnable {
		ThreadArg arg;

		public ThreadBody(ThreadArg arg) {
			this.arg = arg;
		}

		@Override
		public void run() {
			SharedState shared = arg.shared;
			ThreadState thread = arg.thread;
			try {
				shared.mu.lock();
				shared.num_initialized++;
				if (shared.num_initialized >= shared.total) {
					shared.cv.signalAll();
				}
				while (!shared.start) {
					try {
						shared.cv.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.exit(1);
					}
				}
			} finally {
				shared.mu.unlock();
			}

			thread.stats.start();
			arg.bmMethod.run(thread);
			thread.stats.stop();

			try {
				shared.mu.lock();
				shared.num_done++;
				if (shared.num_done >= shared.total) {
					shared.cv.signalAll();
				}
			} finally {
				shared.mu.unlock();
			}
		}
	}

	static class Benchmark {
		Cache cache;
		FilterPolicy filter_policy;
		DB db;
		int num;
		int value_size;
		int entries_per_batch;
		WriteOptions write_options = new WriteOptions();
		int reads;
		int heap_counter;

		void printHeader() {
			int kKeySize = 16;
			printEnvironment();
			System.out.printf("Keys:       %d bytes each\n", kKeySize);
			System.out.printf("Values:     %d bytes each (%d bytes after compression)\n", FLAGS_value_size, (int) (FLAGS_value_size * FLAGS_compression_ratio + 0.5));
			System.out.printf("Entries:    %d\n", num);
			System.out.printf("RawSize:    %.1f MB (estimated)\n", (((long) (kKeySize + FLAGS_value_size) * num) / 1048576.0));
			System.out.printf("FileSize:   %.1f MB (estimated)\n", (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num) / 1048576.0));
			printWarnings();
			System.out.printf("------------------------------------------------\n");
		}

		void printWarnings() {
			// See if snappy is working by attempting to compress a compressible string
			byte text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy".getBytes();
			ByteBuf compressed = ByteBufFactory.newUnpooled();
			if (!Snappy.compress(text, 0, text.length, compressed)) {
				System.out.printf("WARNING: Snappy compression is not enabled\n");
			} else if (compressed.size() >= text.length) {
				System.out.printf("WARNING: Snappy compression is not effective\n");
			}
		}

		void printEnvironment() {
			// TODO
		}

		public Benchmark() {
			cache = (FLAGS_cache_size >= 0 ? Cache.newLRUCache(FLAGS_cache_size) : null);

			filter_policy = (FLAGS_bloom_bits >= 0 ? BloomFilterPolicy.newBloomFilterPolicy(FLAGS_bloom_bits) : null);
			db = null;
			num = FLAGS_num;
			value_size = FLAGS_value_size;
			entries_per_batch = 1;
			reads = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
			heap_counter = 0;
			ArrayList<String> files = new ArrayList<>();
			g_env.getChildren(FLAGS_db, files);
			for (int i = 0; i < files.size(); i++) {
				if (files.get(i).startsWith("heap-")) {
					g_env.deleteFile(FLAGS_db + "/" + files.get(i));
				}
			}
			if (!FLAGS_use_existing_db) {
				try {
					LevelDB.destroyDB(FLAGS_db, new Options());
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
		}

		public void delete() {
			if (db != null) {
				db.close();
				db = null;
			}
			if (cache != null) {
				cache.delete();
				cache = null;
			}
			if (filter_policy != null) {
				filter_policy.delete();
				filter_policy = null;
			}
		}

		public void run() {
			printHeader();
			open();

			for (String benchmark : FLAGS_benchmarks2) {
				// Reset parameters that may be overridden below
				num = FLAGS_num;
				reads = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
				value_size = FLAGS_value_size;
				entries_per_batch = 1;
				write_options = new WriteOptions();

				BenchmarkMethod method = null;
				boolean fresh_db = false;
				int num_threads = FLAGS_threads;

				switch (benchmark) {
				case "open": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							openBench(t);
						}
					};
					num /= 10000;
					if (num < 1)
						num = 1;
				}
					break;
				case "fillseq": {
					fresh_db = true;
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							writeSeq(t);
						}
					};
				}
					break;
				case "fillbatch": {
					fresh_db = true;
					entries_per_batch = 1000;
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							writeSeq(t);
						}
					};
				}
					break;
				case "fillrandom": {
					fresh_db = true;
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							writeRandom(t);
						}
					};
				}
					break;
				case "overwrite": {
					fresh_db = false;
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							writeRandom(t);
						}
					};
				}
					break;
				case "fillsync": {
					fresh_db = true;
					num /= 1000;
					write_options.sync = true;
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							writeRandom(t);
						}
					};
				}
					break;
				case "fill100K": {
					fresh_db = true;
					num /= 1000;
					value_size = 100 * 1000;
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							writeRandom(t);
						}
					};
				}
					break;
				case "readseq": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							readSequential(t);
						}
					};
				}
					break;
				case "readreverse": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							readReverse(t);
						}
					};
				}
					break;
				case "readrandom": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							readRandom(t);
						}
					};
				}
					break;
				case "readmissing": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							readMissing(t);
						}
					};
				}
					break;
				case "seekrandom": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							seekRandom(t);
						}
					};
				}
					break;
				case "readhot": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							readHot(t);
						}
					};
				}
					break;
				case "readrandomsmall": {
					reads /= 1000;
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							readRandom(t);
						}
					};
				}
					break;
				case "deleteseq": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							deleteSeq(t);
						}
					};
				}
					break;
				case "deleterandom": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							deleteRandom(t);
						}
					};
				}
					break;
				case "readwhilewriting": {
					num_threads++; // Add extra thread for writing
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							readWhileWriting(t);
						}
					};
				}
					break;
				case "compact": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							compact(t);
						}
					};
				}
					break;
				case "crc32c": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							crc32c(t);
						}
					};
				}
					break;
				case "acquireload": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							acquireLoad(t);
						}
					};
				}
					break;
				case "snappycomp": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							snappyCompress(t);
						}
					};
				}
					break;
				case "snappyuncomp": {
					method = new BenchmarkMethod() {
						public void run(ThreadState t) {
							snappyUncompress(t);
						}
					};
				}
					break;
				case "heapprofile": {
					heapProfile();
				}
					break;
				case "stats": {
					printStats("leveldb.stats");
				}
					break;
				case "sstables": {
					printStats("leveldb.sstables");
				}
					break;
				default: {
					if (!benchmark.equals("")) { // No error message for empty name
						System.err.printf("unknown benchmark '%s'\n", benchmark);
					}
				}
					break;
				}

				if (fresh_db) {
					if (FLAGS_use_existing_db) {
						System.out.printf("%-12s : skipped (--use_existing_db is true)\n", benchmark);
						method = null;
					} else {
						db.close();
						db = null;
						try {
							LevelDB.destroyDB(FLAGS_db, new Options());
						} catch (Exception e) {
							e.printStackTrace();
							System.exit(1);
						}
						open();
					}
				}

				if (method != null) {
					runBenchmark(num_threads, new UnpooledSlice(benchmark), method);
				}
			}

		}

		void runBenchmark(int n, Slice name, BenchmarkMethod bmMethod) {
			SharedState shared = new SharedState();
			shared.total = n;
			shared.num_initialized = 0;
			shared.num_done = 0;
			shared.start = false;

			ThreadArg[] arg = new ThreadArg[n];
			for (int i = 0; i < n; i++) {
				arg[i] = new ThreadArg();
				arg[i].bm = this;
				arg[i].bmMethod = bmMethod;
				arg[i].shared = shared;
				arg[i].thread = new ThreadState(i);
				arg[i].thread.shared = shared;
				g_env.startThread(new ThreadBody(arg[i]));
			}

			shared.mu.lock();
			while (shared.num_initialized < n) {
				try {
					shared.cv.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}

			shared.start = true;
			shared.cv.signalAll();
			while (shared.num_done < n) {
				try {
					shared.cv.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
			shared.mu.unlock();

			for (int i = 1; i < n; i++) {
				arg[0].thread.stats.merge(arg[i].thread.stats);
			}
			arg[0].thread.stats.report(name);

			for (int i = 0; i < n; i++) {
				arg[i].thread = null;
				arg[i] = null;
			}
			arg = null;
		}

		void crc32c(ThreadState thread) {
			// Checksum about 500MB of data total
			int size = 4096;
			String label = "(4K per op)";
			String data0 = TestUtil.makeString(size, 'x');
			byte[] data = data0.getBytes();
			long bytes = 0;
			long crc = 0;
			while (bytes < 500 * 1048576) {
				crc = Crc32C.value(data, 0, data.length);
				thread.stats.finishedSingleOp();
				bytes += size;
			}
			// Print so result is not dead
			System.err.printf("... crc=0x%x\r", crc);

			thread.stats.addBytes(bytes);
			thread.stats.addMessage(new UnpooledSlice(label));
		}

		void acquireLoad(ThreadState thread) {
			// TODO
		}

		void snappyCompress(ThreadState thread) {
			// TODO

		}

		void snappyUncompress(ThreadState thread) {
			// TODO
		}

		void open() {
			assert (db == null);
			Options options = new Options();
			options.env = g_env;
			options.createIfMissing = !FLAGS_use_existing_db;
			options.blockCache = cache;
			options.writeBufferSize = FLAGS_write_buffer_size;
			options.maxFileSize = FLAGS_max_file_size;
			options.blockSize = FLAGS_block_size;
			options.maxOpenFiles = FLAGS_open_files;
			options.filterPolicy = filter_policy;
			options.reuseLogs = FLAGS_reuse_logs;
			Object0<DB> db0 = new Object0<>();

			Status s;
			try {
				s = LevelDB.newDB(options, FLAGS_db, db0);
			} catch (Exception e) {
				s = Status.otherError("" + e);
			}
			if (!s.ok()) {
				System.err.printf("open error: %s\n", s);
				System.exit(1);
			}
		}

		void openBench(ThreadState thread) {
			for (int i = 0; i < num; i++) {
				db.close();
				db = null;
				open();
				thread.stats.finishedSingleOp();
			}
		}

		void writeSeq(ThreadState thread) {
			doWrite(thread, true);
		}

		void writeRandom(ThreadState thread) {
			doWrite(thread, false);
		}

		void doWrite(ThreadState thread, boolean seq) {
			if (num != FLAGS_num) {
				thread.stats.addMessage(new UnpooledSlice(String.format("(%d ops)", num)));
			}

			RandomGenerator gen = new RandomGenerator();
			WriteBatch batch = new WriteBatch();
			Status s = Status.ok0();
			long bytes = 0;
			for (int i = 0; i < num; i += entries_per_batch) {
				batch.clear();
				for (int j = 0; j < entries_per_batch; j++) {
					int k = (int) (seq ? i + j : (thread.rand.next() % FLAGS_num));
					String key = String.format("%016d", k);
					batch.put(new UnpooledSlice(key), gen.generate(value_size));
					bytes += (value_size + key.length());
					thread.stats.finishedSingleOp();
				}

				try {
					s = db.write(write_options, batch);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
				if (!s.ok()) {
					System.err.printf("put error: %s\n", s);
					System.exit(1);
				}
			}
			thread.stats.addBytes(bytes);
		}

		void readSequential(ThreadState thread) {
			Iterator0 iter = db.newIterator(new ReadOptions());
			int i = 0;
			long bytes = 0;
			for (iter.seekToFirst(); i < reads && iter.valid(); iter.next()) {
				bytes += iter.key().size() + iter.value().size();
				thread.stats.finishedSingleOp();
				++i;
			}
			iter.delete();
			;
			thread.stats.addBytes(bytes);
		}

		void readReverse(ThreadState thread) {
			Iterator0 iter = db.newIterator(new ReadOptions());
			int i = 0;
			long bytes = 0;
			for (iter.seekToLast(); i < reads && iter.valid(); iter.prev()) {
				bytes += iter.key().size() + iter.value().size();
				thread.stats.finishedSingleOp();
				++i;
			}
			iter.delete();
			thread.stats.addBytes(bytes);
		}

		void readRandom(ThreadState thread) {
			ReadOptions options = new ReadOptions();
			ByteBuf value = ByteBufFactory.newUnpooled();
			int found = 0;
			for (int i = 0; i < reads; i++) {
				int k = (int) (thread.rand.next() % FLAGS_num);
				String key = String.format("%016d", k);
				try {
					if (db.get(options, new UnpooledSlice(key), value).ok()) {
						found++;
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
				thread.stats.finishedSingleOp();
			}

			String msg = String.format("(%d of %d found)", found, num);
			thread.stats.addMessage(new UnpooledSlice(msg));
		}

		void readMissing(ThreadState thread) {
			ReadOptions options = new ReadOptions();
			ByteBuf value = ByteBufFactory.newUnpooled();
			for (int i = 0; i < reads; i++) {
				int k = (int) (thread.rand.next() % FLAGS_num);
				String key = String.format("%016d.", k);
				try {
					db.get(options, new UnpooledSlice(key), value);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
				thread.stats.finishedSingleOp();
			}
		}

		void readHot(ThreadState thread) {
			ReadOptions options = new ReadOptions();
			ByteBuf value = ByteBufFactory.newUnpooled();
			int range = (FLAGS_num + 99) / 100;
			for (int i = 0; i < reads; i++) {
				int k = (int) (thread.rand.next() % range);
				String key = String.format("%016d", k);
				try {
					db.get(options, new UnpooledSlice(key), value);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
				thread.stats.finishedSingleOp();
			}
		}

		void seekRandom(ThreadState thread) {
			ReadOptions options = new ReadOptions();
			int found = 0;
			for (int i = 0; i < reads; i++) {
				Iterator0 iter = db.newIterator(options);

				int k = (int) (thread.rand.next() % FLAGS_num);
				String key = String.format("%016d", k);
				iter.seek(new UnpooledSlice(key));
				if (iter.valid() && iter.key().equals(new UnpooledSlice(key)))
					found++;
				iter.delete();
				thread.stats.finishedSingleOp();
			}
			String msg = String.format("(%d of %d found)", found, num);
			thread.stats.addMessage(new UnpooledSlice(msg));
		}

		void doDelete(ThreadState thread, boolean seq) {
			RandomGenerator gen = new RandomGenerator();
			WriteBatch batch = new WriteBatch();
			Status s = Status.ok0();
			for (int i = 0; i < num; i += entries_per_batch) {
				batch.clear();
				for (int j = 0; j < entries_per_batch; j++) {
					int k = (int) (seq ? i + j : (thread.rand.next() % FLAGS_num));
					String key = String.format("%016d", k);
					batch.delete(new UnpooledSlice(key));
					thread.stats.finishedSingleOp();
				}
				try {
					s = db.write(write_options, batch);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
				if (!s.ok()) {
					System.err.printf("del error: %s\n", s);
					System.exit(1);
				}
			}
		}

		void deleteSeq(ThreadState thread) {
			doDelete(thread, true);
		}

		void deleteRandom(ThreadState thread) {
			doDelete(thread, false);
		}

		void readWhileWriting(ThreadState thread) {
			if (thread.tid > 0) {
				readRandom(thread);
			} else {
				// Special thread that keeps writing until other threads are done.
				RandomGenerator gen = new RandomGenerator();
				while (true) {
					try {
						thread.shared.mu.lock();
						if (thread.shared.num_done + 1 >= thread.shared.num_initialized) {
							// Other threads have finished
							break;
						}
					} finally {
						thread.shared.mu.unlock();
					}

					int k = (int) (thread.rand.next() % FLAGS_num);
					String key = String.format("%016d", k);
					Status s = Status.ok0();
					try {
						s = db.put(write_options, new UnpooledSlice(key), gen.generate(value_size));
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
					if (!s.ok()) {
						System.err.printf("put error: %s\n", s);
						System.exit(1);
					}
				}

				// Do not count any of the preceding work/delay in stats.
				thread.stats.start();
			}
		}

		void compact(ThreadState thread) {
			try {
				db.compactRange(null, null);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		void printStats(String key) {
			Object0<String> stats0 = new Object0<>();
			if (!db.getProperty(key, stats0)) {
				stats0.setValue("(failed)");
			}
			System.out.printf("\n%s\n", stats0.getValue());
		}

		void writeToFile(Object arg, byte[] buf, int offset, int n) {
			((WritableFile) arg).append(new UnpooledSlice(buf, offset, n));
		}

		//TODO
		void heapProfile() {	
//			String fname = String.format("%s/heap-%04d", FLAGS_db, ++heap_counter);
//		    Object0<WritableFile> file0 = new Object0<>();
//		    Status s = g_env.newWritableFile(fname, file0);
//		    if (!s.ok()) {
//		      System.err.printf("%s\n", s);
//		      return;
//		    }
//		    WritableFile file = file0.getValue();
//		    boolean ok = port::GetHeapProfile(WriteToFile, file);
//		    file.delete();
//		    if (!ok) {
//		    	System.err.printf("heap profiling not supported\n");
//		    	g_env.deleteFile(fname);
//		    }
		}
	}

	public static void main(String args[]) throws Exception {
		FLAGS_write_buffer_size = (new Options()).writeBufferSize;
		FLAGS_max_file_size = (new Options()).maxFileSize;
		FLAGS_block_size = (new Options()).blockSize;
		FLAGS_open_files = (new Options()).maxOpenFiles;
		String default_db_path = null;

		CommandLineParser parser = new BasicParser();
		org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
		options.addOption(null, "benchmarks", true, "Benchmark type list, seperated by comma");
		options.addOption(null, "compression_ratio", true, "");
		options.addOption(null, "histogram", true, "boolean value (true/false)");
		options.addOption(null, "use_existing_db", true, "boolean value (true/false)");
		options.addOption(null, "reuse_logs", true, "boolean value (true/false)");
		options.addOption(null, "num", true, "");
		options.addOption(null, "reads", true, "");
		options.addOption(null, "threads", true, "");
		options.addOption(null, "value_size", true, "");
		options.addOption(null, "write_buffer_size", true, "");
		options.addOption(null, "max_file_size", true, "");
		options.addOption(null, "block_size", true, "");
		options.addOption(null, "cache_size", true, "");
		options.addOption(null, "bloom_bits", true, "");
		options.addOption(null, "open_files", true, "");
		options.addOption(null, "db", true, "");
		options.addOption("h", "help", false, "Print usage message");

		CommandLine commandLine = parser.parse(options, args);
		
		String formatstr = "DBBench [options]";
		
		if (commandLine.hasOption("help")) {
			HelpFormatter hf = new HelpFormatter();
			hf.printHelp(formatstr, "", options, "");
			System.exit(0);
		}
		
		if (commandLine.hasOption("benchmarks")) {
			FLAGS_benchmarks = commandLine.getOptionValue("benchmarks");
			FLAGS_benchmarks2 = FLAGS_benchmarks.split(",");
		}
		if (commandLine.hasOption("compression_ratio")) {
			FLAGS_compression_ratio = Double.parseDouble(commandLine.getOptionValue("compression_ratio"));
		}
		if (commandLine.hasOption("histogram")) {
			FLAGS_histogram = Boolean.parseBoolean(commandLine.getOptionValue("histogram"));
		}
		if (commandLine.hasOption("use_existing_db")) {
			FLAGS_use_existing_db = Boolean.parseBoolean(commandLine.getOptionValue("use_existing_db"));
		}
		if (commandLine.hasOption("reuse_logs")) {
			FLAGS_reuse_logs = Boolean.parseBoolean(commandLine.getOptionValue("reuse_logs"));
		}
		if (commandLine.hasOption("num")) {
			FLAGS_num = Integer.parseInt(commandLine.getOptionValue("num"));
		}
		if (commandLine.hasOption("reads")) {
			FLAGS_reads = Integer.parseInt(commandLine.getOptionValue("reads"));
		}
		if (commandLine.hasOption("threads")) {
			FLAGS_threads = Integer.parseInt(commandLine.getOptionValue("threads"));
		}
		if (commandLine.hasOption("value_size")) {
			FLAGS_value_size = Integer.parseInt(commandLine.getOptionValue("value_size"));
		}
		if (commandLine.hasOption("write_buffer_size")) {
			FLAGS_write_buffer_size = Integer.parseInt(commandLine.getOptionValue("write_buffer_size"));
		}
		if (commandLine.hasOption("max_file_size")) {
			FLAGS_max_file_size = Integer.parseInt(commandLine.getOptionValue("max_file_size"));
		}
		if (commandLine.hasOption("block_size")) {
			FLAGS_block_size = Integer.parseInt(commandLine.getOptionValue("block_size"));
		}
		if (commandLine.hasOption("cache_size")) {
			FLAGS_cache_size = Integer.parseInt(commandLine.getOptionValue("cache_size"));
		}
		if (commandLine.hasOption("bloom_bits")) {
			FLAGS_bloom_bits = Integer.parseInt(commandLine.getOptionValue("bloom_bits"));
		}
		if (commandLine.hasOption("open_files")) {
			FLAGS_open_files = Integer.parseInt(commandLine.getOptionValue("open_files"));
		}
		if (commandLine.hasOption("db")) {
			FLAGS_db = commandLine.getOptionValue("db");
		}

		if (FLAGS_db == null) {
			Object0<String> default_db_path0 = new Object0<>();

			g_env.getTestDirectory(default_db_path0);
			default_db_path = default_db_path0.getValue();
			default_db_path += "/dbbench";
			FLAGS_db = default_db_path;
		}

		Benchmark benchmark = new Benchmark();
		benchmark.run();
	}
}
