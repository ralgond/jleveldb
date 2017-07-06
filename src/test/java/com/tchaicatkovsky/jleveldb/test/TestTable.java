package com.tchaicatkovsky.jleveldb.test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import com.tchaicatkovsky.jleveldb.CompressionType;
import com.tchaicatkovsky.jleveldb.DB;
import com.tchaicatkovsky.jleveldb.Iterator0;
import com.tchaicatkovsky.jleveldb.LevelDB;
import com.tchaicatkovsky.jleveldb.Logger0;
import com.tchaicatkovsky.jleveldb.Options;
import com.tchaicatkovsky.jleveldb.RandomAccessFile0;
import com.tchaicatkovsky.jleveldb.ReadOptions;
import com.tchaicatkovsky.jleveldb.Status;
import com.tchaicatkovsky.jleveldb.WritableFile;
import com.tchaicatkovsky.jleveldb.WriteBatch;
import com.tchaicatkovsky.jleveldb.WriteOptions;
import com.tchaicatkovsky.jleveldb.db.MemTable;
import com.tchaicatkovsky.jleveldb.db.WriteBatchInternal;
import com.tchaicatkovsky.jleveldb.db.format.DBFormat;
import com.tchaicatkovsky.jleveldb.db.format.InternalKeyComparator;
import com.tchaicatkovsky.jleveldb.db.format.ParsedInternalKey;
import com.tchaicatkovsky.jleveldb.db.format.ValueType;
import com.tchaicatkovsky.jleveldb.table.Block;
import com.tchaicatkovsky.jleveldb.table.BlockBuilder;
import com.tchaicatkovsky.jleveldb.table.Table;
import com.tchaicatkovsky.jleveldb.table.TableBuilder;
import com.tchaicatkovsky.jleveldb.table.TableFormat.BlockContents;
import com.tchaicatkovsky.jleveldb.util.ByteBuf;
import com.tchaicatkovsky.jleveldb.util.ByteBufFactory;
import com.tchaicatkovsky.jleveldb.util.BytewiseComparatorImpl;
import com.tchaicatkovsky.jleveldb.util.Comparator0;
import com.tchaicatkovsky.jleveldb.util.Integer0;
import com.tchaicatkovsky.jleveldb.util.ListUtils;
import com.tchaicatkovsky.jleveldb.util.Object0;
import com.tchaicatkovsky.jleveldb.util.Random0;
import com.tchaicatkovsky.jleveldb.util.Slice;
import com.tchaicatkovsky.jleveldb.util.SliceFactory;
import com.tchaicatkovsky.jleveldb.util.Snappy;
import com.tchaicatkovsky.jleveldb.util.Strings;
import com.tchaicatkovsky.jleveldb.util.TestUtil;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestTable {
	// Return reverse of "key".
	// Used to test non-lexicographic comparators.
	static Slice reverse(Slice key) {
		byte[] b = new byte[key.size()];
		for (int i = 0; i < b.length; i++) {
			b[i] = key.getByte(b.length-1-i);
		}
		return SliceFactory.newUnpooled(b, 0, b.length);
	}
	
	static class ReverseKeyComparator extends Comparator0 {
		public String name() {
		    return "leveldb.ReverseBytewiseComparator";
		}

		public int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
			int astart = aoff + asize - 1;
			
			int bstart = boff + bsize - 1;
			
			for (int i = 0; i < asize && i < bsize; i++) {
				byte a0 = a[astart-i];
				byte b0 = b[bstart-i];
				if (a0 == b0) {
					continue;
				}
				
				if (a0 < b0)
					return -1;
				else 
					return 1;
			}
			
			if (asize == bsize) 
				return 0;
			
			return (asize < bsize) ? -1 : +1; 
		}

		public void findShortestSeparator(ByteBuf start, Slice limit) {
			Slice s = reverse(SliceFactory.newUnpooled(start));
		    Slice l = reverse(limit);
		    ByteBuf s0 = ByteBufFactory.newUnpooled();
		    s0.assign(s.data(), s.offset(), s.size());
		    BytewiseComparatorImpl.getInstance().findShortestSeparator(s0, l);
		    Slice s1 = reverse(SliceFactory.newUnpooled(s0));
		    start.assign(s1.data(), s1.offset(), s1.size());
		}

		public void findShortSuccessor(ByteBuf key) {
		    Slice s = reverse(SliceFactory.newUnpooled(key));
		    ByteBuf s0 = ByteBufFactory.newUnpooled();
		    s0.assign(s.data(), s.offset(), s.size());
		    BytewiseComparatorImpl.getInstance().findShortSuccessor(s0);
		    Slice s1 = reverse(SliceFactory.newUnpooled(s0));
		    key.assign(s1.data(), s1.offset(), s1.size());
		}
	};
	
	static ReverseKeyComparator reverse_key_comparator = new ReverseKeyComparator();
	
	static void increment(Comparator0 cmp, ByteBuf key) {
		if (cmp == BytewiseComparatorImpl.getInstance()) {
			key.addByte((byte)0);
		} else {
			assert(cmp == reverse_key_comparator);
			Slice rev0 = reverse(SliceFactory.newUnpooled(key));
			ByteBuf rev = ByteBufFactory.newUnpooled();
			rev.assign(rev0.data(), rev0.offset(), rev0.size());
			
			rev.addByte((byte)0);
			
			Slice s = reverse(SliceFactory.newUnpooled(rev));
			key.assign(s.data(), s.offset(), s.size());
		}
	}

	static class StringSink implements WritableFile {
		
		public void delete() {
			
		}
		
		public ByteBuf contents() { 
			return contents; 
		}

		public Status close() { 
			return Status.ok0(); 
		}
		
		public Status flush() { 
			return Status.ok0();  
		}
		
		public Status sync() { 
			return Status.ok0();
		}

		public Status append(Slice data) {
		    contents.append(data.data(), data.offset(), data.size());
		    return Status.ok0();
		}

		ByteBuf contents = ByteBufFactory.newUnpooled();
	}
	
	static class StringSource implements RandomAccessFile0 {
		public StringSource(Slice s) {
			contents.assign(s.data(), s.offset(), s.size());
		}
		
		public String name() {
			return "StringSource";
		}

		public void delete() {
			
		}
		
		public void close() {
			
		}

		public long  size() { 
			return contents.size(); 
		}

		public Status read(long offset, int n, Slice result, byte[] scratch) {
		    if (offset > contents.size()) {
		      return Status.invalidArgument("invalid Read offset");
		    }
		    if (offset + n > contents.size()) {
		    	n = (int)(contents.size() - offset);
		    }
		    
		    System.arraycopy(contents.data(), (int)offset, scratch, 0, n);
		    
		    result.init(scratch, 0, n);
		    return Status.ok0();
		  }

		  ByteBuf contents = ByteBufFactory.newUnpooled();
	};
	
	static class ByteBufComparator implements Comparator<ByteBuf> {
		Comparator0 cmp;

		public ByteBufComparator() {
			cmp = BytewiseComparatorImpl.getInstance();
		}
		
		public ByteBufComparator(Comparator0 c) {
			cmp = c;
		}
		
		public int compare(ByteBuf a, ByteBuf b) {
		    return cmp.compare(a, b);
		}
	};
	
	static ByteBufComparator defaultByteBufComparator = new ByteBufComparator();
	
	static class ByteBufKeyMapEntryComparator implements Comparator<Map.Entry<ByteBuf,ByteBuf>> {
		Comparator0 cmp;

		public ByteBufKeyMapEntryComparator() {
			cmp = BytewiseComparatorImpl.getInstance();
		}
		
		public ByteBufKeyMapEntryComparator(Comparator0 c) {
			cmp = c;
		}
		
		public int compare(Map.Entry<ByteBuf,ByteBuf> a, Map.Entry<ByteBuf,ByteBuf> b) {
		    return cmp.compare(a.getKey(), b.getKey());
		}
		
		public void setComparator(Comparator0 cmp) {
			this.cmp = cmp;
		}
	};
	
	static ByteBufKeyMapEntryComparator defaultByteBufKeyMapEntryComparator = new ByteBufKeyMapEntryComparator();
	
	// Helper class for tests to unify the interface between
	// BlockBuilder/TableBuilder and Block/Table.
	static abstract class Constructor {
		public TreeMap<ByteBuf, ByteBuf> dataMap;
		public Comparator0 cmp;
		public Constructor(Comparator0 cmp) {
			this.cmp = cmp;
			dataMap = new TreeMap<ByteBuf, ByteBuf>(new ByteBufComparator(cmp));
		}

		public void add(ByteBuf key,Slice value) {
			ByteBuf v = ByteBufFactory.newUnpooled();
			v.assign(value.data(), value.offset(), value.size());
			dataMap.put(key, v);
		}

		// Finish constructing the data structure with all the keys that have
		// been added so far.  Returns the keys in sorted order in "*keys"
		// and stores the key/value pairs in "*kvmap"
		void finish(Options options, ArrayList<ByteBuf> keys, 
				Object0<TreeMap<ByteBuf,ByteBuf>> kvmap) {
	    	kvmap.setValue(dataMap);
	    	keys.clear();
	    	for (ByteBuf key : dataMap.keySet()) {
	    		keys.add(key);
	    	}
	    	
	    	dataMap = new TreeMap<ByteBuf,ByteBuf>(new ByteBufComparator(cmp));
	    	Status s = finishImpl(options, kvmap.getValue());
	    	assertTrue(s.ok());
		}

		// Construct the data structure from the data in "data"
		public abstract Status finishImpl(Options options, TreeMap<ByteBuf,ByteBuf> data);

		public abstract Iterator0 newIterator();

		public TreeMap<ByteBuf,ByteBuf> data() { 
			return dataMap; 
		}

		public DB db() { 
			return null; 
		}  // Overridden in DBConstructor
		
		public abstract void delete();
		
		public abstract void dumpDataRange();
	}
	
	static class BlockConstructor extends Constructor {

		Comparator0 comparator;
		ByteBuf data = ByteBufFactory.newUnpooled();
		Block block;

		public BlockConstructor(Comparator0 cmp) {
			super(cmp);
		    comparator = (cmp);
		    block = null;
		}
		
		public Status finishImpl(Options options, TreeMap<ByteBuf, ByteBuf> dataMap) {
			delete();
			
		    BlockBuilder builder = new BlockBuilder(options);

		    for (Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet()) {
		    	builder.add(SliceFactory.newUnpooled(e.getKey()), SliceFactory.newUnpooled(e.getValue()));
		    }
		    
		    // Open the block
		    Slice s = builder.finish();
		    data.assign(s.data(), s.offset(), s.size());
		    BlockContents contents = new BlockContents();
		    contents.data = SliceFactory.newUnpooled(data);
		    contents.cachable = false;
		    contents.heapAllocated = false;
		    block = new Block(contents);
		    return Status.ok0();
		}
		
		public Iterator0 newIterator() {
		    return block.newIterator(comparator);
		}
		
		public void delete() {
			if (block != null) {
			    block.delete();
			    block = null;
			}
		}
		
		public void dumpDataRange() {
			
		}
	};
	
	static class TableConstructor extends Constructor {
		public TableConstructor(Comparator0 cmp) {
			super(cmp);
			source = null;
			table = null;
		}
		  
		@Override
		public void delete() {
			reset();
		}
		  
		public Status finishImpl(Options options, TreeMap<ByteBuf, ByteBuf> dataMap) {
		    reset();
		    StringSink sink = new StringSink();
		    TableBuilder builder = new TableBuilder(options, sink);

		    for (Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet()) {
		    	builder.add(SliceFactory.newUnpooled(e.getKey()), SliceFactory.newUnpooled(e.getValue()));
		    	assertTrue(builder.status().ok());
		    }
		    Status s = builder.finish();
		    assertTrue(s.ok());

		    assertEquals(sink.contents().size(), builder.fileSize());

		    // Open the table
		    source = new StringSource(SliceFactory.newUnpooled(sink.contents()));
		    Options tableOptions = new Options();
		    tableOptions.comparator = options.comparator;
		    Object0<Table> table0 = new Object0<Table>();
		    s = Table.open(tableOptions, source, sink.contents().size(), table0);
		    table = table0.getValue();
		    return s;
		  }

		public  Iterator0 newIterator() {
		    return table.newIterator(new ReadOptions());
		}

		public long approximateOffsetOf(Slice key) {
		    return table.approximateOffsetOf(key);
		}

		void reset() {
			if (table != null) {
				table.delete();
				table = null;
			}
			if (source != null) {
			    source.delete();
			    source = null;
			}
		}
		
		public void dumpDataRange() {
			
		}

		StringSource source;
		Table table;
	};
	
	
	// A helper class that converts internal format keys into user keys
	static class KeyConvertingIterator extends Iterator0 {

		Status status = Status.ok0();
		Iterator0 iter;
		  
		public KeyConvertingIterator(Iterator0 iter) {
			this.iter = iter;
		}
		
		public void delete() { 
			super.delete();
			iter.delete();
		}
		
		public boolean valid() { 
			return iter.valid();
		}
		
		public void seek(Slice target) {
			ParsedInternalKey ikey = new ParsedInternalKey(target, DBFormat.kMaxSequenceNumber, ValueType.Value);
			ByteBuf encoded = ByteBufFactory.newUnpooled();
			DBFormat.appendInternalKey(encoded, ikey);
			iter.seek(SliceFactory.newUnpooled(encoded));
		}
		
	  	public void seekToFirst() { 
	  		iter.seekToFirst(); 
	  	}
	  	
	  	public void seekToLast() { 
	  		iter.seekToLast(); 
	  	}
	  	
	  	public void next() { 
	  		iter.next(); 
	  	}
	  	
	  	public void prev() { 
	  		iter.prev(); 
	  	}

	  	public Slice key() {
	  		assert(valid());
	  		ParsedInternalKey key = new ParsedInternalKey();
	  		if (!key.parse(iter.key())) {
	  			status = Status.corruption("malformed internal key");
	  			return SliceFactory.newUnpooled("corrupted key");
	  		}
	  		return key.userKey;
	  	}

	  	public Slice value() { 
	  		return iter.value(); 
	  	}
	  	
	  	public Status status() {
	  		return status.ok() ? iter.status() : status;
	  	}
	};
	
	
	static class MemTableConstructor extends Constructor {
		InternalKeyComparator internalComparator;
		MemTable memtable;
	
		public MemTableConstructor(Comparator0 cmp) {
			super(cmp);
			internalComparator = new InternalKeyComparator(cmp);
		    memtable = new MemTable(internalComparator);
		    memtable.ref();
		}
		
		public void delete() {
			memtable.unref();
			memtable = null;
		}
		
		public Status finishImpl(Options options, TreeMap<ByteBuf, ByteBuf> dataMap) {
			delete();
			
		    memtable = new MemTable(internalComparator);
		    memtable.ref();
		    int seq = 1;
		    for (Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet()) {
		    	memtable.add(seq, ValueType.Value, SliceFactory.newUnpooled(e.getKey()), SliceFactory.newUnpooled(e.getValue()));
		    	seq++;
		    }
		    return Status.ok0();
		}
		
		public Iterator0 newIterator() {
		    return new KeyConvertingIterator(memtable.newIterator());
		}
		
		public void dumpDataRange() {
			
		}
	}
	
	
	static class DBConstructor extends Constructor {
		Comparator0 comparator;
		DB db;
		  
		public DBConstructor(Comparator0 cmp) {
		    super(cmp);
		    comparator = cmp;
		    db = null;
		    newDB();
		}
		
		@Override
		public void delete() {
			if (db != null) {
				db.close();
				db = null;
			}
		}
		
		public Status finishImpl(Options options, TreeMap<ByteBuf, ByteBuf> dataMap) {
			delete();
			
		    newDB();
		    for (Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet()) {
		    	WriteBatch batch = new WriteBatch();
		    	batch.put(SliceFactory.newUnpooled(e.getKey()), SliceFactory.newUnpooled(e.getValue()));
		    	try {
		    		Status s = db.write(new WriteOptions(), batch);
		    		assertTrue(s.ok());
		    	} catch (Exception ex) {
		    		ex.printStackTrace();
		    		assertTrue(false);
		    	}
		    }
		    return Status.ok0();
		}
		
		public Iterator0 newIterator() {
		    return db.newIterator(new ReadOptions());
		}

		public DB db() { 
			return db; 
		}

		void newDB() {
		    String name = TestUtil.tmpDir() + "/table_testdb";

		    Status status = Status.ok0();
		    
		    Options options = new Options(comparator);
		    try {
		    	status = LevelDB.destroyDB(name, options);
		    } catch (Exception e) {
		    	e.printStackTrace();
		    	status = Status.otherError(""+e);
		    }
		    assertTrue(status.ok());

		    options.createIfMissing = true;
		    options.errorIfExists = true;
		    options.writeBufferSize = 10000;  // Something small to force merging
		    Object0<DB> db0 = new Object0<DB>();
		    try {
		    	status = LevelDB.newDB(options, name, db0);
		    	db = db0.getValue();
		    } catch (Exception e) {
		    	e.printStackTrace();
		    	status = Status.otherError(""+e);
		    }
		    assertTrue(status.ok());
		}

		public void dumpDataRange() {
			String s = db.debugDataRange();
			System.out.println("DBConstruct.db.dataRange : "+s);
		}
	};
	
	enum TestType {
		  TABLE_TEST,
		  BLOCK_TEST,
		  MEMTABLE_TEST,
		  DB_TEST
	};
	
	static class TestArgs {
		  TestType type;
		  boolean reverse_compare;
		  int restart_interval;
		  
		  public TestArgs(TestType type, boolean reverse_compare, int restart_interval) {
			  this.type = type;
			  this.reverse_compare = reverse_compare;
			  this.restart_interval = restart_interval;
		  }
		  
		  @Override
		  public String toString() {
			  return String.format("{type:%s,reverse_compare:%s,restart_interval:%d", 
					  type.name(), reverse_compare, restart_interval);
		  }
	};
	
	static TestArgs kTestArgList[];
	static {
		ArrayList<TestArgs> l = new ArrayList<>();
		l.add(new TestArgs(TestType.TABLE_TEST, false, 16)); 	//0
		l.add(new TestArgs(TestType.TABLE_TEST, false, 1));  	//1
		l.add(new TestArgs(TestType.TABLE_TEST, false, 1024));	//2
		l.add(new TestArgs(TestType.TABLE_TEST, true, 16));		//3
		l.add(new TestArgs(TestType.TABLE_TEST, true, 1));		//4
		l.add(new TestArgs(TestType.TABLE_TEST, true, 1024));	//5
		
		l.add(new TestArgs(TestType.BLOCK_TEST, false, 16));	//6
		l.add(new TestArgs(TestType.BLOCK_TEST, false, 1));		//7
		l.add(new TestArgs(TestType.BLOCK_TEST, false, 1024));	//8
		l.add(new TestArgs(TestType.BLOCK_TEST, true, 16));		//9
		l.add(new TestArgs(TestType.BLOCK_TEST, true, 1));		//10
		l.add(new TestArgs(TestType.BLOCK_TEST, true, 1024));	//11
		
		// Restart interval does not matter for memtables
		l.add(new TestArgs(TestType.MEMTABLE_TEST, false, 16));	//12
		l.add(new TestArgs(TestType.MEMTABLE_TEST, true, 16));	//13
		
		// Do not bother with restart interval variations for DB
		l.add(new TestArgs(TestType.DB_TEST, false, 16));		//14
		l.add(new TestArgs(TestType.DB_TEST, true, 16));		//15
		
		kTestArgList = new TestArgs[l.size()];
		for (int i = 0; i < kTestArgList.length; i++) {
			kTestArgList[i] = l.get(i);
		}
	}
	
	static class Harness {
		Options options;
		Constructor constructor;
		  
		public Harness() {
			constructor = null;
		}
		
		public void delete() {
			if (constructor != null) {
				constructor.delete();
				constructor = null;
			}
		}
		
		public void init(TestArgs args) {
			delete();
			
			options = new Options();
			
		    options.blockRestartInterval = args.restart_interval;
		    // Use shorter block size for tests to exercise block boundary
		    // conditions more.
		    options.blockSize = 256;
		    if (args.reverse_compare) {
		    	options.comparator = reverse_key_comparator;
		    }
		    switch (args.type) {
		    case TABLE_TEST:
		        constructor = new TableConstructor(options.comparator);
		        break;
		    case BLOCK_TEST:
		        constructor = new BlockConstructor(options.comparator);
		        break;
		    case MEMTABLE_TEST:
		        constructor = new MemTableConstructor(options.comparator);
		        break;
		    case DB_TEST:
		        constructor = new DBConstructor(options.comparator);
		        break;
		    }
		}
		
		public void add(ByteBuf key, Slice value) {
		    constructor.add(key, value);
		}
		
		void test(Random0 rnd) {
		    ArrayList<ByteBuf> keys = new ArrayList<>();
		    Object0<TreeMap<ByteBuf,ByteBuf>> dataMap0 = new Object0<>();
		    constructor.finish(options, keys, dataMap0);
		    TreeMap<ByteBuf,ByteBuf> dataMap = dataMap0.getValue();

		    testForwardScan(keys, dataMap);
		    testBackwardScan(keys, dataMap);
		    testRandomAccess(rnd, keys, dataMap);
		}
		
		public void testForwardScan(ArrayList<ByteBuf> keys, TreeMap<ByteBuf,ByteBuf> dataMap) {
			if (kVerbose) 
				System.out.printf("\n[DEBUG] testForwardScan\n");
			Logger0.setDebug(true);
			//constructor.dumpDataRange();
			Logger0.setDebug(false);
			
			Iterator0 iter = constructor.newIterator();
			
			int readCnt = 1;
			assertTrue(!iter.valid());
			iter.seekToFirst();
			Iterator<Map.Entry<ByteBuf, ByteBuf>> it = dataMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<ByteBuf, ByteBuf> e = it.next();
				//String itStr = toString(it);
				String itStr = toString(e);
				String iterStr = toString(iter);
				if (!itStr.equals(iterStr)) {
					System.out.printf("[DEBUG] testForwardScan NOT-EQUALS, [%d] it:%s  -  iter:%s\n", 
							readCnt, itStr, iterStr);
					
					//constructor.dumpDataRange();
					
					{
						Iterator0 iter0 = constructor.newIterator();
						iter0.seek(SliceFactory.newUnpooled(e.getKey()));
						if (iter0.valid()) {
							System.out.printf("[DEBUG] testForwardScan NOT-EQUALS, check, key0=%s, value0=%s\n", 
								Strings.escapeString(e.getKey()), null);
						} else {
							Slice value0 = iter0.value();
							System.out.printf("[DEBUG] testForwardScan NOT-EQUALS, check, key0=%s, value0=%s\n", 
									Strings.escapeString(e.getKey()), Strings.escapeString(value0));	
						}
						iter0.delete();
						iter0 = null;
					}
				}
				
				assertEquals(itStr, iterStr);

				iter.next();
				readCnt++;
			}
			assertEquals(toString(it), toString(iter));
			
			assertTrue(!iter.valid());
			
			iter.delete();
		}
		
		public void testBackwardScan(ArrayList<ByteBuf> keys, TreeMap<ByteBuf,ByteBuf> dataMap) {
			if (kVerbose) 
				System.out.printf("\n[DEBUG] testBackwardScan\n");
			Iterator0 iter = constructor.newIterator();
			assertTrue(!iter.valid());
			iter.seekToLast();
			Iterator<Map.Entry<ByteBuf, ByteBuf>> it = dataMap.descendingMap().entrySet().iterator();
			int readCnt = 1;
			while (it.hasNext()) {
				Map.Entry<ByteBuf, ByteBuf> e = it.next();
				//String itStr = toString(it);
				String itStr = toString(e);
				String iterStr = toString(iter);
				if (!itStr.equals(iterStr)) {
					System.out.printf("[DEBUG] testBackwardScan NOT-EQUALS, [%d] it:%s  -  iter:%s\n", 
							readCnt, itStr, iterStr);
				}
				
				iter.prev();
				
				readCnt++;
			}
			assertEquals(toString(it), toString(iter));
			
			assertTrue(!iter.valid());
			
			iter.delete();
		}
		
		static String toString(Map.Entry<ByteBuf, ByteBuf> e) {
			return "'" + Strings.escapeString(e.getKey()) + "->" + Strings.escapeString(e.getValue())  + "'";
		}
		
		static String toString(Iterator<Map.Entry<ByteBuf, ByteBuf>> it) {
		    if (!it.hasNext()) {
		    	return "END";
		    } else {
		    	Map.Entry<ByteBuf, ByteBuf> e = it.next();
		    	//return "'" + e.getKey().encodeToString() + "->" + e.getValue().encodeToString() + "'";
		    	return "'" + Strings.escapeString(e.getKey()) + "->" + Strings.escapeString(e.getValue())  + "'";
		    }
		}
		
		static String toString(ArrayList<Map.Entry<ByteBuf, ByteBuf>> l, int pos) {
		    if (pos + 1 > l.size()) {
		    	return "END";
		    } else {
		    	Map.Entry<ByteBuf, ByteBuf> e = l.get(pos);
		    	//return "'" + e.getKey().encodeToString() + "->" + e.getValue().encodeToString() + "'";
		    	return "'" + Strings.escapeString(e.getKey()) + "->" + Strings.escapeString(e.getValue())  + "'";
		    }
		}
		
		
		static String toString(Iterator0 it) {
			if (!it.valid()) {
				return "END";
			} else {
				//return "'" + it.key().encodeToString() + "->" + it.value().encodeToString() + "'";
				return "'" + Strings.escapeString(it.key()) + "->" + Strings.escapeString(it.value()) + "'";
			}
		}
		
		static boolean kVerbose = false;
		
		static class Entry0<K,V> implements Map.Entry<K,V> {
			K k;
			V v;
			
			public Entry0(K k, V v) {
				this.k = k;
				this.v = v;
			}
			
	        public K getKey() {
	        	return k;
	        }

	        public V getValue() {
	        	return v;
	        }

	        public V setValue(V value) {
	        	V old = v;
	        	v = value;
	        	return old;
	        }
		}
		
		void testRandomAccess(Random0 rnd, ArrayList<ByteBuf> keys, TreeMap<ByteBuf,ByteBuf> dataMap) {
			defaultByteBufKeyMapEntryComparator.setComparator(constructor.cmp);
			Iterator0 iter = constructor.newIterator();
			assertTrue(!iter.valid());
			Integer0 modelPos = new Integer0();
			ArrayList<Map.Entry<ByteBuf, ByteBuf>> entryList = new ArrayList<>();
			for(Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet())
				entryList.add(e);
						
			if (kVerbose)
				System.err.println("---");
			
			for (int i = 0; i < 200; i++) {
			//for (int i = 0; i < 20; i++) {
				int toss = (int)rnd.uniform(5);
				switch (toss) {
					case 0: {
						if (iter.valid()) {
							if (kVerbose) 
								System.err.println("Next");
							iter.next();
							modelPos.incrementAndGet(1);
							assertEquals(toString(entryList, modelPos.getValue()), toString(iter));
						}
						break;
					}
					
					case 1: {
						if (kVerbose) 
							System.err.println("SeekToFirst");
						iter.seekToFirst();
						modelPos.setValue(0);
						assertEquals(toString(entryList, modelPos.getValue()), toString(iter));
						break;
					}
					
					case 2: {			
						ByteBuf key = pickRandomKey(rnd, keys);			
						Entry0<ByteBuf,ByteBuf> target = new Entry0<ByteBuf,ByteBuf>(key, null);
						modelPos.setValue(ListUtils.lowerBound(entryList, target, defaultByteBufKeyMapEntryComparator));
						
						if (kVerbose) {
							System.err.printf("Seek '%s' , l.size=%d, lowerBound=%d\n", 
									Strings.escapeString(key), entryList.size(), modelPos.getValue());
						}
						
						iter.seek(SliceFactory.newUnpooled(key));
						assertEquals(toString(entryList, modelPos.getValue()), toString(iter));
						break;
					}
					
					case 3: {
						if (iter.valid()) {
							if (kVerbose) System.err.printf("Prev, modelPos=%d\n", modelPos.getValue());
							iter.prev();
							if (modelPos.getValue() == 0) {
								modelPos.setValue(entryList.size());  // Wrap around to invalid value
							} else {
								modelPos.incrementAndGet(-1);
							}
							
							String s1 = toString(entryList, modelPos.getValue());
							String s2 = toString(iter);
							
							assertEquals(s1, s2);
						}
						break;
					}
				
					case 4: {
						if (kVerbose) System.err.printf("SeekToLast\n");
						iter.seekToLast();
						if (keys.isEmpty()) {
							modelPos.setValue(entryList.size());
						} else {
							Map.Entry<ByteBuf, ByteBuf> last = entryList.get(entryList.size()-1); //modelPos = data.lower_bound(last);
							int lowerBound = ListUtils.lowerBound(entryList, last, defaultByteBufKeyMapEntryComparator);
							modelPos.setValue(lowerBound);
						}
						String s1 = toString(entryList, modelPos.getValue());
						String s2 = toString(iter);
						//System.out.printf("SeekToLast: toString(l, modelPos)=%s, toString(iter)=%s\n", s1, s2);
						
						assertEquals(s1, s2);
						break;
					}
				}
			}
			iter.delete();
		}
		
		public ByteBuf pickRandomKey(Random0 rnd, ArrayList<ByteBuf> keys) {
		    if (keys.isEmpty()) {
		    	byte[] b = "foo".getBytes();
		    	ByteBuf buf = ByteBufFactory.newUnpooled(b, b.length);
		    	return buf;
		    } else {
		    	int index = (int)rnd.uniform(keys.size());
		    	ByteBuf result = keys.get(index).clone();
		    	switch ((int)rnd.uniform(3)) {
			        case 0:
			        	// Return an existing key
			        	break;
			        case 1: {
			        	// Attempt to return something smaller than an existing key
			        	if (result.size() > 0) {
			        		int val = (result.getByte(result.size()-1) & 0xff);
			        		if (val > 0x00)
			        			result.setByte(result.size()-1, (byte)(val--));
			        	}
			        	break;
			        }
			        case 2: {
			        	// Return something larger than an existing key
			        	increment(options.comparator, result);
			        	break;
			        }
		        }
		        return result;
		    }
		}
		
		// Returns NULL if not running against a DB
		public DB db() { 
			return constructor.db(); 
		}
	}
	

	
	@Test
	public void testEmpty() {
		Harness h = new Harness();
		for (int i = 0; i < kTestArgList.length; i++) {
			h.init(kTestArgList[i]);
			Random0 rnd = new Random0(TestUtil.randomSeed() + 1);
			h.test(rnd);
		}
		h.delete();
	}
	
	// Special test for a block with no restart entries.  The C++ leveldb
	// code never generates such blocks, but the Java version of leveldb
	// seems to.
	@Test
	public void testZeroRestartPointsInBlock() {
		byte[] data = new byte[4];
		BlockContents contents = new BlockContents();
		contents.data = SliceFactory.newUnpooled(data, 0, data.length);
		contents.cachable = false;
		contents.heapAllocated = false;
		Block block = new Block(contents);
		Iterator0 iter = block.newIterator(BytewiseComparatorImpl.getInstance());
		iter.seekToFirst();
		assertTrue(!iter.valid());
		iter.seekToLast();
		assertTrue(!iter.valid());
		iter.seek(SliceFactory.newUnpooled("foo"));
		assertTrue(!iter.valid());
		iter.delete();
	}
	
	static ByteBuf str2ByteBuf(String s) {
		byte[] b = s.getBytes();
	    return ByteBufFactory.newUnpooled(b, b.length);
	}
	
	@Test
	public void testSimpleEmptyKey() {
		Harness h = new Harness();
		for (int i = 0; i < kTestArgList.length; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 1);
		   
		    h.add(str2ByteBuf(""), SliceFactory.newUnpooled("v"));
		    h.test(rnd);
		}
		h.delete();
	}
	
	@Test
	public void testSimpleSingle() {
		Harness h = new Harness();
		for (int i = 0; i < kTestArgList.length; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 2);
		    
		    h.add(str2ByteBuf("abc"), SliceFactory.newUnpooled("v"));
		    h.test(rnd);
		}
		h.delete();
	}
	
	@Test
	public void testSimpleMulti() {
		Harness h = new Harness();
		for (int i = 0; i < kTestArgList.length; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 3);
		    
		    h.add(str2ByteBuf("abc"), SliceFactory.newUnpooled("v"));
		    h.add(str2ByteBuf("abcd"), SliceFactory.newUnpooled("v"));
		    h.add(str2ByteBuf("ac"), SliceFactory.newUnpooled("v2"));
		    h.test(rnd);
		}
		h.delete();
	}
	
	@Test
	public void testSimpleSpecialKey() {
		byte[] b = new byte[] {(byte)0xff, (byte)0xff};
		
		Harness h = new Harness();
		for (int i = 0; i < kTestArgList.length; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 4);
		    h.add(ByteBufFactory.newUnpooled(b, b.length), SliceFactory.newUnpooled("v3"));
		    h.test(rnd);
		}
		h.delete();
	}
	
	@Test
	public void testRandomized() {
		Harness h = new Harness();
		for (int i = 0; i < kTestArgList.length; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 5);
		    for (int num_entries = 0; num_entries < 2000;
		         num_entries += (num_entries < 50 ? 1 : 200)) {
		    	if ((num_entries % 10) == 0) {
		    		System.err.printf("case %d of %d: num_entries = %d\n",
		                (i + 1), kTestArgList.length, num_entries);
		    	}
		    	for (int e = 0; e < num_entries; e++) {
		    		ByteBuf v = ByteBufFactory.newUnpooled();
		    		h.add(TestUtil.randomKey(rnd, (int)rnd.skewed(4)),
		    				TestUtil.randomString(rnd, (int)rnd.skewed(5), v));
		    	}
		    	h.test(rnd);
		    }
		    h.delete();
		}
	}
	
	@Test
	public void testRandomizedLongDB() {
		Random0 rnd = new Random0(TestUtil.randomSeed());
		TestArgs args = new TestArgs( TestType.DB_TEST, false, 16 );
		Harness h = new Harness();
		h.init(args);
		int num_entries = 100000;
		for (int i = 0; i < num_entries; i++) {
		    ByteBuf v = ByteBufFactory.newUnpooled();
		    h.add(TestUtil.randomKey(rnd, (int)rnd.skewed(4)),
		    		TestUtil.randomString(rnd, (int)rnd.skewed(5), v));
		}
		h.test(rnd);
		
		// We must have created enough data to force merging
		int files = 0;
		for (int level = 0; level < DBFormat.kNumLevels; level++) {
		    Object0<String> value0 = new Object0<String>();
		    String name = String.format("leveldb.num-files-at-level%d", level);
		    assertTrue(h.db().getProperty(name, value0));
		    files += Integer.valueOf(value0.getValue());
		}
		
		assertTrue(files > 0);
		h.delete();
	}
	
	@Test
	public void testMemTableTestSimple() {
		InternalKeyComparator cmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		MemTable memtable = new MemTable(cmp);
		memtable.ref();
		WriteBatch batch = new WriteBatch();
		WriteBatchInternal.setSequence(batch, 100);
		batch.put(SliceFactory.newUnpooled("k1"), SliceFactory.newUnpooled("v1"));
		batch.put(SliceFactory.newUnpooled("k2"), SliceFactory.newUnpooled("v2"));
		batch.put(SliceFactory.newUnpooled("k3"), SliceFactory.newUnpooled("v3"));
		batch.put(SliceFactory.newUnpooled("largekey"), SliceFactory.newUnpooled("vlarge"));
		assertTrue(WriteBatchInternal.insertInto(batch, memtable).ok());

		Iterator0 iter = memtable.newIterator();
		iter.seekToFirst();
		while (iter.valid()) {
		    System.err.printf("key: '%s' -> '%s'\n",
		            iter.key().encodeToString(),
		            iter.value().encodeToString());
		    iter.next();
		}

		iter.delete();
		memtable.unref();
	}
	
	static boolean between(long val, long low, long high) {
		boolean result = (val >= low) && (val <= high);
		if (!result) {
		    System.err.printf("Value %d is not in range [%d, %d]\n", val, low, high);
		}
		  
		return result;
	}
	
	@Test
	public void testTableTestApproximateOffsetOfPlain() {
		TableConstructor c = new TableConstructor(BytewiseComparatorImpl.getInstance());

		c.add(str2ByteBuf("k01"), SliceFactory.newUnpooled("hello"));
		c.add(str2ByteBuf("k02"), SliceFactory.newUnpooled("hello2"));
		c.add(str2ByteBuf("k03"), SliceFactory.newUnpooled(TestUtil.makeString(10000, 'x')));
		c.add(str2ByteBuf("k04"), SliceFactory.newUnpooled(TestUtil.makeString(200000, 'x')));
		c.add(str2ByteBuf("k05"), SliceFactory.newUnpooled(TestUtil.makeString(300000, 'x')));
		c.add(str2ByteBuf("k06"), SliceFactory.newUnpooled("hello3"));
		c.add(str2ByteBuf("k07"), SliceFactory.newUnpooled(TestUtil.makeString(100000, 'x')));
		
		ArrayList<ByteBuf> keys = new ArrayList<>();
		Object0<TreeMap<ByteBuf,ByteBuf>> kvmap0 = new Object0<>();
		Options options = new Options();
		options.blockSize = 1024;
		options.compression = CompressionType.kNoCompression;
		c.finish(options, keys, kvmap0);

		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("abc")),       0,      0));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k01")),       0,      0));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k01a")),      0,      0));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k02")),       0,      0));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k03")),       0,      0));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k04")),   10000,  11000));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k04a")), 210000, 211000));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k05")),  210000, 211000));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k06")),  510000, 511000));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k07")),  510000, 511000));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("xyz")),  610000, 612000));
	}

	static boolean SnappyCompressionSupported() {
		ByteBuf out = ByteBufFactory.newUnpooled();
		Slice in = SliceFactory.newUnpooled("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
		return Snappy.compress(in.data(), in.offset(), in.size(), out);
	}
	
	//TODO
	public void testTableTestApproximateOffsetOfCompressed() {
		if (!SnappyCompressionSupported()) {
		    System.err.println("skipping compression tests");
		    return;
		}

		Random0 rnd = new Random0(301);
		TableConstructor c = new TableConstructor(BytewiseComparatorImpl.getInstance());
		ByteBuf tmp = ByteBufFactory.newUnpooled();
		c.add(str2ByteBuf("k01"), SliceFactory.newUnpooled("hello"));
		c.add(str2ByteBuf("k02"), TestUtil.compressibleString(rnd, 0.25, 10000, tmp));
		c.add(str2ByteBuf("k03"), SliceFactory.newUnpooled("hello3"));
		c.add(str2ByteBuf("k04"), TestUtil.compressibleString(rnd, 0.25, 10000, tmp));
		  
		ArrayList<ByteBuf> keys = new ArrayList<>();
		Object0<TreeMap<ByteBuf,ByteBuf>> kvmap0 = new Object0<>();
		Options options = new Options();
		options.blockSize = 1024;
		options.compression = CompressionType.kSnappyCompression;
		c.finish(options, keys, kvmap0);

		  // Expected upper and lower bounds of space used by compressible strings.
		int kSlop = 1000;  // Compressor effectiveness varies.
		int expected = 2500;  // 10000 * compression ratio (0.25)
		int min_z = expected - kSlop;
		int max_z = expected + kSlop;

		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("abc")), 0, kSlop));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k01")), 0, kSlop));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k02")), 0, kSlop));
		// Have now emitted a large compressible string, so adjust expected offset.
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k03")), min_z, max_z));
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("k04")), min_z, max_z));
		// Have now emitted two large compressible strings, so adjust expected offset.
		assertTrue(between(c.approximateOffsetOf(SliceFactory.newUnpooled("xyz")), 2 * min_z, 2 * max_z));
	}

	@Test
	public void testReverseKeyComparator() {
		TreeMap<ByteBuf,String> m = new TreeMap<ByteBuf,String>(new ByteBufComparator(new ReverseKeyComparator()));
		m.put(ByteBufFactory.newUnpooled("abc"), "abc");
		m.put(ByteBufFactory.newUnpooled("abcd"), "abcd");
		m.put(ByteBufFactory.newUnpooled("ac"), "abcd");
		
		for (ByteBuf buf : m.keySet()) {
			System.out.println(Strings.escapeString(buf));
		}
	}
}
