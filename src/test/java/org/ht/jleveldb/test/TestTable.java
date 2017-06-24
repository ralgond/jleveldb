package org.ht.jleveldb.test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.ht.jleveldb.CompressionType;
import org.ht.jleveldb.DB;
import org.ht.jleveldb.Iterator0;
import org.ht.jleveldb.LevelDB;
import org.ht.jleveldb.Options;
import org.ht.jleveldb.RandomAccessFile0;
import org.ht.jleveldb.ReadOptions;
import org.ht.jleveldb.Status;
import org.ht.jleveldb.WritableFile;
import org.ht.jleveldb.WriteBatch;
import org.ht.jleveldb.WriteOptions;
import org.ht.jleveldb.db.MemTable;
import org.ht.jleveldb.db.WriteBatchInternal;
import org.ht.jleveldb.db.format.DBFormat;
import org.ht.jleveldb.db.format.InternalKeyComparator;
import org.ht.jleveldb.db.format.ParsedInternalKey;
import org.ht.jleveldb.db.format.ValueType;
import org.ht.jleveldb.table.Block;
import org.ht.jleveldb.table.BlockBuilder;
import org.ht.jleveldb.table.Format.BlockContents;
import org.ht.jleveldb.table.Table;
import org.ht.jleveldb.table.TableBuilder;
import org.ht.jleveldb.util.ByteBuf;
import org.ht.jleveldb.util.ByteBufFactory;
import org.ht.jleveldb.util.BytewiseComparatorImpl;
import org.ht.jleveldb.util.Comparator0;
import org.ht.jleveldb.util.Object0;
import org.ht.jleveldb.util.Integer0;
import org.ht.jleveldb.util.ListUtils;
import org.ht.jleveldb.util.Random0;
import org.ht.jleveldb.util.Slice;
import org.ht.jleveldb.util.Snappy;
import org.ht.jleveldb.util.Strings;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
public class TestTable {
	// Return reverse of "key".
	// Used to test non-lexicographic comparators.
	static Slice reverse(Slice key) {
		String str = key.encodeToString();
		byte[] b = str.getBytes();
		int size = b.length;
		for (int i = 0; i < size/2; i++) {
			byte tmp = b[i];
			b[i] = b[size-1-i];
			b[size-1-i] = tmp;
		}
		return new Slice(b, 0, b.length);
	}
	
	static class ReverseKeyComparator extends Comparator0 {
		public String name() {
		    return "leveldb.ReverseBytewiseComparator";
		}

		public int compare(Slice a, Slice b) {
		    return BytewiseComparatorImpl.getInstance().compare(reverse(a), reverse(b));
		}
		
		public int compare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
			return BytewiseComparatorImpl.getInstance().compare(a, aoff, asize, b, boff, bsize);
		}

		public void findShortestSeparator(ByteBuf start, Slice limit) {
			Slice s = reverse(new Slice(start));
		    Slice l = reverse(limit);
		    ByteBuf s0 = ByteBufFactory.defaultByteBuf();
		    s0.assign(s.data, s.offset, s.size());
		    BytewiseComparatorImpl.getInstance().findShortestSeparator(s0, l);
		    Slice s1 = reverse(new Slice(s0));
		    start.assign(s1.data, s1.offset, s1.size());
		}

		public void findShortSuccessor(ByteBuf key) {
		    Slice s = reverse(new Slice(key));
		    ByteBuf s0 = ByteBufFactory.defaultByteBuf();
		    s0.assign(s.data, s.offset, s.size());
		    BytewiseComparatorImpl.getInstance().findShortSuccessor(s0);
		    Slice s1 = reverse(new Slice(s0));
		    key.assign(s1.data, s1.offset, s1.size());
		}
	};
	
	static ReverseKeyComparator reverse_key_comparator;
	
	static void increment(Comparator0 cmp, ByteBuf key) {
		if (cmp == BytewiseComparatorImpl.getInstance()) {
			key.addByte((byte)0);
		} else {
			assert(cmp == reverse_key_comparator);
			Slice rev0 = reverse(new Slice(key));
			ByteBuf rev = ByteBufFactory.defaultByteBuf();
			rev.assign(rev0.data, rev0.offset, rev0.size());
			
			rev.addByte((byte)0);
			
			Slice s = reverse(new Slice(rev));
			key.assign(s.data, s.offset, s.size());
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
		    contents.append(data.data(), data.offset, data.size());
		    return Status.ok0();
		}

		ByteBuf contents = ByteBufFactory.defaultByteBuf();
	}
	
	static class StringSource implements RandomAccessFile0 {
		public StringSource(Slice s) {
			contents.assign(s.data(), s.offset, s.size());
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

		  ByteBuf contents = ByteBufFactory.defaultByteBuf();
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
	};
	
	static ByteBufKeyMapEntryComparator defaultByteBufKeyMapEntryComparator = new ByteBufKeyMapEntryComparator();
	
	// Helper class for tests to unify the interface between
	// BlockBuilder/TableBuilder and Block/Table.
	static abstract class Constructor {
		public TreeMap<ByteBuf, ByteBuf> data;
		
		public Constructor(Comparator0 cmp) {
			data = new TreeMap<ByteBuf, ByteBuf>(new ByteBufComparator(cmp));
		}

		public void add(ByteBuf key,Slice value) {
			ByteBuf s = ByteBufFactory.defaultByteBuf();
			s.assign(value.data, value.offset, value.size());
			data.put(key, s);
		}

		// Finish constructing the data structure with all the keys that have
		// been added so far.  Returns the keys in sorted order in "*keys"
		// and stores the key/value pairs in "*kvmap"
		void finish(Options options, ArrayList<ByteBuf> keys, 
				Object0<TreeMap<ByteBuf,ByteBuf>> kvmap) {
	    	kvmap.setValue(data);
	    	keys.clear();
	    	for (ByteBuf key : data.keySet()) {
	    		keys.add(key);
	    	}
	    	
	    	data.clear();
	    	Status s = finishImpl(options, kvmap.getValue());
	    	assertTrue(s.ok());
		}

		// Construct the data structure from the data in "data"
		public abstract Status finishImpl(Options options, TreeMap<ByteBuf,ByteBuf> data);

		public abstract Iterator0 newIterator();

		public TreeMap<ByteBuf,ByteBuf> data() { 
			return data; 
		}

		public DB db() { 
			return null; 
		}  // Overridden in DBConstructor
	}
	
	static class BlockConstructor extends Constructor {

		Comparator0 comparator;
		ByteBuf data = ByteBufFactory.defaultByteBuf();
		Block block;

		public BlockConstructor(Comparator0 cmp) {
			super(cmp);
		    comparator = (cmp);
		    block = null;
		}
		
		public Status finishImpl(Options options, TreeMap<ByteBuf, ByteBuf> dataMap) {
			if (block != null) {
			    block.delete();
			    block = null;
			}
		    BlockBuilder builder = new BlockBuilder(options);

		    for (Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet()) {
		    	builder.add(new Slice(e.getKey()), new Slice(e.getValue()));
		    }
		    
		    // Open the block
		    Slice s = builder.finish();
		    data.assign(s.data, s.offset, s.size());
		    BlockContents contents = new BlockContents();
		    contents.data = new Slice(data);
		    contents.cachable = false;
		    contents.heapAllocated = false;
		    block = new Block(contents);
		    return Status.ok0();
		}
		
		public Iterator0 newIterator() {
		    return block.newIterator(comparator);
		}
	};
	
	static class TableConstructor extends Constructor {
		public TableConstructor(Comparator0 cmp) {
			super(cmp);
			source = null;
			table = null;
		}
		  
		public void delete() {
			reset();
		}
		  
		public Status finishImpl(Options options, TreeMap<ByteBuf, ByteBuf> dataMap) {
		    reset();
		    StringSink sink = new StringSink();
		    TableBuilder builder = new TableBuilder(options, sink);

		    for (Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet()) {
		    	builder.add(new Slice(e.getKey()), new Slice(e.getValue()));
		    	assertTrue(builder.status().ok());
		    }
		    Status s = builder.finish();
		    assertTrue(s.ok());

		    assertEquals(sink.contents().size(), builder.fileSize());

		    // Open the table
		    source = new StringSource(new Slice(sink.contents()));
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
			iter.delete();
		}
		
		public boolean valid() { 
			return iter.valid(); 
		}
		
		public void seek(Slice target) {
			ParsedInternalKey ikey = new ParsedInternalKey(target, DBFormat.kMaxSequenceNumber, ValueType.Value);
			ByteBuf encoded = ByteBufFactory.defaultByteBuf();
			DBFormat.appendInternalKey(encoded, ikey);
			iter.seek(new Slice(encoded));
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
	  			return new Slice("corrupted key");
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
		}
		
		public Status finishImpl(Options options, TreeMap<ByteBuf, ByteBuf> dataMap) {
			if (memtable != null)
				memtable.unref();
		    memtable = new MemTable(internalComparator);
		    memtable.ref();
		    int seq = 1;
		    for (Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet()) {
		    	memtable.add(seq, ValueType.Value, new Slice(e.getKey()), new Slice(e.getValue()));
		    	seq++;
		    }
		    return Status.ok0();
		}
		
		public Iterator0 newIterator() {
		    return new KeyConvertingIterator(memtable.newIterator());
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
		
		public void delete() {
			db = null;
		}
		
		public Status finishImpl(Options options, TreeMap<ByteBuf, ByteBuf> dataMap) {
		    db = null;
		    newDB();
		    for (Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet()) {
		    	WriteBatch batch = new WriteBatch();
		    	batch.put(new Slice(e.getKey()), new Slice(e.getValue()));
		    	try {
		    		assertTrue(db.write(new WriteOptions(), batch).ok());
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
		    System.out.println(status);
		    assertTrue(status.ok());
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
	};
	
	static TestArgs kTestArgList[];
	static int kNumTestArgs;
	static {
		ArrayList<TestArgs> l = new ArrayList<>();
		l.add(new TestArgs(TestType.TABLE_TEST, false, 16));
		l.add(new TestArgs(TestType.TABLE_TEST, false, 1));
		l.add(new TestArgs(TestType.TABLE_TEST, false, 1024));
		l.add(new TestArgs(TestType.TABLE_TEST, true, 16));
		l.add(new TestArgs(TestType.TABLE_TEST, true, 1));
		l.add(new TestArgs(TestType.TABLE_TEST, true, 1024));
		
		l.add(new TestArgs(TestType.BLOCK_TEST, false, 16));
		l.add(new TestArgs(TestType.BLOCK_TEST, false, 1));
		l.add(new TestArgs(TestType.BLOCK_TEST, false, 1024));
		l.add(new TestArgs(TestType.BLOCK_TEST, true, 16));
		l.add(new TestArgs(TestType.BLOCK_TEST, true, 1));
		l.add(new TestArgs(TestType.BLOCK_TEST, true, 1024));
		
		// Restart interval does not matter for memtables
		l.add(new TestArgs(TestType.MEMTABLE_TEST, false, 16));
		l.add(new TestArgs(TestType.MEMTABLE_TEST, true, 16));
		
		// Do not bother with restart interval variations for DB
		l.add(new TestArgs(TestType.DB_TEST, false, 16));
		l.add(new TestArgs(TestType.DB_TEST, true, 16));
		
		kTestArgList = new TestArgs[l.size()];
		for (int i = 0; i < kTestArgList.length; i++) {
			kTestArgList[i] = l.get(i);
		}
		kNumTestArgs = kTestArgList.length;
	}
	
	static class Harness {
		Options options;
		Constructor constructor;
		  
		public Harness() {
			constructor = null;
		}
		
		public void delete() {
			constructor = null;
		}
		
		public void init(TestArgs args) {
		    constructor = null;
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
			Iterator0 iter = constructor.newIterator();
			assertTrue(!iter.valid());
			iter.seekToFirst();
			Iterator<Map.Entry<ByteBuf, ByteBuf>> it = dataMap.entrySet().iterator();
			while (it.hasNext()) {
				assertEquals(toString(it), toString(iter));
				iter.next();
			}
			assertEquals(toString(it), toString(iter));
			
			assertTrue(!iter.valid());
			
			iter.delete();
		}
		
		public void testBackwardScan(ArrayList<ByteBuf> keys, TreeMap<ByteBuf,ByteBuf> dataMap) {
			Iterator0 iter = constructor.newIterator();
			assertTrue(!iter.valid());
			iter.seekToLast();
			Iterator<Map.Entry<ByteBuf, ByteBuf>> it = dataMap.descendingMap().entrySet().iterator();
			while (it.hasNext()) {
				assertEquals(toString(it), toString(iter));
				iter.next();
			}
			assertEquals(toString(it), toString(iter));
			
			assertTrue(!iter.valid());
			
			iter.delete();
		}
		
		static String toString(Iterator<Map.Entry<ByteBuf, ByteBuf>> it) {
		    if (!it.hasNext()) {
		    	return "END";
		    } else {
		    	Map.Entry<ByteBuf, ByteBuf> e = it.next();
		    	return "'" + e.getKey().encodeToString() + "->" + e.getValue().encodeToString() + "'";
		    }
		}
		
		static String toString(ArrayList<Map.Entry<ByteBuf, ByteBuf>> l, Integer0 pos0) {
			int pos = pos0.getValue();
		    if (pos + 1 > l.size()) {
		    	return "END";
		    } else {
		    	Map.Entry<ByteBuf, ByteBuf> e = l.get(pos0.getAndIncrement(1));
		    	return "'" + e.getKey().encodeToString() + "->" + e.getValue().encodeToString() + "'";
		    }
		}
		
		
		static String toString(Iterator0 it) {
			if (!it.valid()) {
				return "END";
			} else {
				return "'" + it.key().encodeToString() + "->" + it.value().encodeToString() + "'";
			}
		}
		
		static boolean kVerbose = false;
		
		
		@SuppressWarnings("unchecked")
		void testRandomAccess(Random0 rnd, ArrayList<ByteBuf> keys, TreeMap<ByteBuf,ByteBuf> dataMap) {
			
			Iterator0 iter = constructor.newIterator();
			assertTrue(!iter.valid());
			Integer0 modelPos = new Integer0();
			ArrayList<Map.Entry<ByteBuf, ByteBuf>> l = new ArrayList<>();
			for(Map.Entry<ByteBuf, ByteBuf> e : dataMap.entrySet())
				l.add(e);

			if (kVerbose)
				System.err.println("---");
			
			for (int i = 0; i < 200; i++) {
				int toss = (int)rnd.uniform(5);
				
				switch (toss) {
					case 0: {
						if (iter.valid()) {
							if (kVerbose) System.err.println("Next");
							iter.next();
							modelPos.incrementAndGet(1);
							assertEquals(toString(l, modelPos), toString(iter));
						}
						break;
					}
					
					case 1: {
						if (kVerbose) System.err.println("SeekToFirst");
						iter.seekToFirst();
						modelPos.setValue(0);
						assertEquals(toString(l, modelPos), toString(iter));
						break;
					}
					
					case 2: {
						ByteBuf key = pickRandomKey(rnd, keys);
						Map.Entry<ByteBuf, ByteBuf> target = 
								(Map.Entry<ByteBuf, ByteBuf>)dataMap.get(key); //modelPos = data.lower_bound(key);
						modelPos.setValue(ListUtils.lowerBound(l, target, defaultByteBufKeyMapEntryComparator));
						if (kVerbose) System.err.printf("Seek '%s'\n", Strings.escapeString(key));
						iter.seek(new Slice(key));
						assertEquals(toString(l, modelPos), toString(iter));
						break;
					}
					
					case 3: {
						if (iter.valid()) {
							if (kVerbose) System.err.println("Prev\n");
							iter.prev();
							if (modelPos.getValue() == 0) {
								modelPos.setValue(l.size());  // Wrap around to invalid value
							} else {
								modelPos.incrementAndGet(-1);
							}
							assertEquals(toString(l, modelPos), toString(iter));
						}
						break;
					}
				
					case 4: {
						if (kVerbose) System.err.printf("SeekToLast\n");
						iter.seekToLast();
						if (keys.isEmpty()) {
							modelPos.setValue(l.size());;
						} else {
							Map.Entry<ByteBuf, ByteBuf> last = l.get(l.size()-1); //modelPos = data.lower_bound(last);
							modelPos.setValue(ListUtils.lowerBound(l, last, defaultByteBufKeyMapEntryComparator));
						}
						assertEquals(toString(l, modelPos), toString(iter));
						break;
					}
				}
			}
			iter.delete();
		}
		
		public ByteBuf pickRandomKey(Random0 rnd, ArrayList<ByteBuf> keys) {
		    if (keys.isEmpty()) {
		    	byte[] b = "foo".getBytes();
		    	ByteBuf buf = ByteBufFactory.defaultByteBuf(b, b.length);
		    	return buf;
		    } else {
		    	int index = (int)rnd.uniform(keys.size());
		    	ByteBuf result = keys.get(index);
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
		for (int i = 0; i < kNumTestArgs; i++) {
			h.init(kTestArgList[i]);
			Random0 rnd = new Random0(TestUtil.randomSeed() + 1);
			h.test(rnd);
		}
	}
	
	// Special test for a block with no restart entries.  The C++ leveldb
	// code never generates such blocks, but the Java version of leveldb
	// seems to.
	@Test
	public void testZeroRestartPointsInBlock() {
		byte[] data = new byte[4];
		BlockContents contents = new BlockContents();
		contents.data = new Slice(data, 0, data.length);
		contents.cachable = false;
		contents.heapAllocated = false;
		Block block = new Block(contents);
		Iterator0 iter = block.newIterator(BytewiseComparatorImpl.getInstance());
		iter.seekToFirst();
		assertTrue(!iter.valid());
		iter.seekToLast();
		assertTrue(!iter.valid());
		iter.seek(new Slice("foo"));
		assertTrue(!iter.valid());
		iter.delete();
	}
	
	static ByteBuf str2ByteBuf(String s) {
		byte[] b = s.getBytes();
	    return ByteBufFactory.defaultByteBuf(b, b.length);
	}
	
	@Test
	public void testSimpleEmptyKey() {
		Harness h = new Harness();
		for (int i = 0; i < kNumTestArgs; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 1);
		   
		    h.add(str2ByteBuf(""), new Slice("v"));
		    h.test(rnd);
		}
	}
	
	@Test
	public void testSimpleSingle() {
		Harness h = new Harness();
		for (int i = 0; i < kNumTestArgs; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 2);
		    
		    h.add(str2ByteBuf("abc"), new Slice("v"));
		    h.test(rnd);
		}
	}
	
	@Test
	public void testSimpleMulti() {
		Harness h = new Harness();
		for (int i = 0; i < kNumTestArgs; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 3);
		    
		    h.add(str2ByteBuf("abc"), new Slice("v"));
		    h.add(str2ByteBuf("abcd"), new Slice("v"));
		    h.add(str2ByteBuf("ac"), new Slice("v2"));
		    h.test(rnd);
		}
	}
	
	@Test
	public void testSimpleSpecialKey() {
		byte[] b = new byte[] {(byte)0xff, (byte)0xff};
		
		Harness h = new Harness();
		for (int i = 0; i < kNumTestArgs; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 4);
		    h.add(ByteBufFactory.defaultByteBuf(b, b.length), new Slice("v3"));
		    h.test(rnd);
		}
	}
	
	@Test
	public void testRandomized() {
		Harness h = new Harness();
		for (int i = 0; i < kNumTestArgs; i++) {
		    h.init(kTestArgList[i]);
		    Random0 rnd = new Random0(TestUtil.randomSeed() + 5);
		    for (int num_entries = 0; num_entries < 2000;
		         num_entries += (num_entries < 50 ? 1 : 200)) {
		    	if ((num_entries % 10) == 0) {
		    		System.err.printf("case %d of %d: num_entries = %d\n",
		                (i + 1), kNumTestArgs, num_entries);
		    	}
		    	for (int e = 0; e < num_entries; e++) {
		    		ByteBuf v = ByteBufFactory.defaultByteBuf();
		    		h.add(TestUtil.randomKey(rnd, (int)rnd.skewed(4)),
		    				TestUtil.randomString(rnd, (int)rnd.skewed(5), v));
		    	}
		    	h.test(rnd);
		    }
		}
	}
	
	@Test
	public void testRandomizedLongDB() {
		Random0 rnd = new Random0(TestUtil.randomSeed());
		TestArgs args = new TestArgs( TestType.DB_TEST, false, 16 );
		Harness h = new Harness();
		h.init(args);
		int num_entries = 100000;
		for (int e = 0; e < num_entries; e++) {
		    ByteBuf v = ByteBufFactory.defaultByteBuf();
		    h.add(TestUtil.randomKey(rnd, (int)rnd.skewed(4)),
		    		TestUtil.randomString(rnd, (int)rnd.skewed(5), v));
		}
		h.test(rnd);
		
		// We must have created enough data to force merging
		int files = 0;
		for (int level = 0; level < DBFormat.kNumLevels; level++) {
		    ByteBuf value = ByteBufFactory.defaultByteBuf();
		    String name = String.format("leveldb.num-files-at-level%d", level);
		    assertTrue(h.db().getProperty(new Slice(name), value));
		    files += Integer.valueOf(value.encodeToString());
		}
		
		assertEquals(files, 0);
	}
	
	@Test
	public void testMemTableTestSimple() {
		InternalKeyComparator cmp = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
		MemTable memtable = new MemTable(cmp);
		memtable.ref();
		WriteBatch batch = new WriteBatch();
		WriteBatchInternal.setSequence(batch, 100);
		batch.put(new Slice("k1"), new Slice("v1"));
		batch.put(new Slice("k2"), new Slice("v2"));
		batch.put(new Slice("k3"), new Slice("v3"));
		batch.put(new Slice("largekey"), new Slice("vlarge"));
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

		c.add(str2ByteBuf("k01"), new Slice("hello"));
		c.add(str2ByteBuf("k02"), new Slice("hello2"));
		c.add(str2ByteBuf("k03"), new Slice(TestUtil.makeString(10000, 'x')));
		c.add(str2ByteBuf("k04"), new Slice(TestUtil.makeString(200000, 'x')));
		c.add(str2ByteBuf("k05"), new Slice(TestUtil.makeString(300000, 'x')));
		c.add(str2ByteBuf("k06"), new Slice("hello3"));
		c.add(str2ByteBuf("k07"), new Slice(TestUtil.makeString(100000, 'x')));
		
		ArrayList<ByteBuf> keys = new ArrayList<>();
		Object0<TreeMap<ByteBuf,ByteBuf>> kvmap0 = new Object0<>();
		Options options = new Options();
		options.blockSize = 1024;
		options.compression = CompressionType.kNoCompression;
		c.finish(options, keys, kvmap0);

		assertTrue(between(c.approximateOffsetOf(new Slice("abc")),       0,      0));
		assertTrue(between(c.approximateOffsetOf(new Slice("k01")),       0,      0));
		assertTrue(between(c.approximateOffsetOf(new Slice("k01a")),      0,      0));
		assertTrue(between(c.approximateOffsetOf(new Slice("k02")),       0,      0));
		assertTrue(between(c.approximateOffsetOf(new Slice("k03")),       0,      0));
		assertTrue(between(c.approximateOffsetOf(new Slice("k04")),   10000,  11000));
		assertTrue(between(c.approximateOffsetOf(new Slice("k04a")), 210000, 211000));
		assertTrue(between(c.approximateOffsetOf(new Slice("k05")),  210000, 211000));
		assertTrue(between(c.approximateOffsetOf(new Slice("k06")),  510000, 511000));
		assertTrue(between(c.approximateOffsetOf(new Slice("k07")),  510000, 511000));
		assertTrue(between(c.approximateOffsetOf(new Slice("xyz")),  610000, 612000));
	}

	static boolean SnappyCompressionSupported() {
		ByteBuf out = ByteBufFactory.defaultByteBuf();
		Slice in = new Slice("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
		return Snappy.compress(in.data(), in.offset, in.size(), out);
	}
	
	//TODO
	public void testTableTestApproximateOffsetOfCompressed() {
		if (!SnappyCompressionSupported()) {
		    System.err.println("skipping compression tests");
		    return;
		}

		Random0 rnd = new Random0(301);
		TableConstructor c = new TableConstructor(BytewiseComparatorImpl.getInstance());
		ByteBuf tmp = ByteBufFactory.defaultByteBuf();
		c.add(str2ByteBuf("k01"), new Slice("hello"));
		c.add(str2ByteBuf("k02"), TestUtil.compressibleString(rnd, 0.25, 10000, tmp));
		c.add(str2ByteBuf("k03"), new Slice("hello3"));
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

		assertTrue(between(c.approximateOffsetOf(new Slice("abc")), 0, kSlop));
		assertTrue(between(c.approximateOffsetOf(new Slice("k01")), 0, kSlop));
		assertTrue(between(c.approximateOffsetOf(new Slice("k02")), 0, kSlop));
		// Have now emitted a large compressible string, so adjust expected offset.
		assertTrue(between(c.approximateOffsetOf(new Slice("k03")), min_z, max_z));
		assertTrue(between(c.approximateOffsetOf(new Slice("k04")), min_z, max_z));
		// Have now emitted two large compressible strings, so adjust expected offset.
		assertTrue(between(c.approximateOffsetOf(new Slice("xyz")), 2 * min_z, 2 * max_z));
	}
}
