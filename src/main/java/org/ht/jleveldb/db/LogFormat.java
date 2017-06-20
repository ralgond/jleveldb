package org.ht.jleveldb.db;

public class LogFormat {
	public enum RecordType {
		// Zero is reserved for preallocated files
		ZeroType(0),
		
		FullType(1),
		
		// For fragments
		FirstType(2),
		MiddleType(3),
		LastType(4);
		
		int type;
		
		private RecordType(int type) {
			this.type = type;
		}
		
		public int getType() {
			return type;
		}
	}
	
	static final int MaxRecordType = RecordType.LastType.getType();
	
	static final int BlockSize = 32768;
	
	static final int HeaderSize = 4 + 2 + 1;
}
