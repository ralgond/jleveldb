package org.ht.jleveldb.db.format;

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
};
