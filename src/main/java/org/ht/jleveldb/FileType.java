package org.ht.jleveldb;


public enum FileType {
	  LogFile,
	  DBLockFile,
	  TableFile,
	  DescriptorFile,
	  CurrentFile,
	  TempFile,
	  InfoLogFile  // Either the current one, or an old one
};
