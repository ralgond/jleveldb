package org.ht.jleveldb.test;

import java.util.ArrayList;

import org.ht.jleveldb.FileName;
import org.ht.jleveldb.FileType;
import org.ht.jleveldb.util.Object0;
import org.ht.jleveldb.util.Long0;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class TestFileName {
	static class ParseResult {
		String fname;
		long number;
		FileType type;
		
		public ParseResult(String fname, long number, FileType type) {
			this.fname = fname;
			this.number = number;
			this.type = type;
		}
	}
	
	@Test
	public void testParse() {
		Object0<FileType> type0 = new Object0<FileType>();
		Long0 number0 = new Long0();

		  // Successful parses
		  ArrayList<ParseResult> cases = new ArrayList<>();
		  cases.add(new ParseResult("100.log", 100, FileType.LogFile));
		  cases.add(new ParseResult("0.log", 0, FileType.LogFile));
		  cases.add(new ParseResult("0.sst", 0, FileType.TableFile));
		  cases.add(new ParseResult("0.ldb", 0, FileType.TableFile));
		  cases.add(new ParseResult("CURRENT", 0, FileType.CurrentFile));
		  cases.add(new ParseResult("LOCK", 0, FileType.DBLockFile));
		  cases.add(new ParseResult("MANIFEST-2", 2, FileType.DescriptorFile));
		  cases.add(new ParseResult("MANIFEST-7", 7, FileType.DescriptorFile));
		  cases.add(new ParseResult("LOG", 0, FileType.InfoLogFile));
		  cases.add(new ParseResult("LOG.old", 0, FileType.InfoLogFile));
		  cases.add(new ParseResult("8446744073709551615.log", 8446744073709551615L, FileType.LogFile));

		  for (int i = 0; i < cases.size(); i++) {
		    String f = cases.get(i).fname;
		    assertTrue(FileName.parseFileName(f, number0, type0));
		    assertTrue(cases.get(i).type == type0.getValue());
		    assertEquals(cases.get(i).number, number0.getValue());
		  }

		  // Errors
		  String errors[] = new String[] {
		    "",
		    "foo",
		    "foo-dx-100.log",
		    ".log",
		    "",
		    "manifest",
		    "CURREN",
		    "CURRENTX",
		    "MANIFES",
		    "MANIFEST",
		    "MANIFEST-",
		    "XMANIFEST-3",
		    "MANIFEST-3x",
		    "LOC",
		    "LOCKx",
		    "LO",
		    "LOGx",
		    "18446744073709551616.log",
		    "184467440737095516150.log",
		    "100",
		    "100.",
		    "100.lop"
		  };
		  
		  for (int i = 0; i < errors.length; i++) {
			  String f = errors[i];
			  assertFalse(FileName.parseFileName(f, number0, type0));
		  }
	}
}
