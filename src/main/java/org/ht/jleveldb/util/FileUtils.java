package org.ht.jleveldb.util;

import java.io.File;

/**
 * @author Teng Huang ht201509@163.com
 */
public class FileUtils {
	 public static void deletePath(String path) {
		 deletePath(new File(path));
	 }
	 
	 public static void deletePath(File path) {
	    if (!path.exists())
	        return;
	    
	    if (path.isFile()) {
	        path.delete();
	        return;  
	    }
	    
	    File[] files = path.listFiles();  
	    for (int i = 0; i < files.length; i++) {  
	    	deletePath(files[i]);  
	    }
	    
	    path.delete();
	}
}

