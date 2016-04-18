/**
 * FileSystemUtil.java
 * 
 * Provides util operations on the FileSystem.
 * 
 * @author John Koumarelas
 */

package utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemUtil {

	private static FileSystem fs;
	
	static {
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void makeDir(String dirPath) {
		try {
			if(!fs.exists(new Path(dirPath))) {
				fs.mkdirs(new Path(dirPath));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void deleteDir(String dirPath) {
		try {
			if(fs.exists(new Path(dirPath)) && !fs.isFile(new Path(dirPath))) {
				fs.delete(new Path(dirPath), true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void deleteAndMakeDir(String dirPath) {
		deleteDir(dirPath);
		makeDir(dirPath);
	}
	
}
