/**
 * DatasetImporter.java
 * 
 * Imports information about the dataset.
 * 
 * @author John Koumarelas
 */

package utils.importers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DatasetImporter {

	public long getSize(String relation, Path datasetSizes) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(datasetSizes)));
			String line;

			while ((line=br.readLine()) != null){
				 String[] tokens = line.trim().split(",");
				 if(tokens[0].trim().equals(relation)){
					 return Long.valueOf(tokens[1].trim());
				 }
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
}
