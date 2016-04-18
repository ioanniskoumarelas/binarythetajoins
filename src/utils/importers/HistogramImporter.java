/**
 * HistogramImporter.java
 * 
 * Supporting class. It supports methods from importing files related
 * to the histograms.
 * 
 * @author John Koumarelas
 */

package utils.importers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import model.BucketBoundaries;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HistogramImporter {

	/**
	 * Loads the file boundaries.csv
	 */
	public BucketBoundaries[] importBoundaries(String relation,  int buckets, Path boundaries) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		
		BucketBoundaries[] boundariesRelation = new BucketBoundaries[buckets];
		
		int i = 0;
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(boundaries)));
		String line;
		while ((line=br.readLine()) != null){
			String[] values = line.split(",");
			if(values[0].equals(relation)) {
				boundariesRelation[i] = new BucketBoundaries();
				boundariesRelation[i++].set(Long.valueOf(values[1]), Long.valueOf(values[2]));
			}
		}
		br.close();
		
		return boundariesRelation;
	}
	
	/**
	 * Loads the file counts.csv
	 */
	public long[] importCounts(String relation, int buckets, Path counts) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		
		long[] countsRelation = new long[buckets];
		
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(counts)));
		String line;
		while ((line=br.readLine()) != null){
			String[] values = line.split(",");
	    	if(values[0].equals(relation)) {
	    		countsRelation[Integer.parseInt(values[1])] = Long.parseLong(values[2]);
	    	}
		}
		br.close();
		
		return countsRelation;
	}
	
}
