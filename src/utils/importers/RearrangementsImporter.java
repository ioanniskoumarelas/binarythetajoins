/**
 * RearrangementsImporter.java
 * 
 * Imports data from a rearrangement.
 * 
 * @author John Koumarelas
 */

package utils.importers;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;

import model.BucketBoundaries;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RearrangementsImporter {
	
	public int[] importTSPkSolution(File tspkWorkingDirectory) throws NumberFormatException, IOException {
		
		BufferedReader in = new BufferedReader(new InputStreamReader(new DataInputStream(
				new FileInputStream(tspkWorkingDirectory+ File.separator +  "pm.sol"))));
		String strLine;

		LinkedList<Integer> ll = new LinkedList<Integer>();
		
		while ((strLine = in.readLine()) != null) {
			String[] strToks = strLine.split(" ");
			for (int j =  0 ; j < strToks.length ; ++j) {
				ll.add(Integer.valueOf(strToks[j]));
			}
		}
		in.close();
		
		int[] solution = new int[ll.size()];
		
		int counter=0;
		Iterator<Integer> it = ll.iterator();
		while (it.hasNext()) {
			solution[counter++] = it.next();
		}
		
		return solution;
	}

	/**
	 * Loads the file rearrangements.csv
	 */
	public int[] importDefaultToRearranged(String relation,  int buckets, Path rearrangements) throws IOException {
		int[] rearrangementsToDefaultRelation = importRearrangementsToDefault(relation, buckets, rearrangements);
		
		int[] defaultToRearrangementsRelation = new int[buckets];
		
		for(int i = 0 ; i < buckets; ++i) {
			defaultToRearrangementsRelation[ rearrangementsToDefaultRelation[i] ] = i;
		}
		
		return defaultToRearrangementsRelation;
	}
	
	/**
	 * Loads the file rearrangements.csv
	 */
	public int[] importRearrangementsToDefault(String relation,  int buckets, Path rearrangements) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		
		String relationToRowsOrColumns = relation.equals("S")?"rows":"columns";
		
		int[] rearrangementsToDefaultRelation = new int[buckets];

		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(rearrangements)));
		String line;
		while ((line=br.readLine()) != null){
			String[] values = line.split(",");
			if(relationToRowsOrColumns.equals(values[0])) {
				for(int i = 0 ; i < buckets; ++i) {
					rearrangementsToDefaultRelation[i] = Integer.valueOf(values[i+1]);
				}
			}
		}
		br.close();
		
		return rearrangementsToDefaultRelation;
	}
	
}
