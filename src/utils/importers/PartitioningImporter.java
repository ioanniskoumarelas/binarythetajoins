/**
 * PartitioningImporter.java
 * 
 * Imports data from a partitioning.
 * 
 * @author John Koumarelas
 */

package utils.importers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import datatypes.IntPair;

public class PartitioningImporter {
	
	public HashMap<Integer,HashSet<IntPair>> importPartitionToCellsMapping(Path partitionToCellsMapping) throws IOException {
		HashMap<Integer,HashSet<IntPair>> groupToCell = new HashMap<Integer,HashSet<IntPair>>();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(partitionToCellsMapping)));
		
		String line;
		while ((line=br.readLine()) != null){
			/*	Remove the characters '[' , ']' , ' ' */
			String lineCleaned = line.replace("]", "").replace("[","").replace(" ","");
			String[] values = lineCleaned.split(",");
			
			Integer groupID = Integer.valueOf(values[0]);
			HashSet<IntPair> cells = new HashSet<IntPair>();
			for (int i = 1 ; i < values.length; i+=2){
				IntPair cell = new IntPair(Integer.valueOf(values[i]), Integer.valueOf(values[i+1]));
				cells.add(cell);
			}
			
			groupToCell.put(groupID, cells);
		}
		
		br.close();
		
		return groupToCell;
	}

	public void importHistogramIndexToPartitionsMapping(HashMap<Integer, ArrayList<Integer>> hmS,	
			HashMap<Integer, ArrayList<Integer>> hmT, Path histogramIndexToPartitionsMapping) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(histogramIndexToPartitionsMapping)));
		String line;
		while ((line=br.readLine()) != null){
			String[] values = line.split(",");
			
			ArrayList<Integer> al = (values[0].equals("S")?hmS:hmT).get(String.valueOf(values[1]));
			
			if (al == null) {
				al = new ArrayList<Integer>();
			}
			
			for (int i = 2 ; i < values.length; ++i){
				al.add(Integer.valueOf(values[i]));
			}
			
			(values[0].equals("S")?hmS:hmT).put(Integer.valueOf(values[1]), al);
		}
		br.close();
	}
	
}
