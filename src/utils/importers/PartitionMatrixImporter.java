/**
 *	PartitioningImporter.java
 *
 *	Methods for importing the PartitionMatrix
 *	and its properties.
 *
 *	@author John Koumarelas
 */
package utils.importers;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import model.BucketBoundaries;
import model.PartitionMatrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PartitionMatrixImporter {

	public PartitionMatrixImporter() {}

	public PartitionMatrix importPartitionMatrix(long sizeS, long sizeT, int buckets, Path partitionMatrix, Path rearrangements, Path boundaries, Path counts) {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(partitionMatrix)));
			String line;
			
			long[][] matrix = new long[buckets][buckets];
			
			int i=0,j;
			
			while ((line=br.readLine()) != null){
				 String[] values = line.split(",");
				 for(j=0;j<values.length;++j){
					 matrix[i][j] = Long.valueOf(values[j]);
				 }
				 ++i;
			}
			
			PartitionMatrix pm = new PartitionMatrix();
			
			pm.setMatrix(matrix);
			
			BucketBoundaries[] boundariesS = new BucketBoundaries[buckets];
			BucketBoundaries[] boundariesT = new BucketBoundaries[buckets];
			BucketBoundaries[] tmpBoundariesS = new HistogramImporter().importBoundaries("S", buckets, boundaries);
			BucketBoundaries[] tmpBoundariesT = new HistogramImporter().importBoundaries("T", buckets, boundaries);
			
			long[] countsS = new long[buckets];
			long[] countsT = new long[buckets];
			long[] tmpCountsS = new HistogramImporter().importCounts("S", buckets, counts);
			long[] tmpCountsT = new HistogramImporter().importCounts("T", buckets, counts);
			
			if(!rearrangements.equals(new Path(File.separator))) {
				int[] defaultToRearrangedS = new RearrangementsImporter().importDefaultToRearranged("S", buckets, rearrangements);
				int[] defaultToRearrangedT = new RearrangementsImporter().importDefaultToRearranged("T", buckets, rearrangements);
				
				for(i = 0 ; i < buckets; ++i) {
					boundariesS[i] = tmpBoundariesS[defaultToRearrangedS[i]];
					boundariesT[i] = tmpBoundariesT[defaultToRearrangedT[i]];
					countsS[i] = tmpCountsS[defaultToRearrangedS[i]];
					countsT[i] = tmpCountsT[defaultToRearrangedT[i]];
				}
			} else {
				boundariesS = tmpBoundariesS;
				boundariesT = tmpBoundariesT;
				countsS = tmpCountsS;
				countsT = tmpCountsT;
			}
			
			pm.setBoundariesS(boundariesS);
			pm.setBoundariesT(boundariesT);
			
			pm.setCountsS(countsS);
			pm.setCountsT(countsT);
			
			pm.setSizeS(sizeS);
			pm.setSizeT(sizeT);
			
			return pm;
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public HashMap<String,String> importProperties(Path properties) {
		HashMap<String,String> propertiesMap = new HashMap<String,String>();
		
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(properties)));
			String line;
			
			while ((line=br.readLine()) != null){
				if(line.trim() == "") {
					continue;
				}
				
				String[] propertyToks = line.split(",");
				propertiesMap.put(propertyToks[0], propertyToks[1]);
			}
			
			br.close();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return propertiesMap;
	}
	
	public String importProperty(String property, Path properties) {
		HashMap<String,String> propertiesMap = importProperties(properties);
		
		if(propertiesMap.containsKey(property)){
			return propertiesMap.get(property);
		} else {
			return "";
		}
	}
	
}
