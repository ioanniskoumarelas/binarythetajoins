/**
 * TestMappingToReducers.java
 * 
 * In this class we validate the mapping of the partitioner. We examine whether 
 * all combinations that should be sent to reducers, are actually sent.
 * 
 * @author John Koumarelas
 */

package test.model.partitioning;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import model.BucketBoundaries;
import model.PartitionMatrix;

import org.apache.hadoop.fs.Path;

import utils.importers.HistogramImporter;
import utils.importers.PartitionMatrixImporter;
import utils.importers.PartitioningImporter;

public class TestMappingToPartitions {
	
	private PartitionMatrix importPartitionMatrix(Path partitionMatrixDirectory){
		Path partitionMatrix = new Path(partitionMatrixDirectory.toString() + File.separator + "pm.csv");
		Path boundaries = new Path(partitionMatrixDirectory.toString() + File.separator + "boundaries.csv");
		Path counts = new Path(partitionMatrixDirectory.toString() + File.separator + "counts.csv");
		Path properties = new Path(partitionMatrixDirectory.toString() + File.separator + "properties.csv");
		
		PartitionMatrixImporter pmi = new PartitionMatrixImporter();
		
		long sizeS = Long.valueOf(pmi.importProperty("sizeS", properties));
		long sizeT = Long.valueOf(pmi.importProperty("sizeT", properties));
		int buckets = Integer.valueOf(pmi.importProperty("buckets", properties));
		
		PartitionMatrix pm = pmi.importPartitionMatrix(sizeS,sizeT,buckets,partitionMatrix,new Path(File.separator),boundaries,counts);
		
		return pm;
	}
	
	/**
	 * TODO: support the validation of rearrangements
	 */
	private boolean validateMapping(PartitionMatrix pm, BucketBoundaries[] boundariesS, BucketBoundaries[] boundariesT, 
			HashMap<Integer,ArrayList<Integer>> hmS, HashMap<Integer,ArrayList<Integer>> hmT) {
		
		long[][] matrix = pm.getMatrix();
		
		int counterTP=0,counterTN=0,counterFP=0,counterFN=0;
		
		for(int i = 0 ; i < matrix.length; ++i) {
			for(int j = 0 ; j < matrix[0].length; ++j) {
				
				ArrayList<Integer> partitionsS = hmS.get(i);
				ArrayList<Integer> partitionsT = hmT.get(j);
				
				HashSet<Integer> partitionsHashS = new HashSet<Integer>(partitionsS);
				HashSet<Integer> partitionsHashT = new HashSet<Integer>(partitionsT);
				
				HashSet<Integer> intersection = new HashSet<Integer>();
				intersection.addAll(partitionsHashS);
				intersection.retainAll(partitionsHashT);
				
				//partitionsHashS.retainAll(partitionsHashT);
				
				boolean mappingIntersection = (!intersection.isEmpty());

				// Now we want that combination to be in the mapping if it is not 0.
				if(matrix[i][j]>0) {
					if(mappingIntersection) {
						//System.out.println("Combination that should be sent is sent -- OK");
						++counterTP;
					} else {
						//System.out.println("Combination that should be sent is NOT sent");
						++counterFN;
					}
				} else {
					if(mappingIntersection) {
						//System.out.println("Combination that should NOT be sent is sent");
						++counterFP;
					} else {
						//System.out.println("Combination that should NOT be sent is NOT sent -- OK");
						++counterTN;
					}
				}
				
			}
		}
		
		System.out.println("counterTP: " + counterTP);
		System.out.println("counterTN: " + counterTN);
		System.out.println("counterFP (performance error): " + counterFP);
		System.out.println("counterFN (correctness error): " + counterFN);
		
		return true;
	}
	
	public static void main(String[] args) throws IOException {
		// 1. Load the matrix.
		Path partitionMatrixDirectory = new Path("datasets/synthetic_1_100_1/100/1_10_1/");
		
		TestMappingToPartitions tmtr = new TestMappingToPartitions();
		PartitionMatrix pm = tmtr.importPartitionMatrix(partitionMatrixDirectory);
		
		// 2. Load the mapping
		Path histogramIndexToPartitionsMapping = new Path("datasets/synthetic_1_100_1/100/1_10_1/MRI_10/indexToPartitionsMapping.csv");
		BucketBoundaries[] boundariesS;
		BucketBoundaries[] boundariesT;
		HashMap<Integer,ArrayList<Integer>> hmS = new HashMap<Integer,ArrayList<Integer>>();
		HashMap<Integer,ArrayList<Integer>> hmT = new HashMap<Integer,ArrayList<Integer>>();
		
		new PartitioningImporter().importHistogramIndexToPartitionsMapping(hmS, hmT, histogramIndexToPartitionsMapping);
		
		int buckets = Integer.valueOf(100);
		Path boundaries = new Path("datasets/synthetic_1_100_1/100/1_10_1/boundaries.csv");
		
		boundariesS = new HistogramImporter().importBoundaries("S", buckets, boundaries);
		boundariesT = new HistogramImporter().importBoundaries("T", buckets, boundaries);
		
		// 3. Perform the validation.
		tmtr.validateMapping(pm,boundariesS,boundariesT,hmS,hmT);
	}
}
