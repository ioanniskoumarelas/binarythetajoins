/**
 * TSPAlgorithm.java
 * 
 * This is an algorithm that tries to group cells according to the
 * Traveling Salesman Problem.
 * 
 * http://www.cse.wustl.edu/~climer/TSPk.html
 * 
 * @author John Koumarelas
 */

package rearranging;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import model.PartitionMatrix;

import org.apache.hadoop.fs.Path;

import utils.exporters.RearrangementsExporter;
import utils.handlers.partitionMatrix.PartitionMatrixSwapper;
import utils.importers.RearrangementsImporter;
import datatypes.exceptions.RearrangementError;


public class TSPAlgorithm extends Rearrangements {

	private PartitionMatrix pm;
	private int numPartitions;
	private int k;
	
	private String rearrangementPolicy;
	private Path partitionMatrixDirectory;
	
	private static final File tspkProgramDirectory = new File("btj/tspk");
	private static final File tspkWorkingDirectoryBase = new File("btj/tspk_workspace/working");
	private File tspkWorkingDirectory;
	
	public File getTspkWorkingDirectory() {
		return tspkWorkingDirectory;
	}

	public TSPAlgorithm(PartitionMatrix pm, int numPartitions, int k, 
			String rearrangementPolicy, Path partitionMatrixDirectory) {
		this.pm = pm;
		this.numPartitions = numPartitions;
		this.k = k;
		this.rearrangementPolicy = rearrangementPolicy;
		this.partitionMatrixDirectory = partitionMatrixDirectory;
	}
	
	private void executeTSPLocally() throws IOException, InterruptedException {
		// Execute
		String cmd;
		cmd = tspkProgramDirectory.getAbsolutePath() + "/conv "+ tspkWorkingDirectory.getAbsolutePath()+"/pm.csv "+ tspkWorkingDirectory.getAbsolutePath() + "/pm.tsp "+numPartitions+" "+k+" "+k;
		Runtime.getRuntime().exec(cmd,null,tspkWorkingDirectory).waitFor();

		cmd = tspkProgramDirectory.getAbsolutePath()  + "/concorde " + "-o "+ tspkWorkingDirectory.getAbsolutePath()+"/pm.sol "  + tspkWorkingDirectory.getAbsolutePath()+"/pm.tsp";
		Runtime.getRuntime().exec(cmd,null,tspkWorkingDirectory).waitFor();
	}
	
	public static int[] getTSPReorder(int[] solution, int maxValue) {
		int[] mapping = new int[maxValue];
		int counter = 0 ;
		
		int firstDummy = -1;
		
		for(int i = 0 ; i < solution.length; ++i) {
			int value = solution[i];
			
			if(value>=maxValue) {
				firstDummy = counter;
				break;
			}
			++counter;	
		}
		
		counter = 0;
		
		for(int i = firstDummy+1 ; i < solution.length ; ++i) {
			int value = solution[i];
			
			if(value==0) {
				continue;
			}
			
			if(value < maxValue) {
				mapping[counter++] = value;
			} else {
				System.out.println("dummy: " + i + " value: " + value);
			}
		}
		
		for(int i = 0 ; i < firstDummy ; ++i) {
			int value = solution[i];
			
			if(value==0) {
				continue;
			}
			
			if(value < maxValue) {
				mapping[counter++] = value;
			} else {
				System.out.println("dummy: " + i + " value: " + value);
			}
		}
		
		mapping[maxValue-1] = 0;
		
		int[] finalMapping = new int[maxValue];
		
		TreeMap<Integer,Integer> tm = new TreeMap<Integer,Integer>();
		
		for(int i = 0 ; i < mapping.length ; ++i) {
			tm.put(mapping[i], i);
		}
		
		counter=0;
		
		Iterator<Entry<Integer,Integer>> it = tm.entrySet().iterator();
		while(it.hasNext()) {
			Entry<Integer,Integer> entry = it.next();
			finalMapping[entry.getKey()] = entry.getValue();
		}
		
		return finalMapping;
	}
	
	public void initTSPKDirectories() throws IOException {
		if(!tspkWorkingDirectoryBase.exists()) {
			tspkWorkingDirectoryBase.mkdirs();
		}
		
		tspkWorkingDirectory = new File(tspkWorkingDirectoryBase.getPath() + File.separator + 
				rearrangementPolicy + File.separator + numPartitions + File.separator + partitionMatrixDirectory.toString());
		if(tspkWorkingDirectory.exists()) {
			tspkWorkingDirectory.delete();
		}
		tspkWorkingDirectory.mkdirs();
	}
	
	@Override
	void execute() throws RearrangementError {
		try
		{
			initTSPKDirectories();

			new RearrangementsExporter(new Path(tspkWorkingDirectory.getPath().toString())).exportPartitionMatrixNormalizedCSV(pm,tspkWorkingDirectory);
			executeTSPLocally();
			int[] solution = new RearrangementsImporter().importTSPkSolution(tspkWorkingDirectory);
			int[] rowsMapping = getTSPReorder(solution,k);
			
			new PartitionMatrixSwapper(pm).swap(rowsMapping);
			
			this.rowsRearrangements = rowsMapping;
		}
		catch(Exception e) {
			System.err.println("Rearrangement not happened!");
        	e.printStackTrace();
        	throw new RearrangementError(e.getMessage());
		}
	}
}
