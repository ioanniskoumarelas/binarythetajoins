package experiments.execute;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Producer {

	/**
	 * gets 3 parameters
	 * @parametersFolderPath the parameters folder
	 * @number_of_threads the actual running Controller processes.
	 * @sleeptime to be sure that everything runs ok
	 * all 3 parameters are generated automatically by the generateParametersBTJ.py. If I want to configure an experiment this is the script file I should change (lines 23-29)
	 * the actual experiment is executed by the createPMs_rearrange_partition.sh
	 * 
	 * Checks if exist every path to executed. The paths are created by the createPMs_rearrange_partition.sh
	 * To build the jars for the experiments use the ant scripts: build_btj_project.xml -> build_btj_jar_parallel_real.xml
	 * 
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		String parametersFilePath = "parameters/cloudSolarAltitude19/solarAltitude19/realPartitionMatrix.txt";
		int maxRunningThreads = 1;
		int sleepTime = 5;
		
		if(args.length>0) {
			if(args.length == 1) {
				parametersFilePath = args[0];
			} else if(args.length == 3) {
				parametersFilePath = args[0];
				maxRunningThreads = Integer.valueOf(args[1]);
				sleepTime = Integer.valueOf(args[2]);
			} else {
				System.err.println("Please use no argument at all, 1 argument (parametersFolderPath) " + 
						"or 3 arguments (parametersFolderPath,maxRunningThreads,sleepTime)");
				System.exit(-1);
			}
		}
		
		System.out.println("parametersFilePath: " + parametersFilePath);
		System.out.println("maxRunningThreads: " + maxRunningThreads);
		System.out.println("sleepTime: " + sleepTime);
		
		Producer p = new Producer();
		
		File parametersFile = new File(parametersFilePath);
		if(parametersFile.exists()) {
			
			boolean existsAnyMode = false;
			if(	parametersFile.getName().equals("realPartitionMatrix.txt") || parametersFile.getName().equals("rearrangements.txt") ||
				parametersFile.getName().equals("partitioning.txt") || parametersFile.getName().equals("mBucketI.txt")) { 
				
				existsAnyMode = true;
			}
			
			if(existsAnyMode) {
				File completedParametersFile = new File(parametersFile.getParentFile() + "/" + "completed_" + parametersFile.getName());
				
				FileWriter outCompleted = new FileWriter(completedParametersFile, true);
				
				System.out.println("Execution begins: " + parametersFilePath);
				p.execute(parametersFile, outCompleted, maxRunningThreads, sleepTime);
				outCompleted.close();
			} else {
				
				System.out.println("Execution stopped [file does not exist]: " + parametersFilePath);
			}
		}
	}
	
	protected void getRunningConsumers(ArrayList<Consumer> consumers,
			ArrayList<Consumer> aliveConsumers) {
		aliveConsumers.clear();
		for (Consumer cons : consumers) {
			if (cons.isAlive()) {
				aliveConsumers.add(cons);
			}
		}
	}
	
	protected void execute(File parametersFile, FileWriter out,  int maxRunningThreads, int sleepTime) throws InterruptedException {
		LinkedList<String> params = readParameters(parametersFile);
		int numberParameters = params.size();
		String[] parameters = params.toArray(new String[0]);

		int parametersIndex = 0;
		int currentlyRunningThreads = 0;

		ArrayList<Consumer> consumers = new ArrayList<Consumer>();
		ArrayList<Consumer> aliveConsumers = new ArrayList<Consumer>();

		while (true) {
			getRunningConsumers(consumers, aliveConsumers);
			currentlyRunningThreads = aliveConsumers.size();
			consumers.retainAll(aliveConsumers); // removeNonRunningConsumers

			if (aliveConsumers.size() == 0 && (parametersIndex >= numberParameters)) {
				break;
			} else {
				for (int i = 0; i < (maxRunningThreads - currentlyRunningThreads) &&
					 (parametersIndex < numberParameters); ++i) {
					
					System.runFinalization();
					System.gc();
					Consumer cons = new Consumer(parameters[parametersIndex++], out);
					cons.start();
					consumers.add(cons);
				}
				Thread.sleep(sleepTime);
			}
		}
	}
	
	protected void initTSPKProgramDirectory() throws IOException {
		Path tspk = new Path("btj/tspk");
		File tspkProgramDirectory = new File("tmp/tspk/program");
		if (!tspkProgramDirectory.exists()) {
			FileSystem fs = FileSystem.get(new Configuration());
			tspkProgramDirectory.mkdir();
			
			FileStatus[] filesInTSPk = fs.listStatus(tspk);
			
			for (FileStatus fileInTSPk : filesInTSPk) {
				fs.copyToLocalFile(false, fileInTSPk.getPath(), new Path(tspkProgramDirectory.getPath().toString() + 
						File.separator + fileInTSPk.getPath().getName().toString()));
			}
			
			/*	Make files executable	*/
			File convFile = new File(tspkProgramDirectory.getAbsolutePath() + "/conv");
			convFile.setExecutable(true);
			File concordeFile = new File(tspkProgramDirectory.getAbsolutePath()  + "/concorde");
			concordeFile.setExecutable(true);
			File order1File = new File(tspkProgramDirectory.getAbsolutePath()  + "/order1");
			order1File.setExecutable(true);
		}
	}
	
	protected LinkedList<String> readParameters(File parameters) {
		LinkedList<String> params = new LinkedList<String>();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new DataInputStream(new FileInputStream(parameters))));
			String strLine;

			while ((strLine = br.readLine()) != null) {
				if(strLine.trim().equals("")) {
					continue;
				}
				params.add(strLine);
			}

			br.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
		return params;
	}
	
}
