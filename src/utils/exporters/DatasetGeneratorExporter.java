/**
 * DataGeneratorExporter.java
 * 
 * This class implements the execution mode responsible for the generation of
 * data. It supports the following types of data generation:
 * 	-	generateRange: 	Where data is being exported on a specific range [from,to]
 * 	-	generateDistribution: 	Data generated follows a specific distribution.
 * 								(see enum Distribution for the currently available distributions)
 * 
 * @author John Koumarelas, john.koumarel@gmail.com
 */
package utils.exporters;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DatasetGeneratorExporter {
	
	private long executionTime;
	private Random generator = null;
	
	private Path dataset;
	private Path datasetSizes;

	public enum Distribution {
	    UNIFORM("UNIFORM"), GAUSSIAN("GAUSSIAN"), ZIPF("ZIPF");
	    
	    String disName;
	    
	    private Distribution(String disName) {
			this.disName = disName;
		}
	    
	    @Override
	    public String toString() {
	    	return disName;
	    }
	}
	
	public DatasetGeneratorExporter(long generatorSeed, Path dataset, Path datasetSizes) {
		generator = new Random(generatorSeed);
		this.dataset = dataset;
		this.datasetSizes = datasetSizes;
	}

	// Reference: http://c-faq.com/lib/gaussian.html
	private double V1, V2, S;
	private int phase = 0;
	private double distrGaussian() {
		double X;

		if (phase == 0) {
			do {
				double U1 = Math.random();
				double U2 = Math.random();

				V1 = 2 * U1 - 1;
				V2 = 2 * U2 - 1;
				S = V1 * V1 + V2 * V2;
			} while (S >= 1 || S == 0);

			X = V1 * Math.sqrt(-2 * Math.log(S) / S);
		} else {
			X = V2 * Math.sqrt(-2 * Math.log(S) / S);
		}

		phase = 1 - phase;

		return X;
	}
		
	private double distrUniform() {
		return Math.random();
	}
		
		/**
		 * http://mathworld.wolfram.com/RandomNumber.html
		 * http://stackoverflow.com/questions/918736/random-number-generator-that-produces-a-power-law-distribution
		 * Returns a number from 0 to 1
		 */
		private double distrZipf() {
			/*
			 *	x = [(x1^(n+1) - x0^(n+1))*y + x0^(n+1)]^(1/(n+1))
			 *
			 *	where y is a uniform variate, n is the distribution power,
			 *	x0 and x1 define the range of the distribution,
			 *	and x is your power-law distributed variate.
			 */
			
			double y = Math.random();
			double n = 2.0; // bigger --> values concentrate towards x1.
			double x0 = 0.0;
			double x1 = 1.0;

			double res;

			res = Math.pow((Math.pow(x1,n+1) - Math.pow(x0,n+1))*y + Math.pow(x0,n+1),1.0/(n+1.0));
			res = x1 - res; // by doing this the values concentrate towards x0.
			return res;
		}
		
		private double generateNumber(Distribution dis) {
			double number;
			if (dis == Distribution.UNIFORM) {
				return distrUniform();
			}
			else if (dis == Distribution.GAUSSIAN) {
				number = distrGaussian();
				number /= 3.0;
				number /= 2.0;
				number += 0.5;
				if (number < 0.0)
					number = 0.5; // original: 0,0; 
				else if (number > 1.0)
					number = 0.5; // original: 1.0;
				return number;
			}
			else { // dis == Distribution.ZIPF
				number = distrZipf();
				return number;
			}
		}
		
		/**
		 * Generates the dataset according to some distribution.
		 * 
		 * @param from: acts as the lower boundary which a generated number can have.
		 * @param to: acts as the upper boundary which a generated number can have.
		 * @param sizeS: determines the number of tuples from relation S
		 * @param sizeT: determines the number of tuples from relation T
		 * @param randomStringLength: length of the random string generated (gives more emphasis to Communication Costs etc.)
		 * @param distribution: what distribution will the generated numbers follow. Refer to 
		 * 		enum Distribution for more information.
		 */
		public void generateDistribution(long from, long to, long sizeS, long sizeT, String distribution) {
			try {
				long start = System.currentTimeMillis();
				FileSystem fs = FileSystem.get(new Configuration());
				
				BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(dataset,true)));
				
				long curSizeS = 0L;
				long curSizeT = 0L;
				
				double prbS = (double) sizeS / (sizeS + sizeT);
				//double prbT = (double) sizeT / (sizeS + sizeT);
				
				long range = to-from;
				
				Distribution dis = Distribution.valueOf(distribution); 
				
				while(curSizeS < sizeS || curSizeT < sizeT) {
					double tmp = generator.nextDouble();
					if (tmp <= prbS) {
						if (curSizeS < sizeS) {
							out.write( "S" + "," + (long)(generateNumber(dis)*range + from));
							++curSizeS;
						} else {
							out.write( "T" + "," + (long)(generateNumber(dis)*range + from));
							++curSizeT;
						}
					} else{
						if (curSizeT < sizeT) {
							out.write( "T" + "," + (long)(generateNumber(dis)*range + from));
							++curSizeT;
						} else {
							out.write( "S" + "," + (long)(generateNumber(dis)*range + from));
							++curSizeS;
						}
					}
					out.newLine();
				}
				
				out.close();
				
				out = new BufferedWriter(new OutputStreamWriter(fs.create(datasetSizes,true)));
				
				out.write("S" + "," + sizeS);
				out.newLine();
				
				out.write("T" + "," + sizeT);
				out.close();
				
				long end = System.currentTimeMillis();
				executionTime = end-start;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * Generates dataset in a range of values [from, to] and each value is replicated valueReplication times.
		 * 
		 * @param from: acts as the lower boundary which a generated number can have.
		 * @param to: acts as the upper boundary which a generated number can have.
		 * @param valueReplication: Times each value will be replicated (outputted).
		 * @param randomStringLength: length of the random string generated (gives more emphasis to Communication Costs etc.)
		 */
		public void generateRange(long from, long to, long valueReplication) {			
			try {
				long start = System.currentTimeMillis();
				FileSystem fs = FileSystem.get(new Configuration());
				
				BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(dataset,true)));
				
				for (long i = from; i <= to; ++i) {
					for (int repl = 0 ; repl < valueReplication; ++repl){
						out.write("S" + "," + i);
						out.newLine();
						out.write("T" + "," + i);
						out.newLine();
					}
				}
				out.close();
				
				long sizeT = (to - from + 1) * valueReplication;
				long sizeS = (to - from + 1) * valueReplication;
				
				out = new BufferedWriter(new OutputStreamWriter(fs.create(datasetSizes,true)));
				
				out.write("S" + "," + sizeS);
				out.newLine();
				
				out.write("T" + "," + sizeT);
				out.close();
				
				long end = System.currentTimeMillis();
				executionTime = end-start;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void exportExecutionTimes(Path executionTimes) throws IOException {
			FileSystem fs = FileSystem.get(new Configuration());
			
			/*	executionTimes.csv	*/
			BufferedWriter out = new BufferedWriter(
					new OutputStreamWriter(fs.create(executionTimes,true)));
			
			out.write("dataGeneration" + "," + String.valueOf(executionTime));
			
			out.close();
		}
}
